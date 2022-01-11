package table_buffer

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	bufferOne byte = 1
	bufferTwo byte = 2
)

type TableBufferMetrics interface {
	ObserveBufferFlushLatency(tableName string, latency float64)
	IncBufferInsertCounter(tableName string)
	SetBufferLen(tableName string, bufferLen int64)
}

type Repository interface {
	Insert(ctx context.Context, query string, rows []storageRow, tableName string) (int, error)
	FetchTable(
		ctx context.Context,
		tableName string,
		indexFields []string,
		signField string,
	) (
		TableState,
		[]string, // columns
		error,
	)
	FetchColumnNames(ctx context.Context, tableName string) ([]string, error)
}

// Query for modifying the data. updates Storage in RAM and makes 1 or 2 clickhouse inserts.
type Query map[string]interface{} // columnName->data

type storageRow []interface{}

// TableBuffer contains the actual state of CollapsingMergeTree table, batches clickhouse inserts.
type TableBuffer struct {
	tableName      string
	queryString    string
	repo           Repository
	signColumnName string

	rowsFlushPeriod time.Duration // mainBuffer flush interval
	bufferLimit     uint64        // mainBuffer max len
	insertTimeout   time.Duration // clickhouse actual insert timeout

	lockBuffer sync.RWMutex  // lock for modifying the mainBuffer
	mainBuffer *[]storageRow // buffer for incoming new data

	lockFlush   sync.Mutex    // lock for flushBuffer
	flushBuffer *[]storageRow // pointer on data for flushing routine

	lockStorage sync.Mutex            // storage lock
	columns     []string              // table column names, for CollapsingMergeTree only
	indexes     []string              // table primary index, for CollapsingMergeTree only
	storage     map[string]storageRow // CollapsingMergeTree table state, primary key -> fields

	buffer struct {
		num byte // number of working mainBuffer

		// the real buffers pointed by mainBuffer and flushBuffer
		one []storageRow
		two []storageRow
	}

	closeWg     sync.WaitGroup
	done        chan struct{}
	flushErrors chan error
	doneOnce    sync.Once

	metrics TableBufferMetrics
	logger  *zap.Logger
}

func NewTableBuffer(
	rowsFlushPeriod time.Duration,
	bufferSize uint64,
	tableName string,
	signColumnName string,
	repo Repository,
	insertTimeout time.Duration,
	indexes []string,
	metrics TableBufferMetrics,
	logger *zap.Logger,
) *TableBuffer {
	b := &TableBuffer{
		tableName:       tableName,
		signColumnName:  signColumnName,
		repo:            repo,
		indexes:         indexes,
		doneOnce:        sync.Once{},
		done:            make(chan struct{}),
		closeWg:         sync.WaitGroup{},
		storage:         make(map[string]storageRow),
		rowsFlushPeriod: rowsFlushPeriod,
		bufferLimit:     bufferSize,
		insertTimeout:   insertTimeout,
		metrics:         metrics,
		flushErrors:     make(chan error),
		logger:          logger,
	}
	b.buffer.num = bufferOne
	b.buffer.one = make([]storageRow, 0, b.bufferLimit)
	b.buffer.two = make([]storageRow, 0, b.bufferLimit)
	b.mainBuffer = &b.buffer.one
	b.flushBuffer = &b.buffer.two

	b.closeWg.Add(1)

	var startingWg sync.WaitGroup

	startingWg.Add(1)

	go func() {
		defer b.closeWg.Done()

		flushTicker := time.NewTicker(b.rowsFlushPeriod)
		defer flushTicker.Stop()

		startingWg.Done()

		for {
			select {
			case <-flushTicker.C:
				b.Flush()
			case <-b.done:
				b.Flush()

				return
			}
		}
	}()

	startingWg.Wait()

	return b
}

// SetStorage sets the state of CollapsingMergeTree table.
func (tb *TableBuffer) SetStorage(storage TableState) {
	for indexValue, rowMap := range storage {
		for column, data := range rowMap {
			tb.updateCell(indexValue, column, data)
		}
	}
}

// SetColumnDictionary sets the set of columns for building insert queries.
func (tb *TableBuffer) SetColumnDictionary(columns []string) {
	tb.columns = columns
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	BuildInsertQuery(buf, tb.tableName, tb.columns)
	tb.queryString = buf.String()
}

// insertRows inserts rows in the mainBuffer, bypassing storage state.
func (tb *TableBuffer) insertRows(rows ...storageRow) {
	tb.lockBuffer.Lock()
	lenRows := len(*tb.mainBuffer)
	tb.lockBuffer.Unlock()

	if lenRows >= int(tb.bufferLimit) {
		tb.Flush()
	}

	tb.lockBuffer.Lock()
	defer tb.lockBuffer.Unlock()

	for _, row := range rows {
		// grow the buffer size if needed
		if len(*tb.mainBuffer) < cap(*tb.mainBuffer) {
			*tb.mainBuffer = (*tb.mainBuffer)[:len(*tb.mainBuffer)+1]
			(*tb.mainBuffer)[len(*tb.mainBuffer)-1] = row

			continue
		}

		*tb.mainBuffer = append(*tb.mainBuffer, row)

		tb.metrics.IncBufferInsertCounter(tb.tableName)
		tb.metrics.SetBufferLen(tb.tableName, int64(len(*tb.mainBuffer)))
	}
}

// Log inserts a simple rows to mainBuffer, doesn't update a storage state.
func (tb *TableBuffer) Log(queries ...Query) error {
	if len(queries) == 0 {
		return nil
	}

	rows := make([]storageRow, 0, len(queries))
	for _, query := range queries {
		rows = append(rows, tb.fillArgs(query))
	}

	tb.insertRows(rows...)

	return nil
}

// Insert inserts queries to mainBuffer, with storage state updating.
func (tb *TableBuffer) Insert(queries ...Query) error {
	if len(queries) == 0 {
		return nil
	}

	err := tb.insertToMemory(queries...)
	if err != nil {
		return err
	}

	rows := make([]storageRow, 0, len(queries))

	for _, query := range queries {
		// add the sign field
		query[tb.signColumnName] = 1
		rows = append(rows, tb.fillArgs(query))
	}

	tb.insertRows(rows...)

	return nil
}

// insertToMemory inserts rows to memory storage, returns error if row with such primary key is already presented.
func (tb *TableBuffer) insertToMemory(queries ...Query) error {
	tb.lockStorage.Lock()
	defer tb.lockStorage.Unlock()

	for _, query := range queries {
		indexValues := make([]string, 0, len(tb.indexes))

		for _, index := range tb.indexes {
			idx, exists := query[index]
			if !exists {
				return errors.Errorf("index field %s not found in query in table %s", index, tb.tableName)
			}

			indexValues = append(indexValues, fmt.Sprint(idx))
		}

		indexValue := strings.Join(indexValues, "_")

		if _, indexExists := tb.storage[indexValue]; indexExists {
			return errors.Errorf("index %s already exists in table %s", indexValue, tb.tableName)
		}

		tb.storage[indexValue] = make(storageRow, len(query))
		for argName, argData := range query {
			tb.updateCell(indexValue, argName, argData)
		}
	}

	return nil
}

// InsertOrUpdate inserts rows to memory storage, updates the row if such primary key is already presented.
func (tb *TableBuffer) InsertOrUpdate(queries ...Query) error {
	if len(queries) == 0 {
		return nil
	}

	oldVersions, err := tb.insertOrUpdateMemory(queries...)
	if err != nil {
		return err
	}

	var (
		rows   = make([]storageRow, 0, len(queries))
		rowSet = make(map[string]interface{})
	)

	for i, query := range queries {
		oldRowVersion, oldVersionExists := oldVersions[i]
		if !oldVersionExists {
			query[tb.signColumnName] = 1
			rows = append(rows, tb.fillArgs(query))

			continue
		}

		// build cancelling version query
		for k := range rowSet {
			delete(rowSet, k)
		}

		for columnIdx, cell := range oldRowVersion {
			rowSet[tb.columns[columnIdx]] = cell
		}

		rowSet[tb.signColumnName] = -1 // add the cancelling field
		rows = append(rows, tb.fillArgs(rowSet))

		// build query for the new version adding
		for k := range rowSet {
			delete(rowSet, k)
		}

		for columnIdx, cell := range oldRowVersion {
			rowSet[tb.columns[columnIdx]] = cell
		}

		for argName, argData := range query {
			rowSet[argName] = argData
		}

		// add sign to new version
		rowSet[tb.signColumnName] = 1
		rows = append(rows, tb.fillArgs(rowSet))
	}

	tb.insertRows(rows...)

	return nil
}

func (tb *TableBuffer) insertOrUpdateMemory(queries ...Query) (map[int]storageRow, error) {
	tb.lockStorage.Lock()
	defer tb.lockStorage.Unlock()

	oldVersions := make(map[int]storageRow, len(queries)) // key -- index of corresponding query in queries parameter

	for queryIdx, query := range queries {
		indexValues := make([]string, 0, len(tb.indexes))

		for _, index := range tb.indexes {
			idx, exists := query[index]
			if !exists {
				return nil, errors.Errorf("index field %s not found in query in table %s", index, tb.tableName)
			}

			indexValues = append(indexValues, fmt.Sprint(idx))
		}

		indexValue := strings.Join(indexValues, "_")

		// if such row is already presented -- update the storage
		if oldVersion, indexExists := tb.storage[indexValue]; indexExists {
			oldVersionCopy := make(storageRow, len(oldVersion))
			copy(oldVersionCopy, oldVersion)
			oldVersions[queryIdx] = oldVersionCopy
		} else {
			tb.storage[indexValue] = make(storageRow, len(query))
		}

		for argName, argData := range query {
			tb.updateCell(indexValue, argName, argData)
		}
	}

	return oldVersions, nil
}

// Update updates rows in memory storage, insert cancelling and new row to mainBuffer
// returns the error if such primary key is missed.
func (tb *TableBuffer) Update(queries ...Query) error {
	if len(queries) == 0 {
		return nil
	}

	oldVersions, err := tb.updateMemory(queries...)
	if err != nil {
		return err
	}

	if len(oldVersions) == 0 {
		return nil
	}

	var (
		rows   = make([]storageRow, 0, len(queries))
		rowSet = make(map[string]interface{})
	)

	for i, query := range queries {
		oldRowVersion := oldVersions[i]

		// build cancelling version query
		for k := range rowSet {
			delete(rowSet, k)
		}

		for columnIdx, cell := range oldRowVersion {
			rowSet[tb.columns[columnIdx]] = cell
		}

		rowSet[tb.signColumnName] = -1 // add cancelling field
		rows = append(rows, tb.fillArgs(rowSet))

		// build query for the new version adding
		for k := range rowSet {
			delete(rowSet, k)
		}

		for columnIdx, cell := range oldRowVersion {
			rowSet[tb.columns[columnIdx]] = cell
		}

		for argName, argData := range query {
			rowSet[argName] = argData
		}

		// add sign field to new version
		rowSet[tb.signColumnName] = 1
		rows = append(rows, tb.fillArgs(rowSet))
	}

	tb.insertRows(rows...)

	return nil
}

// updateMemory updates memory storage and returns OLD VERSIONS of the rows.
func (tb *TableBuffer) updateMemory(queries ...Query) ([]storageRow, error) {
	tb.lockStorage.Lock()
	defer tb.lockStorage.Unlock()

	oldVersions := make([]storageRow, 0, len(queries))

	for _, query := range queries {
		indexValues := make([]string, 0, len(tb.indexes))

		for _, index := range tb.indexes {
			idx, exists := query[index]
			if !exists {
				return nil, errors.Errorf("index field %s not found in query in table %s", index, tb.tableName)
			}

			indexValues = append(indexValues, fmt.Sprint(idx))
		}

		indexValue := strings.Join(indexValues, "_")

		oldVersion, indexExists := tb.storage[indexValue]

		// need to copy old version cause the original one will be modified in updateCell method
		oldVersionCopy := make(storageRow, len(oldVersion))
		copy(oldVersionCopy, oldVersion)

		if !indexExists {
			return nil, errors.Errorf("index %s doesn't exist in table %s", indexValue, tb.tableName)
		}

		for argName, update := range query {
			tb.updateCell(indexValue, argName, update)
		}

		oldVersions = append(oldVersions, oldVersionCopy)
	}

	return oldVersions, nil
}

// Delete deletes rows from memory storage, insert cancelling row to mainBuffer
// returns the error if such primary key is missed.
func (tb *TableBuffer) Delete(queries ...Query) error {
	if len(queries) == 0 {
		return nil
	}

	oldVersions, err := tb.deleteFromMemory(queries...)
	if err != nil {
		return err
	}

	if len(oldVersions) == 0 {
		return nil
	}

	var (
		rows   = make([]storageRow, 0, len(queries))
		rowSet = make(map[string]interface{})
	)

	for i := range queries {
		for k := range rowSet {
			delete(rowSet, k)
		}

		// build cancelling version query
		for columnIdx, cell := range oldVersions[i] {
			rowSet[tb.columns[columnIdx]] = cell
		}

		// add cancelling sign column
		rowSet[tb.signColumnName] = -1
		rows = append(rows, tb.fillArgs(rowSet))
	}

	tb.insertRows(rows...)

	return nil
}

func (tb *TableBuffer) deleteFromMemory(queries ...Query) ([]storageRow, error) {
	tb.lockStorage.Lock()
	defer tb.lockStorage.Unlock()

	oldVersions := make([]storageRow, 0, len(queries))

	for _, query := range queries {
		indexValues := make([]string, 0, len(tb.indexes))

		for _, index := range tb.indexes {
			idx, exists := query[index]
			if !exists {
				return nil, errors.Errorf("index field %s not found in query in table %s", index, tb.tableName)
			}

			indexValues = append(indexValues, fmt.Sprint(idx))
		}

		indexValue := strings.Join(indexValues, "_")

		oldVersion, indexExists := tb.storage[indexValue]
		if !indexExists {
			return nil, errors.Errorf("index %s doesn't exist in table %s", indexValue, tb.tableName)
		}

		delete(tb.storage, indexValue)

		oldVersions = append(oldVersions, oldVersion)
	}

	return oldVersions, nil
}

// updateCell updates column(columnName) of row(rowIndex).
func (tb *TableBuffer) updateCell(rowIndex string, columnName string, data interface{}) {
	idx := tb.getColumnIndex(columnName)

	for idx > len(tb.storage[rowIndex])-1 {
		tb.storage[rowIndex] = append(tb.storage[rowIndex], nil)
	}

	tb.storage[rowIndex][idx] = data
}

// getColumnIndex gets number of columnName in columns slice.
func (tb *TableBuffer) getColumnIndex(columnName string) int {
	var (
		idx   int
		found bool
	)

	for i, colName := range tb.columns {
		if colName == columnName {
			idx = i
			found = true

			break
		}
	}

	if !found {
		log.Fatal("column not found in storage", zap.String("column", columnName), zap.String("table", tb.tableName))
	}

	return idx
}

// fillArgs adds missing columns to query.
func (tb *TableBuffer) fillArgs(query Query) storageRow {
	filledArgs := make(storageRow, 0, len(tb.columns))

	for _, column := range tb.columns {
		if arg, exists := query[column]; exists {
			filledArgs = append(filledArgs, arg)

			continue
		}

		filledArgs = append(filledArgs, nil)
	}

	return filledArgs
}

func (tb *TableBuffer) Close() {
	tb.doneOnce.Do(func() {
		close(tb.done)
		tb.closeWg.Wait()
		close(tb.flushErrors)
	})
}

func (tb *TableBuffer) Flush() {
	tb.lockFlush.Lock()
	defer tb.lockFlush.Unlock()

	tb.lockBuffer.Lock()
	tb.swapRowsBuffer()
	tb.lockBuffer.Unlock()

	if len(*tb.flushBuffer) == 0 {
		return
	}

	defer func(start time.Time) {
		elapsed := float64(time.Since(start)) / 1_000_000.0
		tb.metrics.ObserveBufferFlushLatency(tb.tableName, elapsed)
	}(time.Now())

	ctx, cancel := context.WithTimeout(context.Background(), tb.insertTimeout)
	defer cancel()

	_, err := tb.repo.Insert(ctx, tb.queryString, *tb.flushBuffer, tb.tableName)
	if err != nil {
		tb.flushErrors <- err

		return
	}

	// сброс данных
	*tb.flushBuffer = (*tb.flushBuffer)[:0]
}

// swapRowsBuffer switch flushBuffer pointer to full buffer, and mainBuffer to empty buffer.
func (tb *TableBuffer) swapRowsBuffer() {
	if len(*tb.flushBuffer) > 0 {
		// do nothing if flushing buffer is not empty. It has to be flushed before
		return
	}

	if len(*tb.mainBuffer) == 0 { // no data for flush
		return
	}

	// swap the real buffer pointers
	switch tb.buffer.num {
	case bufferOne:
		tb.buffer.num = bufferTwo
		tb.mainBuffer = &tb.buffer.two
		tb.flushBuffer = &tb.buffer.one
	case bufferTwo:
		tb.buffer.num = bufferOne
		tb.mainBuffer = &tb.buffer.one
		tb.flushBuffer = &tb.buffer.two
	}
}

func (tb *TableBuffer) Errors() <-chan error {
	return tb.flushErrors
}
