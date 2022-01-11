package clickhouse_buffer

import (
	"context"
	"database/sql"
	"time"

	"github.com/alexkulaga/clickhouse_buffer/metrics"
	"github.com/alexkulaga/clickhouse_buffer/table_buffer"
	"go.uber.org/zap"
)

type CollapsingMergeTreeBuffer struct {
	tableBuffer *table_buffer.TableBuffer
}

func NewCollapsingMergeTreeBuffer(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	indexFields []string,
	signField string,
	insertTimeout time.Duration,
	flushPeriod time.Duration,
	bufferSize uint64,
	metrics metrics.Metrics,
	logger *zap.Logger,
) (*CollapsingMergeTreeBuffer, error) {
	repo := table_buffer.NewRepository(db, metrics, logger)

	state, columns, err := repo.FetchTable(ctx, tableName, indexFields, signField)
	if err != nil {
		return nil, err
	}

	b := table_buffer.NewTableBuffer(
		flushPeriod,
		bufferSize,
		tableName,
		"",
		repo,
		insertTimeout,
		indexFields,
		metrics,
		logger,
	)
	b.SetColumnDictionary(columns)
	b.SetStorage(state)

	return &CollapsingMergeTreeBuffer{tableBuffer: b}, nil
}

func (c *CollapsingMergeTreeBuffer) Insert(queries ...table_buffer.Query) error {
	return c.tableBuffer.Insert(queries...)
}

func (c *CollapsingMergeTreeBuffer) InsertOrUpdate(queries ...table_buffer.Query) error {
	return c.tableBuffer.InsertOrUpdate(queries...)
}

func (c *CollapsingMergeTreeBuffer) Update(queries ...table_buffer.Query) error {
	return c.tableBuffer.Update(queries...)
}

func (c *CollapsingMergeTreeBuffer) Delete(queries ...table_buffer.Query) error {
	return c.tableBuffer.Delete(queries...)
}

func (c *CollapsingMergeTreeBuffer) Errors() <-chan error {
	return c.tableBuffer.Errors()
}

func (c *CollapsingMergeTreeBuffer) Flush() {
	c.tableBuffer.Flush()
}

func (c *CollapsingMergeTreeBuffer) Close() {
	c.tableBuffer.Close()
}
