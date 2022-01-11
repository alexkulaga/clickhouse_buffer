package clickhouse_buffer

import (
	"database/sql"
	"time"

	"github.com/alexkulaga/clickhouse_buffer/metrics"
	"github.com/alexkulaga/clickhouse_buffer/table_buffer"
	"go.uber.org/zap"
)

type MergeTreeBuffer struct {
	tableBuffer *table_buffer.TableBuffer
}

func NewMergeTreeBuffer(
	db *sql.DB,
	tableName string,
	insertTimeout time.Duration,
	flushPeriod time.Duration,
	bufferSize uint64,
	metrics metrics.Metrics,
	logger *zap.Logger,
) *MergeTreeBuffer {
	repo := table_buffer.NewRepository(db, metrics, logger)
	b := table_buffer.NewTableBuffer(flushPeriod, bufferSize, tableName, "", repo, insertTimeout, nil, metrics, logger)

	return &MergeTreeBuffer{tableBuffer: b}
}

func (m *MergeTreeBuffer) Insert(queries ...table_buffer.Query) error {
	return m.tableBuffer.Log(queries...)
}

func (m *MergeTreeBuffer) Flush() {
	m.tableBuffer.Flush()
}

func (m *MergeTreeBuffer) Errors() <-chan error {
	return m.tableBuffer.Errors()
}

func (m *MergeTreeBuffer) Close() {
	m.tableBuffer.Close()
}
