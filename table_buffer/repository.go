package table_buffer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type RepositoryMetrics interface {
	ObserveDBLatency(tableName string, latency float64)
}

type TableState map[string]map[string]interface{} // pk->column->data

type TableMeta struct {
	Offset int64
	Lsn    uint64
}

type repository struct {
	conn    *sql.DB
	logger  *zap.Logger
	metrics RepositoryMetrics
}

func NewRepository(conn *sql.DB, metrics RepositoryMetrics, logger *zap.Logger) *repository {
	return &repository{
		conn:    conn,
		logger:  logger,
		metrics: metrics,
	}
}

func (r *repository) Insert(ctx context.Context, query string, rows []storageRow, tableName string) (int, error) {
	defer func(start time.Time) {
		elapsed := float64(time.Since(start)) / 1_000_000.0
		r.metrics.ObserveDBLatency(tableName, elapsed)
	}(time.Now())

	tx, err := r.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, errors.Wrap(err, "begin transaction")
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			r.logger.Error("rollback error", zap.Error(err))
		}
	}()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return 0, errors.Wrapf(err, "prepare query \"%s\"", query)
	}
	defer stmt.Close()

	for _, row := range rows {
		_, err = stmt.ExecContext(ctx, row...)
		if err != nil {
			return 0, errors.Wrapf(err, "exec row %+v for pepare query %s", row, query)
		}
	}

	err = tx.Commit()
	if err != nil {
		return 0, errors.Wrapf(err, "commit transaction")
	}

	return len(rows), nil
}

func (r *repository) FetchTable( // nolint: cyclop
	ctx context.Context,
	tableName string,
	indexFields []string,
	signField string,
) (
	TableState,
	[]string, // columns
	error,
) {
	result := make(map[string]map[string]interface{})

	rows, err := r.conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s FINAL", tableName))
	if err != nil {
		return nil, nil, errors.Wrapf(err, "get all rows from %s", tableName)
	}
	defer rows.Close()

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "get rows columns from %s", tableName)
	}

	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))

		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, nil, errors.Wrapf(err, "rows scan for %s", tableName)
		}

		var indexedValues []string

		m := make(map[string]interface{})

		for i, colName := range cols {
			if colName == signField {
				continue
			}

			val := columnPointers[i].(*interface{})
			m[colName] = *val

			for _, indexField := range indexFields {
				if colName == indexField {
					indexedValues = append(indexedValues, fmt.Sprint(*val))

					break
				}
			}
		}

		result[strings.Join(indexedValues, "_")] = m
	}

	return result, cols, nil
}

func (r *repository) FetchColumnNames(ctx context.Context, tableName string) ([]string, error) {
	rows, err := r.conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 1", tableName))
	if err != nil {
		return nil, errors.Wrapf(err, "get row from %s", tableName)
	}
	defer rows.Close()

	if err := rows.Err(); err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrapf(err, "get rows columns from %s", tableName)
	}

	return cols, nil
}
