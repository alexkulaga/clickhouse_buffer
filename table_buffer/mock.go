package table_buffer

import (
	"context"
	"sync"
)

type RepositoryMock struct {
	inserts []storageRow
	lock *sync.Mutex
}

func NewRepositoryMock() *RepositoryMock {
	return &RepositoryMock{lock: &sync.Mutex{}}
}

func (r *RepositoryMock) Insert(ctx context.Context, query string, rows []storageRow, tableName string) (int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.inserts = append(r.inserts, rows...)
	return len(rows), nil
}

func (r *RepositoryMock) FetchTable(ctx context.Context, tableName string, indexFields []string, signField string) (TableState, []string, error) {
	return nil, nil, nil
}

func (r *RepositoryMock) FetchColumnNames(ctx context.Context, tableName string) ([]string, error) {
	return nil, nil
}
