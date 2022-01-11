package table_buffer

import (
	"github.com/alexkulaga/clickhouse_buffer/metrics"
	"go.uber.org/zap"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTableBuffer_InsertRows(t *testing.T) {
	repoMock := NewRepositoryMock()

	m := metrics.NewMetrics("test_subsystem")
	defer m.Close()

	buffer := NewTableBuffer(
		time.Millisecond,
		100,
		"test_table",
		"sys_sign",
		repoMock,
		time.Second,
		[]string{"first_col"},
		m,
		zap.NewExample(),
	)
	buffer.columns = []string{"first", "second"}

	err := buffer.Log([]Query{{"first": "1", "second": "2"}, {"first": "3", "second": "4"}}...)
	require.NoError(t, err)
	buffer.Close()

	require.Len(t, repoMock.inserts, 2)
	require.Equal(t, storageRow([]interface{}{"1", "2"}), repoMock.inserts[0])
	require.Equal(t, storageRow([]interface{}{"3", "4"}), repoMock.inserts[1])
}

func TestTableBuffer_Mutations(t *testing.T) {
	repoMock := NewRepositoryMock()
	m := metrics.NewMetrics("test_subsystem")
	defer m.Close()

	buffer := NewTableBuffer(
		time.Millisecond,
		100,
		"test_table",
		"sys_sign",
		repoMock,
		time.Second,
		[]string{"first_col"},
		m,
		zap.NewExample(),
	)

	buffer.columns = []string{"first_col", "second_col", "sys_sign"}
	err := buffer.Insert(Query{"first_col": "1"})
	require.NoError(t, err)

	err = buffer.Update(Query{
			"first_col":  "1",
			"second_col": "updated value"},
	)
	require.NoError(t, err)

	err = buffer.Delete(Query{"first_col": "1"})
	require.NoError(t, err)
	buffer.Close()

	require.Len(t, repoMock.inserts, 4)

	require.ElementsMatch(t, storageRow([]interface{}{"1", nil, 1}), repoMock.inserts[0])
	require.ElementsMatch(t, storageRow([]interface{}{"1", nil, -1}), repoMock.inserts[1])
	require.ElementsMatch(t, storageRow([]interface{}{"1", "updated value", 1}), repoMock.inserts[2])
	require.ElementsMatch(t, storageRow([]interface{}{"1", "updated value", -1}), repoMock.inserts[3])
}

func TestTableBuffer_InsertOrUpdate(t *testing.T) {
	repoMock := NewRepositoryMock()
	m := metrics.NewMetrics("test_subsystem")
	defer m.Close()

	buffer := NewTableBuffer(
		time.Millisecond,
		100,
		"test_table",
		"sys_sign",
		repoMock,
		time.Second,
		[]string{"first_col"},
		m,
		zap.NewExample(),
	)
	buffer.columns = []string{"first_col", "second_col", "sys_sign"}

	err := buffer.InsertOrUpdate(
		Query{
			"first_col":  "first value",
			"second_col": "second value",
		},
		Query{
			"first_col":  "first value",
			"second_col": "updated second value",
		},
	)
	require.NoError(t, err)
	buffer.Close()

	require.Len(t, repoMock.inserts, 3)
	require.ElementsMatch(t, storageRow([]interface{}{"first value", "second value", 1}), repoMock.inserts[0])
	require.ElementsMatch(t, storageRow([]interface{}{"first value", "second value", -1}), repoMock.inserts[1])
	require.ElementsMatch(t, storageRow([]interface{}{"first value", "updated second value", 1}), repoMock.inserts[2])
}
