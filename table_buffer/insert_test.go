package table_buffer

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildInsertQuery(t *testing.T) {
	query := bytes.NewBuffer(make([]byte, 0, 1024))
	BuildInsertQuery(query, "test_table", []string{"fisrt_column", "second_column", "third_column"})

	require.Equal(t,
		"INSERT INTO test_table(fisrt_column,second_column,third_column) VALUES(?,?,?)",
		query.String(),
	)
}
