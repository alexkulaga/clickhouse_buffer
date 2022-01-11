package table_buffer

import (
	"bytes"
)

func BuildInsertQuery(query *bytes.Buffer, tableName string, columns []string) {
	query.WriteString("INSERT INTO ")
	query.WriteString(tableName)
	query.WriteString("(")

	queryValues := bytes.NewBuffer(make([]byte, 0, 1024))
	queryValues.WriteString(" VALUES(")

	for i, column := range columns {
		query.WriteString(column)
		queryValues.WriteString("?")

		if i != len(columns)-1 {
			query.WriteString(",")
			queryValues.WriteString(",")
		}
	}

	query.WriteString(")")
	queryValues.WriteString(")")
	query.Write(queryValues.Bytes())
}
