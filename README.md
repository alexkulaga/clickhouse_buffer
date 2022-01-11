* Queries batching
* CollapsingMergeTree queries managing

Usage:

MergeTree example
```go
buffer := NewMergeTreeBuffer(
    dbConnection, // *sql.DB
    "table_name",
    time.Second*10, // clickhouse insert timeout
    time.Second, // buffer flush period
    200_000, // buffer size
    metrics.NewMetrics("example_subsystem"), // metrics interface
    zap.NewExample(),
)
defer buffer.Close()
defer buffer.Flush()

go func() {
    for err := range buffer.Errors() {
        fmt.Printf("write error: %s\n", err.Error())
    }
}()

err := buffer.Insert(table_buffer.Query{
    "first_column":  "column_value",
    "second column": 125,
    "third_column":  true,
})
if err != nil {
    return err
}

return nil
```
---
CollapsingMergeTree example:
```go
buffer, err := NewCollapsingMergeTreeBuffer(
    context.Background(),
    dbConnection, // *sql.DB
    "table_name",
    []string{"primary_column"}, // primary key
    "sys_sign", // sign field name
    time.Second*10, // clickhouse insert timeout
    time.Second, // buffer flush period
    200_000, // buffer size
    metrics.NewMetrics("example_subsystem"), // metrics interface
    zap.NewExample(),
)
if err != nil {
    return err
}

defer buffer.Close()
defer buffer.Flush()

go func() {
    for err := range buffer.Errors() {
        fmt.Printf("write error: %s\n", err.Error())
    }
}()

err = buffer.Insert(table_buffer.Query{
    "primary_column": "column_value",
    "second column":  "value",
})
if err != nil {
    return err
}

err = buffer.Update(table_buffer.Query{
    "primary_column": "column_value",
    "second column":  "new_value",
})
if err != nil {
    return err
}

err = buffer.Delete(table_buffer.Query{
    "primary_column":  "column_value",
})
if err != nil {
    return err
}

return nil
```

This code will generate the following inserts:
``` clickhouse
INSERT INTO test_table(primary_column,column,sys_sign) VALUES("column_value", "value", 1)
INSERT INTO test_table(primary_column,column,sys_sign) VALUES("column_value", "value", -1)
INSERT INTO test_table(primary_column,column,sys_sign) VALUES("column_value", "new_value", 1)
INSERT INTO test_table(primary_column,column,sys_sign) VALUES("column_value", "new_value", -1)
```
