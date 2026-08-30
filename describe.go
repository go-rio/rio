package rio

import (
	"fmt"
	"reflect"
)

// ColumnSchema is one mapped column of a model, as rio's plan sees it —
// the expectation side of a schema comparison.
type ColumnSchema struct {
	// Name is the column name.
	Name string
	// Field is the Go struct field's name.
	Field string
	// GoType is the field's Go type.
	GoType reflect.Type
	// Nullable reports whether rio can scan a NULL into the field: a pointer
	// field, a sql.Scanner (which receives NULL itself), or the softdelete
	// column's NULL↔zero-time exception.
	Nullable bool
	// PrimaryKey marks the primary-key columns.
	PrimaryKey bool
	// JSON marks columns stored as serialized JSON text.
	JSON bool
}

// TableSchema is a model's mapping under one handle: the table name this
// handle resolves (TableName override, then WithTableNamer, then convention)
// and every mapped column in plan order. It feeds schema tooling — the lint
// package compares it against the live database — and is read-only.
type TableSchema struct {
	// Struct is the model's Go type name.
	Struct string
	// Table is the table name under this handle's naming.
	Table string
	// Columns are the mapped columns in plan order.
	Columns []ColumnSchema
	// PKs are the primary-key column names in declaration order.
	PKs []string
}

// DescribeModel reports how this handle maps model: its resolved table name
// and column expectations. The model must be a mappable struct (or pointer
// to one); relations and countof targets are not columns and do not appear.
func (d *DB) DescribeModel(model any) (*TableSchema, error) {
	t := reflect.TypeOf(model)
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("rio: DescribeModel needs a struct model, got %T", model)
	}
	p, err := planFor(t)
	if err != nil {
		return nil, err
	}
	ts := &TableSchema{
		Struct:  p.structName,
		Table:   d.g.table(p),
		Columns: make([]ColumnSchema, 0, len(p.fields)),
		PKs:     make([]string, 0, len(p.pks)),
	}
	for _, pk := range p.pks {
		ts.PKs = append(ts.PKs, pk.column)
	}
	for _, f := range p.fields {
		ts.Columns = append(ts.Columns, ColumnSchema{
			Name:       f.column,
			Field:      f.name,
			GoType:     f.typ,
			Nullable:   f.typ.Kind() == reflect.Pointer || f.isSoftDelete || f.code.kind == scanScanner,
			PrimaryKey: f.isPK,
			JSON:       f.code.kind == scanJSON,
		})
	}
	return ts, nil
}

// DialectName identifies this handle's dialect: "postgres", "mysql",
// "sqlite", or "clickhouse".
func (d *DB) DialectName() string { return d.g.d.name() }
