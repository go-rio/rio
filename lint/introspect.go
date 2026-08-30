package lint

import (
	"context"

	"github.com/go-rio/rio"
)

// dbColumn is one live column as introspection reports it.
type dbColumn struct {
	name     string
	dataType string // lower-cased dialect type name (SQLite: the declared type)
	nullable bool
	pk       bool
}

// An introspector lists a table's columns; found is false when the table
// does not exist. Queries run through rio's own Raw pipeline, so the ?
// placeholder rebinds per dialect like everything else.
type introspector func(ctx context.Context, db *rio.DB, table string) (cols []dbColumn, found bool, err error)

var introspectors = map[string]introspector{
	"postgres": introspectPostgres,
	"mysql":    introspectMySQL,
	"sqlite":   introspectSQLite,
}

func introspectPostgres(ctx context.Context, db *rio.DB, table string) ([]dbColumn, bool, error) {
	type row struct {
		Name     string
		DataType string
		Nullable string
	}
	rows, err := rio.Raw[row](`
		SELECT column_name AS name, data_type, is_nullable AS nullable
		FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = ?
		ORDER BY ordinal_position`, table).All(ctx, db)
	if err != nil || len(rows) == 0 {
		return nil, false, err
	}
	pks, err := rio.Raw[string](`
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		 AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
		  AND tc.table_schema = current_schema() AND tc.table_name = ?`, table).All(ctx, db)
	if err != nil {
		return nil, false, err
	}
	isPK := make(map[string]bool, len(pks))
	for _, c := range pks {
		isPK[c] = true
	}
	out := make([]dbColumn, 0, len(rows))
	for _, r := range rows {
		out = append(out, dbColumn{
			name:     r.Name,
			dataType: lower(r.DataType),
			nullable: r.Nullable == "YES",
			pk:       isPK[r.Name],
		})
	}
	return out, true, nil
}

func introspectMySQL(ctx context.Context, db *rio.DB, table string) ([]dbColumn, bool, error) {
	type row struct {
		Name     string
		DataType string
		Nullable string
		Key      string
	}
	rows, err := rio.Raw[row](`
		SELECT column_name AS name, data_type, is_nullable AS nullable, column_key AS `+"`key`"+`
		FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ?
		ORDER BY ordinal_position`, table).All(ctx, db)
	if err != nil || len(rows) == 0 {
		return nil, false, err
	}
	out := make([]dbColumn, 0, len(rows))
	for _, r := range rows {
		out = append(out, dbColumn{
			name:     r.Name,
			dataType: lower(r.DataType),
			nullable: r.Nullable == "YES",
			pk:       r.Key == "PRI",
		})
	}
	return out, true, nil
}

func introspectSQLite(ctx context.Context, db *rio.DB, table string) ([]dbColumn, bool, error) {
	type row struct {
		Name    string
		Type    string
		Notnull int64
		Pk      int64
	}
	rows, err := rio.Raw[row](`
		SELECT name, type, "notnull", pk FROM pragma_table_info(?)`, table).All(ctx, db)
	if err != nil || len(rows) == 0 {
		return nil, false, err
	}
	out := make([]dbColumn, 0, len(rows))
	for _, r := range rows {
		out = append(out, dbColumn{
			name:     r.Name,
			dataType: lower(r.Type),
			// An INTEGER PRIMARY KEY is the rowid: NOT NULL in effect even
			// though pragma reports notnull=0.
			nullable: r.Notnull == 0 && r.Pk == 0,
			pk:       r.Pk > 0,
		})
	}
	return out, true, nil
}

func lower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}
