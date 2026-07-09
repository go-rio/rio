package rio

import (
	"context"
	"database/sql"
	"reflect"
)

// RawQuery is the escape hatch: hand-written SQL through the same rebind
// pipeline, hooks, error translation, and scanner as everything else, into
// any target shape — DTO structs, scalars, entities. Like builders it is a
// connection-free value; placeholders are ? with IN (?) expansion.
type RawQuery[T any] struct {
	sql  string
	args []any
}

// Raw builds a raw query. Scanning into a struct matches by column name and
// errors on result columns with no matching field: silently dropped data is
// how schema drift hides. Scanning half an entity and then calling Update
// writes zero values to the columns you did not select — project into DTOs.
func Raw[T any](sqlText string, args ...any) RawQuery[T] {
	return RawQuery[T]{sql: sqlText, args: copyArgs(args)}
}

// All runs the query and scans every row.
func (r RawQuery[T]) All(ctx context.Context, db Queryer) ([]T, error) {
	d := db.gram().d
	sqlText, args, err := rebind(d.lexer(), d.style(), r.sql, r.args)
	if err != nil {
		return nil, err
	}
	args = normalizeArgs(d, args)
	rows, err := runQuery(ctx, db, "raw", "", sqlText, args)
	if err != nil {
		return nil, err
	}
	if isScalarType(reflect.TypeFor[T]()) {
		return scanScalars[T](rows)
	}
	p, err := planOf[T]()
	if err != nil {
		rows.Close()
		return nil, err
	}
	return scanAll[T](rows, p, true)
}

// First returns the first row or ErrNotFound. rio does not append LIMIT to
// hand-written SQL; add your own when it matters.
func (r RawQuery[T]) First(ctx context.Context, db Queryer) (*T, error) {
	rows, err := r.All(ctx, db)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	return &rows[0], nil
}

// Sole returns the single row, ErrNotFound when none match, and
// ErrMultipleRows when several do — same contract as Query.Sole.
func (r RawQuery[T]) Sole(ctx context.Context, db Queryer) (*T, error) {
	rows, err := r.All(ctx, db)
	if err != nil {
		return nil, err
	}
	switch len(rows) {
	case 0:
		return nil, ErrNotFound
	case 1:
		return &rows[0], nil
	}
	return nil, ErrMultipleRows
}

// Exec runs a hand-written statement through the shared pipeline and returns
// the driver result.
func Exec(ctx context.Context, db Queryer, sqlText string, args ...any) (sql.Result, error) {
	d := db.gram().d
	rebound, outArgs, err := rebind(d.lexer(), d.style(), sqlText, copyArgs(args))
	if err != nil {
		return nil, err
	}
	outArgs = normalizeArgs(d, outArgs)
	return run(ctx, db, "exec", "", rebound, outArgs)
}
