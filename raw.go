package rio

import (
	"context"
	"database/sql"
	"iter"
	"reflect"
	"unsafe"
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
// The SQL text is used verbatim; never build it from untrusted input — dynamic
// identifiers belong in column whitelists or rio.WriteColumns constants.
func Raw[T any](sqlText string, args ...any) RawQuery[T] {
	return RawQuery[T]{sql: sqlText, args: copyArgs(args)}
}

// All runs the query and scans every row.
func (r RawQuery[T]) All(ctx context.Context, db Queryer) ([]T, error) {
	d := db.gram().d
	sqlText, args, err := finishSQLText(d, r.sql, r.args)
	if err != nil {
		return nil, err
	}
	rows, finish, err := runQuery(ctx, db, "raw", "", sqlText, args)
	if err != nil {
		return nil, err
	}
	if isScalarType(reflect.TypeFor[T]()) {
		out, err := scanScalars[T](rows)
		finishQuery(finish, err)
		return out, err
	}
	p, err := planOf[T]()
	if err != nil {
		rows.Close()
		finishQuery(finish, err)
		return nil, err
	}
	out, err := scanAll[T](rows, p, true)
	finishQuery(finish, err)
	return out, err
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

// Rows streams the raw query's rows without materializing them, for result
// sets too large to hold: for v, err := range Raw[T](...).Rows(ctx, db).
// Iteration stops on the first error (yielded with a zero T) and the rows
// close automatically, including on early break. Like All it scans scalars,
// DTOs, or entities and holds the result to the same full-column-coverage rule
// — a struct target missing a mapped column is an error, not a silent partial
// scan.
func (r RawQuery[T]) Rows(ctx context.Context, db Queryer) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		d := db.gram().d
		sqlText, args, err := finishSQLText(d, r.sql, r.args)
		if err != nil {
			yield(zero, err)
			return
		}
		rows, finish, err := runQuery(ctx, db, "raw", "", sqlText, args)
		if err != nil {
			yield(zero, err)
			return
		}
		defer rows.Close() // early-break and panic close; mergeClose folds the error on the normal paths

		// fields is the per-column scan plan: a synthetic single column for
		// scalars, else the entity's columns matched by name with full coverage
		// enforced (namedFields) — the same shapes All scans.
		tt := reflect.TypeFor[T]()
		var fields []*field
		if isScalarType(tt) {
			f := &field{name: tt.String(), column: "<scalar>", typ: tt}
			if f.code, err = codecFor(f); err == nil {
				fields = []*field{f}
			}
		} else {
			var p *plan
			if p, err = planOf[T](); err == nil {
				fields, err = namedFields(rows, p)
			}
		}
		if err != nil {
			mergeClose(rows, &err)
			finishQuery(finish, err)
			yield(zero, err)
			return
		}
		rs := newRowScanner(fields, nil)
		defer rs.release()
		for rows.Next() {
			var row T
			if err := rs.scan(rows, unsafe.Pointer(&row)); err != nil {
				mergeClose(rows, &err)
				finishQuery(finish, err)
				yield(zero, err)
				return
			}
			if !yield(row, nil) {
				finishQuery(finish, nil)
				return
			}
		}
		err = rows.Err()
		mergeClose(rows, &err)
		finishQuery(finish, err)
		if err != nil {
			yield(zero, err)
		}
	}
}

// Exec runs a hand-written statement through the shared pipeline and returns
// the driver result.
func Exec(ctx context.Context, db Queryer, sqlText string, args ...any) (sql.Result, error) {
	d := db.gram().d
	rebound, outArgs, err := finishSQLText(d, sqlText, copyArgs(args))
	if err != nil {
		return nil, err
	}
	return run(ctx, db, "exec", "", rebound, outArgs)
}
