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

// All runs the query and scans every row.
func (r RawQuery[T]) All(ctx context.Context, db Queryer) ([]T, error) {
	return r.scan(ctx, db, 0)
}

// First returns the first row or ErrNotFound. rio does not append LIMIT to
// hand-written SQL; add your own when it matters.
func (r RawQuery[T]) First(ctx context.Context, db Queryer) (*T, error) {
	rows, err := r.scan(ctx, db, 1)
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
	rows, err := r.scan(ctx, db, 2)
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
		tt := reflect.TypeFor[T]()
		scalar := isScalarType(tt)
		var p *plan
		var err error
		if !scalar {
			p, err = planOf[T]()
			if err != nil {
				yield(zero, err)
				return
			}
		}
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
		finished := false
		defer func() {
			if !finished {
				_ = finishRows(rows, finish, nil)
			}
		}()

		// fields is the per-column scan plan: a synthetic single column for
		// scalars, else the entity's columns matched by name with full coverage
		// enforced (namedFields) — the same shapes All scans.
		var fields []*field
		if scalar {
			var f *field
			if f, err = scalarField(tt); err == nil {
				fields = []*field{f}
			}
		} else {
			fields, err = namedFields(rows, p)
		}
		if err != nil {
			finished = true
			err = finishRows(rows, finish, err)
			yield(zero, err)
			return
		}
		rs := newRowScanner(fields, nil)
		defer rs.release()
		var row T
		for rows.Next() {
			row = zero
			if err := rs.scan(rows, unsafe.Pointer(&row)); err != nil {
				finished = true
				err = finishRows(rows, finish, err)
				yield(zero, err)
				return
			}
			if !yield(row, nil) {
				finished = true
				_ = finishRows(rows, finish, nil)
				return
			}
		}
		err = rows.Err()
		finished = true
		err = finishRows(rows, finish, err)
		if err != nil {
			yield(zero, err)
		}
	}
}

func (r RawQuery[T]) scan(ctx context.Context, db Queryer, maxRows int) ([]T, error) {
	tt := reflect.TypeFor[T]()
	scalar := isScalarType(tt)
	var p *plan
	var err error
	if !scalar {
		p, err = planOf[T]()
		if err != nil {
			return nil, err
		}
	}
	d := db.gram().d
	sqlText, args, err := finishSQLText(d, r.sql, r.args)
	if err != nil {
		return nil, err
	}
	rows, finish, err := runQuery(ctx, db, "raw", "", sqlText, args)
	if err != nil {
		return nil, err
	}
	if scalar {
		out, err := scanScalarsN[T](rows, maxRows)
		finishQuery(finish, err)
		return out, err
	}
	out, err := scanAllN[T](rows, p, true, maxRows)
	finishQuery(finish, err)
	return out, err
}
