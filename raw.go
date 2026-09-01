package rio

import (
	"context"
	"database/sql"
	"iter"
	"reflect"
)

// RawQuery is hand-written SQL through the shared pipeline, scanning into any
// target shape — DTO structs, scalars, entities. It is a connection-free
// value; placeholders are ? with IN (?) expansion.
type RawQuery[T any] struct {
	sql  string
	args []any
}

// Raw builds a raw query. Struct scanning matches by column name and errors
// on result columns with no matching field. Scanning half an entity and then
// calling Update writes zero values to the unselected columns — project into
// DTOs. The SQL is verbatim; never build it from untrusted input.
func Raw[T any](sqlText string, args ...any) RawQuery[T] {
	return RawQuery[T]{sql: sqlText, args: copyArgs(args)}
}

// Exec runs a hand-written statement through the shared pipeline and returns
// the driver result.
func Exec(ctx context.Context, db Queryer, sqlText string, args ...any) (sql.Result, error) {
	rebound, outArgs, err := finishSQLText(db.gram(), sqlText, copyArgs(args))
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
// ErrMultipleRows when several do.
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

// Rows streams rows without materializing them. Iteration stops on the first
// error (yielded with a zero T) and the rows close automatically, including
// on early break. Struct targets follow All's full-column-coverage rule.
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
		sqlText, args, err := finishSQLText(db.gram(), r.sql, r.args)
		if err != nil {
			yield(zero, err)
			return
		}
		rows, finish, err := runQuery(ctx, db, "raw", "", sqlText, args)
		if err != nil {
			yield(zero, err)
			return
		}
		drainRows(rows, finish, func() ([]*field, error) {
			if scalar {
				f, err := scalarField(tt)
				if err != nil {
					return nil, err
				}
				return []*field{f}, nil
			}
			return namedFields(rows, p)
		}, yield)
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
	sqlText, args, err := finishSQLText(db.gram(), r.sql, r.args)
	if err != nil {
		return nil, err
	}
	rows, finish, err := runQuery(ctx, db, "raw", "", sqlText, args)
	if err != nil {
		return nil, err
	}
	if scalar {
		out, err := scanScalarsN[T](rows, maxRows)
		finishQuery(finish, err, int64(len(out)))
		return out, err
	}
	out, err := scanAllN[T](rows, p, true, maxRows)
	finishQuery(finish, err, int64(len(out)))
	return out, err
}
