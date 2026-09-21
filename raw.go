package rio

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"reflect"
)

// RawQuery is a hand-written SELECT head plus the clauses rio appends (WHERE,
// GROUP BY, HAVING, ORDER BY, bound LIMIT/OFFSET): an immutable value that
// scans into any shape under Query's argument rules.
type RawQuery[T any] struct {
	s     queryState
	cache *queryCache
}

// Raw starts a query from a SELECT head written up to its FROM and JOIN
// clauses; with args the head's placeholders bind inline, without them they
// defer to the terminal call. Struct targets scan by column name and must
// cover every result column. The SQL is verbatim; never build it from
// untrusted input.
func Raw[T any](sqlText string, args ...any) RawQuery[T] {
	var q RawQuery[T]
	q.s.head = cond{expr: sqlText, args: copyArgs(args)}
	q.s.noteCondArity("Raw", sqlText, len(args))
	return q
}

// Where adds an AND-ed verbatim condition; slice arguments expand inside
// IN (?).
func (q RawQuery[T]) Where(expr string, args ...any) RawQuery[T] {
	q.cache = nil
	q.s.wheres = appendOne(q.s.wheres, cond{expr: expr, args: copyArgs(args)})
	q.s.noteCondArity("Where", expr, len(args))
	return q
}

// GroupBy appends a verbatim GROUP BY term.
func (q RawQuery[T]) GroupBy(expr string) RawQuery[T] {
	q.cache = nil
	q.s.groups = appendOne(q.s.groups, expr)
	return q
}

// Having adds an AND-ed verbatim HAVING condition.
func (q RawQuery[T]) Having(expr string, args ...any) RawQuery[T] {
	q.cache = nil
	q.s.havings = appendOne(q.s.havings, cond{expr: expr, args: copyArgs(args)})
	q.s.noteCondArity("Having", expr, len(args))
	return q
}

// OrderBy appends a verbatim ORDER BY term.
func (q RawQuery[T]) OrderBy(expr string) RawQuery[T] {
	q.cache = nil
	q.s.orders = appendOne(q.s.orders, expr)
	return q
}

// Limit caps the result; the value binds as a parameter.
func (q RawQuery[T]) Limit(n int) RawQuery[T] {
	q.cache = nil
	q.s.limit, q.s.limitSet = n, true
	return q
}

// Offset skips n rows; the value binds as a parameter.
func (q RawQuery[T]) Offset(n int) RawQuery[T] {
	q.cache = nil
	q.s.offset, q.s.offsetSet = n, true
	return q
}

// Validate returns the first connection-independent error: argument arity,
// placeholders in GroupBy/OrderBy, or a T rio cannot scan into.
func (q RawQuery[T]) Validate() error {
	if _, _, err := rawTarget[T](); err != nil {
		return err
	}
	return validateRawState(&q.s)
}

// Must panics if Validate fails and returns the query with a private render
// cache keyed per executing handle.
func (q RawQuery[T]) Must() RawQuery[T] {
	if err := q.Validate(); err != nil {
		panic(err)
	}
	q.cache = new(queryCache)
	return q
}

// All scans every row; args fill deferred placeholders in SQL order.
func (q RawQuery[T]) All(ctx context.Context, db Queryer, args ...any) ([]T, error) {
	return q.scan(ctx, db, queryCacheAll, args, 0)
}

// First returns the first row or ErrNotFound. It appends no LIMIT to the
// head; add Limit when it matters.
func (q RawQuery[T]) First(ctx context.Context, db Queryer, args ...any) (*T, error) {
	rows, err := q.scan(ctx, db, queryCacheFirst, args, 1)
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
func (q RawQuery[T]) Sole(ctx context.Context, db Queryer, args ...any) (*T, error) {
	rows, err := q.scan(ctx, db, queryCacheSole, args, 2)
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

// Value is Sole by value: the single row's T, for counts, single columns, and
// other scalar reads.
func (q RawQuery[T]) Value(ctx context.Context, db Queryer, args ...any) (T, error) {
	row, err := q.Sole(ctx, db, args...)
	if err != nil {
		var zero T
		return zero, err
	}
	return *row, nil
}

// Rows streams rows without materializing them; the first error ends the
// iteration with a zero T, and the rows close on completion or early break.
func (q RawQuery[T]) Rows(ctx context.Context, db Queryer, args ...any) iter.Seq2[T, error] {
	execArgs := copyArgs(args)
	return func(yield func(T, error) bool) {
		var zero T
		tt, p, err := rawTarget[T]()
		if err != nil {
			yield(zero, err)
			return
		}
		sqlText, bound, err := q.render(db.gram(), queryCacheRows, selectRows, execArgs)
		if err != nil {
			yield(zero, err)
			return
		}
		rows, finish, err := runQuery(ctx, db, "raw", "", sqlText, bound)
		if err != nil {
			yield(zero, err)
			return
		}
		drainRows(rows, finish, func() ([]*field, error) {
			if p == nil {
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

// Count returns how many rows All would return (groups under GROUP BY);
// Limit and Offset are rejected.
func (q RawQuery[T]) Count(ctx context.Context, db Queryer, args ...any) (int64, error) {
	if q.s.limitSet || q.s.offsetSet {
		return 0, errors.New(
			"rio: Count cannot honor Limit/Offset (COUNT aggregates before LIMIT applies); drop them",
		)
	}
	sqlText, bound, err := q.render(db.gram(), queryCacheCount, selectCount, args)
	if err != nil {
		return 0, err
	}
	rows, finish, err := runQuery(ctx, db, "raw", "", sqlText, bound)
	if err != nil {
		return 0, err
	}
	n, found, err := scanScalarOne[int64](rows)
	finishQuery(finish, err, oneIf(found))
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	return n, nil
}

// Exists reports whether any row matches; Limit and Offset apply first.
func (q RawQuery[T]) Exists(ctx context.Context, db Queryer, args ...any) (bool, error) {
	sqlText, bound, err := q.render(db.gram(), queryCacheExists, selectExists, args)
	if err != nil {
		return false, err
	}
	rows, finish, err := runQuery(ctx, db, "raw", "", sqlText, bound)
	if err != nil {
		return false, err
	}
	found, err := anyRow(rows)
	finishQuery(finish, err, oneIf(found))
	return found, err
}

// SQL renders the statement All would run on db, without executing it.
func (q RawQuery[T]) SQL(db Queryer, args ...any) (string, []any, error) {
	g := db.gram()
	if err := validateRawState(&q.s); err != nil {
		return "", nil, err
	}
	state, err := bindQueryState(g.d, nil, &q.s, args)
	if err != nil {
		return "", nil, err
	}
	return renderRaw(g, &state, selectRows)
}

// render binds args and renders the shape through Must's cache.
func (q RawQuery[T]) render(g *grammar, op queryCacheOp, shape selectShape, execArgs []any) (string, []any, error) {
	key := queryCacheKey{grammar: g.weakSelf, op: op}
	entry, bound, ok, err := q.cache.load(key, g.d, execArgs)
	if err != nil {
		return "", nil, err
	}
	if ok {
		return entry.sql, bound, nil
	}
	if err := validateRawState(&q.s); err != nil {
		return "", nil, err
	}
	state, err := bindQueryState(g.d, nil, &q.s, execArgs)
	if err != nil {
		return "", nil, err
	}
	sqlText, args, err := renderRaw(g, &state, shape)
	if err != nil {
		return "", nil, err
	}
	return q.cache.store(key, g, nil, &q.s, execArgs, sqlText, args)
}

func (q RawQuery[T]) scan(ctx context.Context, db Queryer, op queryCacheOp, args []any, maxRows int) ([]T, error) {
	_, p, err := rawTarget[T]()
	if err != nil {
		return nil, err
	}
	sqlText, bound, err := q.render(db.gram(), op, selectRows, args)
	if err != nil {
		return nil, err
	}
	rows, finish, err := runQuery(ctx, db, "raw", "", sqlText, bound)
	if err != nil {
		return nil, err
	}
	if p == nil {
		out, err := scanScalarsN[T](rows, maxRows)
		finishQuery(finish, err, int64(len(out)))
		return out, err
	}
	out, err := scanAllN[T](rows, p, true, maxRows)
	finishQuery(finish, err, int64(len(out)))
	return out, err
}

// Exec runs a hand-written statement through the shared pipeline and returns
// the driver result. The SQL is verbatim; never build it from untrusted input.
func Exec(ctx context.Context, db Queryer, sqlText string, args ...any) (sql.Result, error) {
	rebound, outArgs, err := finishSQLText(db.gram(), sqlText, copyArgs(args))
	if err != nil {
		return nil, err
	}
	return run(ctx, db, "exec", "", rebound, outArgs)
}

// rawTarget resolves the scan target; a nil plan marks a scalar T.
func rawTarget[T any]() (reflect.Type, *plan, error) {
	tt := reflect.TypeFor[T]()
	if isScalarType(tt) {
		return tt, nil, nil
	}
	p, err := planOf[T]()
	return tt, p, err
}

// validateRawState checks a raw query without a database.
func validateRawState(s *queryState) error {
	if s.err != nil {
		return s.err
	}
	if s.head.expr == "" {
		return errors.New("rio: Raw needs a SELECT head")
	}
	if s.limitSet && s.limit < 0 {
		return fmt.Errorf("rio: Limit requires a non-negative value, got %d", s.limit)
	}
	if s.offsetSet && s.offset < 0 {
		return fmt.Errorf("rio: Offset requires a non-negative value, got %d", s.offset)
	}
	return checkNoArgClauses("Raw", s)
}

// renderRaw appends the builder clauses to the head; Count and Exists wrap
// the statement as a derived table.
func renderRaw(g *grammar, s *queryState, shape selectShape) (string, []any, error) {
	b := make([]byte, 0, len(s.head.expr)+96)
	switch shape {
	case selectCount:
		b = append(b, "SELECT count(*) FROM ("...)
	case selectExists:
		b = append(b, "SELECT 1 FROM ("...)
	}
	b = append(b, s.head.expr...)
	args := append([]any(nil), s.head.args...)
	b, args, err := renderWhere(b, args, g, "", nil, s, nil)
	if err != nil {
		return "", nil, err
	}
	b, args = appendGroupHaving(b, args, s)
	switch shape {
	case selectRows:
		b = appendOrderBy(b, s.orders)
		if b, args, err = appendLimitOffset(b, args, g.d, s); err != nil {
			return "", nil, err
		}
	case selectCount:
		b = append(b, ") AS rio_count"...)
	case selectExists:
		if b, args, err = appendLimitOffset(b, args, g.d, s); err != nil {
			return "", nil, err
		}
		b = append(b, ") AS rio_exists LIMIT ?"...)
		args = append(args, 1)
	}
	return finishSQL(g, b, args)
}

// anyRow reports whether the result has a row and closes it.
func anyRow(rows rows) (found bool, err error) {
	defer mergeClose(rows, &err)
	found = rows.Next()
	return found, rows.Err()
}
