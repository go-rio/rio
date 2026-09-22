package rio

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"slices"
)

// RawQuery is a hand-written SELECT head plus the clauses rio appends (WHERE,
// GROUP BY, HAVING, ORDER BY, bound LIMIT/OFFSET, keyset cursors): an
// immutable value that scans into any shape under Query's argument rules.
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

// WhereAll adds each condition as its own AND-ed Where fragment.
func (q RawQuery[T]) WhereAll(conds ...Condition) RawQuery[T] {
	for _, c := range conds {
		q = q.Where(c.expr, c.args...)
	}
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

// OrderKeys sets the structured ordering cursor pagination requires, over
// T's mapped NOT NULL scalar columns; SortKey.Expr names the SQL that
// produced a column when its bare name would not resolve in the head.
// T's primary-key columns missing from keys are appended as tie-breakers,
// and OrderKeys cannot mix with OrderBy.
func (q RawQuery[T]) OrderKeys(keys ...SortKey) RawQuery[T] {
	q.cache = nil
	q.s.orderKeys = append(append([]SortKey(nil), q.s.orderKeys...), keys...)
	return q
}

// After resumes past the position c marks, as Query.After does.
func (q RawQuery[T]) After(c Cursor) RawQuery[T] {
	q.cache = nil
	q.s.after = &c
	return q
}

// Before selects the page ending at the position c marks, as Query.Before
// does; Rows cannot stream it.
func (q RawQuery[T]) Before(c Cursor) RawQuery[T] {
	q.cache = nil
	q.s.before = &c
	return q
}

// ForUpdate renders FOR UPDATE after the appended clauses, with the given
// LockOptions; a no-op on SQLite, rejected on ClickHouse, ignored by Count.
func (q RawQuery[T]) ForUpdate(opts ...LockOption) RawQuery[T] {
	return q.withLock(lockUpdate, opts)
}

// ForNoKeyUpdate renders FOR NO KEY UPDATE, as Query.ForNoKeyUpdate does.
func (q RawQuery[T]) ForNoKeyUpdate(opts ...LockOption) RawQuery[T] {
	return q.withLock(lockNoKeyUpdate, opts)
}

// ForShare renders FOR SHARE, as Query.ForShare does.
func (q RawQuery[T]) ForShare(opts ...LockOption) RawQuery[T] {
	return q.withLock(lockShare, opts)
}

// ForKeyShare renders FOR KEY SHARE, as Query.ForKeyShare does.
func (q RawQuery[T]) ForKeyShare(opts ...LockOption) RawQuery[T] {
	return q.withLock(lockKeyShare, opts)
}

func (q RawQuery[T]) withLock(mode lockMode, opts []LockOption) RawQuery[T] {
	q.cache = nil
	setLock(&q.s, mode, opts)
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
// placeholders in GroupBy/OrderBy, cursor misuse, or a T rio cannot scan into.
func (q RawQuery[T]) Validate() error {
	_, p, err := rawTarget[T]()
	if err != nil {
		return err
	}
	return validateRawState(p, &q.s)
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

// CursorAt issues the cursor marking row's position under the query's
// OrderKeys; the row must hold the values the database returned.
func (q RawQuery[T]) CursorAt(row *T) (Cursor, error) {
	_, p, err := rawTarget[T]()
	if err != nil {
		return Cursor{}, err
	}
	if p == nil {
		return Cursor{}, errors.New("rio: CursorAt needs struct rows; a scalar Raw query has no sort keys")
	}
	return cursorAt(p, &q.s, reflect.ValueOf(row).Elem())
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
		if q.s.before != nil {
			yield(zero, errors.New("rio: Rows cannot stream Before (the page is read backwards and turned around); use All"))
			return
		}
		tt, p, err := rawTarget[T]()
		if err != nil {
			yield(zero, err)
			return
		}
		sqlText, bound, err := q.render(db.gram(), p, queryCacheRows, selectRows, execArgs)
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

// Chunk yields the matching rows in keyset pages of size rows, as Query.Chunk
// does: pages follow OrderKeys, defaulting to T's primary key; Limit, Offset,
// After, and Before are refused.
func (q RawQuery[T]) Chunk(ctx context.Context, db Queryer, size int, args ...any) iter.Seq2[[]T, error] {
	execArgs := copyArgs(args)
	return func(yield func([]T, error) bool) {
		if size <= 0 {
			yield(nil, fmt.Errorf("rio: Chunk requires a positive size, got %d", size))
			return
		}
		hasPaging := q.s.limitSet || q.s.offsetSet || q.s.after != nil || q.s.before != nil
		if hasPaging {
			yield(nil, errors.New("rio: Chunk owns Limit, Offset, After, and Before; drop them"))
			return
		}
		page := q
		page.cache = nil
		if len(page.s.orderKeys) == 0 {
			_, p, err := rawTarget[T]()
			if err != nil {
				yield(nil, err)
				return
			}
			if p == nil {
				yield(nil, errors.New("rio: Chunk needs struct rows; a scalar Raw query has no sort keys"))
				return
			}
			for _, pk := range p.pks {
				page.s.orderKeys = append(page.s.orderKeys, SortKey{Column: pk.column})
			}
		}
		page.s.limit, page.s.limitSet = size, true
		for {
			rows, err := page.All(ctx, db, execArgs...)
			if err != nil {
				yield(nil, err)
				return
			}
			if len(rows) == 0 {
				return
			}
			if !yield(rows, nil) || len(rows) < size {
				return
			}
			cur, err := page.CursorAt(&rows[len(rows)-1])
			if err != nil {
				yield(nil, err)
				return
			}
			page.s.after = &cur
		}
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
	_, p, err := rawTarget[T]()
	if err != nil {
		return 0, err
	}
	sqlText, bound, err := q.render(db.gram(), p, queryCacheCount, selectCount, args)
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
	_, p, err := rawTarget[T]()
	if err != nil {
		return false, err
	}
	sqlText, bound, err := q.render(db.gram(), p, queryCacheExists, selectExists, args)
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

// Sub embeds the query as a ? argument, for IN (?), EXISTS (?), and scalar
// comparisons; the caller writes the parentheses, the head selects what the
// outer statement expects, and the query's arguments must be inline.
func (q RawQuery[T]) Sub() Subquery {
	return Subquery{render: func(g *grammar) ([]byte, []any, error) {
		_, p, err := rawTarget[T]()
		if err != nil {
			return nil, nil, err
		}
		if err := validateRawState(p, &q.s); err != nil {
			return nil, nil, err
		}
		state, err := bindQueryState(g.d, nil, &q.s, nil)
		if err != nil {
			return nil, nil, err
		}
		return renderRawBytes(g, p, &state, selectRows)
	}}
}

// SQL renders the statement All would run on db, without executing it.
func (q RawQuery[T]) SQL(db Queryer, args ...any) (string, []any, error) {
	g := db.gram()
	_, p, err := rawTarget[T]()
	if err != nil {
		return "", nil, err
	}
	if err := validateRawState(p, &q.s); err != nil {
		return "", nil, err
	}
	state, err := bindQueryState(g.d, nil, &q.s, args)
	if err != nil {
		return "", nil, err
	}
	return renderRaw(g, p, &state, selectRows)
}

// render binds args and renders the shape through Must's cache.
func (q RawQuery[T]) render(
	g *grammar,
	p *plan,
	op queryCacheOp,
	shape selectShape,
	execArgs []any,
) (string, []any, error) {
	key := queryCacheKey{grammar: g.weakSelf, op: op}
	entry, bound, ok, err := q.cache.load(key, g.d, execArgs)
	if err != nil {
		return "", nil, err
	}
	if ok {
		return entry.sql, bound, nil
	}
	if err := validateRawState(p, &q.s); err != nil {
		return "", nil, err
	}
	state, err := bindQueryState(g.d, nil, &q.s, execArgs)
	if err != nil {
		return "", nil, err
	}
	sqlText, args, err := renderRaw(g, p, &state, shape)
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
	sqlText, bound, err := q.render(db.gram(), p, op, selectRows, args)
	if err != nil {
		return nil, err
	}
	rows, finish, err := runQuery(ctx, db, "raw", "", sqlText, bound)
	if err != nil {
		return nil, err
	}
	var out []T
	if p == nil {
		out, err = scanScalarsN[T](rows, maxRows)
	} else {
		out, err = scanAllN[T](rows, p, true, maxRows)
	}
	finishQuery(finish, err, int64(len(out)))
	if err == nil && q.s.before != nil {
		slices.Reverse(out) // the reversed query read the page backwards
	}
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
func validateRawState(p *plan, s *queryState) error {
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
	if err := checkNoArgClauses("Raw", s); err != nil {
		return err
	}
	return validateSortKeys(p, s)
}

// renderRaw appends the builder clauses to the head; Count and Exists wrap
// the statement as a derived table.
func renderRaw(g *grammar, p *plan, s *queryState, shape selectShape) (string, []any, error) {
	b, args, err := renderRawBytes(g, p, s, shape)
	if err != nil {
		return "", nil, err
	}
	return finishSQL(g, b, args)
}

// renderRawBytes is renderRaw before placeholder rebinding, for Sub.
func renderRawBytes(g *grammar, p *plan, s *queryState, shape selectShape) ([]byte, []any, error) {
	var sortKeys []resolvedKey
	hasSortKeys := len(s.orderKeys) > 0 || s.after != nil || s.before != nil
	if hasSortKeys {
		var err error
		if sortKeys, err = resolveSortKeys(p, s); err != nil {
			return nil, nil, err
		}
	}
	b := make([]byte, 0, len(s.head.expr)+96)
	switch shape {
	case selectCount:
		b = append(b, "SELECT count(*) FROM ("...)
	case selectExists:
		b = append(b, "SELECT 1 FROM ("...)
	}
	b = append(b, s.head.expr...)
	args := append([]any(nil), s.head.args...)
	b, args, err := renderWhere(b, args, g, "", nil, s, sortKeys)
	if err != nil {
		return nil, nil, err
	}
	b, args = appendGroupHaving(b, args, s)
	switch shape {
	case selectRows:
		b = appendOrderBy(b, s.orders)
		b = appendOrderKeys(b, g.d, "", sortKeys, s.before != nil)
		if b, args, err = appendLimitOffset(b, args, g.d, s); err != nil {
			return nil, nil, err
		}
		if s.lock != lockNone {
			if b, err = appendLock(b, g.d, s); err != nil {
				return nil, nil, err
			}
		}
	case selectCount:
		b = append(b, ") AS rio_count"...)
	case selectExists:
		if b, args, err = appendLimitOffset(b, args, g.d, s); err != nil {
			return nil, nil, err
		}
		if s.lock != lockNone {
			if b, err = appendLock(b, g.d, s); err != nil {
				return nil, nil, err
			}
		}
		b = append(b, ") AS rio_exists LIMIT ?"...)
		args = append(args, 1)
	}
	return b, args, nil
}

// anyRow reports whether the result has a row and closes it.
func anyRow(rows rows) (found bool, err error) {
	defer mergeClose(rows, &err)
	found = rows.Next()
	return found, rows.Err()
}
