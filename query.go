package rio

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

type cond struct {
	expr string
	args []any
}

type trashMode uint8

const (
	trashDefault trashMode = iota // filter soft-deleted rows out
	trashWith                     // include them
	trashOnly                     // only them
)

// queryState is the non-generic body of Query[T]; renderers and the
// preloader work on it without type parameters.
type queryState struct {
	wheres  []cond
	havings []cond
	joins   []string
	orders  []string
	groups  []string

	limit, offset       int
	limitSet, offsetSet bool

	forUpdate bool
	trashed   trashMode
	allRows   bool

	withs []preloadSpec
}

// Query is an immutable, connection-free query description. Every builder
// method returns a derived copy; deriving several queries from one shared
// base — including concurrently, including package-level bases — never
// cross-contaminates. Queries hold no rendered SQL: rendering happens at the
// execution point against the handle's grammar (Compiled caches it).
type Query[T any] struct {
	s queryState
}

// From starts a query for T's table: rio.From[User]().Where(...).All(ctx, db).
func From[T any]() Query[T] {
	return Query[T]{}
}

// copyArgs detaches variadic argument slices: callers passing slice... would
// otherwise alias the builder's memory.
func copyArgs(args []any) []any {
	if len(args) == 0 {
		return nil
	}
	return append([]any(nil), args...)
}

// appendOne appends onto a full-capacity view so derived queries never share
// growth room — the immutability invariant. Every append in this file must
// go through it (CI greps for bare appends).
func appendOne[E any](s []E, e E) []E {
	return append(s[:len(s):len(s)], e)
}

// Where adds an AND-ed condition written in SQL with ? placeholders.
// Slice arguments expand inside IN (?).
func (q Query[T]) Where(expr string, args ...any) Query[T] {
	q.s.wheres = appendOne(q.s.wheres, cond{expr: expr, args: copyArgs(args)})
	return q
}

// OrderBy appends an ORDER BY term, verbatim SQL ("created_at DESC").
func (q Query[T]) OrderBy(expr string) Query[T] {
	q.s.orders = appendOne(q.s.orders, expr)
	return q
}

// GroupBy appends a GROUP BY term. Entity queries with GROUP BY are almost
// always projections — prefer Raw — but filtering EXISTS-style uses remain.
func (q Query[T]) GroupBy(expr string) Query[T] {
	q.s.groups = appendOne(q.s.groups, expr)
	return q
}

// Having adds an AND-ed HAVING condition.
func (q Query[T]) Having(expr string, args ...any) Query[T] {
	q.s.havings = appendOne(q.s.havings, cond{expr: expr, args: copyArgs(args)})
	return q
}

// Join appends a verbatim JOIN clause, for filtering through other tables:
// Join("INNER JOIN orgs ON orgs.id = users.org_id"). Projections across
// joins go through Raw — entity queries always select exactly T's columns.
func (q Query[T]) Join(clause string) Query[T] {
	q.s.joins = appendOne(q.s.joins, clause)
	return q
}

// Limit caps the result. The value is rendered into the SQL, not bound.
func (q Query[T]) Limit(n int) Query[T] {
	q.s.limit, q.s.limitSet = n, true
	return q
}

// Offset skips n rows.
func (q Query[T]) Offset(n int) Query[T] {
	q.s.offset, q.s.offsetSet = n, true
	return q
}

// ForUpdate renders SELECT ... FOR UPDATE for read-modify-write inside a
// transaction. SQLite locks the whole database anyway; there it is a no-op.
func (q Query[T]) ForUpdate() Query[T] {
	q.s.forUpdate = true
	return q
}

// WithTrashed includes soft-deleted rows.
func (q Query[T]) WithTrashed() Query[T] {
	q.s.trashed = trashWith
	return q
}

// OnlyTrashed selects only soft-deleted rows.
func (q Query[T]) OnlyTrashed() Query[T] {
	q.s.trashed = trashOnly
	return q
}

// AllRows is the explicit opt-in for UpdateAll/DeleteAll without conditions.
func (q Query[T]) AllRows() Query[T] {
	q.s.allRows = true
	return q
}

// With preloads a relation after the main query, using one IN query per
// relation (never JOINs — no row explosion, pagination stays correct).
// Paths nest with dots: With("Posts.Comments"). Options customize the leaf.
func (q Query[T]) With(path string, opts ...RelOption) Query[T] {
	q.s.withs = appendOne(q.s.withs, preloadSpec{path: path, opts: opts})
	return q
}

// --- execution ---

// All runs the query and returns every matching row.
func (q Query[T]) All(ctx context.Context, db Queryer) ([]T, error) {
	p, err := planOf[T]()
	if err != nil {
		return nil, err
	}
	sqlText, args, err := renderSelect(db.gram(), p, &q.s, selectRows)
	if err != nil {
		return nil, err
	}
	rows, err := runQuery(ctx, db, "select", p.structName, sqlText, args)
	if err != nil {
		return nil, err
	}
	out, err := scanAll[T](rows, p, false)
	if err != nil {
		return nil, err
	}
	if err := preloadInto(ctx, db, p, out, q.s.withs); err != nil {
		return nil, err
	}
	return out, nil
}

// First returns the first matching row, or ErrNotFound. No implicit ORDER BY
// is added: like SQL itself, order is undefined unless you ask for one.
func (q Query[T]) First(ctx context.Context, db Queryer) (*T, error) {
	rows, err := q.Limit(1).All(ctx, db)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	return &rows[0], nil
}

// Sole returns the single matching row; ErrNotFound when none match and
// ErrMultipleRows when more than one does.
func (q Query[T]) Sole(ctx context.Context, db Queryer) (*T, error) {
	rows, err := q.Limit(2).All(ctx, db)
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

// Count runs SELECT count(*) with the query's conditions. Combined with
// GroupBy the intent (rows vs groups) is ambiguous — use Raw for that.
func (q Query[T]) Count(ctx context.Context, db Queryer) (int64, error) {
	if len(q.s.groups) > 0 {
		return 0, errors.New("rio: Count with GroupBy is ambiguous (rows or groups?); use Raw")
	}
	p, err := planOf[T]()
	if err != nil {
		return 0, err
	}
	sqlText, args, err := renderSelect(db.gram(), p, &q.s, selectCount)
	if err != nil {
		return 0, err
	}
	rows, err := runQuery(ctx, db, "select", p.structName, sqlText, args)
	if err != nil {
		return 0, err
	}
	ns, err := scanScalars[int64](rows)
	if err != nil {
		return 0, err
	}
	if len(ns) == 0 {
		return 0, nil
	}
	return ns[0], nil
}

// Exists reports whether any row matches.
func (q Query[T]) Exists(ctx context.Context, db Queryer) (bool, error) {
	p, err := planOf[T]()
	if err != nil {
		return false, err
	}
	sqlText, args, err := renderSelect(db.gram(), p, &q.s, selectExists)
	if err != nil {
		return false, err
	}
	rows, err := runQuery(ctx, db, "select", p.structName, sqlText, args)
	if err != nil {
		return false, err
	}
	ns, err := scanScalars[int64](rows)
	if err != nil {
		return false, err
	}
	return len(ns) > 0, nil
}

// Find fetches a row by primary key. Composite keys pass every part in
// struct-field declaration order. The statement shape is fixed, so the SQL
// is rendered once per grammar and cached — Find is the hottest read there
// is.
func Find[T any](ctx context.Context, db Queryer, key ...any) (*T, error) {
	p, err := planOf[T]()
	if err != nil {
		return nil, err
	}
	if len(p.pks) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoPrimaryKey, p.structName)
	}
	if len(key) != len(p.pks) {
		return nil, fmt.Errorf("rio: Find[%s] needs %d key part(s) (%s), got %d",
			p.structName, len(p.pks), pkColumns(p), len(key))
	}
	g := db.gram()
	d := g.d
	sqlText, err := crudSQL(g, p, "find", 0, true, func() []byte {
		table := g.table(p)
		b := make([]byte, 0, 160)
		b = append(b, "SELECT "...)
		for i, f := range p.fields {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = d.quote(b, table)
			b = append(b, '.')
			b = d.quote(b, f.column)
		}
		b = append(b, " FROM "...)
		b = d.quote(b, table)
		for i, pk := range p.pks {
			if i == 0 {
				b = append(b, " WHERE "...)
			} else {
				b = append(b, " AND "...)
			}
			b = d.quote(b, table)
			b = append(b, '.')
			b = d.quote(b, pk.column)
			b = append(b, " = ?"...)
		}
		if p.softDel != nil {
			b = append(b, " AND "...)
			b = d.quote(b, table)
			b = append(b, '.')
			b = d.quote(b, p.softDel.column)
			b = append(b, " IS NULL"...)
		}
		b = append(b, " LIMIT 1"...)
		return b
	})
	if err != nil {
		return nil, err
	}
	rows, err := runQuery(ctx, db, "select", p.structName, sqlText, normalizeArgs(d, key))
	if err != nil {
		return nil, err
	}
	return scanOne[T](rows, p)
}

func pkColumns(p *plan) string {
	s := ""
	for i, pk := range p.pks {
		if i > 0 {
			s += ", "
		}
		s += pk.column
	}
	return s
}

// --- rendering ---

type selectShape int

const (
	selectRows selectShape = iota
	selectCount
	selectExists
)

// renderSelect renders one SELECT in the grammar's dialect. Entity column
// lists are always table-qualified: JOINs never make them ambiguous and the
// golden SQL stays stable.
func renderSelect(g *grammar, p *plan, s *queryState, shape selectShape) (string, []any, error) {
	d := g.d
	table := g.table(p)
	b := make([]byte, 0, 192)
	var args []any

	b = append(b, "SELECT "...)
	switch shape {
	case selectCount:
		b = append(b, "count(*)"...)
	case selectExists:
		b = append(b, '1')
	default:
		for i, f := range p.fields {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = d.quote(b, table)
			b = append(b, '.')
			b = d.quote(b, f.column)
		}
	}
	b = append(b, " FROM "...)
	b = d.quote(b, table)

	for _, j := range s.joins {
		b = append(b, ' ')
		b = append(b, j...)
	}

	b, args = renderWhere(b, args, d, table, p, s)

	if len(s.groups) > 0 {
		b = append(b, " GROUP BY "...)
		for i, gexpr := range s.groups {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = append(b, gexpr...)
		}
	}
	for i, h := range s.havings {
		if i == 0 {
			b = append(b, " HAVING "...)
		} else {
			b = append(b, " AND "...)
		}
		b = append(b, '(')
		b = append(b, h.expr...)
		b = append(b, ')')
		args = append(args, h.args...)
	}
	if shape == selectRows && len(s.orders) > 0 {
		b = append(b, " ORDER BY "...)
		for i, o := range s.orders {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = append(b, o...)
		}
	}
	switch shape {
	case selectRows:
		b = appendLimitOffset(b, d, s)
	case selectExists:
		// Existence needs exactly one probe row; user LIMIT/OFFSET would be
		// meaningless here and doubling LIMIT clauses is invalid SQL.
		b = append(b, " LIMIT 1"...)
	}
	if s.forUpdate && d.caps().forUpdate {
		b = append(b, " FOR UPDATE"...)
	}

	return finishSQL(d, b, args)
}

// appendLimitOffset renders LIMIT/OFFSET. PostgreSQL accepts a bare OFFSET;
// MySQL and SQLite require a LIMIT before it, so one is synthesized with the
// dialect's "no limit" spelling.
func appendLimitOffset(b []byte, d Dialect, s *queryState) []byte {
	if s.limitSet {
		b = append(b, " LIMIT "...)
		b = strconv.AppendInt(b, int64(s.limit), 10)
	} else if s.offsetSet {
		switch d.name() {
		case "mysql":
			b = append(b, " LIMIT 18446744073709551615"...) // MySQL's documented "all rows"
		case "sqlite":
			b = append(b, " LIMIT -1"...)
		}
	}
	if s.offsetSet {
		b = append(b, " OFFSET "...)
		b = strconv.AppendInt(b, int64(s.offset), 10)
	}
	return b
}

// renderWhere renders user conditions plus the soft-delete filter.
func renderWhere(b []byte, args []any, d Dialect, table string, p *plan, s *queryState) ([]byte, []any) {
	first := true
	and := func() {
		if first {
			b = append(b, " WHERE "...)
			first = false
		} else {
			b = append(b, " AND "...)
		}
	}
	for _, w := range s.wheres {
		and()
		b = append(b, '(')
		b = append(b, w.expr...)
		b = append(b, ')')
		args = append(args, w.args...)
	}
	if p != nil && p.softDel != nil {
		switch s.trashed {
		case trashDefault:
			and()
			b = d.quote(b, table)
			b = append(b, '.')
			b = d.quote(b, p.softDel.column)
			b = append(b, " IS NULL"...)
		case trashOnly:
			and()
			b = d.quote(b, table)
			b = append(b, '.')
			b = d.quote(b, p.softDel.column)
			b = append(b, " IS NOT NULL"...)
		}
	}
	return b, args
}

// finishSQL runs the rebind pipeline: IN expansion first, placeholder
// renumbering second, in one lexer pass — then normalizes arguments.
func finishSQL(d Dialect, b []byte, args []any) (string, []any, error) {
	sqlText, outArgs, err := rebind(d.lexer(), d.style(), string(b), args)
	if err != nil {
		return "", nil, err
	}
	return sqlText, normalizeArgs(d, outArgs), nil
}

// normalizeArgs applies the write-side time rule to user-supplied arguments
// (Where, Having, Set, Raw, compiled binds): UTC, microsecond precision,
// dialect encoding. Without it a time compared on SQLite would use the
// driver's own text format and silently miss rio's stored values.
// Copy-on-write: the input is caller-owned (Find keys, compiled exec args,
// a RawQuery's stored args shared across concurrent executions) and is
// never mutated; with no time values the pass allocates nothing.
func normalizeArgs(d Dialect, args []any) []any {
	out := args
	cloned := false
	for i, a := range args {
		var v any
		switch t := a.(type) {
		case time.Time:
			v = d.bindTime(normalizeTime(t))
		case *time.Time:
			if t == nil {
				v = nil
			} else {
				v = d.bindTime(normalizeTime(*t))
			}
		default:
			continue
		}
		if !cloned {
			out = append([]any(nil), args...)
			cloned = true
		}
		out[i] = v
	}
	return out
}
