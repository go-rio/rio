package rio

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"math"
	"strconv"
	"strings"
	"time"
	"unsafe"
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

// queryState is the non-generic body shared by renderers and preloaders.
type queryState struct {
	wheres  []cond
	havings []cond
	joins   []string
	orders  []string
	groups  []string

	// err defers the first builder error until validation or execution.
	err error

	limit, offset       int
	limitSet, offsetSet bool

	forUpdate bool
	final     bool
	trashed   trashMode
	allRows   bool

	withs    []preloadSpec
	hasConds []hasCond
	counts   []string
}

// hasCond describes one WhereHas or WhereHasNot EXISTS predicate.
type hasCond struct {
	path      string
	isNegated bool
	opts      []RelOption
}

// Query is an immutable, connection-free query description safe for
// concurrent reuse. Builder methods return derived values. Must may attach an
// internal SQL-shape cache, but never a database, transaction, or statement.
type Query[T any] struct {
	s     queryState
	cache *queryCache
}

type selectShape int

const (
	selectRows selectShape = iota
	selectCount
	selectExists
)

// From starts a query for T's table.
func From[T any]() Query[T] {
	return Query[T]{}
}

// Where adds an AND-ed condition written in SQL with ? placeholders.
// Slice arguments expand inside IN (?). The expression is included verbatim;
// never build it from untrusted input — dynamic identifiers belong in column
// whitelists or rio.WriteColumns constants.
func (q Query[T]) Where(expr string, args ...any) Query[T] {
	q.cache = nil
	q.s.wheres = appendOne(q.s.wheres, cond{expr: expr, args: copyArgs(args)})
	q.s.noteCondArity("Where", expr, len(args))
	return q
}

// OrderBy appends an ORDER BY term, verbatim SQL ("created_at DESC"). Never
// build the term from untrusted input — dynamic identifiers belong in column
// whitelists or rio.WriteColumns constants.
func (q Query[T]) OrderBy(expr string) Query[T] {
	q.cache = nil
	q.s.orders = appendOne(q.s.orders, expr)
	return q
}

// GroupBy appends a verbatim GROUP BY term. Never build it from untrusted
// input; whitelist dynamic identifiers or use WriteColumns constants.
func (q Query[T]) GroupBy(expr string) Query[T] {
	q.cache = nil
	q.s.groups = appendOne(q.s.groups, expr)
	return q
}

// Having adds an AND-ed HAVING condition. The expression is included verbatim;
// never build it from untrusted input — dynamic identifiers belong in column
// whitelists or rio.WriteColumns constants.
func (q Query[T]) Having(expr string, args ...any) Query[T] {
	q.cache = nil
	q.s.havings = appendOne(q.s.havings, cond{expr: expr, args: copyArgs(args)})
	q.s.noteCondArity("Having", expr, len(args))
	return q
}

// Join appends a verbatim JOIN clause. Entity queries still select only T's
// columns. Never build the clause from untrusted input; whitelist dynamic
// identifiers or use WriteColumns constants.
func (q Query[T]) Join(clause string) Query[T] {
	q.cache = nil
	q.s.joins = appendOne(q.s.joins, clause)
	return q
}

// Limit caps the result. The value is rendered into the SQL, not bound.
func (q Query[T]) Limit(n int) Query[T] {
	q.cache = nil
	q.s.limit, q.s.limitSet = n, true
	return q
}

// Offset skips n rows.
func (q Query[T]) Offset(n int) Query[T] {
	q.cache = nil
	q.s.offset, q.s.offsetSet = n, true
	return q
}

// ForUpdate renders SELECT ... FOR UPDATE for read-modify-write inside a
// transaction. SQLite locks the whole database anyway; there it is a no-op.
// ClickHouse has no row locks at all and rejects it at render.
func (q Query[T]) ForUpdate() Query[T] {
	q.cache = nil
	q.s.forUpdate = true
	return q
}

// Final applies ClickHouse's FINAL modifier to the main SELECT. It does not
// affect preloads, WithCount, or WhereHas subqueries. Other dialects reject it.
func (q Query[T]) Final() Query[T] {
	q.cache = nil
	q.s.final = true
	return q
}

// WithTrashed includes soft-deleted rows.
func (q Query[T]) WithTrashed() Query[T] {
	q.cache = nil
	q.s.trashed = trashWith
	return q
}

// OnlyTrashed selects only soft-deleted rows.
func (q Query[T]) OnlyTrashed() Query[T] {
	q.cache = nil
	q.s.trashed = trashOnly
	return q
}

// AllRows is the explicit opt-in for UpdateAll/DeleteAll without conditions.
func (q Query[T]) AllRows() Query[T] {
	q.cache = nil
	q.s.allRows = true
	return q
}

// Scope applies reusable query functions in order.
func (q Query[T]) Scope(fns ...func(Query[T]) Query[T]) Query[T] {
	for _, fn := range fns {
		q = fn(q)
	}
	return q
}

// WhereHas keeps rows whose relation path has a matching row. Nested paths
// nest EXISTS predicates, and RelWithTrashed applies to the leaf relation.
func (q Query[T]) WhereHas(path string, opts ...RelOption) Query[T] {
	q.cache = nil
	q.s.hasConds = appendOne(q.s.hasConds, hasCond{path: path, opts: opts})
	return q
}

// WhereHasNot keeps rows whose relation path has no matching row.
func (q Query[T]) WhereHasNot(path string, opts ...RelOption) Query[T] {
	q.cache = nil
	q.s.hasConds = appendOne(q.s.hasConds, hasCond{path: path, isNegated: true, opts: opts})
	return q
}

// WithCount fills the tagged int64 count target for a HasMany or ManyToMany
// relation using one GROUP BY query.
func (q Query[T]) WithCount(relation string) Query[T] {
	q.cache = nil
	q.s.counts = appendOne(q.s.counts, relation)
	return q
}

// With preloads a relation with a separate IN query. Dot-separated paths
// preload nested relations; options apply to the leaf.
func (q Query[T]) With(path string, opts ...RelOption) Query[T] {
	q.cache = nil
	q.s.withs = appendOne(q.s.withs, preloadSpec{path: path, opts: opts})
	return q
}

// All runs the query and returns every matching row. args fill deferred
// placeholders from Where and Having fragments in final SQL order.
func (q Query[T]) All(ctx context.Context, db Queryer, args ...any) ([]T, error) {
	return q.all(ctx, db, queryCacheAll, args)
}

// First returns the first matching row or ErrNotFound. It adds LIMIT 1 only
// when no limit was set and never adds an order.
func (q Query[T]) First(ctx context.Context, db Queryer, args ...any) (*T, error) {
	one := q
	if !one.s.limitSet {
		one.s.limit, one.s.limitSet = 1, true
	}
	g := db.gram()
	key := queryCacheKey{grammar: g.weakSelf, op: queryCacheFirst}
	p, state, sqlText, bound, err := prepareCachedSelect[T](
		one.cache,
		key,
		g,
		&one.s,
		selectRows,
		args,
	)
	if err != nil {
		return nil, err
	}
	rows, finish, err := runQuery(
		ctx,
		db,
		"select",
		p.structName,
		sqlText,
		bound,
	)
	if err != nil {
		return nil, err
	}
	out, err := scanOne[T](rows, p)
	finishQuery(finish, err, oneIf(err == nil))
	if err != nil {
		return nil, err
	}
	if len(state.withs) > 0 || len(state.counts) > 0 {
		single := []T{*out}
		if err := loadQueryRelations(ctx, db, p, single, state.withs, state.counts); err != nil {
			return nil, err
		}
		*out = single[0]
	}
	return out, nil
}

// Sole returns the only matching row, ErrNotFound for none, or
// ErrMultipleRows for more than one. It adds LIMIT 2 only when no limit is set.
func (q Query[T]) Sole(ctx context.Context, db Queryer, args ...any) (*T, error) {
	probe := q
	if !probe.s.limitSet {
		probe.s.limit, probe.s.limitSet = 2, true
	}
	g := db.gram()
	key := queryCacheKey{grammar: g.weakSelf, op: queryCacheSole}
	p, state, sqlText, bound, err := prepareCachedSelect[T](
		probe.cache,
		key,
		g,
		&probe.s,
		selectRows,
		args,
	)
	if err != nil {
		return nil, err
	}
	sqlRows, finish, err := runQuery(
		ctx,
		db,
		"select",
		p.structName,
		sqlText,
		bound,
	)
	if err != nil {
		return nil, err
	}
	rows, err := scanAllN[T](sqlRows, p, false, 2)
	finishQuery(finish, err, int64(len(rows)))
	if err != nil {
		return nil, err
	}
	switch len(rows) {
	case 0:
		return nil, ErrNotFound
	case 1:
		if err := loadQueryRelations(ctx, db, p, rows, state.withs, state.counts); err != nil {
			return nil, err
		}
		return &rows[0], nil
	}
	return nil, ErrMultipleRows
}

// Count returns the number of matching rows. GroupBy and Having projections
// are rejected, and so are Limit/Offset — COUNT aggregates before LIMIT
// applies, so honoring them needs a subquery; use Raw for those queries.
func (q Query[T]) Count(ctx context.Context, db Queryer, args ...any) (int64, error) {
	if len(q.s.groups) > 0 || len(q.s.havings) > 0 {
		return 0, errors.New("rio: Count with GroupBy/Having is a projection (rows or groups?); use Raw")
	}
	if q.s.limitSet || q.s.offsetSet {
		// Silently counting the whole match would answer a different
		// question than the windowed query the caller described.
		return 0, errors.New("rio: Count cannot honor Limit/Offset (COUNT aggregates before LIMIT applies); drop them, or count the window with Raw")
	}
	g := db.gram()
	key := queryCacheKey{grammar: g.weakSelf, op: queryCacheCount}
	p, _, sqlText, bound, err := prepareCachedSelect[T](
		q.cache,
		key,
		g,
		&q.s,
		selectCount,
		args,
	)
	if err != nil {
		return 0, err
	}
	rows, finish, err := runQuery(
		ctx,
		db,
		"select",
		p.structName,
		sqlText,
		bound,
	)
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

// Exists reports whether any row matches.
func (q Query[T]) Exists(ctx context.Context, db Queryer, args ...any) (bool, error) {
	g := db.gram()
	key := queryCacheKey{grammar: g.weakSelf, op: queryCacheExists}
	p, _, sqlText, bound, err := prepareCachedSelect[T](
		q.cache,
		key,
		g,
		&q.s,
		selectExists,
		args,
	)
	if err != nil {
		return false, err
	}
	rows, finish, err := runQuery(
		ctx,
		db,
		"select",
		p.structName,
		sqlText,
		bound,
	)
	if err != nil {
		return false, err
	}
	_, found, err := scanScalarOne[int64](rows)
	finishQuery(finish, err, oneIf(found))
	if err != nil {
		return false, err
	}
	return found, nil
}

// Rows streams results and closes them on completion or early break. It yields
// a zero T with the first error. With and WithCount cannot be streamed.
func (q Query[T]) Rows(ctx context.Context, db Queryer, args ...any) iter.Seq2[T, error] {
	execArgs := copyArgs(args)
	return func(yield func(T, error) bool) {
		var zero T
		if len(q.s.withs) > 0 || len(q.s.counts) > 0 {
			yield(zero, errors.New("rio: Rows cannot stream With/WithCount (preloading needs the full result); use All"))
			return
		}
		g := db.gram()
		key := queryCacheKey{grammar: g.weakSelf, op: queryCacheRows}
		p, _, sqlText, bound, err := prepareCachedSelect[T](
			q.cache,
			key,
			g,
			&q.s,
			selectRows,
			execArgs,
		)
		if err != nil {
			yield(zero, err)
			return
		}
		rows, finish, err := runQuery(
			ctx,
			db,
			"select",
			p.structName,
			sqlText,
			bound,
		)
		if err != nil {
			yield(zero, err)
			return
		}
		finished := false
		var yielded int64
		defer func() {
			if !finished {
				_ = finishRows(rows, finish, nil, yielded)
			}
		}()

		fields, err := entityFields(rows, p, 0)
		if err != nil {
			finished = true
			err = finishRows(rows, finish, err, 0)
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
				err = finishRows(rows, finish, err, yielded)
				yield(zero, err)
				return
			}
			yielded++
			if !yield(row, nil) {
				finished = true
				_ = finishRows(rows, finish, nil, yielded)
				return
			}
		}
		err = rows.Err()
		finished = true
		err = finishRows(rows, finish, err, yielded)
		if err != nil {
			yield(zero, err)
		}
	}
}

// Pluck extracts a single column under the query's conditions:
// emails, err := q.Pluck[string](ctx, db, "email", 18). The column must be one
// of T's mapped columns — expressions go through Raw.
func (q Query[T]) Pluck[V any](ctx context.Context, db Queryer, column string, args ...any) ([]V, error) {
	if len(q.s.groups) > 0 || len(q.s.havings) > 0 {
		return nil, errors.New("rio: Pluck with GroupBy/Having is a projection; use Raw")
	}
	g := db.gram()
	key := queryCacheKey{grammar: g.weakSelf, op: queryCachePluck, column: column}
	entry, outArgs, cached, err := q.cache.load(key, g.d, args)
	if err != nil {
		return nil, err
	}
	var p *plan
	var sqlText string
	if cached {
		p, sqlText = entry.plan, entry.sql
	} else {
		var state queryState
		p, state, err = prepareQueryState[T](g.d, &q.s, args)
		if err != nil {
			return nil, err
		}
		f, ok := p.byColumn[column]
		if !ok {
			return nil, fmt.Errorf("rio: Pluck: %s has no column %q (expressions go through Raw)", p.structName, column)
		}
		sqlText, outArgs, err = renderPluck(g, p, f, &state)
		if err != nil {
			return nil, err
		}
		sqlText, outArgs, err = q.cache.store(key, g, p, &q.s, args, sqlText, outArgs)
		if err != nil {
			return nil, err
		}
	}
	rows, finish, err := runQuery(
		ctx,
		db,
		"select",
		p.structName,
		sqlText,
		outArgs,
	)
	if err != nil {
		return nil, err
	}
	out, err := scanScalarsCap[V](rows, 0, queryCapacity(q.s.limit, q.s.limitSet))
	finishQuery(finish, err, int64(len(out)))
	return out, err
}

// Find fetches a row by primary key. Pass composite key parts in struct-field
// declaration order.
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
	keyArgs, err := normalizeArgs(d, key)
	if err != nil {
		return nil, err
	}
	rows, finish, err := runQuery(
		ctx,
		db,
		"select",
		p.structName,
		sqlText,
		keyArgs,
	)
	if err != nil {
		return nil, err
	}
	out, err := scanOne[T](rows, p)
	finishQuery(finish, err, oneIf(err == nil))
	return out, err
}

func (q Query[T]) all(ctx context.Context, db Queryer, op queryCacheOp, args []any) ([]T, error) {
	g := db.gram()
	key := queryCacheKey{grammar: g.weakSelf, op: op}
	p, state, sqlText, bound, err := prepareCachedSelect[T](
		q.cache,
		key,
		g,
		&q.s,
		selectRows,
		args,
	)
	if err != nil {
		return nil, err
	}
	rows, finish, err := runQuery(
		ctx,
		db,
		"select",
		p.structName,
		sqlText,
		bound,
	)
	if err != nil {
		return nil, err
	}
	out, err := scanAllCap[T](rows, p, false, 0, queryCapacity(state.limit, state.limitSet))
	finishQuery(finish, err, int64(len(out)))
	if err != nil {
		return nil, err
	}
	if err := loadQueryRelations(ctx, db, p, out, state.withs, state.counts); err != nil {
		return nil, err
	}
	return out, nil
}

// noteCondArity records an inline-argument mismatch per fragment. Deferred and
// dialect-sensitive fragments are checked at execution time.
func (s *queryState) noteCondArity(clause, expr string, argc int) {
	if argc == 0 || s.err != nil {
		return
	}
	// Avoid the remaining lexer passes on the common, matching path.
	var sqliteCount int
	_, _, _ = rebindCount(sqliteLex, expr, &sqliteCount)
	if sqliteCount == argc {
		return
	}
	var postgresCount, mysqlCount, clickhouseCount int
	_, _, _ = rebindCount(pgLex, expr, &postgresCount)
	_, _, _ = rebindCount(mysqlLex, expr, &mysqlCount)
	_, _, _ = rebindCount(chLex, expr, &clickhouseCount)
	if postgresCount != mysqlCount || mysqlCount != sqliteCount || sqliteCount != clickhouseCount {
		return
	}
	s.err = fmt.Errorf(
		"rio: %s(%q) has %d placeholder(s) but %d argument(s)",
		clause,
		expr,
		postgresCount,
		argc,
	)
}

// copyArgs prevents a caller's variadic slice from aliasing query state.
func copyArgs(args []any) []any {
	if len(args) == 0 {
		return nil
	}
	return append([]any(nil), args...)
}

// appendOne preserves immutability by preventing shared growth capacity.
func appendOne[E any](s []E, e E) []E {
	return append(s[:len(s):len(s)], e)
}

func loadQueryRelations[T any](
	ctx context.Context,
	db Queryer,
	p *plan,
	rows []T,
	withs []preloadSpec,
	counts []string,
) error {
	if err := preloadInto(ctx, db, p, rows, withs); err != nil {
		return err
	}
	remaining, err := countsNotPreloaded(p, rows, withs, counts)
	if err != nil {
		return err
	}
	return countInto(ctx, db, p, rows, remaining)
}

func queryCapacity(limit int, isSet bool) int {
	if isSet && limit > 0 && limit <= 1024 {
		return limit
	}
	return 0
}

func pkColumns(p *plan) string {
	var s strings.Builder
	for i, pk := range p.pks {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(pk.column)
	}
	return s.String()
}

// renderSelect qualifies entity columns so JOINs cannot make them ambiguous.
func renderSelect(g *grammar, p *plan, s *queryState, shape selectShape) (string, []any, error) {
	d := g.d
	if err := checkFinal(d, s); err != nil {
		return "", nil, err
	}
	table := g.table(p)
	b := make([]byte, 0, 192)
	var args []any

	switch shape {
	case selectCount:
		b = append(b, "SELECT count(*) FROM "...)
		b = d.quote(b, table)
	case selectExists:
		b = append(b, "SELECT 1 FROM "...)
		b = d.quote(b, table)
	default:
		head, err := g.cachedSQL(p, "selecthead", 0, 0, upsertCacheKey{}, func() (string, error) {
			hb := make([]byte, 0, 128)
			hb = append(hb, "SELECT "...)
			for i, f := range p.fields {
				if i > 0 {
					hb = append(hb, ", "...)
				}
				hb = d.quote(hb, table)
				hb = append(hb, '.')
				hb = d.quote(hb, f.column)
			}
			hb = append(hb, " FROM "...)
			hb = d.quote(hb, table)
			return string(hb), nil
		})
		if err != nil {
			return "", nil, err
		}
		b = append(b, head...)
	}
	if s.final {
		b = append(b, " FINAL"...) // table modifier: before joins and WHERE
	}

	for _, j := range s.joins {
		b = append(b, ' ')
		b = append(b, j...)
	}

	b, args, err := renderWhere(b, args, g, table, p, s)
	if err != nil {
		return "", nil, err
	}

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
		b, err = appendLimitOffset(b, d, s)
		if err != nil {
			return "", nil, err
		}
	case selectExists:
		// One probe row decides the answer, so any Limit >= 1 collapses to
		// LIMIT 1 — but Limit(0) means "no rows", exactly as it does on All,
		// and Offset shifts which row must exist (paging probes ask "is
		// there a row past this page"), so both render.
		probe := *s
		if !probe.limitSet || probe.limit > 1 {
			probe.limit, probe.limitSet = 1, true
		}
		b, err = appendLimitOffset(b, d, &probe)
		if err != nil {
			return "", nil, err
		}
	}
	// PostgreSQL rejects row locks on aggregate counts.
	if s.forUpdate && shape != selectCount {
		var err error
		if b, err = appendForUpdate(b, d); err != nil {
			return "", nil, err
		}
	}

	return finishSQL(d, b, args)
}

// appendForUpdate renders, elides, or rejects the lock by dialect capability.
func appendForUpdate(b []byte, d Dialect) ([]byte, error) {
	switch d.caps().forUpdate {
	case forUpdateRender:
		return append(b, " FOR UPDATE"...), nil
	case forUpdateReject:
		return nil, unsupportedf(
			"rio: ForUpdate is not supported on %s (no row locks); remove it — reads there are lock-free snapshots",
			d.name(),
		)
	}
	return b, nil // forUpdateElide
}

// checkFinal rejects Final() on dialects without the FINAL table modifier.
func checkFinal(d Dialect, s *queryState) error {
	if s.final && !d.caps().finalTable {
		return unsupportedf(
			"rio: Final() requires a dialect with the FINAL table modifier (clickhouse); remove it on %s",
			d.name(),
		)
	}
	return nil
}

// appendLimitOffset renders LIMIT/OFFSET. PostgreSQL accepts a bare OFFSET;
// MySQL and SQLite require a LIMIT before it, so one is synthesized with the
// dialect's "no limit" spelling.
func appendLimitOffset(b []byte, d Dialect, s *queryState) ([]byte, error) {
	if s.limitSet && s.limit < 0 {
		return nil, fmt.Errorf("rio: Limit requires a non-negative value, got %d", s.limit)
	}
	if s.offsetSet && s.offset < 0 {
		return nil, fmt.Errorf("rio: Offset requires a non-negative value, got %d", s.offset)
	}
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
	return b, nil
}

// renderWhere combines user, relation, and soft-delete predicates.
func renderWhere(
	b []byte,
	args []any,
	g *grammar,
	table string,
	p *plan,
	s *queryState,
) ([]byte, []any, error) {
	if s.err != nil {
		return nil, nil, s.err
	}
	d := g.d
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
	for _, hc := range s.hasConds {
		if p == nil {
			return nil, nil, fmt.Errorf("rio: WhereHas needs an entity query")
		}
		and()
		if hc.isNegated {
			b = append(b, "NOT "...)
		}
		var rq relQuery
		for _, opt := range hc.opts {
			opt(&rq)
		}
		var err error
		b, args, err = renderExists(b, args, g, p, table, hc.path, &rq, 1)
		if err != nil {
			return nil, nil, err
		}
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
	return b, args, nil
}

// renderExists aliases each path level so self-relations cannot collide.
func renderExists(
	b []byte,
	args []any,
	g *grammar,
	owner *plan,
	ownerRef string,
	path string,
	leaf *relQuery,
	depth int,
) ([]byte, []any, error) {
	d := g.d
	head, tail := splitPath(path)
	rel, ok := owner.rels[head]
	if !ok {
		return nil, nil, fmt.Errorf("rio: %s has no relation %q", owner.structName, head)
	}
	res, err := rel.resolve(owner)
	if err != nil {
		return nil, nil, err
	}
	target := res.target
	alias := "rio_h" + strconv.Itoa(depth)

	b = append(b, "EXISTS (SELECT 1 FROM "...)
	joinAlias := ""
	if rel.kind == relManyToMany {
		joinAlias = "rio_j" + strconv.Itoa(depth)
		b = d.quote(b, res.joinTable)
		b = append(b, " AS "...)
		b = d.quote(b, joinAlias)
		b = append(b, " INNER JOIN "...)
		b = d.quote(b, g.table(target))
		b = append(b, " AS "...)
		b = d.quote(b, alias)
		b = append(b, " ON "...)
		b = d.quote(b, joinAlias)
		b = append(b, '.')
		b = d.quote(b, res.joinRef)
		b = append(b, " = "...)
		b = d.quote(b, alias)
		b = append(b, '.')
		b = d.quote(b, res.fk.column)
		b = append(b, " WHERE "...)
		b = d.quote(b, joinAlias)
		b = append(b, '.')
		b = d.quote(b, res.joinFK)
		b = append(b, " = "...)
		b = d.quote(b, ownerRef)
		b = append(b, '.')
		b = d.quote(b, res.ref.column)
	} else {
		b = d.quote(b, g.table(target))
		b = append(b, " AS "...)
		b = d.quote(b, alias)
		b = append(b, " WHERE "...)
		// HasMany/HasOne: child.fk = owner.ref; BelongsTo: target.pk = owner.fk.
		b = d.quote(b, alias)
		b = append(b, '.')
		b = d.quote(b, res.fk.column)
		b = append(b, " = "...)
		b = d.quote(b, ownerRef)
		b = append(b, '.')
		b = d.quote(b, res.ref.column)
	}
	if target.softDel != nil && !(tail == "" && leaf.withTrashed) {
		b = append(b, " AND "...)
		b = d.quote(b, alias)
		b = append(b, '.')
		b = d.quote(b, target.softDel.column)
		b = append(b, " IS NULL"...)
	}
	if tail != "" {
		b = append(b, " AND "...)
		b, args, err = renderExists(b, args, g, target, alias, tail, leaf, depth+1)
		if err != nil {
			return nil, nil, err
		}
	} else {
		for _, w := range leaf.wheres {
			b = append(b, " AND ("...)
			b = append(b, w.expr...)
			b = append(b, ')')
			args = append(args, w.args...)
		}
	}
	return append(b, ')'), args, nil
}

// finishSQL expands slices, rebinds placeholders, checks the bind budget, and
// normalizes arguments. It takes ownership of b.
func finishSQL(d Dialect, b []byte, args []any) (string, []any, error) {
	return finishSQLText(d, byteString(b), args)
}

// finishSQLText is finishSQL for caller-owned SQL strings (Raw, Exec).
func finishSQLText(d Dialect, sqlText string, args []any) (string, []any, error) {
	out, outArgs, err := rebind(d.lexer(), d.style(), sqlText, args)
	if err != nil {
		return "", nil, err
	}
	if err := checkBindCount(d, len(outArgs)); err != nil {
		return "", nil, err
	}
	outArgs, err = normalizeArgs(d, outArgs)
	if err != nil {
		return "", nil, err
	}
	return out, outArgs, nil
}

// checkBindCount enforces the dialect's post-expansion bind budget before
// execution. Internal batch operations chunk automatically; user queries do not.
func checkBindCount(d Dialect, n int) error {
	if limit := d.caps().maxBindParams; n > limit {
		return fmt.Errorf(
			"rio: statement binds %d parameters, over the %s limit of %d; "+
				"chunk the query yourself (preloading via With chunks automatically)",
			n,
			d.name(),
			limit,
		)
	}
	return nil
}

// normalizeArgs applies rio's time and ClickHouse byte encoding rules without
// mutating caller-owned arguments. It also rejects out-of-range DateTime64 values.
func normalizeArgs(d Dialect, args []any) ([]any, error) {
	out := args
	cloned := false
	for i, a := range args {
		var v any
		switch t := a.(type) {
		case time.Time:
			nt := normalizeTime(t)
			if err := checkBindTime(d, nt); err != nil {
				return nil, err
			}
			v = d.bindTime(nt)
		case *time.Time:
			if t == nil {
				v = nil
			} else {
				nt := normalizeTime(*t)
				if err := checkBindTime(d, nt); err != nil {
					return nil, err
				}
				v = d.bindTime(nt)
			}
		case sql.NullTime:
			if !t.Valid {
				v = nil
			} else {
				nt := normalizeTime(t.Time)
				if err := checkBindTime(d, nt); err != nil {
					return nil, err
				}
				v = d.bindTime(nt)
			}
		case sql.Null[time.Time]:
			if !t.Valid {
				v = nil
			} else {
				nt := normalizeTime(t.V)
				if err := checkBindTime(d, nt); err != nil {
					return nil, err
				}
				v = d.bindTime(nt)
			}
		case uint64:
			if t <= math.MaxInt64 {
				continue
			}
			// database/sql rejects uint64 values above MaxInt64.
			v = strconv.FormatUint(t, 10)
		case uint:
			if uint64(t) <= math.MaxInt64 {
				continue
			}
			v = strconv.FormatUint(uint64(t), 10)
		case []byte:
			if !d.caps().bindBytesAsString {
				continue
			}
			if t == nil {
				v = nil
			} else {
				v = string(t)
			}
		default:
			if !d.caps().bindBytesAsString {
				continue
			}
			nv, ok := chByteArg(a)
			if !ok {
				continue
			}
			v = nv
		}
		if !cloned {
			out = append([]any(nil), args...)
			cloned = true
		}
		out[i] = v
	}
	return out, nil
}

func renderPluck(g *grammar, p *plan, f *field, s *queryState) (string, []any, error) {
	d := g.d
	if err := checkFinal(d, s); err != nil {
		return "", nil, err
	}
	table := g.table(p)
	b := make([]byte, 0, 128)
	b = append(b, "SELECT "...)
	b = d.quote(b, table)
	b = append(b, '.')
	b = d.quote(b, f.column)
	b = append(b, " FROM "...)
	b = d.quote(b, table)
	if s.final {
		b = append(b, " FINAL"...)
	}
	for _, j := range s.joins {
		b = append(b, ' ')
		b = append(b, j...)
	}
	var args []any
	var err error
	b, args, err = renderWhere(b, args, g, table, p, s)
	if err != nil {
		return "", nil, err
	}
	if len(s.orders) > 0 {
		b = append(b, " ORDER BY "...)
		for i, order := range s.orders {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = append(b, order...)
		}
	}
	b, err = appendLimitOffset(b, d, s)
	if err != nil {
		return "", nil, err
	}
	if s.forUpdate {
		b, err = appendForUpdate(b, d)
		if err != nil {
			return "", nil, err
		}
	}
	return finishSQL(d, b, args)
}
