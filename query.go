package rio

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"math"
	"strconv"
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

// queryState is the non-generic body of Query[T]; renderers and the
// preloader work on it without type parameters.
type queryState struct {
	wheres  []cond
	havings []cond
	joins   []string
	orders  []string
	groups  []string

	// err is the first structural error a builder call detected (a condition
	// fragment whose own placeholder count contradicts its own arguments).
	// Builders cannot return errors, so it surfaces at the render entry
	// points and at Compile — before any SQL is sent.
	err error

	limit, offset       int
	limitSet, offsetSet bool

	forUpdate bool
	trashed   trashMode
	allRows   bool

	withs    []preloadSpec
	hasConds []hasCond
	counts   []string
}

// hasCond is one WhereHas/WhereHasNot: filter parents by the existence of
// related rows, rendered as an EXISTS subquery — no row explosion, and the
// same shape on all three dialects.
type hasCond struct {
	path string
	not  bool
	opts []RelOption
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
	q.s.noteCondArity("Where", expr, len(args))
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
	q.s.noteCondArity("Having", expr, len(args))
	return q
}

// noteCondArity checks one condition fragment's placeholder count against
// its own arguments at build time. The statement-level total in rebind cannot
// see fragment boundaries: complementary mismatches across fragments
// (Where("name = ?", "alice", 30).Where("age = ?")) balance the total and
// silently shift bindings, so each fragment carrying arguments must account
// for itself. A slice argument counts as one — it expands inside IN (?) at
// render. Fragments with no arguments stay legal: that is the compiled
// exec-parameterized shape, and uncompiled they still fail loudly at the
// statement level. When the three dialect lexers disagree on the count the
// fragment is dialect-sensitive and the decision defers to render, which
// knows the real lexer.
func (s *queryState) noteCondArity(clause, expr string, argc int) {
	if argc == 0 || s.err != nil {
		return
	}
	// One lexer pass decides the happy path: when the sqlite count matches,
	// the full three-lexer comparison could only end in agreement (pass) or
	// disagreement (defer) — never an error — so the other two passes are
	// skipped.
	lite := 0
	_, _, _ = rebindCount(sqliteLex, expr, &lite)
	if lite == argc {
		return
	}
	pg, my := 0, 0
	_, _, _ = rebindCount(pgLex, expr, &pg)
	_, _, _ = rebindCount(mysqlLex, expr, &my)
	if pg != my || my != lite {
		return
	}
	s.err = fmt.Errorf("rio: %s(%q) has %d placeholder(s) but %d argument(s)", clause, expr, pg, argc)
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

// Scope applies reusable query functions in order, keeping the chain
// readable — a scope is just func(Query[T]) Query[T], no registry and no
// magic:
//
//	func Active(q rio.Query[User]) rio.Query[User] { return q.Where("active") }
//	users, err := rio.From[User]().Scope(Active, Recent).All(ctx, db)
func (q Query[T]) Scope(fns ...func(Query[T]) Query[T]) Query[T] {
	for _, fn := range fns {
		q = fn(q)
	}
	return q
}

// WhereHas keeps only rows whose relation at path has at least one matching
// row, rendered as EXISTS (...). Options constrain the related rows; nested
// paths ("Posts.Comments") nest the EXISTS. Related soft-delete models are
// filtered like preloading; RelWithTrashed applies to the leaf.
func (q Query[T]) WhereHas(path string, opts ...RelOption) Query[T] {
	q.s.hasConds = appendOne(q.s.hasConds, hasCond{path: path, opts: opts})
	return q
}

// WhereHasNot keeps only rows whose relation at path has no matching row —
// NOT EXISTS (...).
func (q Query[T]) WhereHasNot(path string, opts ...RelOption) Query[T] {
	q.s.hasConds = appendOne(q.s.hasConds, hasCond{path: path, not: true, opts: opts})
	return q
}

// WithCount fills the relation's count target field — declared as
// `PostsCount int64 \`rio:",countof:Posts"\“ — with one GROUP BY query per
// relation, the aggregate sibling of With. HasMany and ManyToMany only.
func (q Query[T]) WithCount(relation string) Query[T] {
	q.s.counts = appendOne(q.s.counts, relation)
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
	if err := validateRelSpecs(p, &q.s); err != nil {
		return nil, err
	}
	sqlText, args, err := renderSelect(db.gram(), p, &q.s, selectRows)
	if err != nil {
		return nil, err
	}
	rows, finish, err := runQuery(ctx, db, "select", p.structName, sqlText, args)
	if err != nil {
		return nil, err
	}
	out, err := scanAll[T](rows, p, false)
	finishQuery(finish, err)
	if err != nil {
		return nil, err
	}
	if err := preloadInto(ctx, db, p, out, q.s.withs); err != nil {
		return nil, err
	}
	if err := countInto(ctx, db, p, out, q.s.counts); err != nil {
		return nil, err
	}
	return out, nil
}

// First returns the first matching row, or ErrNotFound. No implicit ORDER BY
// is added: like SQL itself, order is undefined unless you ask for one.
// Without a caller Limit, First fetches with LIMIT 1; an explicit Limit is
// respected — Limit(0).First matches no rows and returns ErrNotFound, just
// like All returning none.
func (q Query[T]) First(ctx context.Context, db Queryer) (*T, error) {
	p, err := planOf[T]()
	if err != nil {
		return nil, err
	}
	// Before the query: a miss returns ErrNotFound without reaching the
	// preloader, which would otherwise hide a misspelled With until a row
	// happened to match.
	if err := validateRelSpecs(p, &q.s); err != nil {
		return nil, err
	}
	one := q
	if !one.s.limitSet {
		one = one.Limit(1)
	}
	sqlText, args, err := renderSelect(db.gram(), p, &one.s, selectRows)
	if err != nil {
		return nil, err
	}
	rows, finish, err := runQuery(ctx, db, "select", p.structName, sqlText, args)
	if err != nil {
		return nil, err
	}
	out, err := scanOne[T](rows, p)
	finishQuery(finish, err)
	if err != nil {
		return nil, err
	}
	if len(q.s.withs) > 0 || len(q.s.counts) > 0 {
		single := []T{*out}
		if err := preloadInto(ctx, db, p, single, q.s.withs); err != nil {
			return nil, err
		}
		if err := countInto(ctx, db, p, single, q.s.counts); err != nil {
			return nil, err
		}
		*out = single[0]
	}
	return out, nil
}

// Sole returns the single matching row; ErrNotFound when none match and
// ErrMultipleRows when more than one does. Without a caller Limit it probes
// with LIMIT 2; an explicit Limit is respected (like Compiled.Sole running
// the compiled SQL as-is), so Limit(1) cannot detect a second row.
func (q Query[T]) Sole(ctx context.Context, db Queryer) (*T, error) {
	probe := q
	if !probe.s.limitSet {
		probe = probe.Limit(2)
	}
	rows, err := probe.All(ctx, db)
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
// GroupBy or Having the intent (rows vs groups) is ambiguous, and a bare
// HAVING would filter the single implicit aggregate group and silently
// return 0 — both route through Raw instead.
func (q Query[T]) Count(ctx context.Context, db Queryer) (int64, error) {
	if len(q.s.groups) > 0 || len(q.s.havings) > 0 {
		return 0, errors.New("rio: Count with GroupBy/Having is a projection (rows or groups?); use Raw")
	}
	p, err := planOf[T]()
	if err != nil {
		return 0, err
	}
	sqlText, args, err := renderSelect(db.gram(), p, &q.s, selectCount)
	if err != nil {
		return 0, err
	}
	rows, finish, err := runQuery(ctx, db, "select", p.structName, sqlText, args)
	if err != nil {
		return 0, err
	}
	ns, err := scanScalars[int64](rows)
	finishQuery(finish, err)
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
	rows, finish, err := runQuery(ctx, db, "select", p.structName, sqlText, args)
	if err != nil {
		return false, err
	}
	ns, err := scanScalars[int64](rows)
	finishQuery(finish, err)
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
	rows, finish, err := runQuery(ctx, db, "select", p.structName, sqlText, normalizeArgs(d, key))
	if err != nil {
		return nil, err
	}
	out, err := scanOne[T](rows, p)
	finishQuery(finish, err)
	return out, err
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

	switch shape {
	case selectCount:
		b = append(b, "SELECT count(*) FROM "...)
		b = d.quote(b, table)
	case selectExists:
		b = append(b, "SELECT 1 FROM "...)
		b = d.quote(b, table)
	default:
		// The qualified column list never changes per plan and grammar;
		// render it once.
		head, err := g.cachedSQL(p, "selecthead", 0, 0, func() (string, error) {
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
		// Existence needs exactly one probe row; user LIMIT/OFFSET would be
		// meaningless here and doubling LIMIT clauses is invalid SQL.
		b = append(b, " LIMIT 1"...)
	}
	// FOR UPDATE never reaches the count shape: PostgreSQL rejects row locks
	// on aggregates, and counting locks nothing meaningful anyway. Exists
	// keeps it — locking the probe row is well-defined.
	if s.forUpdate && d.caps().forUpdate && shape != selectCount {
		b = append(b, " FOR UPDATE"...)
	}

	return finishSQL(d, b, args)
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

// renderWhere renders user conditions, EXISTS relation filters, and the
// soft-delete filter. Every statement that carries user conditions renders
// through here, so this is where a builder-recorded fragment error surfaces.
func renderWhere(b []byte, args []any, g *grammar, table string, p *plan, s *queryState) ([]byte, []any, error) {
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
		if hc.not {
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

// renderExists renders one EXISTS(...) level for a relation path. The
// related table always gets a depth-numbered alias so self-referencing
// relations never collide with the outer table, and rendering stays uniform.
func renderExists(b []byte, args []any, g *grammar, owner *plan, ownerRef string, path string, leaf *relQuery, depth int) ([]byte, []any, error) {
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

// finishSQL runs the rebind pipeline: IN expansion first, placeholder
// renumbering second, in one lexer pass — then normalizes arguments.
//
// finishSQL takes ownership of b: the returned SQL may alias its memory
// (rebind returns its input unchanged when nothing rewrites), so the caller
// must not read or append to b afterwards. Every call site builds b locally
// and drops it at this call — keep it that way.
func finishSQL(d Dialect, b []byte, args []any) (string, []any, error) {
	sqlText, outArgs, err := rebind(d.lexer(), d.style(), byteString(b), args)
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
		case sql.NullTime:
			// Left alone, the driver's Valuer path would encode the inner
			// time in its own format, missing rio's stored SQLite text.
			if !t.Valid {
				v = nil
			} else {
				v = d.bindTime(normalizeTime(t.Time))
			}
		case sql.Null[time.Time]:
			if !t.Valid {
				v = nil
			} else {
				v = d.bindTime(normalizeTime(t.V))
			}
		case uint64:
			if t <= math.MaxInt64 {
				continue
			}
			// database/sql refuses uint64 with the high bit set; the
			// decimal literal binds fine everywhere (mirrors bindArg).
			v = strconv.FormatUint(t, 10)
		case uint:
			if uint64(t) <= math.MaxInt64 {
				continue
			}
			v = strconv.FormatUint(uint64(t), 10)
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

// Rows streams the result without materializing it, for result sets too
// large to hold: for u, err := range q.Rows(ctx, db). Iteration stops on the
// first error (yielded with a zero T) and closing happens automatically,
// including on early break. Preloading needs the full set and cannot stream
// — With/WithCount on a streamed query yields an error immediately.
func (q Query[T]) Rows(ctx context.Context, db Queryer) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		if len(q.s.withs) > 0 || len(q.s.counts) > 0 {
			yield(zero, errors.New("rio: Rows cannot stream With/WithCount (preloading needs the full result); use All"))
			return
		}
		p, err := planOf[T]()
		if err != nil {
			yield(zero, err)
			return
		}
		sqlText, args, err := renderSelect(db.gram(), p, &q.s, selectRows)
		if err != nil {
			yield(zero, err)
			return
		}
		rows, finish, err := runQuery(ctx, db, "select", p.structName, sqlText, args)
		if err != nil {
			yield(zero, err)
			return
		}
		defer rows.Close()

		fields, err := entityFields(rows, p, 0)
		if err != nil {
			finishQuery(finish, err)
			yield(zero, err)
			return
		}
		rs := newRowScanner(fields, nil)
		for rows.Next() {
			var row T
			if err := rs.scan(rows, unsafe.Pointer(&row)); err != nil {
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
		finishQuery(finish, err)
		if err != nil {
			yield(zero, err)
		}
	}
}

// Pluck extracts a single column under the query's conditions:
// emails, err := rio.Pluck[string](ctx, db, q, "email"). The column must be
// one of T's mapped columns — expressions go through Raw.
func Pluck[V any, T any](ctx context.Context, db Queryer, q Query[T], column string) ([]V, error) {
	if len(q.s.groups) > 0 || len(q.s.havings) > 0 {
		return nil, errors.New("rio: Pluck with GroupBy/Having is a projection; use Raw")
	}
	p, err := planOf[T]()
	if err != nil {
		return nil, err
	}
	f, ok := p.byColumn[column]
	if !ok {
		return nil, fmt.Errorf("rio: Pluck: %s has no column %q (expressions go through Raw)", p.structName, column)
	}
	g := db.gram()
	d := g.d
	table := g.table(p)

	b := make([]byte, 0, 128)
	b = append(b, "SELECT "...)
	b = d.quote(b, table)
	b = append(b, '.')
	b = d.quote(b, f.column)
	b = append(b, " FROM "...)
	b = d.quote(b, table)
	for _, j := range q.s.joins {
		b = append(b, ' ')
		b = append(b, j...)
	}
	var args []any
	b, args, err = renderWhere(b, args, g, table, p, &q.s)
	if err != nil {
		return nil, err
	}
	if len(q.s.orders) > 0 {
		b = append(b, " ORDER BY "...)
		for i, o := range q.s.orders {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = append(b, o...)
		}
	}
	b, err = appendLimitOffset(b, d, &q.s)
	if err != nil {
		return nil, err
	}
	if q.s.forUpdate && d.caps().forUpdate {
		b = append(b, " FOR UPDATE"...)
	}

	sqlText, outArgs, err := finishSQL(d, b, args)
	if err != nil {
		return nil, err
	}
	rows, finish, err := runQuery(ctx, db, "select", p.structName, sqlText, outArgs)
	if err != nil {
		return nil, err
	}
	out, err := scanScalars[V](rows)
	finishQuery(finish, err)
	return out, err
}
