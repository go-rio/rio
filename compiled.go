package rio

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"unsafe"
)

// Compiled is a query compiled once and executed many times, in the spirit
// of regexp.MustCompile: declare it at package level, bind parameters at the
// call site. Structural validation runs eagerly; SQL renders lazily per
// grammar (dialect + namer) and is cached, so the hot path only binds.
type Compiled[T any] struct {
	q      Query[T]
	inline bool // frozen constant query: exec args must be empty

	cache sync.Map // *grammar → *compiledSQL | error
}

type compiledSQL struct {
	sql  string
	args []any // inline mode: the frozen bind arguments
	argc int   // exec mode: expected argument count
}

// MustCompile compiles a connection-free query for reuse, panicking on
// structural problems — bad tags, unknown relation paths, mixed inline and
// exec-time parameters. Passing MustCompile does not mean execution cannot
// fail: dialect capabilities, exact placeholder arity, and driver errors
// surface at the execution point. Queries are either fully inline (every ?
// already has its argument — a frozen constant query) or fully
// exec-parameterized (no inline arguments; every ? binds at the call).
// Slice expansion inside IN (?) needs value shapes and is inline-only.
//
// Rendered SQL is cached per DB handle for the Compiled value's lifetime.
// Treat *DB as the long-lived object it is meant to be; churning through
// short-lived handles against package-level Compiled queries accumulates one
// cache entry per handle.
func MustCompile[T any](q Query[T]) *Compiled[T] {
	c, err := Compile(q)
	if err != nil {
		panic(err)
	}
	return c
}

// Compile is MustCompile returning an error.
func Compile[T any](q Query[T]) (*Compiled[T], error) {
	p, err := planOf[T]()
	if err != nil {
		return nil, err
	}
	if err := validatePaths(p, q.s.withs); err != nil {
		return nil, err
	}
	if err := validateHasPaths(p, q.s.hasConds); err != nil {
		return nil, err
	}
	if err := validateCounts(p, q.s.counts); err != nil {
		return nil, err
	}

	if err := checkHasCondArity(p, &q.s); err != nil {
		return nil, err
	}
	inlineArgs := 0
	for _, w := range q.s.wheres {
		inlineArgs += len(w.args)
	}
	for _, h := range q.s.havings {
		inlineArgs += len(h.args)
	}
	holes, exact := approxPlaceholders(&q.s)

	c := &Compiled[T]{q: q}
	switch {
	case inlineArgs == 0 && holes == 0:
		c.inline = true // constant query
	case inlineArgs == 0:
		if hasArgsInHasConds(&q.s) {
			return nil, fmt.Errorf("rio: Compile[%s]: WhereHas arguments are inline and cannot mix with exec-time parameters",
				p.structName)
		}
		c.inline = false
	case exact && holes == inlineArgs:
		c.inline = true
	case exact && hasArgsInHasConds(&q.s):
		return nil, fmt.Errorf("rio: Compile[%s]: WhereHas arguments are inline; compile a fully inline query or run it uncompiled",
			p.structName)
	case exact: // some holes filled, some not
		return nil, fmt.Errorf("rio: Compile[%s]: %d placeholder(s) but %d inline argument(s); a compiled query is either fully inline or fully exec-parameterized",
			p.structName, holes, inlineArgs)
	default:
		// Dialect-sensitive lexing made the count ambiguous; require the
		// unambiguous all-or-nothing shape.
		return nil, fmt.Errorf("rio: Compile[%s]: cannot verify placeholder count independent of dialect; use only exec-time parameters or only inline ones",
			p.structName)
	}
	return c, nil
}

// checkHasCondArity requires every WhereHas condition to carry exactly its
// own arguments at build time. WhereHas conditions live inside the EXISTS
// subquery and always bind inline — a bare ? there cannot be an exec-time
// parameter, and letting it slip through would surface later as a confusing
// renumbering error (inline mode) or diverge between finishers (exec mode).
func checkHasCondArity(p *plan, s *queryState) error {
	holes, args := 0, 0
	ambiguous := false
	for _, hc := range s.hasConds {
		var rq relQuery
		for _, opt := range hc.opts {
			opt(&rq)
		}
		for _, w := range rq.wheres {
			args += len(w.args)
			pg, my, lite := 0, 0, 0
			_, _, _ = rebindCount(pgLex, w.expr, &pg)
			_, _, _ = rebindCount(mysqlLex, w.expr, &my)
			_, _, _ = rebindCount(sqliteLex, w.expr, &lite)
			if pg != my || my != lite {
				ambiguous = true
			}
			holes += pg
		}
	}
	if ambiguous {
		return fmt.Errorf("rio: Compile[%s]: cannot verify WhereHas placeholder count independent of dialect", p.structName)
	}
	if holes != args {
		return fmt.Errorf("rio: Compile[%s]: WhereHas conditions bind inline; %d placeholder(s) need %d argument(s) at build time",
			p.structName, holes, args)
	}
	return nil
}

// hasArgsInHasConds reports whether any WhereHas carries inline arguments.
func hasArgsInHasConds(s *queryState) bool {
	for _, hc := range s.hasConds {
		var rq relQuery
		for _, opt := range hc.opts {
			opt(&rq)
		}
		for _, w := range rq.wheres {
			if len(w.args) > 0 {
				return true
			}
		}
	}
	return false
}

// validateRelSpecs eagerly checks every With path and WithCount name at the
// query entry points, before any row comes back — a typo must fail on every
// execution, not only when the result set happens to be non-empty. Metadata
// only: map lookups, no database work, no allocation on the success path.
func validateRelSpecs(p *plan, s *queryState) error {
	if len(s.withs) == 0 && len(s.counts) == 0 {
		return nil
	}
	if err := validatePaths(p, s.withs); err != nil {
		return err
	}
	return validateCounts(p, s.counts)
}

// validatePaths walks With paths through relation metadata only — no
// database, no key resolution (that stays lazy).
func validatePaths(p *plan, specs []preloadSpec) error {
	for _, s := range specs {
		if err := validateRelationPath(p, s.path); err != nil {
			return err
		}
	}
	return nil
}

func validateHasPaths(p *plan, conds []hasCond) error {
	for _, hc := range conds {
		if err := validateRelationPath(p, hc.path); err != nil {
			return err
		}
	}
	return nil
}

func validateRelationPath(p *plan, path string) error {
	cur := p
	full := path
	for path != "" {
		head, tail := splitPath(path)
		rel, ok := cur.rels[head]
		if !ok {
			return fmt.Errorf("rio: %s has no relation %q (path %q)", cur.structName, head, full)
		}
		next, err := planFor(rel.target)
		if err != nil {
			return err
		}
		cur, path = next, tail
	}
	return nil
}

func validateCounts(p *plan, counts []string) error {
	for _, name := range counts {
		rel, ok := p.rels[name]
		if !ok {
			return fmt.Errorf("rio: %s has no relation %q", p.structName, name)
		}
		if _, ok := p.counts[name]; !ok {
			return fmt.Errorf("rio: %s has no count target for %q; declare a field tagged `rio:\",countof:%s\"`", p.structName, name, name)
		}
		if rel.kind != relHasMany && rel.kind != relManyToMany {
			return fmt.Errorf("rio: WithCount(%q): counting a %s relation is meaningless (0 or 1); load it instead", name, rel.kind)
		}
	}
	return nil
}

// approxPlaceholders counts ? holes in user conditions. When the three
// dialect lexers agree the count is exact; otherwise the caller falls back
// to the conservative rule.
func approxPlaceholders(s *queryState) (n int, exact bool) {
	count := func(p lexProfile) int {
		total := 0
		for _, w := range s.wheres {
			c := 0
			_, _, _ = rebindCount(p, w.expr, &c)
			total += c
		}
		for _, h := range s.havings {
			c := 0
			_, _, _ = rebindCount(p, h.expr, &c)
			total += c
		}
		return total
	}
	pg, my, lite := count(pgLex), count(mysqlLex), count(sqliteLex)
	if pg == my && my == lite {
		return pg, true
	}
	return pg, false
}

// sqlFor renders (or fetches) this query's SQL under the handle's grammar.
func (c *Compiled[T]) sqlFor(db Queryer) (*compiledSQL, error) {
	g := db.gram()
	if v, ok := c.cache.Load(g); ok {
		if cs, ok := v.(*compiledSQL); ok {
			return cs, nil
		}
		return nil, v.(error)
	}
	cs, err := c.render(g)
	if err != nil {
		c.cache.LoadOrStore(g, err)
		return nil, err
	}
	actual, _ := c.cache.LoadOrStore(g, cs)
	if cs, ok := actual.(*compiledSQL); ok {
		return cs, nil
	}
	return nil, actual.(error)
}

func (c *Compiled[T]) render(g *grammar) (*compiledSQL, error) {
	p, err := planOf[T]()
	if err != nil {
		return nil, err
	}
	if c.inline {
		sqlText, args, err := renderSelect(g, p, &c.q.s, selectRows)
		if err != nil {
			return nil, err
		}
		return &compiledSQL{sql: sqlText, args: args}, nil
	}
	// Exec mode: render with placeholder renumbering but no argument
	// knowledge. Arity is verified precisely here and enforced per call.
	raw, _, err := renderSelectRaw(g, p, &c.q.s)
	if err != nil {
		return nil, err
	}
	sqlText, argc, err := rebindTemplate(g.d.lexer(), g.d.style(), raw)
	if err != nil {
		return nil, err
	}
	return &compiledSQL{sql: sqlText, argc: argc}, nil
}

// All executes the compiled query, binding args in placeholder order.
func (c *Compiled[T]) All(ctx context.Context, db Queryer, args ...any) ([]T, error) {
	p, err := planOf[T]()
	if err != nil {
		return nil, err
	}
	cs, err := c.sqlFor(db)
	if err != nil {
		return nil, err
	}
	bound, err := c.bind(db.gram().d, cs, args)
	if err != nil {
		return nil, err
	}
	rows, finish, err := runQuery(ctx, db, "select", p.structName, cs.sql, bound)
	if err != nil {
		return nil, err
	}
	out, err := scanAll[T](rows, p, false)
	finishQuery(finish, err)
	if err != nil {
		return nil, err
	}
	if err := preloadInto(ctx, db, p, out, c.q.s.withs); err != nil {
		return nil, err
	}
	if err := countInto(ctx, db, p, out, c.q.s.counts); err != nil {
		return nil, err
	}
	return out, nil
}

// First executes the compiled query and returns the first row or
// ErrNotFound. Compile with Limit(1) to avoid over-fetching.
func (c *Compiled[T]) First(ctx context.Context, db Queryer, args ...any) (*T, error) {
	rows, err := c.All(ctx, db, args...)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	return &rows[0], nil
}

// Sole returns the single matching row, ErrNotFound, or ErrMultipleRows.
// Like First it runs the compiled SQL as-is: to distinguish one row from many
// it scans whatever the query returns, so compile with Limit(2) to keep it
// from materializing a large result set just to detect duplicates.
func (c *Compiled[T]) Sole(ctx context.Context, db Queryer, args ...any) (*T, error) {
	rows, err := c.All(ctx, db, args...)
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

// Rows streams the compiled query without materializing the result; see
// Query.Rows for semantics. Compiled queries carrying With/WithCount refuse
// to stream.
func (c *Compiled[T]) Rows(ctx context.Context, db Queryer, args ...any) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		if len(c.q.s.withs) > 0 || len(c.q.s.counts) > 0 {
			yield(zero, errors.New("rio: Rows cannot stream With/WithCount (preloading needs the full result); use All"))
			return
		}
		p, err := planOf[T]()
		if err != nil {
			yield(zero, err)
			return
		}
		cs, err := c.sqlFor(db)
		if err != nil {
			yield(zero, err)
			return
		}
		bound, err := c.bind(db.gram().d, cs, args)
		if err != nil {
			yield(zero, err)
			return
		}
		rows, finish, err := runQuery(ctx, db, "select", p.structName, cs.sql, bound)
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

// Count runs the compiled conditions under SELECT count(*). Its shape differs
// from the cached row SELECT, so Count renders on every call — when a count
// sits on a hot path, measure it, and reach for Raw if rendering shows up.
func (c *Compiled[T]) Count(ctx context.Context, db Queryer, args ...any) (int64, error) {
	q := c.q
	if c.inline && len(args) > 0 {
		return 0, fmt.Errorf("rio: compiled query is fully inline; it takes no execution arguments")
	}
	if !c.inline {
		var err error
		q, err = c.rebindInline(db, args)
		if err != nil {
			return 0, err
		}
	}
	return q.Count(ctx, db)
}

// Exists reports whether any row matches. Like Count it renders per call —
// its shape differs from the cached row SELECT.
func (c *Compiled[T]) Exists(ctx context.Context, db Queryer, args ...any) (bool, error) {
	q := c.q
	if c.inline && len(args) > 0 {
		return false, fmt.Errorf("rio: compiled query is fully inline; it takes no execution arguments")
	}
	if !c.inline {
		var err error
		q, err = c.rebindInline(db, args)
		if err != nil {
			return false, err
		}
	}
	return q.Exists(ctx, db)
}

// rebindInline reconstructs a regular query with exec args distributed back
// into the conditions, for the shapes (Count/Exists) that render differently
// from the cached row SELECT. Placeholders fill left to right.
func (c *Compiled[T]) rebindInline(db Queryer, args []any) (Query[T], error) {
	q := c.q
	lex := db.gram().d.lexer()
	idx := 0
	fill := func(conds []cond) ([]cond, error) {
		out := make([]cond, len(conds))
		for i, w := range conds {
			n := 0
			_, _, _ = rebindCount(lex, w.expr, &n)
			if idx+n > len(args) {
				return nil, fmt.Errorf("rio: compiled query expects more arguments: got %d", len(args))
			}
			out[i] = cond{expr: w.expr, args: args[idx : idx+n]}
			idx += n
		}
		return out, nil
	}
	var err error
	if q.s.wheres, err = fill(q.s.wheres); err != nil {
		return q, err
	}
	if q.s.havings, err = fill(q.s.havings); err != nil {
		return q, err
	}
	if idx != len(args) {
		return q, fmt.Errorf("rio: compiled query takes %d argument(s), got %d", idx, len(args))
	}
	return q, nil
}

func (c *Compiled[T]) bind(d Dialect, cs *compiledSQL, args []any) ([]any, error) {
	if c.inline {
		if len(args) != 0 {
			return nil, fmt.Errorf("rio: compiled query is fully inline; it takes no execution arguments")
		}
		return cs.args, nil
	}
	if len(args) != cs.argc {
		return nil, fmt.Errorf("rio: compiled query takes %d argument(s), got %d", cs.argc, len(args))
	}
	for i, a := range args {
		if _, isSlice := sliceElems(a); isSlice {
			return nil, fmt.Errorf("rio: compiled queries cannot expand slice argument %d; IN (?) needs inline values", i+1)
		}
	}
	return normalizeArgs(d, args), nil
}

// renderSelectRaw renders the row-SELECT with unified ? placeholders and no
// rebinding, for exec-mode compilation.
func renderSelectRaw(g *grammar, p *plan, s *queryState) (string, []any, error) {
	d := g.d
	table := g.table(p)
	b := make([]byte, 0, 192)
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
	for _, j := range s.joins {
		b = append(b, ' ')
		b = append(b, j...)
	}
	b, _, err := renderWhere(b, nil, g, table, p, s)
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
	}
	if len(s.orders) > 0 {
		b = append(b, " ORDER BY "...)
		for i, o := range s.orders {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = append(b, o...)
		}
	}
	b, err = appendLimitOffset(b, d, s)
	if err != nil {
		return "", nil, err
	}
	if s.forUpdate && d.caps().forUpdate {
		b = append(b, " FOR UPDATE"...)
	}
	return string(b), nil, nil
}

// rebindTemplate renumbers placeholders without argument knowledge,
// returning the dialect-form SQL and the placeholder count.
func rebindTemplate(p lexProfile, style bindStyle, query string) (string, int, error) {
	// Feed rebind a placeholder-count probe first, then real args of the
	// right length so slice detection stays off.
	n := 0
	if _, _, err := rebindCount(p, query, &n); err != nil {
		return "", 0, err
	}
	dummy := make([]any, n)
	sqlText, _, err := rebind(p, style, query, dummy)
	if err != nil {
		return "", 0, err
	}
	return sqlText, n, nil
}
