package rio

import "fmt"

// Validate checks q without accessing a database. Deferred Where and Having
// arguments are checked by the terminal method under its dialect.
func (q Query[T]) Validate() error {
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	return validateQueryState(p, &q.s)
}

// Must panics if Validate fails. The returned query is suitable for reuse:
// it carries a private render cache keyed per executing handle, and a
// handle's entries are reclaimed with the handle — a package-level query
// run against churning short-lived DBs does not grow without bound.
func (q Query[T]) Must() Query[T] {
	if err := q.Validate(); err != nil {
		panic(err)
	}
	q.cache = new(queryCache)
	return q
}

func validateQueryState(p *plan, s *queryState) error {
	if s.err != nil {
		return s.err
	}
	if s.limitSet && s.limit < 0 {
		return fmt.Errorf("rio: Limit requires a non-negative value, got %d", s.limit)
	}
	if s.offsetSet && s.offset < 0 {
		return fmt.Errorf("rio: Offset requires a non-negative value, got %d", s.offset)
	}
	if err := checkNoArgClauses(p, s); err != nil {
		return err
	}
	if len(s.orderKeys) > 0 || s.after != nil {
		if _, err := resolveSortKeys(p, s); err != nil {
			return err
		}
	}
	if err := validatePaths(p, s.withs); err != nil {
		return err
	}
	if err := validateHasPaths(p, s.hasConds); err != nil {
		return err
	}
	if err := validateCounts(p, s.counts); err != nil {
		return err
	}
	return validateRelOptions(p, s)
}

// checkNoArgClauses rejects a placeholder recognized by any supported lexer.
func checkNoArgClauses(p *plan, s *queryState) error {
	check := func(clause, expr string) error {
		holes := maxPlaceholderCount(expr)
		if holes == 0 {
			return nil
		}
		return fmt.Errorf(
			"rio: Validate[%s]: %s(%q) contains %d placeholder(s), but %s has no argument channel; "+
				"put parameterized conditions in Where/Having or inline the value",
			p.structName,
			clause,
			expr,
			holes,
			clause,
		)
	}
	for _, j := range s.joins {
		if err := check("Join", j); err != nil {
			return err
		}
	}
	for _, g := range s.groups {
		if err := check("GroupBy", g); err != nil {
			return err
		}
	}
	for _, o := range s.orders {
		if err := check("OrderBy", o); err != nil {
			return err
		}
	}
	return nil
}

func maxPlaceholderCount(expr string) int {
	holes := 0
	for _, lex := range [...]lexProfile{pgLex, mysqlLex, sqliteLex, chLex} {
		n := 0
		_, _, _ = rebindCount(lex, expr, &n)
		holes = max(holes, n)
	}
	return holes
}

func validateRelOptions(p *plan, s *queryState) error {
	for _, spec := range s.withs {
		if err := validateRelOptionSet(p, "With", spec.path, spec.opts); err != nil {
			return err
		}
	}
	for _, hc := range s.hasConds {
		clause := "WhereHas"
		if hc.isNegated {
			clause = "WhereHasNot"
		}
		if err := validateRelOptionSet(p, clause, hc.path, hc.opts); err != nil {
			return err
		}
	}
	return nil
}

// Relation options require inline arguments because they execute separately
// or inside a nested query with explicit argument order.
func validateRelOptionSet(p *plan, clause, path string, opts []RelOption) error {
	var rq relQuery
	for _, opt := range opts {
		opt(&rq)
	}
	for _, w := range rq.wheres {
		pg, my, sqlite, clickhouse := placeholderCounts(w.expr)
		if pg != my || my != sqlite || sqlite != clickhouse {
			return fmt.Errorf(
				"rio: Validate[%s]: %s(%q) cannot verify RelWhere(%q) placeholder count independent of dialect",
				p.structName,
				clause,
				path,
				w.expr,
			)
		}
		if pg != len(w.args) {
			return fmt.Errorf(
				"rio: Validate[%s]: %s(%q) RelWhere(%q) must bind inline; "+
					"%d placeholder(s) but %d argument(s) were provided at build time",
				p.structName,
				clause,
				path,
				w.expr,
				pg,
				len(w.args),
			)
		}
	}
	for _, order := range rq.orders {
		if holes := maxPlaceholderCount(order); holes > 0 {
			return fmt.Errorf(
				"rio: Validate[%s]: %s(%q) RelOrder(%q) contains %d placeholder(s), but RelOrder has no argument channel",
				p.structName,
				clause,
				path,
				order,
				holes,
			)
		}
	}
	if rq.limitSet && rq.limit < 0 {
		return fmt.Errorf("rio: Validate[%s]: %s(%q) RelLimit requires a non-negative value, got %d",
			p.structName, clause, path, rq.limit)
	}
	return nil
}

func placeholderCounts(expr string) (pg, mysql, sqlite, clickhouse int) {
	_, _, _ = rebindCount(pgLex, expr, &pg)
	_, _, _ = rebindCount(mysqlLex, expr, &mysql)
	_, _, _ = rebindCount(sqliteLex, expr, &sqlite)
	_, _, _ = rebindCount(chLex, expr, &clickhouse)
	return pg, mysql, sqlite, clickhouse
}

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
	if path == "" {
		return fmt.Errorf("rio: %s relation path is empty", p.structName)
	}
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
			return fmt.Errorf(
				"rio: %s has no count target for %q; declare a field tagged `rio:\",countof:%s\"`",
				p.structName,
				name,
				name,
			)
		}
		if rel.kind != relHasMany && rel.kind != relManyToMany {
			return fmt.Errorf(
				"rio: WithCount(%q): counting a %s relation is meaningless (0 or 1); load it instead",
				name,
				rel.kind,
			)
		}
	}
	return nil
}
