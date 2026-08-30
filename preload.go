package rio

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sort"
	"strconv"
)

// resolvedRel is the lazily resolved wiring of one relation.
type resolvedRel struct {
	target *plan

	// fk is the foreign-key field: on the target plan for HasMany/HasOne,
	// on the owner plan for BelongsTo, the target's PK for ManyToMany.
	fk *field
	// ref is the key collected from the owner side: the owner's PK for
	// HasMany/HasOne/ManyToMany, the owner's FK column for BelongsTo.
	ref *field

	joinTable       string // ManyToMany only
	joinFK, joinRef string // join-table columns: owner side, target side
}

// preloadSpec is one With() request: a dot path plus leaf options.
type preloadSpec struct {
	path string
	opts []RelOption
}

// RelOption customizes how one preloaded relation is fetched.
type RelOption func(*relQuery)

type relQuery struct {
	wheres       []cond
	orders       []string
	withTrashed  bool
	limit        int
	limitSet     bool
	changesCount bool
}

// RelWhere restricts the preloaded rows. The condition runs inside the
// preload's own query, so it can only reference the related table's columns.
// The expression is included verbatim; never build it from untrusted input.
func RelWhere(expr string, args ...any) RelOption {
	return func(rq *relQuery) {
		rq.wheres = append(rq.wheres, cond{expr: expr, args: copyArgs(args)})
		rq.changesCount = true
	}
}

// RelOrder orders the preloaded rows before they are grouped per parent.
// The term is included verbatim; never build it from untrusted input.
func RelOrder(expr string) RelOption {
	return func(rq *relQuery) { rq.orders = append(rq.orders, expr) }
}

// RelWithTrashed includes soft-deleted rows in the preload when the target
// model declares a softdelete column.
func RelWithTrashed() RelOption {
	return func(rq *relQuery) {
		rq.withTrashed = true
		rq.changesCount = true
	}
}

// RelLimit caps the preloaded rows per parent, not overall. Order within
// each parent follows RelOrder, defaulting to the target's primary key.
// Requires window functions (PostgreSQL, MySQL 8+, SQLite 3.25+).
// RelLimit(0) loads no children, not all of them.
func RelLimit(n int) RelOption {
	return func(rq *relQuery) {
		rq.limit, rq.limitSet = n, true
		rq.changesCount = true
	}
}

func (r *relField) resolve(owner *plan) (*resolvedRel, error) {
	r.once.Do(func() {
		r.resolved, r.rerr = resolveRel(owner, r)
		if r.rerr != nil {
			r.rerr = fmt.Errorf("rio: relation %s.%s: %w", owner.structName, r.name, r.rerr)
		}
	})
	return r.resolved, r.rerr
}

func resolveRel(owner *plan, r *relField) (*resolvedRel, error) {
	target, err := planFor(r.target)
	if err != nil {
		return nil, err
	}
	res := &resolvedRel{target: target}

	singlePK := func(p *plan, side string) (*field, error) {
		if len(p.pks) == 1 {
			return p.pks[0], nil
		}
		return nil, fmt.Errorf(
			"%s %s needs exactly one primary key column for convention-based relations (has %d); "+
				"set ref: explicitly or restructure",
			side,
			p.structName,
			len(p.pks),
		)
	}
	m2mPK := func(p *plan, side string) (*field, error) {
		if len(p.pks) == 1 {
			return p.pks[0], nil
		}
		return nil, fmt.Errorf(
			"ManyToMany across composite primary keys is not supported in v1 "+
				"(%s %s has %d primary key columns); give it a single-column surrogate key, "+
				"or query the join table by hand",
			side,
			p.structName,
			len(p.pks),
		)
	}

	switch r.kind {
	case relHasMany, relHasOne:
		fkCol := r.fkTag
		if fkCol == "" {
			fkCol = snakeCase(owner.structName) + "_id"
		}
		fk, ok := target.byColumn[fkCol]
		if !ok {
			return nil, fmt.Errorf("%s has no column %q; declare the foreign key or override with fk:", target.structName, fkCol)
		}
		res.fk = fk
		if r.refTag != "" {
			ref, ok := owner.byColumn[r.refTag]
			if !ok {
				return nil, fmt.Errorf("%s has no column %q for ref:", owner.structName, r.refTag)
			}
			res.ref = ref
		} else if res.ref, err = singlePK(owner, "owner"); err != nil {
			return nil, err
		}
	case relBelongsTo:
		fkCol := r.fkTag
		if fkCol == "" {
			fkCol = snakeCase(r.name) + "_id"
		}
		fk, ok := owner.byColumn[fkCol]
		if !ok {
			return nil, fmt.Errorf("%s has no column %q; declare the foreign key or override with fk:", owner.structName, fkCol)
		}
		res.ref = fk // collected from the owner side
		if r.refTag != "" {
			ref, ok := target.byColumn[r.refTag]
			if !ok {
				return nil, fmt.Errorf("%s has no column %q for ref:", target.structName, r.refTag)
			}
			res.fk = ref
		} else if res.fk, err = singlePK(target, "target"); err != nil {
			return nil, err
		}
	case relManyToMany:
		ownPK, err := m2mPK(owner, "owner")
		if err != nil {
			return nil, err
		}
		targetPK, err := m2mPK(target, "target")
		if err != nil {
			return nil, err
		}
		res.ref, res.fk = ownPK, targetPK
		res.joinTable = r.joinTag
		if res.joinTable == "" {
			a, b := snakeCase(owner.structName), snakeCase(target.structName)
			if b < a {
				a, b = b, a
			}
			res.joinTable = a + "_" + b
		}
		// On ManyToMany, fk:/ref: name the join table's owner-side and
		// target-side columns.
		res.joinFK = r.fkTag
		if res.joinFK == "" {
			res.joinFK = snakeCase(owner.structName) + "_id"
		}
		res.joinRef = r.refTag
		if res.joinRef == "" {
			res.joinRef = snakeCase(target.structName) + "_id"
		}
		if res.joinFK == res.joinRef {
			return nil, fmt.Errorf(
				"both join-table columns would be %q; a self-referential ManyToMany "+
					"needs explicit fk: and ref: tags naming the two columns",
				res.joinFK,
			)
		}
	}
	if r.kind != relManyToMany && keyFamily(res.fk.typ) != keyFamily(res.ref.typ) {
		// ManyToMany is exempt: its grouping key is re-scanned from the join
		// table as the owner key's type, never compared across the two PKs.
		return nil, fmt.Errorf(
			"cannot match %s.%s (%s) against %s.%s (%s): the key types never compare equal "+
				"and every preload would silently come back empty; align the Go types "+
				"(integer kinds are interchangeable; string matches []byte) "+
				"or point fk:/ref: at compatible columns",
			owner.structName,
			res.ref.name,
			res.ref.typ,
			target.structName,
			res.fk.name,
			res.fk.typ,
		)
	}
	return res, nil
}

// keyFamily buckets a key type by the canonical form canonKey folds it
// into; keys from different families can never compare equal.
func keyFamily(t reflect.Type) any {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.String:
		return "string"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "string"
		}
	}
	return t
}

// preloadValues loads relation paths into one nested layer's rows, an
// addressable []T value.
func preloadValues(ctx context.Context, db Queryer, p *plan, rows reflect.Value, specs []preloadSpec) error {
	stmts, finishes, err := collectRelationLayer(db, p, rows, specs)
	if err != nil {
		return err
	}
	return runRelLayer(ctx, db, stmts, finishes)
}

// collectRelationLayer renders one layer's preload statements and finishes,
// so the top level can merge them with WithCount's into a single round trip.
func collectRelationLayer(
	db Queryer,
	p *plan,
	rows reflect.Value,
	specs []preloadSpec,
) ([]relStatement, []func(context.Context) error, error) {
	if rows.Len() == 0 || len(specs) == 0 {
		return nil, nil, nil
	}
	type group struct {
		opts  []RelOption
		tails []preloadSpec
	}
	groups := make(map[string]*group, len(specs))
	order := make([]string, 0, len(specs))
	for _, s := range specs {
		head, tail := splitPath(s.path)
		g, ok := groups[head]
		if !ok {
			g = &group{}
			groups[head] = g
			order = append(order, head)
		}
		if tail == "" {
			g.opts = append(g.opts, s.opts...)
		} else {
			g.tails = append(g.tails, preloadSpec{path: tail, opts: s.opts})
		}
	}
	sort.Strings(order) // deterministic query order run to run

	var stmts []relStatement
	finishes := make([]func(context.Context) error, 0, len(order))
	for _, head := range order {
		rel, ok := p.rels[head]
		if !ok {
			return nil, nil, fmt.Errorf("rio: %s has no relation %q", p.structName, head)
		}
		g := groups[head]
		relStmts, finish, err := prepareRelationLoad(db, p, rel, rows, g.opts, g.tails)
		if err != nil {
			return nil, nil, err
		}
		stmts = append(stmts, relStmts...)
		finishes = append(finishes, finish)
	}
	return stmts, finishes, nil
}

func splitPath(path string) (head, tail string) {
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			return path[:i], path[i+1:]
		}
	}
	return path, ""
}

// prepareRelationLoad renders one relation's preload queries plus a finish
// step; finish must run only after every returned statement was consumed.
func prepareRelationLoad(
	db Queryer,
	owner *plan,
	rel *relField,
	owners reflect.Value,
	opts []RelOption,
	tails []preloadSpec,
) ([]relStatement, func(context.Context) error, error) {
	res, err := rel.resolve(owner)
	if err != nil {
		return nil, nil, err
	}
	if rel.kind == relManyToMany && len(res.target.pks) != 1 {
		return nil, nil, fmt.Errorf(
			"rio: relation %s.%s: ManyToMany across composite primary keys is not supported",
			owner.structName,
			rel.name,
		)
	}
	var rq relQuery
	for _, opt := range opts {
		opt(&rq)
	}

	// canonKey groups; the IN (?) binds the original value — a stringified
	// []byte would not match a BLOB/BYTEA column.
	seen := make(map[any]struct{}, owners.Len())
	keys := make([]any, 0, owners.Len())
	parentKey := make([]any, owners.Len())
	for i := 0; i < owners.Len(); i++ {
		kv := owners.Index(i).FieldByIndex(res.ref.index)
		if kv.Kind() == reflect.Pointer {
			if kv.IsNil() {
				parentKey[i] = nil
				continue
			}
			kv = kv.Elem()
		}
		k := canonKey(kv)
		parentKey[i] = k
		if _, dup := seen[k]; !dup {
			seen[k] = struct{}{}
			keys = append(keys, kv.Interface())
		}
	}

	target := res.target
	elemType := target.typ
	// The buffer must be addressable: scanRel grows it in place.
	buf := reflect.New(reflect.SliceOf(elemType)).Elem()
	buf.Set(reflect.MakeSlice(reflect.SliceOf(elemType), 0, len(keys)))
	var bufKeys []any

	var stmts []relStatement
	if len(keys) > 0 {
		relArgs := 0
		for _, w := range rq.wheres {
			for _, a := range w.args {
				if elems, ok := sliceValue(a); ok {
					relArgs += elems.Len() // IN (?) expansion counts per element
				} else {
					relArgs++
				}
			}
		}
		limit := db.gram().d.caps().maxBindParams
		if relArgs >= limit {
			return nil, nil, fmt.Errorf(
				"rio: preload relation %s.%s uses %d bind parameter(s) in RelWhere, "+
					"leaving none for parent keys (dialect limit %d)",
				owner.structName,
				rel.name,
				relArgs,
				limit,
			)
		}
		chunk := limit - relArgs
		for start := 0; start < len(keys); start += chunk {
			end := min(start+chunk, len(keys))
			sqlText, args, keyed, err := renderRelSelect(db.gram(), res, rel.kind, keys[start:end], &rq)
			if err != nil {
				return nil, nil, err
			}
			stmts = append(stmts, relStatement{
				phase:   "preload",
				model:   target.structName,
				sqlText: sqlText,
				args:    args,
				consume: func(sqlRows rows) (int64, error) {
					before := buf.Len()
					part, partKeys, err := scanRel(sqlRows, target, buf, keyed, res)
					if err != nil {
						return int64(part.Len() - before), err
					}
					buf = part
					bufKeys = append(bufKeys, partKeys...)
					return int64(part.Len() - before), nil
				},
			})
		}
	}

	finish := func(ctx context.Context) error {
		// Nested paths load into buf first, so the copies below carry
		// assembled children.
		if len(tails) > 0 && buf.Len() > 0 {
			if err := preloadValues(ctx, db, target, buf, tails); err != nil {
				return err
			}
		}

		type indexSpan struct {
			start int
			next  int
			end   int
		}
		byKey := make(map[any]indexSpan, len(keys))
		if rel.kind == relManyToMany {
			for _, k := range bufKeys {
				span := byKey[k]
				span.end++
				byKey[k] = span
			}
		} else {
			// res.fk is the buffered rows' grouping key for every non-m2m kind.
			keyField := res.fk
			bufKeys = make([]any, buf.Len())
			for i := 0; i < buf.Len(); i++ {
				kv := buf.Index(i).FieldByIndex(keyField.index)
				if kv.Kind() == reflect.Pointer {
					if kv.IsNil() {
						continue // bufKeys[i] stays nil, skipped below
					}
					kv = kv.Elem()
				}
				k := canonKey(kv)
				bufKeys[i] = k
				span := byKey[k]
				span.end++
				byKey[k] = span
			}
		}
		grouped := make([]int, buf.Len())
		offset := 0
		for k, span := range byKey {
			count := span.end
			span.start, span.next, span.end = offset, offset, offset+count
			byKey[k] = span
			offset += count
		}
		for i, k := range bufKeys {
			if k == nil {
				continue // a NULL child-side key groups under no parent
			}
			span := byKey[k]
			grouped[span.next] = i
			span.next++
			byKey[k] = span
		}

		ptrType := reflect.PointerTo(elemType)
		for i := 0; i < owners.Len(); i++ {
			container := owners.Index(i).FieldByIndex(rel.index).Addr().Interface().(relContainer)
			var matches []int
			if parentKey[i] != nil {
				span := byKey[parentKey[i]]
				matches = grouped[span.start:span.end]
			}
			switch rel.kind {
			case relHasMany, relManyToMany:
				out := reflect.MakeSlice(reflect.SliceOf(elemType), len(matches), len(matches))
				for k, idx := range matches {
					out.Index(k).Set(buf.Index(idx))
				}
				container.setLoaded(out)
			case relHasOne, relBelongsTo:
				if len(matches) == 0 {
					container.setLoaded(reflect.Zero(ptrType))
					continue
				}
				if rel.kind == relHasOne && len(matches) > 1 {
					return fmt.Errorf(
						"rio: relation %s.%s: HasOne loaded %d rows for one parent; "+
							"the schema evidently allows several — use HasMany, or make %s.%s unique",
						owner.structName,
						rel.name,
						len(matches),
						target.structName,
						res.fk.column,
					)
				}
				cp := reflect.New(elemType)
				cp.Elem().Set(buf.Index(matches[0]))
				container.setLoaded(cp)
			}
		}
		return nil
	}
	return stmts, finish, nil
}

// renderRelSelect renders the preload query. keyed reports whether an extra
// join-key column is appended after the entity columns (ManyToMany).
func renderRelSelect(
	g *grammar,
	res *resolvedRel,
	kind relKind,
	keys []any,
	rq *relQuery,
) (string, []any, bool, error) {
	if rq.limitSet {
		return renderRelSelectLimited(g, res, kind, keys, rq)
	}
	d := g.d
	target := res.target
	table := g.table(target)
	b := make([]byte, 0, 192)
	var args []any
	keyed := kind == relManyToMany

	b = append(b, "SELECT "...)
	for i, f := range target.fields {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = d.quote(b, table)
		b = append(b, '.')
		b = d.quote(b, f.column)
	}

	switch kind {
	case relManyToMany:
		b = append(b, ", "...)
		b = d.quote(b, res.joinTable)
		b = append(b, '.')
		b = d.quote(b, res.joinFK)
		b = append(b, " FROM "...)
		b = d.quote(b, table)
		b = append(b, " INNER JOIN "...)
		b = d.quote(b, res.joinTable)
		b = append(b, " ON "...)
		b = d.quote(b, res.joinTable)
		b = append(b, '.')
		b = d.quote(b, res.joinRef)
		b = append(b, " = "...)
		b = d.quote(b, table)
		b = append(b, '.')
		b = d.quote(b, res.fk.column)
		b = append(b, " WHERE "...)
		b = d.quote(b, res.joinTable)
		b = append(b, '.')
		b = d.quote(b, res.joinFK)
	default:
		b = append(b, " FROM "...)
		b = d.quote(b, table)
		b = append(b, " WHERE "...)
		b = d.quote(b, table)
		b = append(b, '.')
		b = d.quote(b, res.fk.column)
	}
	b = append(b, " IN (?)"...)
	args = append(args, keys)

	if target.softDel != nil && !rq.withTrashed {
		b = append(b, " AND "...)
		b = d.quote(b, table)
		b = append(b, '.')
		b = d.quote(b, target.softDel.column)
		b = append(b, " IS NULL"...)
	}
	for _, w := range rq.wheres {
		b = append(b, " AND ("...)
		b = append(b, w.expr...)
		b = append(b, ')')
		args = append(args, w.args...)
	}
	for i, o := range rq.orders {
		if i == 0 {
			b = append(b, " ORDER BY "...)
		} else {
			b = append(b, ", "...)
		}
		b = append(b, o...)
	}

	sqlText, outArgs, err := finishSQL(d, b, args)
	return sqlText, outArgs, keyed, err
}

// scanRel appends scanned rows to buf, returning the grown slice and, when
// keyed, one owner key per appended row.
func scanRel(
	rows rows,
	p *plan,
	buf reflect.Value,
	keyed bool,
	res *resolvedRel,
) (out reflect.Value, keys []any, err error) {
	defer mergeClose(rows, &err)
	extra := 0
	if keyed {
		extra = 1
	}
	fields, err := entityFields(rows, p, extra)
	if err != nil {
		return buf, nil, err
	}

	var keyCell colScanner
	var extras []any
	if keyed {
		kf, err := synthField("join key", res.joinFK, res.ref.typ)
		if err != nil {
			return buf, nil, err
		}
		keyCell.f = kf
		extras = []any{&keyCell}
	}

	rs := newRowScanner(fields, extras)
	defer rs.release()
	keyBuf := reflect.New(res.ref.typ)
	for rows.Next() {
		n := buf.Len()
		if n == buf.Cap() {
			buf.Grow(1) // amortized doubling
		}
		buf.SetLen(n + 1)
		elem := buf.Index(n)
		if keyed {
			keyCell.base = keyBuf.UnsafePointer()
		}
		if err := rs.scan(rows, elem.Addr().UnsafePointer()); err != nil {
			return buf, nil, err
		}
		if keyed {
			keys = append(keys, canonKey(keyBuf.Elem()))
		}
	}
	if err := rows.Err(); err != nil {
		return buf, nil, err
	}
	return buf, keys, nil
}

// canonKey normalizes key values into comparable, cross-type-equal map keys:
// integers widen, []byte becomes string, pointers dereference (nil to nil).
func canonKey(v reflect.Value) any {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// Sign-normalize so signed and unsigned keys group together; values
		// above MaxInt64 keep their own key space.
		n := v.Uint()
		if n <= math.MaxInt64 {
			return int64(n)
		}
		return n
	case reflect.String:
		return v.String()
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return string(v.Bytes())
		}
		return v.Interface()
	default:
		return v.Interface()
	}
}

func relOptionsChangeCount(opts []RelOption) bool {
	var rq relQuery
	for _, opt := range opts {
		opt(&rq)
	}
	return rq.changesCount
}

// scanCounts drains (key, count) pairs into the grouping map and reports how
// many it read.
func scanCounts(rows rows, keyType reflect.Type, byKey map[any]int64) (scanned int64, err error) {
	defer mergeClose(rows, &err)
	keyBuf := reflect.New(keyType)
	kf, err := synthField("count key", "<key>", keyType)
	if err != nil {
		return 0, err
	}
	// One escaping box carries cell, count, and dest: a fresh variadic slice
	// would heap-allocate per row (see scanScalars).
	var box struct {
		cell colScanner
		n    int64
		dest [2]any
	}
	box.cell = colScanner{f: kf, base: keyBuf.UnsafePointer()}
	box.dest[0], box.dest[1] = &box.cell, &box.n
	for rows.Next() {
		if err := rows.Scan(box.dest[:]...); err != nil {
			return scanned, err
		}
		byKey[canonKey(keyBuf.Elem())] = box.n
		scanned++
	}
	return scanned, rows.Err()
}

// renderRelSelectLimited wraps the preload in a window subquery so the limit
// applies per parent; the row number never leaves the subquery.
func renderRelSelectLimited(
	g *grammar,
	res *resolvedRel,
	kind relKind,
	keys []any,
	rq *relQuery,
) (string, []any, bool, error) {
	if rq.limit < 0 {
		return "", nil, false, fmt.Errorf("rio: RelLimit requires a non-negative value, got %d", rq.limit)
	}
	d := g.d
	target := res.target
	table := g.table(target)
	b := make([]byte, 0, 256)
	var args []any
	keyed := kind == relManyToMany

	b = append(b, "SELECT "...)
	for i, f := range target.fields {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = d.quote(b, f.column)
	}
	if keyed {
		b = append(b, ", "...)
		b = d.quote(b, "__rio_key")
	}
	b = append(b, " FROM (SELECT "...)
	for i, f := range target.fields {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = d.quote(b, table)
		b = append(b, '.')
		b = d.quote(b, f.column)
	}
	partition := table + "." + res.fk.column
	if keyed {
		b = append(b, ", "...)
		b = d.quote(b, res.joinTable)
		b = append(b, '.')
		b = d.quote(b, res.joinFK)
		b = append(b, " AS "...)
		b = d.quote(b, "__rio_key")
		partition = res.joinTable + "." + res.joinFK
	}
	b = append(b, ", ROW_NUMBER() OVER (PARTITION BY "...)
	b = d.quote(b, partition)
	b = append(b, " ORDER BY "...)
	if len(rq.orders) > 0 {
		for i, o := range rq.orders {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = append(b, o...)
		}
	} else {
		// Deterministic default: the target's primary key.
		pkCol := target.fields[0].column
		if len(target.pks) > 0 {
			pkCol = target.pks[0].column
		}
		b = d.quote(b, table)
		b = append(b, '.')
		b = d.quote(b, pkCol)
	}
	b = append(b, ") AS "...)
	b = d.quote(b, "__rio_rn")
	b = append(b, " FROM "...)
	b = d.quote(b, table)

	switch kind {
	case relManyToMany:
		b = append(b, " INNER JOIN "...)
		b = d.quote(b, res.joinTable)
		b = append(b, " ON "...)
		b = d.quote(b, res.joinTable)
		b = append(b, '.')
		b = d.quote(b, res.joinRef)
		b = append(b, " = "...)
		b = d.quote(b, table)
		b = append(b, '.')
		b = d.quote(b, res.fk.column)
		b = append(b, " WHERE "...)
		b = d.quote(b, res.joinTable)
		b = append(b, '.')
		b = d.quote(b, res.joinFK)
	default:
		b = append(b, " WHERE "...)
		b = d.quote(b, table)
		b = append(b, '.')
		b = d.quote(b, res.fk.column)
	}
	b = append(b, " IN (?)"...)
	args = append(args, keys)

	if target.softDel != nil && !rq.withTrashed {
		b = append(b, " AND "...)
		b = d.quote(b, table)
		b = append(b, '.')
		b = d.quote(b, target.softDel.column)
		b = append(b, " IS NULL"...)
	}
	for _, w := range rq.wheres {
		b = append(b, " AND ("...)
		b = append(b, w.expr...)
		b = append(b, ')')
		args = append(args, w.args...)
	}
	b = append(b, ") AS "...)
	b = d.quote(b, "rio_w")
	b = append(b, " WHERE "...)
	b = d.quote(b, "rio_w")
	b = append(b, '.')
	b = d.quote(b, "__rio_rn")
	b = append(b, " <= "...)
	b = strconv.AppendInt(b, int64(rq.limit), 10)
	// The inner ORDER BY only decides which rows survive; without this outer
	// ORDER BY (partition, then row number) the RelOrder order is lost.
	b = append(b, " ORDER BY "...)
	if keyed {
		b = d.quote(b, "rio_w")
		b = append(b, '.')
		b = d.quote(b, "__rio_key")
	} else {
		b = d.quote(b, "rio_w")
		b = append(b, '.')
		b = d.quote(b, res.fk.column)
	}
	b = append(b, ", "...)
	b = d.quote(b, "rio_w")
	b = append(b, '.')
	b = d.quote(b, "__rio_rn")

	sqlText, outArgs, err := finishSQL(d, b, args)
	return sqlText, outArgs, keyed, err
}

// splitCounts partitions WithCount targets: a relation the same query fully
// preloads reads its count off the loaded containers; the rest query.
func splitCounts(p *plan, specs []preloadSpec, counts []string) (queried, reusable []string) {
	if len(counts) == 0 {
		return nil, nil
	}
	full := make(map[string]bool, len(specs))
	for _, spec := range specs {
		head, tail := splitPath(spec.path)
		if _, seen := full[head]; !seen {
			full[head] = true
		}
		if tail == "" && relOptionsChangeCount(spec.opts) {
			full[head] = false
		}
	}
	for _, name := range counts {
		if slices.Contains(queried, name) || slices.Contains(reusable, name) {
			continue
		}
		rel, ok := p.rels[name]
		_, hasTarget := p.counts[name]
		countable := ok && hasTarget && (rel.kind == relHasMany || rel.kind == relManyToMany)
		if full[name] && countable {
			reusable = append(reusable, name)
		} else {
			queried = append(queried, name)
		}
	}
	return queried, reusable
}

// prepareCountLoads renders every WithCount relation's queries, ready to
// share the preload layer's round trip.
func prepareCountLoads(
	db Queryer,
	p *plan,
	rows reflect.Value,
	counts []string,
) ([]relStatement, []func(context.Context) error, error) {
	if rows.Len() == 0 || len(counts) == 0 {
		return nil, nil, nil
	}
	// counts arrives deduplicated by splitCounts.
	var stmts []relStatement
	var finishes []func(context.Context) error
	for _, name := range counts {
		cs, finish, err := prepareCountLoad(db, p, name, rows)
		if err != nil {
			return nil, nil, err
		}
		stmts = append(stmts, cs...)
		finishes = append(finishes, finish)
	}
	return stmts, finishes, nil
}

// prepareCountLoad renders one WithCount relation's GROUP BY queries plus the
// finish that writes counts back; finish must run only after every returned
// statement was consumed.
func prepareCountLoad(
	db Queryer,
	owner *plan,
	name string,
	owners reflect.Value,
) ([]relStatement, func(context.Context) error, error) {
	rel, ok := owner.rels[name]
	if !ok {
		return nil, nil, fmt.Errorf("rio: %s has no relation %q", owner.structName, name)
	}
	target, ok := owner.counts[name]
	if !ok {
		return nil, nil, fmt.Errorf(
			"rio: %s has no count target for %q; declare a field tagged `rio:\",countof:%s\"`",
			owner.structName,
			name,
			name,
		)
	}
	if rel.kind != relHasMany && rel.kind != relManyToMany {
		return nil, nil, fmt.Errorf(
			"rio: WithCount(%q): counting a %s relation is meaningless (0 or 1); load it instead",
			name,
			rel.kind,
		)
	}
	res, err := rel.resolve(owner)
	if err != nil {
		return nil, nil, err
	}

	// canonKey groups; the IN (?) binds the original value (see prepareRelationLoad).
	seen := make(map[any]struct{}, owners.Len())
	keys := make([]any, 0, owners.Len())
	parentKey := make([]any, owners.Len())
	for i := 0; i < owners.Len(); i++ {
		kv := owners.Index(i).FieldByIndex(res.ref.index)
		if kv.Kind() == reflect.Pointer {
			if kv.IsNil() {
				parentKey[i] = nil
				continue
			}
			kv = kv.Elem()
		}
		k := canonKey(kv)
		parentKey[i] = k
		if _, dup := seen[k]; !dup {
			seen[k] = struct{}{}
			keys = append(keys, kv.Interface())
		}
	}

	g := db.gram()
	d := g.d
	byKey := make(map[any]int64, len(keys))
	var stmts []relStatement
	chunk := d.caps().maxBindParams
	for start := 0; start < len(keys); start += chunk {
		end := min(start+chunk, len(keys))
		b := make([]byte, 0, 160)
		var keyCol string
		b = append(b, "SELECT "...)
		if rel.kind == relManyToMany {
			keyCol = res.joinFK
			b = d.quote(b, res.joinTable)
			b = append(b, '.')
			b = d.quote(b, keyCol)
			b = append(b, ", count(*) FROM "...)
			b = d.quote(b, res.joinTable)
			// INNER JOIN the target exactly as the With load does, so the
			// count matches the rows With would return.
			b = append(b, " INNER JOIN "...)
			b = d.quote(b, g.table(res.target))
			b = append(b, " ON "...)
			b = d.quote(b, g.table(res.target))
			b = append(b, '.')
			b = d.quote(b, res.fk.column)
			b = append(b, " = "...)
			b = d.quote(b, res.joinTable)
			b = append(b, '.')
			b = d.quote(b, res.joinRef)
			if res.target.softDel != nil {
				b = append(b, " AND "...)
				b = d.quote(b, g.table(res.target))
				b = append(b, '.')
				b = d.quote(b, res.target.softDel.column)
				b = append(b, " IS NULL"...)
			}
			b = append(b, " WHERE "...)
			b = d.quote(b, res.joinTable)
			b = append(b, '.')
			b = d.quote(b, keyCol)
		} else {
			keyCol = res.fk.column
			table := g.table(res.target)
			b = d.quote(b, table)
			b = append(b, '.')
			b = d.quote(b, keyCol)
			b = append(b, ", count(*) FROM "...)
			b = d.quote(b, table)
			b = append(b, " WHERE "...)
			b = d.quote(b, table)
			b = append(b, '.')
			b = d.quote(b, keyCol)
		}
		b = append(b, " IN (?)"...)
		args := []any{keys[start:end]}
		if rel.kind != relManyToMany && res.target.softDel != nil {
			b = append(b, " AND "...)
			b = d.quote(b, g.table(res.target))
			b = append(b, '.')
			b = d.quote(b, res.target.softDel.column)
			b = append(b, " IS NULL"...)
		}
		b = append(b, " GROUP BY "...)
		if rel.kind == relManyToMany {
			b = d.quote(b, res.joinTable)
		} else {
			b = d.quote(b, g.table(res.target))
		}
		b = append(b, '.')
		b = d.quote(b, keyCol)

		sqlText, outArgs, err := finishSQL(d, b, args)
		if err != nil {
			return nil, nil, err
		}
		stmts = append(stmts, relStatement{
			phase:   "count",
			model:   res.target.structName,
			sqlText: sqlText,
			args:    outArgs,
			consume: func(sqlRows rows) (int64, error) {
				return scanCounts(sqlRows, res.ref.typ, byKey)
			},
		})
	}

	finish := func(context.Context) error {
		for i := 0; i < owners.Len(); i++ {
			n := byKey[parentKey[i]]
			owners.Index(i).FieldByIndex(target).SetInt(n)
		}
		return nil
	}
	return stmts, finish, nil
}

// reuseCounts fills count targets from containers the preload just loaded.
func reuseCounts(p *plan, rv reflect.Value, reusable []string) error {
	for _, name := range reusable {
		rel := p.rels[name]
		target := p.counts[name]
		for i := 0; i < rv.Len(); i++ {
			container := rv.Index(i).FieldByIndex(rel.index).Addr().Interface().(relContainer)
			n, loaded := container.loadedLen()
			if !loaded {
				return fmt.Errorf("rio: relation %s.%s was not loaded before count reuse", p.structName, name)
			}
			rv.Index(i).FieldByIndex(target).SetInt(int64(n))
		}
	}
	return nil
}
