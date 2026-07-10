package rio

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"unsafe"
)

// resolvedRel is the lazily computed wiring of one relation. Resolution
// happens on first use, never at plan build time: mutually referencing
// models (User ↔ Post) would otherwise recurse forever.
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

	// singlePK serves the direct-FK kinds, where ref: names the key column
	// and is real advice. ManyToMany goes through m2mPK: there ref: names a
	// join-table column and cannot substitute for a composite key.
	singlePK := func(p *plan, side string) (*field, error) {
		if len(p.pks) == 1 {
			return p.pks[0], nil
		}
		return nil, fmt.Errorf("%s %s needs exactly one primary key column for convention-based relations (has %d); set ref: explicitly or restructure",
			side, p.structName, len(p.pks))
	}
	m2mPK := func(p *plan, side string) (*field, error) {
		if len(p.pks) == 1 {
			return p.pks[0], nil
		}
		return nil, fmt.Errorf("ManyToMany across composite primary keys is not supported in v1 (%s %s has %d primary key columns); give it a single-column surrogate key, or query the join table by hand",
			side, p.structName, len(p.pks))
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
		// On ManyToMany, fk:/ref: name the join table's two columns: fk is
		// the owner side, ref the target side.
		res.joinFK = r.fkTag
		if res.joinFK == "" {
			res.joinFK = snakeCase(owner.structName) + "_id"
		}
		res.joinRef = r.refTag
		if res.joinRef == "" {
			res.joinRef = snakeCase(target.structName) + "_id"
		}
		if res.joinFK == res.joinRef {
			// Self-referential m2m (friends, follows): the convention would
			// name both join columns identically — physically impossible.
			return nil, fmt.Errorf("both join-table columns would be %q; a self-referential ManyToMany needs explicit fk: and ref: tags naming the two columns", res.joinFK)
		}
	}
	if r.kind != relManyToMany && keyFamily(res.fk.typ) != keyFamily(res.ref.typ) {
		// The assembly loop pairs owner-side and child-side keys through
		// canonKey; keys from different families can never compare equal, so
		// every row the IN query returns would silently assemble empty.
		// ManyToMany is exempt: its grouping key is re-scanned from the join
		// table as the owner key's type, never compared across the two PKs.
		return nil, fmt.Errorf("cannot match %s.%s (%s) against %s.%s (%s): the key types never compare equal and every preload would silently come back empty; align the Go types (integer kinds are interchangeable; string matches []byte) or point fk:/ref: at compatible columns",
			owner.structName, res.ref.name, res.ref.typ, target.structName, res.fk.name, res.fk.typ)
	}
	return res, nil
}

// keyFamily buckets a key type by the canonical form canonKey folds it into:
// every integer kind shares one family, string and []byte share another, and
// any other type groups only with itself. Pointers bucket by their element.
// resolveRel refuses FK/ref pairs from different families — their canonical
// keys can never be equal.
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

// preloadSpec is one With() request: a dot path plus options for the leaf.
type preloadSpec struct {
	path string
	opts []RelOption
}

// RelOption customizes how one preloaded relation is fetched.
type RelOption func(*relQuery)

type relQuery struct {
	wheres      []cond
	orders      []string
	withTrashed bool
	limit       int
	limitSet    bool
}

// RelWhere restricts the preloaded rows. The condition runs inside the
// preload's own query, so it can only reference the related table's columns.
func RelWhere(expr string, args ...any) RelOption {
	return func(rq *relQuery) {
		rq.wheres = append(rq.wheres, cond{expr: expr, args: copyArgs(args)})
	}
}

// RelOrder orders the preloaded rows before they are grouped per parent.
func RelOrder(expr string) RelOption {
	return func(rq *relQuery) { rq.orders = append(rq.orders, expr) }
}

// RelWithTrashed includes soft-deleted rows in the preload when the target
// model declares a softdelete column.
func RelWithTrashed() RelOption {
	return func(rq *relQuery) { rq.withTrashed = true }
}

// RelLimit caps the preloaded rows per parent (not overall), via
// ROW_NUMBER() OVER (PARTITION BY the foreign key) — the query stays one
// round trip and pagination-correct. Order within each parent follows
// RelOrder, defaulting to the target's primary key for determinism.
// Requires window functions: PostgreSQL, MySQL 8+, SQLite 3.25+. RelLimit(0)
// loads no children per parent (like Query.Limit(0)), not all of them.
func RelLimit(n int) RelOption {
	return func(rq *relQuery) { rq.limit, rq.limitSet = n, true }
}

// preloadInto loads relation paths into rows of one plan.
func preloadInto[T any](ctx context.Context, db Queryer, p *plan, rows []T, specs []preloadSpec) error {
	if len(rows) == 0 || len(specs) == 0 {
		return nil
	}
	return preloadValues(ctx, db, p, reflect.ValueOf(rows), specs)
}

// preloadValues is the reflection core shared by the generic entry point and
// nested recursion. rows is a []T value; slice elements are addressable.
func preloadValues(ctx context.Context, db Queryer, p *plan, rows reflect.Value, specs []preloadSpec) error {
	if rows.Len() == 0 || len(specs) == 0 {
		return nil
	}
	type group struct {
		opts  []RelOption
		tails []preloadSpec
	}
	groups := make(map[string]*group)
	var order []string
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

	for _, head := range order {
		rel, ok := p.rels[head]
		if !ok {
			return fmt.Errorf("rio: %s has no relation %q", p.structName, head)
		}
		g := groups[head]
		if err := loadRelation(ctx, db, p, rel, rows, g.opts, g.tails); err != nil {
			return err
		}
	}
	return nil
}

func splitPath(path string) (head, tail string) {
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			return path[:i], path[i+1:]
		}
	}
	return path, ""
}

func loadRelation(ctx context.Context, db Queryer, owner *plan, rel *relField, rows reflect.Value, opts []RelOption, tails []preloadSpec) error {
	res, err := rel.resolve(owner)
	if err != nil {
		return err
	}
	if rel.kind == relManyToMany && len(res.target.pks) != 1 {
		return fmt.Errorf("rio: relation %s.%s: ManyToMany across composite primary keys is not supported", owner.structName, rel.name)
	}
	var rq relQuery
	for _, opt := range opts {
		opt(&rq)
	}

	// Collect owner-side keys, deduplicated. A nil pointer key (NULL FK on
	// BelongsTo) resolves that parent to loaded-nil without querying.
	// canonKey groups (it stringifies []byte, which is not a comparable map
	// key); the child IN (?) binds the *original* value — a stringified
	// []byte would not match a BLOB/BYTEA column, silently loading nothing.
	seen := make(map[any]struct{})
	var keys []any
	parentKey := make([]any, rows.Len())
	for i := 0; i < rows.Len(); i++ {
		kv := rows.Index(i).FieldByIndex(res.ref.index)
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
	buf := reflect.MakeSlice(reflect.SliceOf(elemType), 0, len(keys))
	var bufKeys []any

	if len(keys) > 0 {
		relArgs := 0
		for _, w := range rq.wheres {
			for _, a := range w.args {
				if elems, ok := sliceElems(a); ok {
					relArgs += len(elems) // IN (?) expansion counts per element
				} else {
					relArgs++
				}
			}
		}
		limit := db.gram().d.caps().maxBindParams
		if relArgs >= limit {
			return fmt.Errorf("rio: preload relation %s.%s uses %d bind parameter(s) in RelWhere, leaving none for parent keys (dialect limit %d)",
				owner.structName, rel.name, relArgs, limit)
		}
		chunk := limit - relArgs
		for start := 0; start < len(keys); start += chunk {
			end := min(start+chunk, len(keys))
			sqlText, args, keyed, err := renderRelSelect(db.gram(), res, rel.kind, keys[start:end], &rq)
			if err != nil {
				return err
			}
			sqlRows, finish, err := runQuery(ctx, db, "select", target.structName, sqlText, args)
			if err != nil {
				return err
			}
			part, partKeys, err := scanRel(sqlRows, target, buf, keyed, res)
			finishQuery(finish, err)
			if err != nil {
				return err
			}
			buf = part
			bufKeys = append(bufKeys, partKeys...)
		}
	}

	// Nested paths load into the scan buffer first; the copy into parent
	// containers below then carries the fully assembled children.
	if len(tails) > 0 && buf.Len() > 0 {
		if err := preloadValues(ctx, db, target, buf, tails); err != nil {
			return err
		}
	}

	// Group buffered children by owner key.
	byKey := make(map[any][]int, buf.Len())
	if rel.kind == relManyToMany {
		for i, k := range bufKeys {
			byKey[k] = append(byKey[k], i)
		}
	} else {
		// res.fk is the child-side column for HasMany/HasOne and the
		// target-side referenced column for BelongsTo — either way, the
		// grouping key on the buffered rows.
		keyField := res.fk
		for i := 0; i < buf.Len(); i++ {
			kv := buf.Index(i).FieldByIndex(keyField.index)
			if kv.Kind() == reflect.Pointer {
				if kv.IsNil() {
					continue
				}
				kv = kv.Elem()
			}
			k := canonKey(kv)
			byKey[k] = append(byKey[k], i)
		}
	}

	ptrType := reflect.PointerTo(elemType)
	for i := 0; i < rows.Len(); i++ {
		container := rows.Index(i).FieldByIndex(rel.index).Addr().Interface().(relContainer)
		matches := []int(nil)
		if parentKey[i] != nil {
			matches = byKey[parentKey[i]]
		}
		switch rel.kind {
		case relHasMany, relManyToMany:
			out := reflect.MakeSlice(reflect.SliceOf(elemType), 0, len(matches))
			for _, idx := range matches {
				out = reflect.Append(out, buf.Index(idx)) // value copy per parent
			}
			container.setLoaded(out)
		case relHasOne, relBelongsTo:
			if len(matches) == 0 {
				container.setLoaded(reflect.Zero(ptrType))
				continue
			}
			if rel.kind == relHasOne && len(matches) > 1 {
				// HasOne declares "one" — silently keeping whichever row the
				// driver returned first would be a nondeterministic answer.
				return fmt.Errorf("rio: relation %s.%s: HasOne loaded %d rows for one parent; the schema evidently allows several — use HasMany, or make %s.%s unique",
					owner.structName, rel.name, len(matches), target.structName, res.fk.column)
			}
			cp := reflect.New(elemType)
			cp.Elem().Set(buf.Index(matches[0]))
			container.setLoaded(cp)
		}
	}
	return nil
}

// renderRelSelect renders the preload query. keyed reports whether an extra
// join-key column is appended after the entity columns (ManyToMany).
func renderRelSelect(g *grammar, res *resolvedRel, kind relKind, keys []any, rq *relQuery) (string, []any, bool, error) {
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
func scanRel(rows rows, p *plan, buf reflect.Value, keyed bool, res *resolvedRel) (out reflect.Value, keys []any, err error) {
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
		kf := &field{name: "join key", column: res.joinFK, typ: res.ref.typ}
		codec, err := codecFor(kf)
		if err != nil {
			return buf, nil, err
		}
		kf.code = codec
		keyCell.f = kf
		extras = []any{&keyCell}
	}

	rs := newRowScanner(fields, extras)
	defer rs.release()
	keyBuf := reflect.New(res.ref.typ) // cell the key scans into
	elemType := p.typ
	for rows.Next() {
		buf = reflect.Append(buf, reflect.Zero(elemType))
		elem := buf.Index(buf.Len() - 1)
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

// canonKey normalizes join-key values so int32 FKs match int64 PKs in the
// grouping map. []byte keys (binary UUIDs) convert to string — slices are
// not comparable and would panic as map keys. Pointers dereference to their
// value (nil to nil): an address would never equal the value key scanned
// from the other side.
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
		// Sign-normalize so an int64 PK groups with a uint32 FK; values
		// above MaxInt64 keep their own key space (they can never equal a
		// signed key anyway).
		if n := v.Uint(); n <= math.MaxInt64 {
			return int64(n)
		} else {
			return n
		}
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

var _ = unsafe.Pointer(nil) // unsafe is used via UnsafePointer accessors

// countInto fills WithCount targets: one GROUP BY query per relation, the
// aggregate sibling of selectin preloading.
func countInto[T any](ctx context.Context, db Queryer, p *plan, rows []T, counts []string) error {
	if len(rows) == 0 || len(counts) == 0 {
		return nil
	}
	rv := reflect.ValueOf(rows)
	sorted := append([]string(nil), counts...)
	sort.Strings(sorted)
	prev, first := "", true
	for _, name := range sorted {
		if !first && name == prev {
			continue // deduplicate: WithCount("X").WithCount("X") counts once, like With
		}
		first, prev = false, name
		if err := countRelation(ctx, db, p, name, rv); err != nil {
			return err
		}
	}
	return nil
}

func countRelation(ctx context.Context, db Queryer, owner *plan, name string, rows reflect.Value) error {
	rel, ok := owner.rels[name]
	if !ok {
		return fmt.Errorf("rio: %s has no relation %q", owner.structName, name)
	}
	target, ok := owner.counts[name]
	if !ok {
		return fmt.Errorf("rio: %s has no count target for %q; declare a field tagged `rio:\",countof:%s\"`", owner.structName, name, name)
	}
	if rel.kind != relHasMany && rel.kind != relManyToMany {
		return fmt.Errorf("rio: WithCount(%q): counting a %s relation is meaningless (0 or 1); load it instead", name, rel.kind)
	}
	res, err := rel.resolve(owner)
	if err != nil {
		return err
	}

	// canonKey groups; the IN (?) binds the original value (see loadRelation).
	seen := make(map[any]struct{})
	var keys []any
	parentKey := make([]any, rows.Len())
	for i := 0; i < rows.Len(); i++ {
		kv := rows.Index(i).FieldByIndex(res.ref.index)
		if kv.Kind() == reflect.Pointer {
			if kv.IsNil() {
				parentKey[i] = nil // nil pointer key: nothing to count against
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
			// Always INNER JOIN the target, exactly as the With load does, so
			// the count matches the number of rows With would return: a join
			// row pointing at a missing target counts zero either way. The
			// softdelete predicate additionally excludes tombstoned targets.
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
			return err
		}
		sqlRows, finish, err := runQuery(ctx, db, "select", res.target.structName, sqlText, outArgs)
		if err != nil {
			return err
		}
		err = scanCounts(sqlRows, res.ref.typ, byKey)
		finishQuery(finish, err)
		if err != nil {
			return err
		}
	}

	for i := 0; i < rows.Len(); i++ {
		n := byKey[parentKey[i]]
		rows.Index(i).FieldByIndex(target).SetInt(n)
	}
	return nil
}

// scanCounts drains (key, count) pairs into the grouping map.
func scanCounts(rows rows, keyType reflect.Type, byKey map[any]int64) (err error) {
	defer mergeClose(rows, &err)
	keyBuf := reflect.New(keyType)
	kf := &field{name: "count key", column: "<key>", typ: keyType}
	codec, err := codecFor(kf)
	if err != nil {
		return err
	}
	kf.code = codec
	// One escaping box carries the cell, the count slot, and their dest
	// slice: a fresh variadic slice at the interface call would otherwise
	// heap-allocate per row (see scanScalars).
	var box struct {
		cell colScanner
		n    int64
		dest [2]any
	}
	box.cell = colScanner{f: kf, base: keyBuf.UnsafePointer()}
	box.dest[0], box.dest[1] = &box.cell, &box.n
	for rows.Next() {
		if err := rows.Scan(box.dest[:]...); err != nil {
			return err
		}
		byKey[canonKey(keyBuf.Elem())] = box.n
	}
	return rows.Err()
}

// renderRelSelectLimited wraps the preload in a window subquery so the limit
// applies per parent: the inner query numbers rows within each foreign-key
// partition, the outer one keeps the first N and projects exactly the entity
// columns (plus the join key) — the row number never leaves the subquery.
func renderRelSelectLimited(g *grammar, res *resolvedRel, kind relKind, keys []any, rq *relQuery) (string, []any, bool, error) {
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
	// The inner window ORDER BY only decides which rows survive rn <= n; the
	// outer query needs its own ORDER BY or the per-parent order the user
	// asked for via RelOrder is lost. Order by the partition then the row
	// number: contiguous per-parent blocks, each in the requested order.
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
