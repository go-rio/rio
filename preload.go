package rio

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"sort"
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

	singlePK := func(p *plan, side string) (*field, error) {
		if len(p.pks) == 1 {
			return p.pks[0], nil
		}
		return nil, fmt.Errorf("%s %s needs exactly one primary key column for convention-based relations (has %d); set ref: explicitly or restructure",
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
		ownPK, err := singlePK(owner, "owner")
		if err != nil {
			return nil, err
		}
		targetPK, err := singlePK(target, "target")
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
		res.joinFK = snakeCase(owner.structName) + "_id"
		res.joinRef = snakeCase(target.structName) + "_id"
	}
	return res, nil
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
			keys = append(keys, k)
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
		chunk := db.gram().d.caps().maxBindParams - relArgs
		if chunk < 1 {
			chunk = 1
		}
		for start := 0; start < len(keys); start += chunk {
			end := min(start+chunk, len(keys))
			sqlText, args, keyed, err := renderRelSelect(db.gram(), res, rel.kind, keys[start:end], &rq)
			if err != nil {
				return err
			}
			sqlRows, err := runQuery(ctx, db, "select", target.structName, sqlText, args)
			if err != nil {
				return err
			}
			part, partKeys, err := scanRel(sqlRows, target, buf, keyed, res)
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
			byKey[canonKey(kv)] = append(byKey[canonKey(kv)], i)
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
func scanRel(rows *sql.Rows, p *plan, buf reflect.Value, keyed bool, res *resolvedRel) (reflect.Value, []any, error) {
	defer rows.Close()
	extra := 0
	if keyed {
		extra = 1
	}
	fields, err := entityFields(rows, p, extra)
	if err != nil {
		return buf, nil, err
	}

	var keys []any
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
// not comparable and would panic as map keys.
func canonKey(v reflect.Value) any {
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
