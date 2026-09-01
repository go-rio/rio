package rio

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"unsafe"
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

// preloadSpec is one With() request: a dot path plus its leaf options.
type preloadSpec struct {
	path string
	rq   relQuery
}

// countSpec is one WithCount() request.
type countSpec struct {
	relation string
	rq       relQuery
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

// relOptions evaluates opts once, at build time.
func relOptions(opts []RelOption) relQuery {
	var rq relQuery
	for _, opt := range opts {
		opt(&rq)
	}
	return rq
}

// merge folds another request for the same relation into rq.
func (rq *relQuery) merge(o *relQuery) {
	rq.wheres = append(rq.wheres, o.wheres...)
	rq.orders = append(rq.orders, o.orders...)
	rq.withTrashed = rq.withTrashed || o.withTrashed
	if o.limitSet {
		rq.limit, rq.limitSet = o.limit, true
	}
	rq.changesCount = rq.changesCount || o.changesCount
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

// RelOrderBy orders the preloaded rows before they are grouped per parent.
// The term is included verbatim; never build it from untrusted input.
func RelOrderBy(expr string) RelOption {
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
// each parent follows RelOrderBy, defaulting to the target's primary key.
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

// preloadValues loads relation paths into one nested layer's rows, an
// addressable []T value.
func preloadValues(ctx context.Context, db Queryer, p *plan, rows reflect.Value, specs []preloadSpec) error {
	stmts, finishes, err := collectRelationLayer(db, p, rows, specs, nil, nil)
	if err != nil {
		return err
	}
	return runRelLayer(ctx, db, stmts, finishes)
}

// collectRelationLayer renders one layer's preload statements and finishes,
// appending to the caller's slices so the top level merges them with
// WithCount's into a single round trip.
func collectRelationLayer(
	db Queryer,
	p *plan,
	rows reflect.Value,
	specs []preloadSpec,
	stmts []relStatement,
	finishes []relFinisher,
) ([]relStatement, []relFinisher, error) {
	if rows.Len() == 0 || len(specs) == 0 {
		return stmts, finishes, nil
	}
	type group struct {
		rq    relQuery
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
			g.rq.merge(&s.rq)
		} else {
			g.tails = append(g.tails, preloadSpec{path: tail, rq: s.rq})
		}
	}
	sort.Strings(order) // deterministic query order run to run

	if stmts == nil {
		stmts = make([]relStatement, 0, len(order))
	}
	if finishes == nil {
		finishes = make([]relFinisher, 0, len(order))
	}
	for _, head := range order {
		rel, ok := p.rels[head]
		if !ok {
			return nil, nil, fmt.Errorf("rio: %s has no relation %q", p.structName, head)
		}
		g := groups[head]
		var l relFinisher
		var err error
		stmts, l, err = prepareRelationLoad(db, p, rel, rows, &g.rq, g.tails, stmts)
		if err != nil {
			return nil, nil, err
		}
		finishes = append(finishes, l)
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

// keyer canonicalizes one relation's grouping keys into a typed key space,
// so the grouping maps and slices hold K instead of boxed any values. ok is
// false for NULL pointers — rows and parents without a key group nowhere.
// anyKeyer is the fallback instance with canonKey's exact semantics; the
// typed instances exist because boxing every key costs one allocation per
// row once values leave the runtime's small-int cache.
type keyer[K comparable] interface {
	key(reflect.Value) (K, bool)
	// newBinds and appendBind collect the IN () bind values where they
	// differ from the canonical keys: typed keyers bind the keys themselves
	// (nil binds, no-op appends); anyKeyer preserves original values.
	newBinds(n int) []any
	appendBind(binds []any, kv reflect.Value) []any
}

type intKeyer struct{}

func (intKeyer) key(v reflect.Value) (int64, bool) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0, false
		}
		v = v.Elem()
	}
	return v.Int(), true
}

func (intKeyer) newBinds(int) []any                            { return nil }
func (intKeyer) appendBind(binds []any, _ reflect.Value) []any { return binds }

type uintKeyer struct{}

func (uintKeyer) key(v reflect.Value) (uint64, bool) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0, false
		}
		v = v.Elem()
	}
	return v.Uint(), true
}

func (uintKeyer) newBinds(int) []any                            { return nil }
func (uintKeyer) appendBind(binds []any, _ reflect.Value) []any { return binds }

type strKeyer struct{}

func (strKeyer) key(v reflect.Value) (string, bool) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "", false
		}
		v = v.Elem()
	}
	return v.String(), true
}

func (strKeyer) newBinds(int) []any                            { return nil }
func (strKeyer) appendBind(binds []any, _ reflect.Value) []any { return binds }

type anyKeyer struct{}

func (anyKeyer) key(v reflect.Value) (any, bool) {
	k := canonKey(v)
	return k, k != nil
}

func (anyKeyer) newBinds(n int) []any { return make([]any, 0, n) }

func (anyKeyer) appendBind(binds []any, kv reflect.Value) []any {
	if kv.Kind() == reflect.Pointer {
		kv = kv.Elem()
	}
	return append(binds, kv.Interface())
}

// pkey is one owner's or buffered row's grouping key; ok is false when the
// key column was NULL.
type pkey[K comparable] struct {
	k  K
	ok bool
}

const (
	kfInt = iota
	kfUint
	kfStr
	kfAny
)

// relKeyFam picks the typed key space both key columns fit; mixed
// signedness and exotic types keep canonKey's any semantics.
func relKeyFam(types ...reflect.Type) int {
	fam := -1
	for _, t := range types {
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Implements(valuerType) {
			// A Valuer key must bind its original value so the driver calls
			// Value(); the canonical widened key would bypass it and match
			// nothing against the stored form.
			return kfAny
		}
		var f int
		switch t.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			f = kfInt
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			f = kfUint
		case reflect.String:
			f = kfStr
		default:
			// []byte included: its grouping key (string) and bind value
			// ([]byte) differ, which only the any pipeline carries.
			return kfAny
		}
		if fam == -1 {
			fam = f
		} else if fam != f {
			return kfAny
		}
	}
	return fam
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

// relLoadBase carries one relation load's key-space-independent inputs.
type relLoadBase struct {
	db     Queryer
	owner  *plan
	rel    *relField
	res    *resolvedRel
	owners reflect.Value
	tails  []preloadSpec
}

// relLoad is one relation's two-phase load: prepare renders the chunk
// statements, each consumes rows into buf, finish assembles buf into the
// owners. One heap object serves the whole relation; finish must run only
// after every statement was consumed.
type relLoad[K comparable, KR keyer[K]] struct {
	relLoadBase
	kr          KR
	keys        []K
	groupOf     map[K]int32 // key → index in keys
	parentGroup []int32     // per owner; -1 for a NULL key
	keyed       bool
	// buf must stay addressable: scanRel grows it in place.
	buf     reflect.Value
	bufKeys []pkey[K]
}

func (l *relLoad[K, KR]) consume(sqlRows rows) (int64, error) {
	before := l.buf.Len()
	part, partKeys, err := scanRel(sqlRows, l.res.target, l.buf, l.keyed, l.res, l.kr, l.bufKeys)
	if err != nil {
		return int64(part.Len() - before), err
	}
	l.buf = part
	l.bufKeys = partKeys
	return int64(part.Len() - before), nil
}

// prepareRelationLoad renders one relation's preload queries onto stmts and
// returns the loader that consumes and finishes them, in the typed key space
// both key columns fit.
func prepareRelationLoad(
	db Queryer,
	owner *plan,
	rel *relField,
	owners reflect.Value,
	rq *relQuery,
	tails []preloadSpec,
	stmts []relStatement,
) ([]relStatement, relFinisher, error) {
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
	base := relLoadBase{db: db, owner: owner, rel: rel, res: res, owners: owners, tails: tails}
	switch relKeyFam(res.ref.typ, res.fk.typ) {
	case kfInt:
		return prepareRel[int64, intKeyer](base, rq, stmts)
	case kfUint:
		return prepareRel[uint64, uintKeyer](base, rq, stmts)
	case kfStr:
		return prepareRel[string, strKeyer](base, rq, stmts)
	default:
		return prepareRel[any, anyKeyer](base, rq, stmts)
	}
}

// prepareRel instantiates the load in its key space.
func prepareRel[K comparable, KR keyer[K]](base relLoadBase, rq *relQuery, stmts []relStatement) ([]relStatement, relFinisher, error) {
	l := &relLoad[K, KR]{relLoadBase: base}
	stmts, err := l.prepare(rq, stmts)
	return stmts, l, err
}

// prepare renders the relation's chunked preload statements onto stmts.
func (l *relLoad[K, KR]) prepare(rq *relQuery, stmts []relStatement) ([]relStatement, error) {
	// Typed keys group; the IN (?) binds the canonical value, whose family
	// matches the column's.
	owners := l.owners
	groupOf := make(map[K]int32, owners.Len())
	keys := make([]K, 0, owners.Len())
	binds := l.kr.newBinds(owners.Len()) // non-nil only when binds differ from keys
	l.parentGroup = make([]int32, owners.Len())
	for i := 0; i < owners.Len(); i++ {
		kv := owners.Index(i).FieldByIndex(l.res.ref.index)
		k, ok := l.kr.key(kv)
		if !ok {
			l.parentGroup[i] = -1
			continue
		}
		g, dup := groupOf[k]
		if !dup {
			g = int32(len(keys))
			groupOf[k] = g
			keys = append(keys, k)
			binds = l.kr.appendBind(binds, kv)
		}
		l.parentGroup[i] = g
	}

	target := l.res.target
	l.keys, l.groupOf = keys, groupOf
	l.keyed = l.rel.kind == relManyToMany
	l.buf = reflect.New(reflect.SliceOf(target.typ)).Elem()
	l.buf.Set(reflect.MakeSlice(reflect.SliceOf(target.typ), 0, len(keys)))

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
		caps := l.db.gram().d.caps()
		limit := caps.maxBindParams
		if relArgs >= limit {
			return nil, fmt.Errorf(
				"rio: preload relation %s.%s uses %d bind parameter(s) in RelWhere, "+
					"leaving none for parent keys (dialect limit %d)",
				l.owner.structName,
				l.rel.name,
				relArgs,
				limit,
			)
		}
		// Typed keys bind as one array where the dialect takes them.
		chunk := limit - relArgs
		array := caps.arrayBind && binds == nil
		if array {
			chunk = len(keys)
		}
		for start := 0; start < len(keys); start += chunk {
			end := min(start+chunk, len(keys))
			var bindChunk any = keys[start:end]
			if binds != nil {
				bindChunk = binds[start:end]
			}
			if array {
				bindChunk = arrayParam{bindChunk}
			}
			sqlText, args, keyed, err := renderRelSelect(l.db.gram(), l.res, l.rel.kind, bindChunk, rq)
			if err != nil {
				return nil, err
			}
			l.keyed = keyed
			stmts = append(stmts, relStatement{
				phase:   "preload",
				model:   target.structName,
				sqlText: sqlText,
				args:    args,
				load:    l,
			})
		}
	}

	return stmts, nil
}

func (l *relLoad[K, KR]) finish(ctx context.Context) error {
	rel, res, buf := l.rel, l.res, l.buf
	// Nested paths load into buf first, so the regrouped copies carry
	// assembled children.
	if len(l.tails) > 0 && buf.Len() > 0 {
		if err := preloadValues(ctx, l.db, res.target, buf, l.tails); err != nil {
			return err
		}
	}

	// Counting sort by owner group keeps the query's row order per owner.
	n := buf.Len()
	rowGroup := make([]int32, n)
	if rel.kind == relManyToMany {
		for i, bk := range l.bufKeys {
			rowGroup[i] = l.group(bk)
		}
	} else {
		// res.fk is the buffered rows' grouping key for every non-m2m kind.
		keyField := res.fk
		for i := range n {
			k, ok := l.kr.key(buf.Index(i).FieldByIndex(keyField.index))
			rowGroup[i] = l.group(pkey[K]{k: k, ok: ok})
		}
	}
	groups := len(l.keys)
	starts := make([]int, groups+1)
	for _, g := range rowGroup {
		if g >= 0 {
			starts[g+1]++
		}
	}
	for g := range groups {
		starts[g+1] += starts[g]
	}
	order := make([]int, starts[groups])
	fill := append([]int(nil), starts[:groups]...)
	for i, g := range rowGroup {
		if g >= 0 {
			order[fill[g]] = i
			fill[g]++
		}
	}
	spans := make([]span, len(l.parentGroup))
	for i, g := range l.parentGroup {
		if g >= 0 {
			spans[i] = span{starts[g], starts[g+1]}
		}
	}
	if rel.kind == relHasOne {
		for _, s := range spans {
			if s.end-s.start > 1 {
				return fmt.Errorf(
					"rio: relation %s.%s: HasOne loaded %d rows for one parent; "+
						"the schema evidently allows several — use HasMany, or make %s.%s unique",
					l.owner.structName,
					rel.name,
					s.end-s.start,
					res.target.structName,
					res.fk.column,
				)
			}
		}
	}
	rel.proto.regroup(l.owners.UnsafePointer(), l.owner.typ.Size(), rel.offset, spans, buf.Interface(), order)
	return nil
}

// group maps a buffered row's key to its owner group, -1 when it has none.
func (l *relLoad[K, KR]) group(k pkey[K]) int32 {
	if !k.ok {
		return -1
	}
	if g, ok := l.groupOf[k.k]; ok {
		return g
	}
	return -1
}

// renderRelSelect renders the preload query. keyed reports whether an extra
// join-key column is appended after the entity columns (ManyToMany).
func renderRelSelect(
	g *grammar,
	res *resolvedRel,
	kind relKind,
	keys any,
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
	b = appendKeySet(b, keys)
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

// appendKeySet renders the owner-key predicate: one array parameter, or the
// expanded IN list.
func appendKeySet(b []byte, keys any) []byte {
	if _, ok := keys.(arrayParam); ok {
		return append(b, " = ANY(?)"...)
	}
	return append(b, " IN (?)"...)
}

// scanRel appends scanned rows to buf, returning the grown slice and, when
// keyed, one owner key per appended row (appended to keys).
func scanRel[K comparable, KR keyer[K]](
	rows rows,
	p *plan,
	buf reflect.Value,
	keyed bool,
	res *resolvedRel,
	kr KR,
	keys []pkey[K],
) (out reflect.Value, _ []pkey[K], err error) {
	defer mergeClose(rows, &err)
	extra := 0
	if keyed {
		extra = 1
	}
	fields, err := entityFields(rows, p, extra)
	if err != nil {
		return buf, keys, err
	}

	var keyCell colScanner
	var extras []any
	if keyed {
		kf, err := synthField("join key", res.joinFK, res.ref.typ)
		if err != nil {
			return buf, keys, err
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
			k, ok := kr.key(keyBuf.Elem())
			keys = append(keys, pkey[K]{k: k, ok: ok})
		}
	}
	if err := rows.Err(); err != nil {
		return buf, nil, err
	}
	return buf, keys, nil
}

// renderRelSelectLimited wraps the preload in a window subquery so the limit
// applies per parent; the row number never leaves the subquery.
func renderRelSelectLimited(
	g *grammar,
	res *resolvedRel,
	kind relKind,
	keys any,
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
	b = appendKeySet(b, keys)
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
	// ORDER BY (partition, then row number) the RelOrderBy order is lost.
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

// splitCounts partitions WithCount targets: an unfiltered count of a relation
// the same query fully preloads reads off the loaded containers; the rest
// query.
func splitCounts(p *plan, specs []preloadSpec, counts []countSpec) (queried []countSpec, reusable []string) {
	if len(counts) == 0 {
		return nil, nil
	}
	full := make(map[string]bool, len(specs))
	for _, spec := range specs {
		head, tail := splitPath(spec.path)
		if _, seen := full[head]; !seen {
			full[head] = true
		}
		if tail == "" && spec.rq.changesCount {
			full[head] = false
		}
	}
	seen := make(map[string]bool, len(counts))
	for _, c := range counts {
		if seen[c.relation] {
			continue
		}
		seen[c.relation] = true
		rel, ok := p.rels[c.relation]
		_, hasTarget := p.counts[c.relation]
		countable := ok && hasTarget && (rel.kind == relHasMany || rel.kind == relManyToMany)
		if full[c.relation] && countable && !c.rq.changesCount {
			reusable = append(reusable, c.relation)
		} else {
			queried = append(queried, c)
		}
	}
	return queried, reusable
}

// prepareCountLoads renders every WithCount relation's queries onto the
// caller's slices, ready to share the preload layer's round trip.
func prepareCountLoads(
	db Queryer,
	p *plan,
	rows reflect.Value,
	counts []countSpec,
	stmts []relStatement,
	finishes []relFinisher,
) ([]relStatement, []relFinisher, error) {
	if rows.Len() == 0 || len(counts) == 0 {
		return stmts, finishes, nil
	}
	// counts arrives deduplicated by splitCounts.
	for i := range counts {
		var l relFinisher
		var err error
		stmts, l, err = prepareCountLoad(db, p, &counts[i], rows, stmts)
		if err != nil {
			return nil, nil, err
		}
		finishes = append(finishes, l)
	}
	return stmts, finishes, nil
}

// countLoad is one WithCount relation's two-phase load: statements consume
// GROUP BY rows into byKey, finish writes the counts back to the owners.
// countLoadBase carries one count load's key-space-independent inputs.
type countLoadBase struct {
	db     Queryer
	res    *resolvedRel
	kind   relKind
	rq     *relQuery
	target countTarget
	owners reflect.Value
}

// countLoad is one WithCount relation's two-phase load: prepare renders the
// GROUP BY statements, each consumes into byKey, finish writes the counts
// back to the owners.
type countLoad[K comparable, KR keyer[K]] struct {
	countLoadBase
	kr        KR
	parentKey []pkey[K]
	byKey     map[K]int64
}

func (l *countLoad[K, KR]) consume(sqlRows rows) (int64, error) {
	return scanCounts(sqlRows, l.res.ref.typ, l.byKey, l.kr)
}

func (l *countLoad[K, KR]) finish(context.Context) error {
	base := l.owners.UnsafePointer()
	stride := l.owners.Type().Elem().Size()
	for i := 0; i < l.owners.Len(); i++ {
		var n int64
		if pk := l.parentKey[i]; pk.ok {
			n = l.byKey[pk.k]
		}
		*(*int64)(unsafe.Add(base, uintptr(i)*stride+l.target.offset)) = n
	}
	return nil
}

// prepareCountLoad renders one WithCount relation's GROUP BY queries plus the
// finish that writes counts back; finish must run only after every returned
// statement was consumed.
func prepareCountLoad(
	db Queryer,
	owner *plan,
	spec *countSpec,
	owners reflect.Value,
	stmts []relStatement,
) ([]relStatement, relFinisher, error) {
	name := spec.relation
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
	base := countLoadBase{db: db, res: res, kind: rel.kind, rq: &spec.rq, target: target, owners: owners}
	switch relKeyFam(res.ref.typ) {
	case kfInt:
		return prepareCount[int64, intKeyer](base, stmts)
	case kfUint:
		return prepareCount[uint64, uintKeyer](base, stmts)
	case kfStr:
		return prepareCount[string, strKeyer](base, stmts)
	default:
		return prepareCount[any, anyKeyer](base, stmts)
	}
}

// prepareCount instantiates the load in its key space.
func prepareCount[K comparable, KR keyer[K]](base countLoadBase, stmts []relStatement) ([]relStatement, relFinisher, error) {
	l := &countLoad[K, KR]{countLoadBase: base}
	stmts, err := l.prepare(stmts)
	return stmts, l, err
}

// prepare renders the relation's chunked GROUP BY statements onto stmts.
func (l *countLoad[K, KR]) prepare(stmts []relStatement) ([]relStatement, error) {
	owners := l.owners
	seen := make(map[K]struct{}, owners.Len())
	keys := make([]K, 0, owners.Len())
	binds := l.kr.newBinds(owners.Len())
	l.parentKey = make([]pkey[K], owners.Len())
	for i := 0; i < owners.Len(); i++ {
		kv := owners.Index(i).FieldByIndex(l.res.ref.index)
		k, ok := l.kr.key(kv)
		if !ok {
			continue
		}
		l.parentKey[i] = pkey[K]{k: k, ok: true}
		if _, dup := seen[k]; !dup {
			seen[k] = struct{}{}
			keys = append(keys, k)
			binds = l.kr.appendBind(binds, kv)
		}
	}

	g := l.db.gram()
	d := g.d
	res, kind := l.res, l.kind
	l.byKey = make(map[K]int64, len(keys))
	chunk := d.caps().maxBindParams
	array := d.caps().arrayBind && binds == nil
	if array {
		chunk = len(keys)
	}
	for start := 0; start < len(keys); start += chunk {
		end := min(start+chunk, len(keys))
		b := make([]byte, 0, 160)
		var keyCol string
		b = append(b, "SELECT "...)
		if kind == relManyToMany {
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
			if res.target.softDel != nil && !l.rq.withTrashed {
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
		var bindChunk any = keys[start:end]
		if binds != nil {
			bindChunk = binds[start:end]
		}
		if array {
			bindChunk = arrayParam{bindChunk}
		}
		b = appendKeySet(b, bindChunk)
		args := []any{bindChunk}
		if kind != relManyToMany && res.target.softDel != nil && !l.rq.withTrashed {
			b = append(b, " AND "...)
			b = d.quote(b, g.table(res.target))
			b = append(b, '.')
			b = d.quote(b, res.target.softDel.column)
			b = append(b, " IS NULL"...)
		}
		for _, w := range l.rq.wheres {
			b = append(b, " AND ("...)
			b = append(b, w.expr...)
			b = append(b, ')')
			args = append(args, w.args...)
		}
		b = append(b, " GROUP BY "...)
		if kind == relManyToMany {
			b = d.quote(b, res.joinTable)
		} else {
			b = d.quote(b, g.table(res.target))
		}
		b = append(b, '.')
		b = d.quote(b, keyCol)

		sqlText, outArgs, err := finishSQL(d, b, args)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, relStatement{
			phase:   "count",
			model:   res.target.structName,
			sqlText: sqlText,
			args:    outArgs,
			load:    l,
		})
	}
	return stmts, nil
}

// scanCounts drains (key, count) pairs into the grouping map and reports how
// many it read.
func scanCounts[K comparable, KR keyer[K]](rows rows, keyType reflect.Type, byKey map[K]int64, kr KR) (scanned int64, err error) {
	defer mergeClose(rows, &err)
	keyBuf := reflect.New(keyType)
	kf, err := synthField("count key", "<key>", keyType)
	if err != nil {
		return 0, err
	}
	nf := countField()
	// One escaping box carries cells, count, and dest: a fresh variadic slice
	// would heap-allocate per row (see scanScalars). Both slots are cells, so
	// every driver serves this through its one scan path.
	var box struct {
		cell  colScanner
		nCell colScanner
		n     int64
		dest  [2]any
	}
	box.cell = colScanner{f: kf, base: keyBuf.UnsafePointer()}
	box.nCell = colScanner{f: nf, base: unsafe.Pointer(&box.n)}
	box.dest[0], box.dest[1] = &box.cell, &box.nCell
	for rows.Next() {
		if err := rows.Scan(box.dest[:]...); err != nil {
			return scanned, err
		}
		if k, ok := kr.key(keyBuf.Elem()); ok {
			byKey[k] = box.n
		}
		scanned++
	}
	return scanned, rows.Err()
}

// countField is the synthetic int64 cell target scanCounts reuses.
var countField = sync.OnceValue(func() *field {
	f, err := synthField("count", "<count>", reflect.TypeFor[int64]())
	if err != nil {
		panic(err) // int64 always has a codec
	}
	return f
})

// reuseCounts fills count targets from containers the preload just loaded.
func reuseCounts(p *plan, rv reflect.Value, reusable []string) error {
	base := rv.UnsafePointer()
	stride := p.typ.Size()
	for _, name := range reusable {
		rel := p.rels[name]
		target := p.counts[name]
		for i := 0; i < rv.Len(); i++ {
			container := rv.Index(i).FieldByIndex(rel.index).Addr().Interface().(relContainer)
			n, loaded := container.loadedLen()
			if !loaded {
				return fmt.Errorf("rio: relation %s.%s was not loaded before count reuse", p.structName, name)
			}
			*(*int64)(unsafe.Add(base, uintptr(i)*stride+target.offset)) = int64(n)
		}
	}
	return nil
}
