package rio

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

// TableNamer overrides the convention-derived table name for one model.
type TableNamer interface {
	TableName() string
}

// plan is the immutable mapping of one struct type, cached and shared by
// every DB handle; nothing grammar-dependent lives here.
type plan struct {
	typ           reflect.Type
	structName    string
	tableOverride string // from TableName(), "" otherwise
	defaultTable  string // convention-derived

	fields    []*field
	byColumn  map[string]*field
	pks       []*field
	updatable []*field // full-column Update set, in field order
	autoIncr  *field
	version   *field
	softDel   *field
	created   *field
	updated   *field

	// Precomputed insert partitions: every column (allBits), or every column
	// but the auto-increment PK (insCols/insBack/insBits).
	insCols     []*field
	insBack     []*field
	allBits     uint64
	insBits     uint64
	hasOmitZero bool

	rels     map[string]*relField
	relNames []string
	counts   map[string][]int // relation name → field index of its count target
}

// field maps one struct field to one column.
type field struct {
	name    string
	column  string
	index   []int   // reflect traversal path (embedding)
	offset  uintptr // cumulative offset — valid because only value embedding is allowed
	ordinal int     // position in plan.fields, the bit in SQL-cache bitmaps
	typ     reflect.Type

	isPK, isAutoIncr, omitZero, jsonCol bool
	isVersion, isSoftDelete             bool
	isCreated, isUpdated                bool
	noAutoIncr                          bool

	code fieldCodec // scan/bind strategy, decided once at plan time
}

// relField is a relation declaration. Resolution is deferred to first use:
// eager resolution would recurse on mutually referencing models.
type relField struct {
	name   string
	kind   relKind
	index  []int
	target reflect.Type

	fkTag, refTag, joinTag string

	once     sync.Once
	resolved *resolvedRel
	rerr     error
}

// rawField is one struct field as collected before shadowing resolution:
// every exported field and every anonymous embedded struct, at every depth.
type rawField struct {
	sf      reflect.StructField
	owner   reflect.Type // declaring struct, for collision messages
	index   []int
	offset  uintptr
	tag     string
	opts    tagOpts
	flatten bool // anonymous value struct that flattens rather than mapping
}

type tagOpts struct {
	skip       bool
	pk         bool
	omitZero   bool
	json       bool
	version    bool
	softDelete bool
	noStamp    bool
	noAutoIncr bool
	fk, ref    string
	join       string
	countOf    string
}

var plans sync.Map // reflect.Type → *plan | error

var (
	timeType    = reflect.TypeFor[time.Time]()
	timePtrType = reflect.TypeFor[*time.Time]()
)

func (r *rawField) depth() int { return len(r.index) - 1 }

func (p *plan) addFields(t reflect.Type) error {
	var errs []error
	var raw []rawField
	collectFields(t, nil, 0, &raw, &errs)

	// Shadowing matches encoding/json: for one Go field name the shallowest
	// declaration wins, even a rio:"-" or renamed one. Two at the same
	// shallowest depth are a Go ambiguous selector; rio rejects them.
	type nameState struct {
		winner int // index into raw of the shallowest occurrence
		clash  int // second occurrence at the same depth, -1 when unique
	}
	names := make(map[string]*nameState, len(raw))
	for i := range raw {
		r := &raw[i]
		st, ok := names[r.sf.Name]
		if !ok {
			names[r.sf.Name] = &nameState{winner: i, clash: -1}
			continue
		}
		switch d, w := r.depth(), raw[st.winner].depth(); {
		case d < w:
			st.winner, st.clash = i, -1
		case d == w && st.clash < 0:
			st.clash = i
		}
	}

	var idConv *field // ID-convention candidate, decided after all fields exist
	for i := range raw {
		r := &raw[i]
		st := names[r.sf.Name]
		if st.clash >= 0 {
			if st.winner == i {
				other := &raw[st.clash]
				errs = append(errs, fmt.Errorf(
					"fields %s.%s and %s.%s are embedded at the same depth; Go can address neither — rename one",
					r.owner.Name(), r.sf.Name, other.owner.Name(), other.sf.Name))
			}
			continue
		}
		if st.winner != i {
			continue // shadowed by a shallower field
		}
		if r.opts.skip {
			continue
		}
		if r.flatten {
			// A role tag on a flattened embed would silently vanish.
			if opt := roleOptName(r.opts); opt != "" {
				errs = append(errs, fmt.Errorf(
					"field %s: %s does not apply to a flattened embedded struct; tag the embedded type's fields",
					r.sf.Name, opt))
			}
			continue // inner fields already collected
		}
		sf, tag, opts := r.sf, r.tag, r.opts

		if isRelContainer(sf.Type) {
			kind, target := containerInfo(sf.Type)
			if tag != "" {
				errs = append(errs, fmt.Errorf("field %s: relation containers take no column name", sf.Name))
				continue
			}
			p.rels[sf.Name] = &relField{
				name: sf.Name, kind: kind, index: r.index, target: target,
				fkTag: opts.fk, refTag: opts.ref, joinTag: opts.join,
			}
			p.relNames = append(p.relNames, sf.Name)
			continue
		}
		if opts.countOf != "" {
			// Count targets are populated by WithCount, never mapped to a column.
			if tag != "" {
				errs = append(errs, fmt.Errorf("field %s: countof targets take no column name", sf.Name))
				continue
			}
			if sf.Type.Kind() != reflect.Int64 {
				errs = append(errs, fmt.Errorf("field %s: countof targets must be int64, got %s", sf.Name, sf.Type))
				continue
			}
			if prev, dup := p.counts[opts.countOf]; dup {
				errs = append(errs, fmt.Errorf(
					"fields %s and %s both declare countof:%s; a count has one target",
					p.typ.FieldByIndex(prev).Name, sf.Name, opts.countOf))
				continue
			}
			p.counts[opts.countOf] = r.index
			continue
		}
		if opts.fk != "" || opts.ref != "" || opts.join != "" {
			errs = append(errs, fmt.Errorf("field %s: fk/ref/join apply only to relation containers", sf.Name))
			continue
		}

		// Pointer embedding would break offset-based scanning (nil hop).
		if sf.Anonymous && sf.Type.Kind() == reflect.Pointer {
			errs = append(errs, fmt.Errorf("field %s: embed the struct by value, not by pointer", sf.Name))
			continue
		}
		if sf.Anonymous && !sf.IsExported() {
			// reflect refuses Interface() on unexported embedded fields;
			// binding would panic on the first write.
			errs = append(errs, fmt.Errorf(
				"field %s: an unexported embedded type cannot map to a column itself; "+
					"export the type or use an exported named field",
				sf.Name,
			))
			continue
		}

		f := &field{
			name:   sf.Name,
			column: tag,
			index:  r.index,
			offset: r.offset,
			typ:    sf.Type,

			isPK:     opts.pk,
			omitZero: opts.omitZero,
			jsonCol:  opts.json,
		}
		if f.column == "" {
			f.column = snakeCase(sf.Name)
		}
		if opts.version {
			f.isVersion = true
		}
		if opts.softDelete {
			f.isSoftDelete = true
		}
		if !opts.noStamp && !opts.softDelete && !opts.version &&
			(sf.Type == timeType || sf.Type == timePtrType) {
			// The CreatedAt/UpdatedAt convention is name-based; an explicit
			// role tag wins. *time.Time is stamped too.
			switch sf.Name {
			case "CreatedAt":
				f.isCreated = true
			case "UpdatedAt":
				f.isUpdated = true
			}
		}
		if sf.Name == "ID" && !opts.pk && !opts.json && !opts.version && !opts.softDelete {
			idConv = f
		}
		f.noAutoIncr = opts.noAutoIncr

		if prev, dup := p.byColumn[f.column]; dup {
			errs = append(errs, fmt.Errorf("fields %s and %s map to the same column %q", prev.name, f.name, f.column))
			continue
		}
		p.fields = append(p.fields, f)
		p.byColumn[f.column] = f

		codec, err := codecFor(f)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		f.code = codec
	}

	// The ID primary-key convention survives role-neutral tags; it yields
	// only to an explicit pk tag or an incompatible role.
	if idConv != nil {
		explicit := false
		for _, f := range p.fields {
			if f.isPK {
				explicit = true
				break
			}
		}
		if !explicit {
			idConv.isPK = true
		}
	}
	return errors.Join(errs...)
}

// classify wires up the single-role fields and validates their types.
func (p *plan) classify() []error {
	var errs []error
	single := func(name string, cur, f *field) *field {
		if cur != nil {
			errs = append(errs, fmt.Errorf("fields %s and %s both declare %s", cur.name, f.name, name))
			return cur
		}
		return f
	}
	for _, f := range p.fields {
		if f.isVersion {
			switch f.typ.Kind() {
			case reflect.Int, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint32, reflect.Uint64:
				// wide enough: version = version + 1 forever
			case reflect.Int8, reflect.Int16, reflect.Uint8, reflect.Uint16:
				errs = append(errs, fmt.Errorf(
					"version field %s is %s, too narrow to count updates "+
						"(wraps at its maximum and then reports ErrStaleObject forever); use int64",
					f.name,
					f.typ,
				))
			default:
				errs = append(errs, fmt.Errorf("version field %s must be an integer type, got %s", f.name, f.typ))
			}
			p.version = single("version", p.version, f)
		}
		if f.isSoftDelete {
			if f.typ != timeType && f.typ != timePtrType {
				errs = append(errs, fmt.Errorf("softdelete field %s must be time.Time or *time.Time, got %s", f.name, f.typ))
			}
			p.softDel = single("softdelete", p.softDel, f)
		}
		if f.isCreated {
			p.created = single("CreatedAt", p.created, f)
		}
		if f.isUpdated {
			p.updated = single("UpdatedAt", p.updated, f)
		}
	}
	if len(p.pks) == 1 {
		pk := p.pks[0]
		if isIntKind(pk.typ.Kind()) && !pk.noAutoIncr {
			pk.isAutoIncr = true
			p.autoIncr = pk
		}
	}
	if p.version != nil && p.version.isPK {
		errs = append(errs, errors.New("the version column cannot be part of the primary key"))
	}
	for _, f := range p.fields {
		if f.isPK || f.isCreated || f.isVersion || f.isSoftDelete {
			// Softdelete stays out of updatable: a full-column Update would
			// write deleted_at back to NULL and resurrect the row.
			continue
		}
		p.updatable = append(p.updatable, f)
	}
	for _, f := range p.fields {
		if f.omitZero {
			p.hasOmitZero = true
		}
		if f.ordinal < 64 {
			p.allBits |= 1 << uint(f.ordinal)
		}
		if f != p.autoIncr {
			p.insCols = append(p.insCols, f)
		}
	}
	p.insBits = p.allBits
	if p.autoIncr != nil {
		p.insBack = []*field{p.autoIncr}
		if p.autoIncr.ordinal < 64 {
			p.insBits &^= 1 << uint(p.autoIncr.ordinal)
		}
	}
	return errs
}
func planOf[T any]() (*plan, error) {
	return planFor(reflect.TypeFor[T]())
}

func planFor(t reflect.Type) (*plan, error) {
	if v, ok := plans.Load(t); ok {
		if p, ok := v.(*plan); ok {
			return p, nil
		}
		return nil, v.(error)
	}
	p, err := buildPlan(t)
	if err != nil {
		plans.LoadOrStore(t, err)
		return nil, err
	}
	actual, _ := plans.LoadOrStore(t, p)
	if p, ok := actual.(*plan); ok {
		return p, nil
	}
	return nil, actual.(error)
}

func buildPlan(t reflect.Type) (*plan, error) {
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("rio: model must be a struct, got %s", t)
	}
	p := &plan{
		typ:          t,
		structName:   t.Name(),
		defaultTable: TableName(t.Name()),
		byColumn:     make(map[string]*field),
		rels:         make(map[string]*relField),
		counts:       make(map[string][]int),
	}
	if tn, ok := reflect.New(t).Interface().(TableNamer); ok {
		p.tableOverride = tn.TableName()
	}

	var errs []error
	if err := p.addFields(t); err != nil {
		errs = append(errs, err)
	}
	for i, f := range p.fields {
		f.ordinal = i
		if f.isPK {
			p.pks = append(p.pks, f)
		}
	}
	errs = append(errs, p.classify()...)
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("rio: invalid model %s: %w", t.Name(), err)
	}
	// Only valid models register: a rejected shape must not poison the hint.
	for name, rf := range p.rels {
		registerRelFieldName(t.FieldByIndex(rf.index).Type, name)
	}
	return p, nil
}

// collectFields gathers the raw field set depth-first; only tag syntax
// errors are reported here — the rest waits for shadowing resolution.
func collectFields(t reflect.Type, prefix []int, baseOffset uintptr, raw *[]rawField, errs *[]error) {
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			// Unexported embedded structs still promote exported fields (as
			// encoding/json does); genuinely private fields stay skipped.
			embeddedStruct := sf.Anonymous && (sf.Type.Kind() == reflect.Struct ||
				(sf.Type.Kind() == reflect.Pointer && sf.Type.Elem().Kind() == reflect.Struct))
			if !embeddedStruct {
				continue
			}
		}
		tag, opts, err := parseTag(sf)
		if err != nil {
			*errs = append(*errs, err)
			continue
		}
		index := append(append([]int(nil), prefix...), i)
		r := rawField{
			sf: sf, owner: t, index: index, offset: baseOffset + sf.Offset,
			tag: tag, opts: opts,

			// Anonymous value structs flatten unless a tag makes them a column;
			// the field still shadows, and is shadowed, by its Go name.
			flatten: sf.Anonymous && !opts.skip && tag == "" && !opts.json &&
				sf.Type.Kind() == reflect.Struct && sf.Type != timeType && !isRelContainer(sf.Type)}
		*raw = append(*raw, r)
		if r.flatten {
			collectFields(sf.Type, index, r.offset, raw, errs)
		}
	}
}

// roleOptName names the first role option set on a tag, "" when none is set.
func roleOptName(o tagOpts) string {
	switch {
	case o.pk:
		return "pk"
	case o.omitZero:
		return "omitzero"
	case o.version:
		return "version"
	case o.softDelete:
		return "softdelete"
	case o.noStamp:
		return "nostamp"
	case o.noAutoIncr:
		return "noautoincr"
	case o.fk != "":
		return "fk"
	case o.ref != "":
		return "ref"
	case o.join != "":
		return "join"
	case o.countOf != "":
		return "countof"
	}
	return ""
}

func parseTag(sf reflect.StructField) (column string, opts tagOpts, err error) {
	raw, ok := sf.Tag.Lookup("rio")
	if !ok {
		return "", tagOpts{}, nil
	}
	if raw == "-" {
		return "", tagOpts{skip: true}, nil
	}
	parts := strings.Split(raw, ",")
	column = parts[0]
	for _, part := range parts[1:] {
		switch {
		case part == "pk":
			opts.pk = true
		case part == "omitzero":
			opts.omitZero = true
		case part == "json":
			opts.json = true
		case part == "version":
			opts.version = true
		case part == "softdelete":
			opts.softDelete = true
		case part == "nostamp":
			opts.noStamp = true
		case part == "noautoincr":
			opts.noAutoIncr = true
		case strings.HasPrefix(part, "fk:"):
			opts.fk = part[len("fk:"):]
		case strings.HasPrefix(part, "ref:"):
			opts.ref = part[len("ref:"):]
		case strings.HasPrefix(part, "join:"):
			opts.join = part[len("join:"):]
		case strings.HasPrefix(part, "countof:"):
			opts.countOf = part[len("countof:"):]
		case part == "":
			// tolerated: `rio:"name,"`
		default:
			return "", opts, fmt.Errorf("field %s: unknown rio tag option %q", sf.Name, part)
		}
	}
	return column, opts, nil
}

func isIntKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}
