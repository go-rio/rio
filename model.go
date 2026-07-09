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

// plan is the immutable mapping of one struct type. Plans are built once,
// cached forever, and shared by every DB handle; nothing grammar-dependent
// (table names as rendered SQL) lives here.
type plan struct {
	typ           reflect.Type
	structName    string
	tableOverride string // from TableName(), "" otherwise
	defaultTable  string // convention-derived, pre-computed

	fields    []*field
	byColumn  map[string]*field
	pks       []*field
	updatable []*field // full-column Update set, in field order
	autoIncr  *field
	version   *field
	softDel   *field
	created   *field
	updated   *field

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

// relField is a relation declaration. Target plan and key resolution are
// deferred to first use: eager resolution would recurse forever on mutually
// referencing models (User ↔ Post).
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

var plans sync.Map // reflect.Type → *plan | error

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
		defaultTable: tableName(t.Name()),
		byColumn:     make(map[string]*field),
		rels:         make(map[string]*relField),
		counts:       make(map[string][]int),
	}
	if tn, ok := reflect.New(t).Interface().(TableNamer); ok {
		p.tableOverride = tn.TableName()
	}

	var errs []error
	if err := p.addFields(t, nil, 0); err != nil {
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
	return p, nil
}

func (p *plan) addFields(t reflect.Type, prefix []int, baseOffset uintptr) error {
	var errs []error
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			// An embedded struct promotes its exported fields even when the
			// embedded type's own name is unexported — encoding/json flattens
			// these too, and silently dropping mapped columns is exactly the
			// surprise rio refuses. Genuinely private fields stay skipped.
			embeddedStruct := sf.Anonymous && (sf.Type.Kind() == reflect.Struct ||
				(sf.Type.Kind() == reflect.Pointer && sf.Type.Elem().Kind() == reflect.Struct))
			if !embeddedStruct {
				continue
			}
		}
		tag, opts, err := parseTag(sf)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if opts.skip {
			continue
		}
		index := append(append([]int(nil), prefix...), i)
		offset := baseOffset + sf.Offset

		if isRelContainer(sf.Type) {
			kind, target := containerInfo(sf.Type)
			if tag != "" {
				errs = append(errs, fmt.Errorf("field %s: relation containers take no column name", sf.Name))
				continue
			}
			p.rels[sf.Name] = &relField{
				name: sf.Name, kind: kind, index: index, target: target,
				fkTag: opts.fk, refTag: opts.ref, joinTag: opts.join,
			}
			p.relNames = append(p.relNames, sf.Name)
			continue
		}
		if opts.countOf != "" {
			// A count target is populated by WithCount, never mapped to a
			// column of its own.
			if sf.Type.Kind() != reflect.Int64 {
				errs = append(errs, fmt.Errorf("field %s: countof targets must be int64, got %s", sf.Name, sf.Type))
				continue
			}
			p.counts[opts.countOf] = index
			continue
		}
		if opts.fk != "" || opts.ref != "" || opts.join != "" {
			errs = append(errs, fmt.Errorf("field %s: fk/ref/join apply only to relation containers", sf.Name))
			continue
		}

		// Anonymous value structs flatten into the parent unless a tag makes
		// them a column. Pointer embedding would break offset-based scanning
		// (nil hop) and is refused.
		if sf.Anonymous && tag == "" && !opts.json && sf.Type.Kind() == reflect.Struct && sf.Type != timeType {
			if err := p.addFields(sf.Type, index, offset); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if sf.Anonymous && sf.Type.Kind() == reflect.Pointer {
			errs = append(errs, fmt.Errorf("field %s: embed the struct by value, not by pointer", sf.Name))
			continue
		}

		f := &field{
			name:   sf.Name,
			column: tag,
			index:  index,
			offset: offset,
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
			// The CreatedAt/UpdatedAt convention is name-based, so an explicit
			// role tag wins: a field a user deliberately tagged softdelete is
			// not also the updated_at stamp just because it is named UpdatedAt.
			// *time.Time is accepted like softdelete — setTime/stampForInsert
			// maintain the pointer form, so it must not silently go unstamped.
			switch sf.Name {
			case "CreatedAt":
				f.isCreated = true
			case "UpdatedAt":
				f.isUpdated = true
			}
		}
		if sf.Name == "ID" && !opts.tagged {
			f.isPK = true
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
			if !isIntKind(f.typ.Kind()) {
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
		if f.isPK || f.isCreated || f.isVersion {
			continue
		}
		p.updatable = append(p.updatable, f)
	}
	return errs
}

type tagOpts struct {
	skip       bool
	tagged     bool // any rio tag present (an explicitly tagged ID is not auto-PK)
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

func parseTag(sf reflect.StructField) (column string, opts tagOpts, err error) {
	raw, ok := sf.Tag.Lookup("rio")
	if !ok {
		return "", tagOpts{}, nil
	}
	if raw == "-" {
		return "", tagOpts{skip: true}, nil
	}
	opts.tagged = true
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

var (
	timeType    = reflect.TypeFor[time.Time]()
	timePtrType = reflect.TypeFor[*time.Time]()
)

func isIntKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}
