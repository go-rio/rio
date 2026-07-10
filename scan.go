package rio

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// fieldCodec is the per-field scan/bind strategy, decided once at plan time
// so row scanning does zero classification work.
type fieldCodec struct {
	kind          scanKind
	bits          int  // integer/float width for overflow checks
	bindValuer    bool // bind the field value itself so Value() runs
	bindPtrValuer bool // bind the field address so pointer-receiver Value() runs

	// elemKind/elemBits are scanPtr's element strategy, classified at plan
	// time like everything else: the per-cell path then allocates only the
	// *T cell itself instead of rebuilding scaffolding per row.
	elemKind scanKind
	elemBits int
}

type scanKind uint8

const (
	scanInt scanKind = iota // fast path: unsafe.Add + direct store
	scanUint
	scanFloat
	scanBool
	scanString
	scanBytes
	scanTime
	scanJSON    // slow path: reflect + encoding/json
	scanScanner // slow path: delegate to sql.Scanner, including NULL
	scanPtr     // *T: one reflect.New per non-NULL cell, element written via the plan-time elem codec
)

var (
	scannerType         = reflect.TypeFor[sql.Scanner]()
	nullTimeType        = reflect.TypeFor[sql.NullTime]()
	nullTimeGenericType = reflect.TypeFor[sql.Null[time.Time]]()
)

// codecFor classifies a field. Order is the documented priority chain:
// json tag > sql.Scanner > pointer > []byte > basics. Anything else is a
// plan-time error, not a runtime surprise.
func codecFor(f *field) (fieldCodec, error) {
	t := f.typ
	if f.jsonCol {
		return fieldCodec{kind: scanJSON}, nil
	}
	if t.Kind() == reflect.Interface {
		// An interface satisfying sql.Scanner would pass the Implements check
		// below, but a zero struct holds a nil interface: scanning has nothing
		// to Scan into, and a panic inside Rows.Scan wedges rows.Close forever.
		return fieldCodec{}, fmt.Errorf(
			"field %s: interface-typed fields cannot be mapped to a column; use a concrete type implementing sql.Scanner, or exclude it with `rio:\"-\"`",
			f.name)
	}
	if t.Implements(scannerType) || reflect.PointerTo(t).Implements(scannerType) {
		return fieldCodec{kind: scanScanner}, nil
	}
	if t.Kind() == reflect.Pointer {
		elem := t.Elem()
		ec, err := basicCodec(elem, f)
		if err != nil {
			return fieldCodec{}, fmt.Errorf("field %s: unsupported pointer type %s", f.name, t)
		}
		return fieldCodec{
			kind: scanPtr, bindValuer: t.Implements(valuerType),
			elemKind: ec.kind, elemBits: ec.bits,
		}, nil
	}
	c, err := basicCodec(t, f)
	if err != nil {
		return c, err
	}
	// A basic-kind type that customizes its stored form through driver.Valuer
	// (value receiver, so a bound value triggers it) must bind through Value(),
	// not the unsafe fast read that would hand the driver the raw underlying
	// value. Scan stays on the fast path: the column already holds the encoded
	// form. (Scanner types are handled above and bind correctly via reflect.)
	if t.Implements(valuerType) {
		c.bindValuer = true
	} else if reflect.PointerTo(t).Implements(valuerType) {
		c.bindPtrValuer = true
	}
	return c, nil
}

func basicCodec(t reflect.Type, f *field) (fieldCodec, error) {
	if t == timeType {
		return fieldCodec{kind: scanTime}, nil
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fieldCodec{kind: scanInt, bits: t.Bits()}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fieldCodec{kind: scanUint, bits: t.Bits()}, nil
	case reflect.Float32, reflect.Float64:
		return fieldCodec{kind: scanFloat, bits: t.Bits()}, nil
	case reflect.Bool:
		return fieldCodec{kind: scanBool}, nil
	case reflect.String:
		return fieldCodec{kind: scanString}, nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return fieldCodec{kind: scanBytes}, nil
		}
	}
	return fieldCodec{}, fmt.Errorf(
		"field %s: cannot map %s to a column; tag it `rio:\",json\"`, implement sql.Scanner, or exclude it with `rio:\"-\"`",
		f.name, t)
}

// colScanner adapts one field to sql.Rows.Scan. One instance per column per
// query, rebased per row — O(cols) allocations per query, not per row.
type colScanner struct {
	f    *field
	base unsafe.Pointer // start of the row's struct, set per row
}

func (s *colScanner) Scan(src any) error {
	f := s.f
	p := unsafe.Add(s.base, f.offset)

	if src == nil {
		switch f.code.kind {
		case scanScanner:
			return s.slowScanner(nil)
		case scanPtr:
			reflect.NewAt(f.typ, p).Elem().SetZero()
			return nil
		case scanBytes:
			*(*[]byte)(p) = nil
			return nil
		case scanJSON:
			// The write side stores SQL NULL for a nil *T json field; the
			// round trip must come back as nil, not as an error.
			if f.typ.Kind() == reflect.Pointer {
				reflect.NewAt(f.typ, p).Elem().SetZero()
				return nil
			}
		case scanTime:
			if f.isSoftDelete {
				// NULL means "not deleted": the softdelete tag opts this
				// column into the zero-time exception.
				*(*time.Time)(p) = time.Time{}
				return nil
			}
		}
		return fmt.Errorf("rio: column %q is NULL but field %s is %s; use a pointer, sql.Null, or a Scanner",
			f.column, f.name, f.typ)
	}

	// publish is the scanPtr hand-off: the case below allocates the element
	// cell, retargets p at it, swaps in the plan-time elem codec, and jumps
	// back — one switch body serves fields and pointer elements without a
	// per-cell function call on the plain path. The field slot is written
	// only after the element scanned cleanly, so a conversion error leaves
	// the struct untouched, exactly like every other kind.
	kind, bits := f.code.kind, f.code.bits
	var publish unsafe.Pointer

scan:
	switch kind {
	case scanInt:
		n, err := srcInt(src, f)
		if err != nil {
			return err
		}
		if bits < 64 && (n > int64(1)<<(bits-1)-1 || n < -int64(1)<<(bits-1)) {
			return fmt.Errorf("rio: column %q value %d overflows %s", f.column, n, f.typ)
		}
		switch bits {
		case 8:
			*(*int8)(p) = int8(n)
		case 16:
			*(*int16)(p) = int16(n)
		case 32:
			*(*int32)(p) = int32(n)
		default:
			*(*int64)(p) = n // int is 64-bit on every supported platform
		}
	case scanUint:
		n, err := srcUint(src, f)
		if err != nil {
			return err
		}
		if bits < 64 && n > uint64(1)<<bits-1 {
			return fmt.Errorf("rio: column %q value %d overflows %s", f.column, n, f.typ)
		}
		switch bits {
		case 8:
			*(*uint8)(p) = uint8(n)
		case 16:
			*(*uint16)(p) = uint16(n)
		case 32:
			*(*uint32)(p) = uint32(n)
		default:
			*(*uint64)(p) = n
		}
	case scanFloat:
		fl, err := srcFloat(src, f)
		if err != nil {
			return err
		}
		if bits == 32 {
			if !math.IsInf(fl, 0) && (fl > math.MaxFloat32 || fl < -math.MaxFloat32) {
				return fmt.Errorf("rio: column %q value overflows float32", f.column)
			}
			*(*float32)(p) = float32(fl)
		} else {
			*(*float64)(p) = fl
		}
	case scanBool:
		b, err := srcBool(src, f)
		if err != nil {
			return err
		}
		*(*bool)(p) = b
	case scanString:
		switch v := src.(type) {
		case string:
			*(*string)(p) = v
		case []byte:
			*(*string)(p) = string(v) // copies out of the driver buffer
		default:
			return convErr(f, src)
		}
	case scanBytes:
		switch v := src.(type) {
		case []byte:
			cp := make([]byte, len(v))
			copy(cp, v) // driver buffers are reused after the next Next()
			*(*[]byte)(p) = cp
		case string:
			*(*[]byte)(p) = []byte(v)
		default:
			return convErr(f, src)
		}
	case scanTime:
		t, err := srcTime(src, f)
		if err != nil {
			return err
		}
		*(*time.Time)(p) = t
	case scanJSON:
		var data []byte
		switch v := src.(type) {
		case []byte:
			data = v
		case string:
			data = []byte(v)
		default:
			return convErr(f, src)
		}
		dst := reflect.NewAt(f.typ, p)
		if err := json.Unmarshal(data, dst.Interface()); err != nil {
			return fmt.Errorf("rio: column %q: decoding JSON into %s: %w", f.column, f.typ, err)
		}
	case scanScanner:
		return s.slowScanner(src)
	case scanPtr:
		// The element was classified at plan time (codecFor): allocate the
		// cell — the one allocation *T semantics requires — and re-dispatch
		// on the elem codec, instead of rebuilding field/colScanner
		// scaffolding and re-deriving the codec on every row. basicCodec
		// never yields scanPtr, so this jump cannot recurse.
		cell := reflect.New(f.typ.Elem())
		publish, p = p, cell.UnsafePointer()
		kind, bits = f.code.elemKind, f.code.elemBits
		goto scan
	default:
		return convErr(f, src)
	}
	if publish != nil {
		*(*unsafe.Pointer)(publish) = p
	}
	return nil
}

func (s *colScanner) slowScanner(src any) error {
	f := s.f
	p := unsafe.Add(s.base, f.offset)
	v := reflect.NewAt(f.typ, p)
	if sc, ok := v.Interface().(sql.Scanner); ok {
		return sc.Scan(src)
	}
	if f.typ.Kind() == reflect.Pointer {
		// The field itself is a pointer to a Scanner (*sql.NullString):
		// scanning through a zero struct means the pointer is nil, so
		// allocate before delegating — and map NULL back to nil.
		elem := v.Elem()
		if src == nil {
			elem.SetZero()
			return nil
		}
		if elem.IsNil() {
			elem.Set(reflect.New(f.typ.Elem()))
		}
		return elem.Interface().(sql.Scanner).Scan(src)
	}
	// The value type implements Scanner directly (rare, value receiver).
	// comma-ok, not a bare assertion: a panic here happens while database/sql
	// holds closemu.RLock, turning it into a permanently blocked rows.Close.
	sc, ok := v.Elem().Interface().(sql.Scanner)
	if !ok {
		return fmt.Errorf("rio: column %q: field %s (%s) does not implement sql.Scanner as a value", f.column, f.name, f.typ)
	}
	return sc.Scan(src)
}

func convErr(f *field, src any) error {
	return fmt.Errorf("rio: column %q: cannot convert %T into field %s (%s)", f.column, src, f.name, f.typ)
}

func srcInt(src any, f *field) (int64, error) {
	switch v := src.(type) {
	case int64:
		return v, nil
	case []byte:
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, convErr(f, src)
		}
		return n, nil
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, convErr(f, src)
		}
		return n, nil
	}
	return 0, convErr(f, src)
}

func srcUint(src any, f *field) (uint64, error) {
	switch v := src.(type) {
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("rio: column %q value %d is negative but field %s is unsigned", f.column, v, f.name)
		}
		return uint64(v), nil
	case []byte:
		// MySQL delivers BIGINT UNSIGNED above MaxInt64 as bytes.
		n, err := strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return 0, convErr(f, src)
		}
		return n, nil
	case string:
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, convErr(f, src)
		}
		return n, nil
	}
	return 0, convErr(f, src)
}

func srcFloat(src any, f *field) (float64, error) {
	switch v := src.(type) {
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case []byte:
		fl, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return 0, convErr(f, src)
		}
		return fl, nil
	case string:
		fl, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, convErr(f, src)
		}
		return fl, nil
	}
	return 0, convErr(f, src)
}

func srcBool(src any, f *field) (bool, error) {
	switch v := src.(type) {
	case bool:
		return v, nil
	case int64:
		return v != 0, nil
	case []byte:
		return parseBool(string(v), f)
	case string:
		return parseBool(v, f)
	}
	return false, convErr(f, src)
}

func parseBool(s string, f *field) (bool, error) {
	switch s {
	case "0", "false", "FALSE", "f":
		return false, nil
	case "1", "true", "TRUE", "t":
		return true, nil
	}
	return false, convErr(f, s)
}

// timeFormats are the accepted text encodings, most specific first. Formats
// without an offset are interpreted as UTC — rio always writes UTC.
var timeFormats = []string{
	"2006-01-02 15:04:05.999999999-07:00",
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02",
}

func srcTime(src any, f *field) (time.Time, error) {
	switch v := src.(type) {
	case time.Time:
		return v, nil
	case []byte:
		return parseTime(string(v), f)
	case string:
		return parseTime(v, f)
	}
	return time.Time{}, convErr(f, src)
}

func parseTime(s string, f *field) (time.Time, error) {
	for _, layout := range timeFormats {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("rio: column %q: cannot parse %q as time.Time", f.column, s)
}

// rowScanner scans consecutive rows into values of one plan. dests and cells
// are allocated once and rebased per row.
type rowScanner struct {
	fields []*field
	cells  []colScanner
	dests  []any
}

// newRowScanner returns by value: the struct stays on the caller's stack
// (only cells and dests live on the heap — two allocations per query, and
// the cell pointers inside dests keep working across the copy because slice
// headers share their backing arrays).
func newRowScanner(fields []*field, extras []any) rowScanner {
	rs := rowScanner{
		fields: fields,
		cells:  make([]colScanner, len(fields)),
		dests:  make([]any, 0, len(fields)+len(extras)),
	}
	for i, f := range fields {
		rs.cells[i].f = f
		rs.dests = append(rs.dests, &rs.cells[i])
	}
	rs.dests = append(rs.dests, extras...)
	return rs
}

func (rs *rowScanner) scan(rows *sql.Rows, base unsafe.Pointer) error {
	for i := range rs.cells {
		rs.cells[i].base = base
	}
	return rows.Scan(rs.dests...)
}

// entityFields verifies the result set matches the plan's column order — the
// last line of defense against schema drift between render and execution.
func entityFields(rows *sql.Rows, p *plan, extras int) ([]*field, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(cols) != len(p.fields)+extras {
		return nil, fmt.Errorf("rio: %s: expected %d columns, result has %d", p.structName, len(p.fields)+extras, len(cols))
	}
	for i, f := range p.fields {
		if cols[i] != f.column {
			return nil, fmt.Errorf("rio: %s: column %d is %q, expected %q", p.structName, i, cols[i], f.column)
		}
	}
	return p.fields, nil
}

// namedFields maps result columns to plan fields by name (Raw queries).
// Unknown columns are an error — silently dropping data is how schema drift
// hides — and so are missing ones: a partially scanned entity handed to
// Update would overwrite the unselected columns with zero values. Partial
// projections belong in DTO types, whose plan the result then covers fully.
func namedFields(rows *sql.Rows, p *plan) ([]*field, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	fields := make([]*field, len(cols))
	seen := make(map[string]bool, len(cols))
	for i, c := range cols {
		f, ok := p.byColumn[c]
		if !ok {
			return nil, fmt.Errorf("rio: no field of %s maps to result column %q", p.structName, c)
		}
		if seen[c] {
			return nil, fmt.Errorf("rio: result column %q appears more than once for %s; use distinct aliases", c, p.structName)
		}
		fields[i] = f
		seen[c] = true
	}
	var missing []string
	for _, f := range p.fields {
		if !seen[f.column] {
			missing = append(missing, f.column)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf(
			"rio: result covers only part of %s (missing %s); a partial entity risks zeroed-out writes — scan into a DTO type instead",
			p.structName, strings.Join(missing, ", "))
	}
	return fields, nil
}

// scanAll drains rows into a []T. byName selects Raw-style column matching;
// entity queries use plan order.
func scanAll[T any](rows *sql.Rows, p *plan, byName bool) ([]T, error) {
	defer rows.Close()
	var (
		fields []*field
		err    error
	)
	if byName {
		fields, err = namedFields(rows, p)
	} else {
		fields, err = entityFields(rows, p, 0)
	}
	if err != nil {
		return nil, err
	}
	rs := newRowScanner(fields, nil)
	out := []T{}
	for rows.Next() {
		out = append(out, *new(T))
		if err := rs.scan(rows, unsafe.Pointer(&out[len(out)-1])); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// scanScalars drains a single-column result into basic values. The codec is
// classified once for the whole result, not per row.
func scanScalars[T any](rows *sql.Rows) ([]T, error) {
	defer rows.Close()
	t := reflect.TypeFor[T]()
	f := &field{name: t.String(), column: "<scalar>", typ: t}
	codec, err := codecFor(f)
	if err != nil {
		return nil, err
	}
	f.code = codec
	cs := colScanner{f: f}
	out := []T{}
	for rows.Next() {
		out = append(out, *new(T))
		cs.base = unsafe.Pointer(&out[len(out)-1])
		if err := rows.Scan(&cs); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// isScalarType reports whether T scans as a single column rather than a
// struct row. time.Time is a struct but scans as one column.
func isScalarType(t reflect.Type) bool {
	if t == timeType || t == timePtrType {
		return true
	}
	if t.Implements(scannerType) || reflect.PointerTo(t).Implements(scannerType) {
		return true
	}
	k := t.Kind()
	if k == reflect.Pointer {
		return isScalarType(t.Elem())
	}
	return k != reflect.Struct
}

// binder is one write call's binding context: the dialect plus the call's
// clock instant with its rendered bind value memoized. Stamped columns all
// carry the same now, so SQLite's text encoding and the interface boxing
// happen once per call instead of once per stamped column — batch paths
// would otherwise re-render the identical instant on every row. Callers
// build it on the stack; zero now (no stamps in play) still memoizes
// correctly because the memo is keyed by value equality.
type binder struct {
	d       Dialect
	now     time.Time // normalized clock instant, zero when the call stamps nothing
	nowBind any       // lazily rendered d.bindTime(now)
}

func (b *binder) time(nt time.Time) any {
	if nt == b.now {
		if b.nowBind == nil {
			b.nowBind = b.d.bindTime(nt)
		}
		return b.nowBind
	}
	return b.d.bindTime(nt)
}

// bindArgFast extracts a bind value through the field's offset, skipping the
// reflect.Value round-trip for fixed-layout kinds. ok=false falls back to
// bindArg. The same discipline as the scan fast path applies: offsets only
// cross value-embedded structs, never pointers.
func bindArgFast(f *field, base unsafe.Pointer, b *binder) (any, bool, error) {
	p := unsafe.Add(base, f.offset)
	switch f.code.kind {
	case scanInt:
		switch f.code.bits {
		case 8:
			return int64(*(*int8)(p)), true, nil
		case 16:
			return int64(*(*int16)(p)), true, nil
		case 32:
			return int64(*(*int32)(p)), true, nil
		default:
			return *(*int64)(p), true, nil
		}
	case scanUint:
		var n uint64
		switch f.code.bits {
		case 8:
			n = uint64(*(*uint8)(p))
		case 16:
			n = uint64(*(*uint16)(p))
		case 32:
			n = uint64(*(*uint32)(p))
		default:
			n = *(*uint64)(p)
		}
		if n > math.MaxInt64 {
			v, err := bindOverflowUint(b.d, n)
			return v, true, err
		}
		return int64(n), true, nil
	case scanFloat:
		if f.code.bits == 32 {
			return float64(*(*float32)(p)), true, nil
		}
		return *(*float64)(p), true, nil
	case scanBool:
		return *(*bool)(p), true, nil
	case scanString:
		return *(*string)(p), true, nil
	case scanBytes:
		b := *(*[]byte)(p)
		if b == nil {
			return nil, true, nil
		}
		return b, true, nil
	case scanTime:
		t := *(*time.Time)(p)
		if t.IsZero() && f.isSoftDelete {
			return nil, true, nil
		}
		nt := normalizeTime(t)
		if nt != t {
			// Write the normalized form back (representation compare — the
			// instant is unchanged): the struct then holds exactly what the
			// database stores, so insert-then-reload compares Equal even for
			// caller-provided nanosecond or zoned times.
			*(*time.Time)(p) = nt
		}
		return b.time(nt), true, nil
	}
	return nil, false, nil
}

// zeroFast reports whether the field is zero, through the offset when the
// layout allows. ok=false falls back to reflect.
func zeroFast(f *field, base unsafe.Pointer) (isZero, ok bool) {
	p := unsafe.Add(base, f.offset)
	switch f.code.kind {
	case scanInt:
		switch f.code.bits {
		case 8:
			return *(*int8)(p) == 0, true
		case 16:
			return *(*int16)(p) == 0, true
		case 32:
			return *(*int32)(p) == 0, true
		default:
			return *(*int64)(p) == 0, true
		}
	case scanUint:
		switch f.code.bits {
		case 8:
			return *(*uint8)(p) == 0, true
		case 16:
			return *(*uint16)(p) == 0, true
		case 32:
			return *(*uint32)(p) == 0, true
		default:
			return *(*uint64)(p) == 0, true
		}
	case scanFloat:
		if f.code.bits == 32 {
			return *(*float32)(p) == 0, true
		}
		return *(*float64)(p) == 0, true
	case scanBool:
		return !*(*bool)(p), true
	case scanString:
		return *(*string)(p) == "", true
	case scanBytes:
		return *(*[]byte)(p) == nil, true
	case scanTime:
		return (*time.Time)(p).IsZero(), true
	}
	return false, false
}

// fieldValue binds one field, fast path first. A driver.Valuer basic type
// skips the fast path so bindArg hands the driver the value itself, letting
// Value() run.
func fieldValue(f *field, base unsafe.Pointer, rv reflect.Value, b *binder) (any, error) {
	if !f.code.bindValuer && !f.code.bindPtrValuer {
		if a, ok, err := bindArgFast(f, base, b); ok {
			return a, err
		}
	}
	return bindArg(f, rv.FieldByIndex(f.index), b)
}

// fieldIsZero checks one field, fast path first.
func fieldIsZero(f *field, base unsafe.Pointer, rv reflect.Value) bool {
	if z, ok := zeroFast(f, base); ok {
		return z
	}
	return rv.FieldByIndex(f.index).IsZero()
}

// scanOne scans exactly one row into a fresh T, the Find fast path.
func scanOne[T any](rows *sql.Rows, p *plan) (*T, error) {
	defer rows.Close()
	fields, err := entityFields(rows, p, 0)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, ErrNotFound
	}
	out := new(T)
	rs := newRowScanner(fields, nil)
	if err := rs.scan(rows, unsafe.Pointer(out)); err != nil {
		return nil, err
	}
	return out, rows.Err()
}

// bindArg converts a field value into a driver-facing argument.
func bindArg(f *field, v reflect.Value, b *binder) (any, error) {
	if f.jsonCol {
		if v.Kind() == reflect.Pointer && v.IsNil() {
			return nil, nil // a nil *T stores SQL NULL, not the string "null"
		}
		data, err := json.Marshal(v.Interface())
		if err != nil {
			return nil, fmt.Errorf("rio: field %s: encoding JSON: %w", f.name, err)
		}
		return data, nil
	}
	if f.code.bindValuer {
		// Hand the driver the value itself so its Value() runs; skip the
		// numeric/time normalization below, which would strip the type.
		if v.Kind() == reflect.Pointer && v.IsNil() {
			return nil, nil
		}
		return v.Interface(), nil
	}
	if f.code.bindPtrValuer && v.CanAddr() {
		return v.Addr().Interface(), nil
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, nil
		}
		v = v.Elem()
	}
	// Mirror normalizeArgs: left to the driver's Valuer path, the inner time
	// would skip microsecond truncation and rio's SQLite text encoding —
	// the same field would then store differently under Insert and Upsert.
	// As on the fast path, a changed normalization is written back so the
	// struct holds exactly what the database stores.
	if v.Type() == nullTimeType {
		if nv := v.Interface().(sql.NullTime); nv.Valid {
			nt := normalizeTime(nv.Time)
			if nt != nv.Time && v.CanSet() {
				v.Set(reflect.ValueOf(sql.NullTime{Time: nt, Valid: true}))
			}
			return b.time(nt), nil
		}
		return nil, nil
	}
	if v.Type() == nullTimeGenericType {
		if nv := v.Interface().(sql.Null[time.Time]); nv.Valid {
			nt := normalizeTime(nv.V)
			if nt != nv.V && v.CanSet() {
				v.Set(reflect.ValueOf(sql.Null[time.Time]{V: nt, Valid: true}))
			}
			return b.time(nt), nil
		}
		return nil, nil
	}
	if v.Type() == timeType {
		t := v.Interface().(time.Time)
		if t.IsZero() && f.isSoftDelete {
			return nil, nil // zero time on the softdelete column stores NULL
		}
		nt := normalizeTime(t)
		if nt != t && v.CanSet() {
			v.Set(reflect.ValueOf(nt))
		}
		return b.time(nt), nil
	}
	if isUintKind(v.Kind()) {
		if n := v.Uint(); n > math.MaxInt64 {
			return bindOverflowUint(b.d, n)
		}
	}
	return v.Interface(), nil
}

// bindOverflowUint binds a uint64 whose high bit is set. database/sql refuses
// it, so MySQL (BIGINT UNSIGNED) and PostgreSQL (numeric) take the decimal
// literal as a string. SQLite has no unsigned 64-bit integer: an INTEGER
// column silently coerces the oversized literal to REAL and loses precision,
// so rio fails loudly there instead of corrupting the row.
func bindOverflowUint(d Dialect, n uint64) (any, error) {
	if d.name() == "sqlite" {
		return nil, fmt.Errorf("rio: value %d exceeds SQLite's signed 64-bit integer range and cannot be stored losslessly; use a TEXT column or a signed type", n)
	}
	return strconv.FormatUint(n, 10), nil
}

// normalizeTime is the single write-side time rule: UTC, no monotonic
// reading, microsecond precision (the ceiling shared by PG and MySQL —
// nanoseconds would make insert-then-reload comparisons fail forever).
func normalizeTime(t time.Time) time.Time {
	return t.UTC().Round(0).Truncate(time.Microsecond)
}
