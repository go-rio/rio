package rio

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
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

	// elemSlice/elemSize serve scanPtr's chunked cell allocator: []T for
	// reflect.MakeSlice and T's byte size for slot arithmetic.
	elemSlice reflect.Type
	elemSize  int
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
		c := fieldCodec{kind: scanScanner}
		// A Scanner type that also customizes its stored form through
		// driver.Valuer needs the same treatment as the basic kinds below:
		// binding the bare value would only run a value-receiver Value() —
		// database/sql never takes an address — so a pointer-receiver Valuer
		// would silently store the raw underlying value. The two null-time
		// types are excluded: rio owns their encoding (bindArg normalizes and
		// dialect-encodes the inner time), and their value-receiver Value()
		// must not preempt that.
		if t != nullTimeType && t != nullTimeGenericType {
			if t.Implements(valuerType) {
				c.bindValuer = true
			} else if reflect.PointerTo(t).Implements(valuerType) {
				c.bindPtrValuer = true
			}
		}
		return c, nil
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
			elemSlice: reflect.SliceOf(elem), elemSize: int(elem.Size()),
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
	// form. (Scanner types get the same flags in their branch above.)
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

	// scanPtr cell chunk: non-NULL *T cells come out of a shared per-column
	// backing array (1, 4, 16, 64, then 128-cell chunks) instead of one
	// reflect.New per cell — a single row still pays exactly one minimal
	// allocation, a hundred rows pay five per column. Each cell remains an
	// independent slot (rescans hand out a fresh one, writes never alias);
	// the one observable consequence is lifetime: a surviving *T keeps its
	// chunk — at most 128 cells, never the whole column — alive. chunk is an
	// unsafe.Pointer field, so the GC sees the backing array while cells are
	// being handed out; afterwards the published pointers themselves keep it
	// alive.
	chunk unsafe.Pointer
	used  int // cells handed out of the current chunk
	csize int // current chunk capacity, in cells
}

// nextCell returns a zeroed slot for one non-NULL scanPtr cell, growing the
// chunk when exhausted.
func (s *colScanner) nextCell() unsafe.Pointer {
	if s.used == s.csize {
		n := s.csize * 4
		if n == 0 {
			n = 1
		} else if n > 128 {
			n = 128
		}
		s.chunk = reflect.MakeSlice(s.f.code.elemSlice, n, n).UnsafePointer()
		s.used, s.csize = 0, n
	}
	p := unsafe.Add(s.chunk, s.used*s.f.code.elemSize)
	s.used++
	return p
}

func (s *colScanner) Scan(src any) error {
	if src == nil {
		return s.scanNull()
	}
	f := s.f
	p := unsafe.Add(s.base, f.offset)

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
		if err := storeInt(f, p, bits, n); err != nil {
			return err
		}
	case scanUint:
		n, err := srcUint(src, f)
		if err != nil {
			return err
		}
		if err := storeUint(f, p, bits, n); err != nil {
			return err
		}
	case scanFloat:
		fl, err := srcFloat(src, f)
		if err != nil {
			return err
		}
		if err := storeFloat(f, p, bits, fl); err != nil {
			return err
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
			storeBytes(p, v)
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
		if err := storeJSON(f, p, data); err != nil {
			return err
		}
	case scanScanner:
		return s.slowScanner(src)
	case scanPtr:
		// The element was classified at plan time (codecFor): take a fresh
		// cell from the column's chunk — the allocation *T semantics
		// requires, amortized — and re-dispatch on the elem codec, instead
		// of rebuilding field/colScanner scaffolding and re-deriving the
		// codec on every row. basicCodec never yields scanPtr, so this jump
		// cannot recurse.
		publish, p = p, s.nextCell()
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

// scanNull applies the NULL rules for one cell. Scan(nil) and SetNull are
// both this function, so the two channels' NULL semantics — pointer nil-out,
// bytes nil, nil *T for json, the softdelete zero-time exception, and the
// error text everything else gets — cannot drift.
func (s *colScanner) scanNull() error {
	f := s.f
	p := unsafe.Add(s.base, f.offset)
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

// The store helpers below write one decoded value into a field slot, with
// the width and overflow rules attached. Scan (fed by the src converters)
// and the NativeCell typed sinks both run these same functions, so the two
// channels' storage semantics are equal by construction, not by test alone.
// Error construction lives in out-of-line funcs to keep the helpers within
// the inlining budget — the hot path stays call-free, as before the split.

func storeInt(f *field, p unsafe.Pointer, bits int, n int64) error {
	if bits == 64 { // int64 and int, the dominant widths: stays inlined
		*(*int64)(p) = n
		return nil
	}
	return storeIntNarrow(f, p, bits, n)
}

func storeIntNarrow(f *field, p unsafe.Pointer, bits int, n int64) error {
	if n > int64(1)<<(bits-1)-1 || n < -int64(1)<<(bits-1) {
		return intOverflowErr(f, n)
	}
	switch bits {
	case 8:
		*(*int8)(p) = int8(n)
	case 16:
		*(*int16)(p) = int16(n)
	default:
		*(*int32)(p) = int32(n)
	}
	return nil
}

func storeUint(f *field, p unsafe.Pointer, bits int, n uint64) error {
	if bits == 64 {
		*(*uint64)(p) = n
		return nil
	}
	return storeUintNarrow(f, p, bits, n)
}

func storeUintNarrow(f *field, p unsafe.Pointer, bits int, n uint64) error {
	if n > uint64(1)<<bits-1 {
		return uintOverflowErr(f, n)
	}
	switch bits {
	case 8:
		*(*uint8)(p) = uint8(n)
	case 16:
		*(*uint16)(p) = uint16(n)
	default:
		*(*uint32)(p) = uint32(n)
	}
	return nil
}

func storeFloat(f *field, p unsafe.Pointer, bits int, fl float64) error {
	if bits == 64 {
		*(*float64)(p) = fl
		return nil
	}
	return storeFloat32(f, p, fl)
}

func storeFloat32(f *field, p unsafe.Pointer, fl float64) error {
	if !math.IsInf(fl, 0) && (fl > math.MaxFloat32 || fl < -math.MaxFloat32) {
		return fmt.Errorf("rio: column %q value overflows float32", f.column)
	}
	*(*float32)(p) = float32(fl)
	return nil
}

func storeBytes(p unsafe.Pointer, v []byte) {
	cp := make([]byte, len(v))
	copy(cp, v) // driver buffers are reused after the next Next()
	*(*[]byte)(p) = cp
}

func storeJSON(f *field, p unsafe.Pointer, data []byte) error {
	dst := reflect.NewAt(f.typ, p)
	if err := json.Unmarshal(data, dst.Interface()); err != nil {
		return fmt.Errorf("rio: column %q: decoding JSON into %s: %w", f.column, f.typ, err)
	}
	return nil
}

func intOverflowErr(f *field, n int64) error {
	return fmt.Errorf("rio: column %q value %d overflows %s", f.column, n, f.typ)
}

func uintOverflowErr(f *field, n uint64) error {
	return fmt.Errorf("rio: column %q value %d overflows %s", f.column, n, f.typ)
}

// uintFromInt64 is the negative-value gate shared by srcUint's int64 arm and
// SetInt64's unsigned arm.
func uintFromInt64(f *field, v int64) (uint64, error) {
	if v < 0 {
		return 0, negativeUintErr(f, v)
	}
	return uint64(v), nil
}

func negativeUintErr(f *field, v int64) error {
	return fmt.Errorf("rio: column %q value %d is negative but field %s is unsigned", f.column, v, f.name)
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

// --- NativeCell: the typed sinks of the native channel ---
//
// Each Set method is its Scan(any) equivalent with the interface boxing
// removed: SetInt64(v) ≡ Scan(int64(v)), SetNull() ≡ Scan(nil) — the same
// conversion rules through the same store helpers, so equivalence holds by
// construction. Only the arms a native driver actually routes (matching kind,
// or the text forms of the numeric kinds) are boxing-free; mismatched kinds
// and Scanner fields fall back through boxed slow paths, exactly the calls
// that are slow or failing on the stdlib channel too.

var _ NativeCell = (*colScanner)(nil)

// ScanKind reports the cell's plan-time scan strategy so a NativeRows
// implementation can pick its typed decode path per column. Pointer fields
// report the element's kind: the sinks allocate and publish the *T cell
// internally, and SetNull stores nil — pointer-ness never crosses the SPI.
func (s *colScanner) ScanKind() NativeScanKind {
	k := s.f.code.kind
	if k == scanPtr {
		k = s.f.code.elemKind
	}
	switch k {
	case scanInt:
		return NativeKindInt
	case scanUint:
		return NativeKindUint
	case scanFloat:
		return NativeKindFloat
	case scanBool:
		return NativeKindBool
	case scanString:
		return NativeKindString
	case scanBytes:
		return NativeKindBytes
	case scanTime:
		return NativeKindTime
	case scanJSON:
		return NativeKindJSON
	default:
		return NativeKindScanner
	}
}

// sinkTarget resolves one typed store's destination: the field slot, or —
// for pointer fields — a fresh cell from the column's chunk, with the field
// slot returned as the publish target. Callers publish only after a clean
// store; a conversion error leaves the struct untouched (Scan's discipline,
// including the consumed cell).
func (s *colScanner) sinkTarget() (p, publish unsafe.Pointer, kind scanKind, bits int) {
	f := s.f
	p = unsafe.Add(s.base, f.offset)
	kind, bits = f.code.kind, f.code.bits
	if kind == scanPtr {
		publish, p = p, s.nextCell()
		kind, bits = f.code.elemKind, f.code.elemBits
	}
	return p, publish, kind, bits
}

// sinkSlow is the boxed tail of the typed sinks: Scanner fields delegate the
// value exactly as Scan would, and every impossible conversion reports the
// same error the stdlib channel does.
func (s *colScanner) sinkSlow(src any) error {
	if s.f.code.kind == scanScanner {
		return s.slowScanner(src)
	}
	return convErr(s.f, src)
}

func (s *colScanner) SetInt64(v int64) error {
	p, publish, kind, bits := s.sinkTarget()
	var err error
	switch kind {
	case scanInt:
		err = storeInt(s.f, p, bits, v)
	case scanUint:
		var n uint64
		if n, err = uintFromInt64(s.f, v); err == nil {
			err = storeUint(s.f, p, bits, n)
		}
	case scanFloat:
		err = storeFloat(s.f, p, bits, float64(v))
	case scanBool:
		*(*bool)(p) = v != 0
	default:
		return s.sinkSlow(v)
	}
	if err != nil || publish == nil {
		return err
	}
	*(*unsafe.Pointer)(publish) = p
	return nil
}

func (s *colScanner) SetFloat64(v float64) error {
	p, publish, kind, bits := s.sinkTarget()
	if kind != scanFloat {
		return s.sinkSlow(v)
	}
	if err := storeFloat(s.f, p, bits, v); err != nil {
		return err
	}
	if publish != nil {
		*(*unsafe.Pointer)(publish) = p
	}
	return nil
}

func (s *colScanner) SetBool(v bool) error {
	p, publish, kind, _ := s.sinkTarget()
	if kind != scanBool {
		return s.sinkSlow(v)
	}
	*(*bool)(p) = v
	if publish != nil {
		*(*unsafe.Pointer)(publish) = p
	}
	return nil
}

func (s *colScanner) SetString(v string) error {
	p, publish, kind, bits := s.sinkTarget()
	var err error
	switch kind {
	case scanString:
		*(*string)(p) = v
	case scanBytes:
		*(*[]byte)(p) = []byte(v)
	case scanJSON:
		err = storeJSON(s.f, p, []byte(v))
	case scanInt:
		var n int64
		if n, err = srcInt(v, s.f); err == nil {
			err = storeInt(s.f, p, bits, n)
		}
	case scanUint:
		var n uint64
		if n, err = srcUint(v, s.f); err == nil {
			err = storeUint(s.f, p, bits, n)
		}
	case scanFloat:
		var fl float64
		if fl, err = srcFloat(v, s.f); err == nil {
			err = storeFloat(s.f, p, bits, fl)
		}
	case scanBool:
		var b bool
		if b, err = srcBool(v, s.f); err == nil {
			*(*bool)(p) = b
		}
	case scanTime:
		var t time.Time
		if t, err = srcTime(v, s.f); err == nil {
			*(*time.Time)(p) = t
		}
	default:
		return s.sinkSlow(v)
	}
	if err != nil || publish == nil {
		return err
	}
	*(*unsafe.Pointer)(publish) = p
	return nil
}

func (s *colScanner) SetBytes(v []byte) error {
	p, publish, kind, bits := s.sinkTarget()
	var err error
	switch kind {
	case scanBytes:
		storeBytes(p, v) // copies: v is driver memory
	case scanString:
		*(*string)(p) = string(v)
	case scanJSON:
		err = storeJSON(s.f, p, v) // Unmarshal never retains its input
	case scanInt:
		var n int64
		if n, err = srcInt(v, s.f); err == nil {
			err = storeInt(s.f, p, bits, n)
		}
	case scanUint:
		var n uint64
		if n, err = srcUint(v, s.f); err == nil {
			err = storeUint(s.f, p, bits, n)
		}
	case scanFloat:
		var fl float64
		if fl, err = srcFloat(v, s.f); err == nil {
			err = storeFloat(s.f, p, bits, fl)
		}
	case scanBool:
		var b bool
		if b, err = srcBool(v, s.f); err == nil {
			*(*bool)(p) = b
		}
	case scanTime:
		var t time.Time
		if t, err = srcTime(v, s.f); err == nil {
			*(*time.Time)(p) = t
		}
	default:
		return s.sinkSlow(v)
	}
	if err != nil || publish == nil {
		return err
	}
	*(*unsafe.Pointer)(publish) = p
	return nil
}

func (s *colScanner) SetTime(v time.Time) error {
	p, publish, kind, _ := s.sinkTarget()
	if kind != scanTime {
		return s.sinkSlow(v)
	}
	*(*time.Time)(p) = v
	if publish != nil {
		*(*unsafe.Pointer)(publish) = p
	}
	return nil
}

func (s *colScanner) SetNull() error { return s.scanNull() }

// The src* converters accept, beyond database/sql's canonical driver values,
// the natively typed values clickhouse-go delivers (it bypasses the canonical
// set via its NamedValueChecker, so UInt64 columns arrive as uint64, Int32 as
// int32, Float32 as float32, and so on). Pure additions: the canonical arms
// are untouched, so the other drivers' behavior cannot move.

func srcInt(src any, f *field) (int64, error) {
	switch v := src.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("rio: column %q value %d overflows %s", f.column, v, f.typ)
		}
		return int64(v), nil
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
		return uintFromInt64(f, v)
	case uint64:
		return v, nil
	case uint:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint8:
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
	case float32:
		return float64(v), nil
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

// rowScanner scans consecutive rows into values of one plan. cells and dests
// are pooled across queries — zero steady-state allocations per query.
type rowScanner struct {
	cells []colScanner
	dests []any
}

var rsPool = sync.Pool{New: func() any { return new(rowScanner) }}

// newRowScanner acquires a scanner from the pool, sized and fully reset for
// this query's field list — a pooled cell must never leak chunk state, or a
// query could hand out cells another query's structs already point at.
// Callers release() once the last Scan returned; the driver never touches
// dests after rows.Scan returns, so releasing before rows.Close is fine.
func newRowScanner(fields []*field, extras []any) *rowScanner {
	rs := rsPool.Get().(*rowScanner)
	n := len(fields)
	if cap(rs.cells) < n || cap(rs.dests) < n+len(extras) {
		rs.cells = make([]colScanner, n)
		rs.dests = make([]any, 0, n+len(extras))
	}
	rs.cells = rs.cells[:n]
	rs.dests = rs.dests[:0]
	for i, f := range fields {
		rs.cells[i] = colScanner{f: f}
		rs.dests = append(rs.dests, &rs.cells[i])
	}
	rs.dests = append(rs.dests, extras...)
	return rs
}

// release returns the scanner to the pool. The last chunk pointer rides
// along until reuse resets it or the pool drops the object under GC pressure
// — bounded at one chunk per column of the released query.
func (rs *rowScanner) release() { rsPool.Put(rs) }

func (rs *rowScanner) scan(rows rows, base unsafe.Pointer) error {
	for i := range rs.cells {
		rs.cells[i].base = base
	}
	return rows.Scan(rs.dests...)
}

// entityFields verifies the result set matches the plan's column order — the
// last line of defense against schema drift between render and execution.
func entityFields(rows rows, p *plan, extras int) ([]*field, error) {
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
func namedFields(rows rows, p *plan) ([]*field, error) {
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

// mergeClose closes rows and, when consumption itself succeeded, promotes the
// Close error to the result. For a result set left undrained (single-row
// reads, probes), Close is where drivers surface deferred protocol and
// connection errors — pgx reads the trailing command status there — so a
// deferred rows.Close() that drops its return would report a failed statement
// as success. err points at the caller's named return; on a fully drained
// result Close is a no-op (database/sql already auto-closed at EOF) and
// nothing changes.
func mergeClose(rows rows, err *error) {
	if cerr := rows.Close(); cerr != nil && *err == nil {
		*err = cerr
	}
}

// scanAll drains rows into a []T. byName selects Raw-style column matching;
// entity queries use plan order.
func scanAll[T any](rows rows, p *plan, byName bool) (out []T, err error) {
	defer mergeClose(rows, &err)
	var fields []*field
	if byName {
		fields, err = namedFields(rows, p)
	} else {
		fields, err = entityFields(rows, p, 0)
	}
	if err != nil {
		return nil, err
	}
	rs := newRowScanner(fields, nil)
	defer rs.release()
	out = []T{}
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
func scanScalars[T any](rows rows) (out []T, err error) {
	defer mergeClose(rows, &err)
	t := reflect.TypeFor[T]()
	f := &field{name: t.String(), column: "<scalar>", typ: t}
	codec, err := codecFor(f)
	if err != nil {
		return nil, err
	}
	f.code = codec
	// The cell and its dest slot share one escaping box: the cell's address
	// reaches the heap through the dest interface anyway, and a variadic
	// slice built fresh at the interface call would heap-allocate per row
	// (rows is an interface, so the callee is opaque to escape analysis).
	var box struct {
		cell colScanner
		dest [1]any
	}
	box.cell = colScanner{f: f}
	box.dest[0] = &box.cell
	out = []T{}
	for rows.Next() {
		out = append(out, *new(T))
		box.cell.base = unsafe.Pointer(&out[len(out)-1])
		if err := rows.Scan(box.dest[:]...); err != nil {
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
// correctly because the memo is keyed by instant equality — every time rio
// binds is already normalized, so Equal here is exact.
type binder struct {
	d       Dialect
	now     time.Time // normalized clock instant, zero when the call stamps nothing
	nowBind any       // lazily rendered d.bindTime(now)
}

func (b *binder) time(nt time.Time) any {
	if nt.Equal(b.now) {
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
		bs := *(*[]byte)(p)
		if bs == nil {
			return nil, true, nil
		}
		if b.d.name() == "clickhouse" {
			// clickhouse-go interpolates a []byte argument as an Array(UInt8)
			// literal; String is ClickHouse's byte container, so byte
			// payloads bind as strings there.
			return string(bs), true, nil
		}
		return bs, true, nil
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
		if err := checkBindTime(b.d, nt); err != nil {
			return nil, true, err
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
func scanOne[T any](rows rows, p *plan) (out *T, err error) {
	defer mergeClose(rows, &err)
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
	out = new(T)
	rs := newRowScanner(fields, nil)
	defer rs.release()
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
		if b.d.name() == "clickhouse" {
			// clickhouse-go interpolates []byte as an Array(UInt8) literal;
			// JSON is UTF-8 text and must land in a String column as a string.
			return string(data), nil
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
			if err := checkBindTime(b.d, nt); err != nil {
				return nil, err
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
			if err := checkBindTime(b.d, nt); err != nil {
				return nil, err
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
		if err := checkBindTime(b.d, nt); err != nil {
			return nil, err
		}
		return b.time(nt), nil
	}
	if isUintKind(v.Kind()) {
		if n := v.Uint(); n > math.MaxInt64 {
			return bindOverflowUint(b.d, n)
		}
	}
	if b.d.name() == "clickhouse" && v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		// Byte payloads bind as strings on ClickHouse, as on the fast path.
		return string(v.Bytes()), nil
	}
	return v.Interface(), nil
}

// chByteArg converts a byte-slice argument — named types like
// json.RawMessage included — into the string form ClickHouse needs
// (normalizeArgs' slow case; the exact []byte case is handled inline).
// driver.Valuer implementers are left alone so their Value() runs, and a
// typed nil slice stays SQL NULL.
func chByteArg(a any) (any, bool) {
	t := reflect.TypeOf(a)
	if t == nil || t.Kind() != reflect.Slice || t.Elem().Kind() != reflect.Uint8 || t.Implements(valuerType) {
		return nil, false
	}
	v := reflect.ValueOf(a)
	if v.IsNil() {
		return nil, true
	}
	return string(v.Bytes()), true
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
