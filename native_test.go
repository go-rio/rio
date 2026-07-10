package rio

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// sinkWide covers every scan kind the sinks and Scan share: all integer
// widths, both float widths, bool, string, bytes, time, a value-typed
// softdelete column, json (value and pointer), a Scanner field, and the
// pointer form of each basic kind.
type sinkWide struct {
	ID  int64
	I8  int8
	I16 int16
	I32 int32
	I   int64
	U8  uint8
	U16 uint16
	U32 uint32
	U   uint64
	F32 float32
	F   float64
	B   bool
	S   string
	Bs  []byte
	T   time.Time
	Del time.Time      `rio:",softdelete"`
	J   map[string]int `rio:",json"`
	JP  *[]string      `rio:",json"`
	NS  sql.NullString
	PI8 *int8
	PU  *uint16
	PF  *float64
	PB  *bool
	PS  *string
	PBs *[]byte
	PT  *time.Time
}

// sinkDispatch routes one value to its typed sink, the way a native driver's
// codecs would.
func sinkDispatch(c *colScanner, v any) error {
	switch tv := v.(type) {
	case nil:
		return c.SetNull()
	case int64:
		return c.SetInt64(tv)
	case float64:
		return c.SetFloat64(tv)
	case bool:
		return c.SetBool(tv)
	case string:
		return c.SetString(tv)
	case []byte:
		return c.SetBytes(tv)
	case time.Time:
		return c.SetTime(tv)
	}
	panic("no sink for value")
}

// TestNativeCellSinkEquivalence pins the SPI's core promise over the full
// kind × value matrix: SetX(v) behaves exactly like Scan(v) — same stored
// value (including pointer cells and NULL rules), same success/failure, same
// error text. The two paths share their store helpers, so this is the test
// that would catch anyone unsharing them.
func TestNativeCellSinkEquivalence(t *testing.T) {
	p, err := planOf[sinkWide]()
	if err != nil {
		t.Fatal(err)
	}
	values := []any{
		nil,
		int64(42), int64(-7), int64(300), int64(70000), int64(1) << 40, int64(1) << 33,
		float64(2.5), float64(-2.5), float64(3.4e39), float64(1e300),
		true, false,
		"hello", "42", "-7", "2.5", "300", "70000", "true", "0", "not-a-number",
		"2026-07-09 12:00:00+00:00", `{"a":1}`, `["x","y"]`, "not json {",
		[]byte("bytes"), []byte("42"), []byte("2.5"), []byte("1"), []byte(`{"a":1}`), []byte(`["x"]`),
		testNow,
	}
	for _, f := range p.fields {
		for _, v := range values {
			var a, b sinkWide
			cellScan := colScanner{f: f, base: unsafe.Pointer(&a)}
			cellSink := colScanner{f: f, base: unsafe.Pointer(&b)}

			scanErr := cellScan.Scan(v)
			sinkErr := sinkDispatch(&cellSink, v)

			if (scanErr == nil) != (sinkErr == nil) {
				t.Fatalf("%s <- %#v: Scan err = %v, sink err = %v", f.name, v, scanErr, sinkErr)
			}
			if scanErr != nil {
				if scanErr.Error() != sinkErr.Error() {
					t.Fatalf("%s <- %#v: error text drifted:\n scan: %s\n sink: %s", f.name, v, scanErr, sinkErr)
				}
				continue
			}
			if !structFieldsEqual(f, &a, &b) {
				t.Fatalf("%s <- %#v: stored values differ:\n scan: %+v\n sink: %+v", f.name, v, a, b)
			}
		}
	}
}

// structFieldsEqual compares one field of two sinkWide values, following
// pointers (distinct cells holding equal values must compare equal).
func structFieldsEqual(f *field, a, b *sinkWide) bool {
	av := fieldOf(f, a)
	bv := fieldOf(f, b)
	switch x := av.(type) {
	case time.Time:
		return x.Equal(bv.(time.Time))
	case *time.Time:
		y := bv.(*time.Time)
		if x == nil || y == nil {
			return x == nil && y == nil
		}
		return x.Equal(*y)
	}
	return reflect.DeepEqual(av, bv)
}

func fieldOf(f *field, s *sinkWide) any {
	rv := reflect.ValueOf(s).Elem().FieldByIndex(f.index)
	return rv.Interface()
}

// TestNativeCellSinkBytesCopyDiscipline pins SetBytes' contract: the driver
// buffer may be reused the instant the call returns, and nothing rio stored
// may alias it.
func TestNativeCellSinkBytesCopyDiscipline(t *testing.T) {
	p, err := planOf[sinkWide]()
	if err != nil {
		t.Fatal(err)
	}
	var w sinkWide
	buf := []byte("payload")

	byName := map[string]*field{}
	for _, f := range p.fields {
		byName[f.name] = f
	}

	cell := colScanner{f: byName["Bs"], base: unsafe.Pointer(&w)}
	if err := cell.SetBytes(buf); err != nil {
		t.Fatal(err)
	}
	ptrCell := colScanner{f: byName["PBs"], base: unsafe.Pointer(&w)}
	if err := ptrCell.SetBytes(buf); err != nil {
		t.Fatal(err)
	}
	strCell := colScanner{f: byName["S"], base: unsafe.Pointer(&w)}
	if err := strCell.SetBytes(buf); err != nil {
		t.Fatal(err)
	}

	copy(buf, "XXXXXXX") // the driver reuses its buffer

	if string(w.Bs) != "payload" || string(*w.PBs) != "payload" || w.S != "payload" {
		t.Fatalf("driver buffer reuse leaked into stored values: %q %q %q", w.Bs, *w.PBs, w.S)
	}
}

func TestNativeCellScanKinds(t *testing.T) {
	p, err := planOf[sinkWide]()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]NativeScanKind{
		"ID": NativeKindInt, "I8": NativeKindInt, "I16": NativeKindInt, "I32": NativeKindInt, "I": NativeKindInt,
		"U8": NativeKindUint, "U16": NativeKindUint, "U32": NativeKindUint, "U": NativeKindUint,
		"F32": NativeKindFloat, "F": NativeKindFloat,
		"B": NativeKindBool, "S": NativeKindString, "Bs": NativeKindBytes,
		"T": NativeKindTime, "Del": NativeKindTime,
		"J": NativeKindJSON, "JP": NativeKindJSON,
		"NS": NativeKindScanner,
		// Pointer fields report their element's kind: the sinks own the cell
		// allocation, so the adapter dispatches them like the plain kinds.
		"PI8": NativeKindInt, "PU": NativeKindUint, "PF": NativeKindFloat, "PB": NativeKindBool,
		"PS": NativeKindString, "PBs": NativeKindBytes, "PT": NativeKindTime,
	}
	for _, f := range p.fields {
		cell := colScanner{f: f}
		wantKind, ok := want[f.name]
		if !ok {
			t.Fatalf("no expectation for field %s", f.name)
		}
		if got := cell.ScanKind(); got != wantKind {
			t.Errorf("%s: ScanKind = %d, want %d", f.name, got, wantKind)
		}
	}
}

// The NULL rules through SetNull, spelled out (the matrix covers them too;
// these name the contracts).
func TestNativeCellSetNullRules(t *testing.T) {
	p, err := planOf[sinkWide]()
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]*field{}
	for _, f := range p.fields {
		byName[f.name] = f
	}
	var w sinkWide
	w.Del = testNow
	w.Bs = []byte("old")
	pi := int8(3)
	w.PI8 = &pi
	sl := []string{"x"}
	w.JP = &sl
	w.NS = sql.NullString{String: "old", Valid: true}

	set := func(name string) error {
		c := colScanner{f: byName[name], base: unsafe.Pointer(&w)}
		return c.SetNull()
	}

	if err := set("Del"); err != nil || !w.Del.IsZero() {
		t.Fatalf("softdelete NULL must zero the time: %v %v", err, w.Del)
	}
	if err := set("Bs"); err != nil || w.Bs != nil {
		t.Fatalf("bytes NULL must store nil: %v %v", err, w.Bs)
	}
	if err := set("PI8"); err != nil || w.PI8 != nil {
		t.Fatalf("pointer NULL must store nil: %v %v", err, w.PI8)
	}
	if err := set("JP"); err != nil || w.JP != nil {
		t.Fatalf("json pointer NULL must store nil: %v %v", err, w.JP)
	}
	if err := set("NS"); err != nil || w.NS.Valid {
		t.Fatalf("Scanner NULL must delegate to the Scanner: %v %v", err, w.NS)
	}
	if err := set("I"); err == nil || !strings.Contains(err.Error(), "is NULL but field") {
		t.Fatalf("non-nullable NULL must fail with rio's message: %v", err)
	}
	if err := set("J"); err == nil || !strings.Contains(err.Error(), "is NULL but field") {
		t.Fatalf("value json NULL must fail like the stdlib channel: %v", err)
	}
}

// End-to-end pointer-cell independence on the native channel: the chunked
// allocator behind the sinks hands out distinct slots across rows (the
// stdlib channel's TestScanPtrCellsIndependent, replayed over the SPI).
func TestNativeScanPtrCellsIndependent(t *testing.T) {
	nf := newFakeNative()
	db := nf.open(SQLite)
	const n = 100
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{
			int64(i + 1), int64(1), int64(1000 + i), int64(9), 0.5, true, "s", []byte{byte(i)}, testNow,
		}
	}
	nf.queueRows(ptrKindsCols, rows...)
	got, err := From[ptrKinds]().All(t.Context(), db)
	if err != nil || len(got) != n {
		t.Fatalf("All: %v, %d rows", err, len(got))
	}
	seen := make(map[*int64]bool, n)
	for i := range got {
		if seen[got[i].I] {
			t.Fatalf("row %d shares a cell with an earlier row", i)
		}
		seen[got[i].I] = true
	}
	for i := range got {
		if *got[i].I != int64(1000+i) || *got[i].U != 9 || *got[i].S != "s" {
			t.Fatalf("row %d misscanned: %+v", i, got[i])
		}
	}
}
