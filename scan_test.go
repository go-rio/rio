package rio

import (
	"context"
	"database/sql/driver"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

// ptrKinds exercises every element kind the scanPtr plan-time codec covers:
// the direct element write must behave exactly like scanning the plain type,
// plus the NULL→nil rule.
type ptrKinds struct {
	ID    int64
	I8    *int8
	I     *int64
	U     *uint16
	F     *float64
	B     *bool
	S     *string
	Bytes *[]byte
	T     *time.Time
}

var ptrKindsCols = []string{"id", "i8", "i", "u", "f", "b", "s", "bytes", "t"}

func TestScanPtrWritesEveryElemKind(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueRows(ptrKindsCols, []driver.Value{
		int64(1), int64(-8), int64(42), int64(9), 2.5, true, "hi", []byte{1, 2}, testNow,
	})
	got, err := Find[ptrKinds](context.Background(), db, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if got.I8 == nil || *got.I8 != -8 {
		t.Errorf("I8 = %v, want -8", got.I8)
	}
	if got.I == nil || *got.I != 42 {
		t.Errorf("I = %v, want 42", got.I)
	}
	if got.U == nil || *got.U != 9 {
		t.Errorf("U = %v, want 9", got.U)
	}
	if got.F == nil || *got.F != 2.5 {
		t.Errorf("F = %v, want 2.5", got.F)
	}
	if got.B == nil || !*got.B {
		t.Errorf("B = %v, want true", got.B)
	}
	if got.S == nil || *got.S != "hi" {
		t.Errorf("S = %v, want hi", got.S)
	}
	if got.Bytes == nil || string(*got.Bytes) != "\x01\x02" {
		t.Errorf("Bytes = %v, want [1 2]", got.Bytes)
	}
	if got.T == nil || !got.T.Equal(testNow) {
		t.Errorf("T = %v, want %v", got.T, testNow)
	}
}

func TestScanPtrTextSourcesAndNulls(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	// Text-encoded numerics and time (SQLite delivers TEXT affinities), and
	// NULL into every pointer column.
	f.queueRows(ptrKindsCols, []driver.Value{
		int64(1), "-8", "42", "9", "2.5", "1", []byte("hi"), "raw", "2026-07-09 12:00:00+00:00",
	})
	got, err := Find[ptrKinds](context.Background(), db, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if got.I8 == nil || *got.I8 != -8 || got.U == nil || *got.U != 9 || got.F == nil || *got.F != 2.5 {
		t.Errorf("text numerics = %v %v %v", got.I8, got.U, got.F)
	}
	if got.B == nil || !*got.B || got.S == nil || *got.S != "hi" || got.Bytes == nil || string(*got.Bytes) != "raw" {
		t.Errorf("text bool/string/bytes = %v %v %v", got.B, got.S, got.Bytes)
	}
	if got.T == nil || !got.T.Equal(testNow) {
		t.Errorf("T = %v, want %v", got.T, testNow)
	}

	f.queueRows(ptrKindsCols, []driver.Value{
		int64(2), nil, nil, nil, nil, nil, nil, nil, nil,
	})
	got, err = Find[ptrKinds](context.Background(), db, int64(2))
	if err != nil {
		t.Fatal(err)
	}
	if got.I8 != nil || got.I != nil || got.U != nil || got.F != nil ||
		got.B != nil || got.S != nil || got.Bytes != nil || got.T != nil {
		t.Errorf("NULL columns must scan to nil pointers, got %+v", got)
	}
}

// TestScanPtrRescanReplacesPointer pins the rebase semantics: scanning a new
// row over the same struct swaps the pointer for a fresh cell — a previously
// scanned pointer held by the caller is never overwritten in place.
func TestScanPtrRescanReplacesPointer(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueRows(ptrKindsCols, []driver.Value{
		int64(1), nil, int64(1), nil, nil, nil, nil, nil, nil,
	})
	first, err := Find[ptrKinds](context.Background(), db, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	held := first.I

	f.queueRows(ptrKindsCols, []driver.Value{
		int64(1), nil, int64(2), nil, nil, nil, nil, nil, nil,
	})
	second, err := Find[ptrKinds](context.Background(), db, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if *held != 1 {
		t.Errorf("previously scanned cell mutated: %d", *held)
	}
	if second.I == held {
		t.Error("rescan reused the previous cell instead of allocating")
	}
	if *second.I != 2 {
		t.Errorf("second scan = %d, want 2", *second.I)
	}
}

// TestScanPtrCellsIndependent pins the chunked cell allocator's aliasing
// contract: cells handed out across rows are distinct slots — writing through
// one row's pointers never changes another row's values — and values stay
// intact once the scan (and its chunk bookkeeping) is gone.
func TestScanPtrCellsIndependent(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	const n = 100 // spans several chunk sizes (1, 4, 16, 64, 128)
	rows := make([][]driver.Value, n)
	for i := range rows {
		rows[i] = []driver.Value{
			int64(i + 1), int64(1), int64(1000 + i), int64(9), 0.5, true, "s", []byte{byte(i)}, testNow,
		}
	}
	f.queueRows(ptrKindsCols, rows...)
	got, err := From[ptrKinds]().All(context.Background(), db)
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
		*got[i].I = -int64(i) // write through every pointer...
	}
	runtime.GC() // ...and make sure chunks survive on the strength of the cells alone
	for i := range got {
		if *got[i].I != -int64(i) {
			t.Fatalf("row %d cell = %d after neighbor writes, want %d", i, *got[i].I, -i)
		}
		if *got[i].I8 != 1 || *got[i].U != 9 || *got[i].F != 0.5 || *got[i].S != "s" {
			t.Fatalf("row %d neighbors mutated: %+v", i, got[i])
		}
	}
}

func TestScanPtrOverflowAndConversionErrors(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		row  []driver.Value
		want string
	}{
		{"int8 overflow", []driver.Value{int64(1), int64(1000), nil, nil, nil, nil, nil, nil, nil}, "overflows"},
		{"negative uint", []driver.Value{int64(1), nil, nil, int64(-3), nil, nil, nil, nil, nil}, "negative"},
		{"uint16 overflow", []driver.Value{int64(1), nil, nil, int64(70000), nil, nil, nil, nil, nil}, "overflows"},
		{"bad time text", []driver.Value{int64(1), nil, nil, nil, nil, nil, nil, nil, "not-a-time"}, "cannot parse"},
		{"bad conversion", []driver.Value{int64(1), nil, 3.5, nil, nil, nil, nil, nil, nil}, "cannot convert"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeDB()
			db := f.open(SQLite)
			f.queueRows(ptrKindsCols, tc.row)
			_, err := Find[ptrKinds](ctx, db, int64(1))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

// entityFields is the last line of defense against schema drift between
// render and execution: an entity result set that does not match the plan's
// column count and order must error by name, never misassign silently.
func TestEntityColumnMismatchErrors(t *testing.T) {
	ctx := context.Background()
	run := func(cols []string) error {
		f := newFakeDB()
		db := f.open(SQLite)
		f.queueRows(cols)
		_, err := From[User]().All(ctx, db)
		return err
	}

	t.Run("missing column", func(t *testing.T) {
		err := run(userCols[:len(userCols)-1])
		if err == nil || !strings.Contains(err.Error(), "rio: User: expected 8 columns, result has 7") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("extra column", func(t *testing.T) {
		err := run(append(append([]string(nil), userCols...), "extra"))
		if err == nil || !strings.Contains(err.Error(), "rio: User: expected 8 columns, result has 9") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("duplicated column", func(t *testing.T) {
		cols := append([]string(nil), userCols...)
		cols[1] = "id" // right count, duplicate name: caught by position
		err := run(cols)
		if err == nil || !strings.Contains(err.Error(), `rio: User: column 1 is "id", expected "email"`) {
			t.Fatalf("err = %v", err)
		}
	})
}

// Codex audit #2, read side: scanOne stops after its single row, so the
// result is never drained and rows.Close performs the real close — an error
// there (connection loss mid-protocol) must reach the caller instead of
// returning a half-trusted row as success.
func TestFindReportsRowsCloseError(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	closeErr := errors.New("driver: connection reset while closing rows")
	f.queueRowsCloseErr(closeErr, userCols, userRow(1, "a@x"))

	_, err := Find[User](context.Background(), db, int64(1))
	if !errors.Is(err, closeErr) {
		t.Fatalf("Find must surface the rows.Close error, got: %v", err)
	}
}
