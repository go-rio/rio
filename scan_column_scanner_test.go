package rio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type columnScannerText string

func (s *columnScannerText) Scan(src any) error {
	switch v := src.(type) {
	case string:
		*s = columnScannerText("scan:" + v)
	case []byte:
		*s = columnScannerText("scan:" + string(v))
	case nil:
		*s = ""
	default:
		return fmt.Errorf("columnScannerText: unsupported %T", src)
	}
	return nil
}

type columnScannerRow struct {
	ID      int64
	Name    string
	Blob    []byte
	Maybe   *int64
	Null    sql.NullString
	Custom  columnScannerText
	Meta    map[string]string `rio:",json"`
	Created time.Time
}

func TestRowsColumnScannerMatchesLegacyScan(t *testing.T) {
	ctx := context.Background()
	cols := []string{"id", "name", "blob", "maybe", "null", "custom", "meta", "created"}
	wantTime := time.Date(2026, 8, 2, 4, 5, 6, 0, time.UTC)

	run := func(t *testing.T, direct bool) (columnScannerRow, *fakeDB, []byte) {
		t.Helper()
		f := newFakeDB()
		var db *DB
		if direct {
			db = f.openColumnScanner(Postgres)
		} else {
			db = f.open(Postgres)
		}
		blob := []byte("payload")
		f.queueRows(cols, []driver.Value{
			int64(7), []byte("alice"), blob, nil, []byte("nullable"), []byte("custom"), []byte(`{"role":"admin"}`), wantTime,
		})
		rows, err := From[columnScannerRow]().All(ctx, db)
		if err != nil {
			t.Fatalf("All: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("rows = %d", len(rows))
		}
		return rows[0], f, blob
	}

	legacy, legacyDB, legacyBlob := run(t, false)
	direct, directDB, directBlob := run(t, true)
	if !reflect.DeepEqual(direct, legacy) {
		t.Fatalf("direct scan differs:\n direct: %#v\n legacy: %#v", direct, legacy)
	}
	if direct.ID != 7 ||
		direct.Name != "alice" ||
		direct.Maybe != nil ||
		!direct.Null.Valid ||
		direct.Null.String != "nullable" ||
		direct.Custom != "scan:custom" ||
		direct.Meta["role"] != "admin" ||
		!direct.Created.Equal(wantTime) {
		t.Fatalf("direct row = %#v", direct)
	}

	legacyBlob[0] = 'X'
	directBlob[0] = 'Y'
	if string(legacy.Blob) != "payload" || string(direct.Blob) != "payload" {
		t.Fatalf("driver byte buffer was retained: legacy=%q direct=%q", legacy.Blob, direct.Blob)
	}
	if directDB.nextCalls != 0 {
		t.Fatalf("Rows.Next called %d time(s) on RowsColumnScanner", directDB.nextCalls)
	}
	if directDB.scanCalls != len(cols) {
		t.Fatalf("ScanColumn called %d time(s), want %d", directDB.scanCalls, len(cols))
	}
	if legacyDB.scanCalls != 0 {
		t.Fatalf("legacy path called ScanColumn %d time(s)", legacyDB.scanCalls)
	}
}

func TestRowsColumnScannerMatchesLegacyErrors(t *testing.T) {
	ctx := context.Background()
	cols := []string{"id", "name", "blob", "maybe", "null", "custom", "meta", "created"}

	run := func(direct bool, row []driver.Value) error {
		f := newFakeDB()
		var db *DB
		if direct {
			db = f.openColumnScanner(Postgres)
		} else {
			db = f.open(Postgres)
		}
		f.queueRows(cols, row)
		_, err := From[columnScannerRow]().All(ctx, db)
		return err
	}

	tests := []struct {
		name string
		row  []driver.Value
	}{
		{
			name: "NULL into non-nullable integer",
			row:  []driver.Value{nil, "alice", []byte("x"), nil, nil, nil, []byte(`{}`), time.Time{}},
		},
		{
			name: "invalid integer text",
			row:  []driver.Value{"not-an-int", "alice", []byte("x"), nil, nil, nil, []byte(`{}`), time.Time{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			legacy := run(false, tt.row)
			direct := run(true, tt.row)
			if legacy == nil || direct == nil {
				t.Fatalf("legacy=%v direct=%v", legacy, direct)
			}
			if direct.Error() != legacy.Error() {
				t.Fatalf("error text differs:\n direct: %v\n legacy: %v", direct, legacy)
			}
		})
	}
}
