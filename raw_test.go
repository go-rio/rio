package rio

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

type rawGroupRow struct {
	OwnerID int64 `rio:"owner_id"`
	Total   int64
}

func TestRawAppendsClausesAndBindsLimit(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	q := Raw[rawStreamRow]("SELECT t.id, t.name FROM t JOIN u ON u.id = t.user_id").
		Where("t.user_id = ?").
		Where("u.active = ?", true).
		OrderBy("t.id DESC").
		Limit(20).
		Offset(40).
		Must()

	f.queueRows([]string{"id", "name"}, []driver.Value{int64(1), "a"})
	rows, err := q.All(ctx, db, 7)
	if err != nil || len(rows) != 1 || rows[0].Name != "a" {
		t.Fatalf("All: %v %+v", err, rows)
	}
	want := `SELECT t.id, t.name FROM t JOIN u ON u.id = t.user_id WHERE (t.user_id = $1) AND (u.active = $2) ORDER BY t.id DESC LIMIT $3 OFFSET $4`
	got := f.loggedContaining("SELECT")[0]
	if got.sql != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", got.sql, want)
	}
	if len(got.args) != 4 || got.args[0] != int64(7) || got.args[1] != true || got.args[2] != int64(20) || got.args[3] != int64(40) {
		t.Fatalf("args: %v", got.args)
	}

	f.queueRows([]string{"id", "name"})
	if _, err := q.All(ctx, db, 8); err != nil {
		t.Fatalf("cached All: %v", err)
	}
	second := f.loggedContaining("SELECT")[1]
	if second.sql != want || second.args[0] != int64(8) || second.args[2] != int64(20) {
		t.Fatalf("cached run: %s %v", second.sql, second.args)
	}
	entries := 0
	q.cache.entries.Range(func(_, _ any) bool { entries++; return true })
	if entries != 1 {
		t.Fatalf("cache entries = %d", entries)
	}
}

func TestRawHeadDefersPlaceholdersBeforeWhere(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	q := Raw[rawStreamRow]("SELECT t.id, t.name FROM t JOIN u ON u.id = t.user_id AND u.kind = ?").
		Where("t.id IN (?)").
		Must()

	f.queueRows([]string{"id", "name"}, []driver.Value{int64(1), "a"})
	if _, err := q.All(ctx, db, "admin", []int64{1, 2}); err != nil {
		t.Fatalf("All: %v", err)
	}
	got := f.loggedContaining("SELECT")[0]
	want := `SELECT t.id, t.name FROM t JOIN u ON u.id = t.user_id AND u.kind = $1 WHERE (t.id IN ($2, $3))`
	if got.sql != want || len(got.args) != 3 || got.args[0] != "admin" || got.args[2] != int64(2) {
		t.Fatalf("sql: %s %v", got.sql, got.args)
	}

	_, err := q.All(ctx, db, "admin")
	if err == nil || !strings.Contains(err.Error(), "deferred argument") {
		t.Fatalf("missing deferred argument: %v", err)
	}

	f.queueRows([]string{"count"}, []driver.Value{int64(3)})
	n, err := Raw[int64]("SELECT count(*) FROM t WHERE a = ?", 1).Value(ctx, db)
	if err != nil || n != 3 {
		t.Fatalf("inline head: %v %d", err, n)
	}
	if got := f.loggedContaining("count(*)")[0]; got.sql != "SELECT count(*) FROM t WHERE a = $1" || got.args[0] != int64(1) {
		t.Fatalf("inline head sql: %s %v", got.sql, got.args)
	}
}

func TestRawValue(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	q := Raw[string]("SELECT status FROM t").Where("id = ?")

	f.queueRows([]string{"status"}, []driver.Value{"open"})
	if v, err := q.Value(ctx, db, 1); err != nil || v != "open" {
		t.Fatalf("Value: %v %q", err, v)
	}
	f.queueRows([]string{"status"})
	if v, err := q.Value(ctx, db, 2); !errors.Is(err, ErrNotFound) || v != "" {
		t.Fatalf("miss: %v %q", err, v)
	}
	f.queueRows([]string{"status"}, []driver.Value{"a"}, []driver.Value{"b"})
	if _, err := q.Value(ctx, db, 3); !errors.Is(err, ErrMultipleRows) {
		t.Fatalf("two rows: %v", err)
	}
}

func TestRawCountAndExistsWrapDerivedTable(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	q := Raw[rawGroupRow]("SELECT owner_id, count(*) AS total FROM t").
		Where("status = ?").
		GroupBy("owner_id").
		Having("count(*) > ?", 1).
		OrderBy("total DESC").
		Must()

	f.queueRows([]string{"count"}, []driver.Value{int64(2)})
	n, err := q.Count(ctx, db, "open")
	if err != nil || n != 2 {
		t.Fatalf("Count: %v %d", err, n)
	}
	inner := `SELECT owner_id, count(*) AS total FROM t WHERE (status = $1) GROUP BY owner_id HAVING (count(*) > $2)`
	got := f.loggedContaining("count(*) FROM (")[0]
	if got.sql != "SELECT count(*) FROM ("+inner+") AS rio_count" || got.args[0] != "open" || got.args[1] != int64(1) {
		t.Fatalf("count sql: %s %v", got.sql, got.args)
	}

	f.queueRows([]string{"1"}, []driver.Value{int64(1)})
	ok, err := q.Exists(ctx, db, "open")
	if err != nil || !ok {
		t.Fatalf("Exists: %v %v", err, ok)
	}
	got = f.loggedContaining("rio_exists")[0]
	if got.sql != "SELECT 1 FROM ("+inner+") AS rio_exists LIMIT $3" || len(got.args) != 3 || got.args[2] != int64(1) {
		t.Fatalf("exists sql: %s %v", got.sql, got.args)
	}

	f.queueRows([]string{"1"})
	ok, err = q.Limit(0).Exists(ctx, db, "open")
	if err != nil || ok {
		t.Fatalf("Limit(0).Exists: %v %v", err, ok)
	}
	got = f.loggedContaining("rio_exists")[1]
	if !strings.HasSuffix(got.sql, "LIMIT $3) AS rio_exists LIMIT $4") || got.args[2] != int64(0) {
		t.Fatalf("limit inside the probe: %s %v", got.sql, got.args)
	}

	if _, err := q.Limit(5).Count(ctx, db, "open"); err == nil || !strings.Contains(err.Error(), "Count cannot honor Limit/Offset") {
		t.Fatalf("Count with Limit: %v", err)
	}
}

func TestRawRowsUsesClauses(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id", "name"}, []driver.Value{int64(1), "a"}, []driver.Value{int64(2), "b"})

	var ids []int64
	for row, err := range Raw[rawStreamRow]("SELECT id, name FROM t").Where("id > ?").OrderBy("id").Rows(ctx, db, 0) {
		if err != nil {
			t.Fatalf("Rows: %v", err)
		}
		ids = append(ids, row.ID)
	}
	if len(ids) != 2 || ids[1] != 2 {
		t.Fatalf("ids = %v", ids)
	}
	if got := f.logged()[0]; got != "SELECT id, name FROM t WHERE (id > $1) ORDER BY id" {
		t.Fatalf("sql: %s", got)
	}
}

func TestRawFirstAppendsNoLimit(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id", "name"}, []driver.Value{int64(1), "a"}, []driver.Value{int64(2), "b"})

	row, err := Raw[rawStreamRow]("SELECT id, name FROM t ORDER BY id").First(ctx, db)
	if err != nil || row.ID != 1 {
		t.Fatalf("First: %v %+v", err, row)
	}
	if got := f.logged()[0]; got != "SELECT id, name FROM t ORDER BY id" {
		t.Fatalf("First must leave the head alone: %s", got)
	}
}

func TestRawLimitBindsPerDialect(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		d    Dialect
		want string
	}{
		{Postgres, "SELECT id FROM t ORDER BY id LIMIT $1 OFFSET $2"},
		{MySQL, "SELECT id FROM t ORDER BY id LIMIT ? OFFSET ?"},
		{SQLite, "SELECT id FROM t ORDER BY id LIMIT ? OFFSET ?"},
		{ClickHouse, "SELECT id FROM t ORDER BY id LIMIT ? OFFSET ?"},
	} {
		f := newFakeDB()
		db := f.open(tc.d)
		f.queueRows([]string{"id"})
		if _, err := Raw[int64]("SELECT id FROM t").OrderBy("id").Limit(10).Offset(5).All(ctx, db); err != nil {
			t.Fatalf("%s: %v", tc.d.name(), err)
		}
		got := f.loggedContaining("SELECT")[0]
		if got.sql != tc.want || len(got.args) != 2 || got.args[0] != int64(10) || got.args[1] != int64(5) {
			t.Fatalf("%s: %s %v", tc.d.name(), got.sql, got.args)
		}
	}
}

func TestRawValidate(t *testing.T) {
	err := Raw[int64]("SELECT 1 FROM t").OrderBy("id = ?").Validate()
	if err == nil || !strings.Contains(err.Error(), "OrderBy") {
		t.Fatalf("placeholder in OrderBy: %v", err)
	}
	err = Raw[int64]("SELECT 1 FROM t WHERE a = ? AND b = ?", 1).Validate()
	if err == nil || !strings.Contains(err.Error(), "2 placeholder(s) but 1 argument(s)") {
		t.Fatalf("head arity: %v", err)
	}
	if err := Raw[int64]("").Validate(); err == nil || !strings.Contains(err.Error(), "SELECT head") {
		t.Fatalf("empty head: %v", err)
	}
	if err := Raw[int64]("SELECT 1").Limit(-1).Validate(); err == nil || !strings.Contains(err.Error(), "Limit requires") {
		t.Fatalf("negative limit: %v", err)
	}
	if err := Raw[int64]("SELECT 1 FROM t").Where("a = ?").Validate(); err != nil {
		t.Fatalf("deferred fragments validate: %v", err)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("Must must panic on an invalid query")
		}
	}()
	Raw[int64]("SELECT 1 FROM t").Where("a = ?", 1, 2).Must()
}
