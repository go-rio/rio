package rio

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestDoUpdateWhereRendersAndReportsRejection(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	o := &Org{ID: 1, Name: "acme"}

	f.queueRows(orgCols, []driver.Value{int64(1), "acme"})
	err := Upsert(ctx, db, o, OnConflict("id"), DoUpdate("name"),
		DoUpdateWhere("excluded.name <> ? AND orgs.name < excluded.name", ""))
	if err != nil {
		t.Fatal(err)
	}
	want := `INSERT INTO "orgs" ("id", "name") VALUES ($1, $2) ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name" ` +
		`WHERE (excluded.name <> $3 AND orgs.name < excluded.name) RETURNING "orgs"."id", "orgs"."name"`
	got := f.loggedContaining("ON CONFLICT")[0]
	if got.sql != want || len(got.args) != 3 || got.args[2] != "" {
		t.Fatalf("sql:\n got: %s %v\nwant: %s", got.sql, got.args, want)
	}

	f.queueRows(orgCols)
	err = Upsert(ctx, db, o, OnConflict("id"), DoUpdate("name"), DoUpdateWhere("orgs.name < excluded.name"))
	if !errors.Is(err, ErrStaleObject) {
		t.Fatalf("rejected update: %v", err)
	}

	f.queueRows(orgCols, []driver.Value{int64(1), "acme!"})
	err = Upsert(ctx, db, o, OnConflict("id"),
		DoUpdateSet(Set{"name": Expr("excluded.name || ?", "!")}),
		DoUpdateWhere("orgs.name <> ?", "x"))
	if err != nil {
		t.Fatal(err)
	}
	got = f.loggedContaining("ON CONFLICT")[2]
	if !strings.Contains(got.sql, `DO UPDATE SET "name" = excluded.name || $3 WHERE (orgs.name <> $4) RETURNING`) ||
		len(got.args) != 4 || got.args[2] != "!" || got.args[3] != "x" {
		t.Fatalf("expression args before where args: %s %v", got.sql, got.args)
	}
}

func TestDoUpdateWhereRejectionIsAnOutcomeForHooks(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	hook := &afterHook{}
	db := f.openWith(Postgres, WithQueryHook(hook))
	o := &Org{ID: 1, Name: "acme"}

	f.queueRows(orgCols)
	err := Upsert(ctx, db, o, OnConflict("id"), DoUpdate("name"), DoUpdateWhere("orgs.name < excluded.name"))
	if !errors.Is(err, ErrStaleObject) {
		t.Fatalf("rejected update: %v", err)
	}
	if len(hook.events) != 1 || hook.events[0].Err != nil || hook.events[0].RowsReturned != 0 {
		t.Fatalf("hooks see a miss, not a failure: %+v", hook.events)
	}
}

func TestRacedCreateRejectsTableOverride(t *testing.T) {
	ctx := context.Background()
	db := newFakeDB().open()
	o := &Org{Name: "acme"}
	for name, run := range map[string]func() error{
		"FirstOrCreate": func() error {
			return From[Org]().Table("orgs_archive").Where("name = ?", "acme").FirstOrCreate(ctx, db, o)
		},
		"CreateOrFirst": func() error {
			return From[Org]().Table("orgs_archive").Where("name = ?", "acme").CreateOrFirst(ctx, db, o)
		},
	} {
		if err := run(); err == nil || !strings.Contains(err.Error(), "Table") {
			t.Fatalf("%s must reject a Table override: %v", name, err)
		}
	}
}

func TestOnConflictWhereRendersPartialIndexPredicate(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	o := &Org{Name: "acme"}

	f.queueRows([]string{"id"}, []driver.Value{int64(9)})
	if err := Upsert(ctx, db, o, OnConflict("name"), OnConflictWhere("id > 0"), DoNothing()); err != nil {
		t.Fatal(err)
	}
	want := `INSERT INTO "orgs" ("name") VALUES ($1) ON CONFLICT ("name") WHERE id > 0 DO NOTHING RETURNING "id"`
	if got := f.logged()[0]; got != want || o.ID != 9 {
		t.Fatalf("sql: %s id=%d", got, o.ID)
	}
}

func TestUpsertConflictPredicatesRejected(t *testing.T) {
	ctx := context.Background()
	db := newFakeDB().open()
	my := newFakeDB().open(MySQL)
	o := &Org{ID: 1, Name: "acme"}

	err := Upsert(ctx, my, o, DoUpdateWhere("x > ?", 1))
	if !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), "DoUpdateWhere") {
		t.Fatalf("mysql DoUpdateWhere: %v", err)
	}
	err = Upsert(ctx, my, o, OnConflict("id"), OnConflictWhere("id > 0"))
	if !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), "OnConflictWhere") {
		t.Fatalf("mysql OnConflictWhere: %v", err)
	}
	err = Upsert(ctx, db, o, OnConflict("id"), DoNothing(), DoUpdateWhere("1 = 1"))
	if err == nil || !strings.Contains(err.Error(), "cannot combine DoNothing") {
		t.Fatalf("DoNothing with DoUpdateWhere: %v", err)
	}
	err = Upsert(ctx, db, o, OnConflict("id"), DoUpdateWhere("a = ? AND b = ?", 1))
	if err == nil || !strings.Contains(err.Error(), "2 placeholder(s) but 1 argument(s)") {
		t.Fatalf("arity: %v", err)
	}
	err = Upsert(ctx, db, o, OnConflictWhere("id > 0"), DoNothing())
	if err == nil || !strings.Contains(err.Error(), "OnConflictWhere needs OnConflict") {
		t.Fatalf("predicate without target: %v", err)
	}
}

func TestWithoutStampsUpsertMayAssignCreatedAt(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	u := &User{ID: 1, Email: "a@x", CreatedAt: testNow, UpdatedAt: testNow}

	err := Upsert(ctx, db, u, OnConflict("id"), DoUpdate("email", "created_at"))
	if err == nil || !strings.Contains(err.Error(), `"created_at" is maintained by rio`) {
		t.Fatalf("stamped handle: %v", err)
	}

	f.queueRows(userCols, userRow(1, "a@x"))
	err = Upsert(ctx, db.WithoutStamps(), u, OnConflict("id"), DoUpdate("email", "created_at"))
	if err != nil {
		t.Fatalf("WithoutStamps: %v", err)
	}
	got := f.loggedContaining("ON CONFLICT")[0].sql
	if !strings.Contains(got, `DO UPDATE SET "email" = excluded."email", "created_at" = excluded."created_at", "updated_at" = excluded."updated_at"`) {
		t.Fatalf("created_at belongs to the caller: %s", got)
	}

	f.queueRows(userCols, userRow(1, "a@x"))
	err = Upsert(ctx, db.WithoutStamps(), u, OnConflict("id"), DoUpdateSet(Set{"created_at": Expr("excluded.created_at")}))
	if err != nil {
		t.Fatalf("DoUpdateSet: %v", err)
	}
}

func TestUpsertAllBindsWhereArgsAfterRows(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	rows := []Org{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}}

	f.queueExec(0, 2)
	err := UpsertAll(ctx, db, rows, OnConflict("id"), DoUpdate("name"),
		DoUpdateWhere("orgs.name <> excluded.name OR ? = 1", 1))
	if err != nil {
		t.Fatal(err)
	}
	want := `INSERT INTO "orgs" ("id", "name") VALUES ($1, $2), ($3, $4) ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name" ` +
		`WHERE (orgs.name <> excluded.name OR $5 = 1)`
	got := f.loggedContaining("ON CONFLICT")[0]
	if got.sql != want || len(got.args) != 5 || got.args[4] != int64(1) {
		t.Fatalf("sql:\n got: %s %v\nwant: %s", got.sql, got.args, want)
	}
}
