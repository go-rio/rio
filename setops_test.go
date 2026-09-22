package rio

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestUpdateAllExprBindsArguments(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	f.queueExec(0, 2)
	n, err := From[Org]().Where("id IN (?)").UpdateAll(ctx, db, Set{"name": Expr("name || ?", "-x")}, []int64{1, 2})
	if err != nil || n != 2 {
		t.Fatalf("UpdateAll: %v %d", err, n)
	}
	got := f.loggedContaining("UPDATE")[0]
	if got.sql != `UPDATE "orgs" SET "name" = name || $1 WHERE (id IN ($2, $3))` ||
		len(got.args) != 3 || got.args[0] != "-x" || got.args[2] != int64(2) {
		t.Fatalf("sql: %s %v", got.sql, got.args)
	}

	f.queueExec(0, 1)
	_, err = From[Org]().AllRows().UpdateAll(ctx, db, Set{
		"name": Expr("CASE WHEN id IN (?) THEN ? ELSE name END", []int64{3, 4}, "hit"),
	})
	if err != nil {
		t.Fatalf("UpdateAll: %v", err)
	}
	got = f.loggedContaining("UPDATE")[1]
	if got.sql != `UPDATE "orgs" SET "name" = CASE WHEN id IN ($1, $2) THEN $3 ELSE name END` ||
		len(got.args) != 3 || got.args[2] != "hit" {
		t.Fatalf("slice inside an expression: %s %v", got.sql, got.args)
	}

	f.queueExec(0, 1)
	_, err = From[User]().Where("id = ?", 1).UpdateAll(ctx, db, Set{"age": Expr("age + ?", 1)})
	if err != nil {
		t.Fatalf("UpdateAll: %v", err)
	}
	got = f.loggedContaining("UPDATE")[2]
	if got.sql != `UPDATE "users" SET "age" = age + $1, "updated_at" = $2 WHERE (id = $3) AND "users"."deleted_at" IS NULL` ||
		len(got.args) != 3 || got.args[0] != int64(1) || got.args[2] != int64(1) {
		t.Fatalf("expression args precede the stamp: %s %v", got.sql, got.args)
	}
}

type orgName struct{ Name string }

type orgID struct{ ID int64 }

type userGone struct {
	ID        int64
	DeletedAt *time.Time
}

func TestUpdateAllIntoAndDeleteAllInto(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	f.queueRows([]string{"name"}, []driver.Value{"ACME"}, []driver.Value{"BETA"})
	names, err := UpdateAllInto[orgName](ctx, db, From[Org]().Where("id IN (?)", []int64{1, 2}), Set{"name": Expr("upper(name)")})
	if err != nil || len(names) != 2 || names[1].Name != "BETA" {
		t.Fatalf("UpdateAllInto: %v %+v", err, names)
	}
	if got := f.loggedContaining("UPDATE")[0].sql; got != `UPDATE "orgs" SET "name" = upper(name) WHERE (id IN ($1, $2)) RETURNING "orgs"."name"` {
		t.Fatalf("update into: %s", got)
	}

	f.queueRows([]string{"id", "deleted_at"}, []driver.Value{int64(1), testNow})
	gone, err := DeleteAllInto[userGone](ctx, db, From[User]().Where("id = ?", 1))
	if err != nil || len(gone) != 1 || gone[0].ID != 1 || gone[0].DeletedAt == nil {
		t.Fatalf("soft DeleteAllInto: %v %+v", err, gone)
	}
	if got := f.loggedContaining(`SET "deleted_at"`)[0].sql; !strings.HasSuffix(got, `AND "users"."deleted_at" IS NULL RETURNING "users"."id", "users"."deleted_at"`) {
		t.Fatalf("soft delete into: %s", got)
	}

	f.queueRows([]string{"id"}, []driver.Value{int64(7)})
	ids, err := DeleteAllInto[orgID](ctx, db, From[Org]().Where("id = ?", 7))
	if err != nil || len(ids) != 1 || ids[0].ID != 7 {
		t.Fatalf("DeleteAllInto: %v %+v", err, ids)
	}
	if got := f.loggedContaining("DELETE")[0].sql; got != `DELETE FROM "orgs" WHERE (id = $1) RETURNING "orgs"."id"` {
		t.Fatalf("delete into: %s", got)
	}

	if _, err := UpdateAllInto[rawPage](ctx, db, From[Org]().AllRows(), Set{"name": "x"}); err == nil || !strings.Contains(err.Error(), `UpdateAllInto: Org has no column "score" for rawPage.Score`) {
		t.Fatalf("unknown column: %v", err)
	}
	if _, err := DeleteAllInto[int64](ctx, db, From[Org]().AllRows()); err == nil || !strings.Contains(err.Error(), "DeleteAllInto: int64 is not a struct") {
		t.Fatalf("scalar target: %v", err)
	}
	my := newFakeDB().open(MySQL)
	if _, err := UpdateAllInto[orgName](ctx, my, From[Org]().AllRows(), Set{"name": "x"}); err == nil || !strings.Contains(err.Error(), "UpdateAllInto is not supported on mysql") {
		t.Fatalf("mysql update: %v", err)
	}
	if _, err := DeleteAllInto[userGone](ctx, my, From[User]().AllRows()); !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), "DeleteAllInto") {
		t.Fatalf("mysql soft delete: %v", err)
	}
}
