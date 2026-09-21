package rio

import (
	"context"
	"testing"
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
