package rio

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestUpdateWhitelistBindsExplicitUpdatedAt(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	at := testNow.Add(-time.Hour)
	u := &User{ID: 5, Email: "w@x", Version: 1, UpdatedAt: at}

	f.queueExec(0, 1)
	if err := Update(ctx, db, u, "email", "updated_at"); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got := f.loggedContaining("UPDATE")[0]
	if got.sql != `UPDATE "users" SET "email" = $1, "updated_at" = $2, "version" = "version" + 1 WHERE "id" = $3 AND "version" = $4` {
		t.Fatalf("sql: %s", got.sql)
	}
	if bound, ok := got.args[1].(time.Time); !ok || !bound.Equal(at) || !u.UpdatedAt.Equal(at) {
		t.Fatalf("explicit updated_at must bind the struct's value: %v %v", got.args[1], u.UpdatedAt)
	}

	f.queueExec(0, 1)
	if err := Update(ctx, db, u, "email"); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got = f.loggedContaining("UPDATE")[1]
	if bound, ok := got.args[1].(time.Time); !ok || !bound.Equal(testNow) || !u.UpdatedAt.Equal(testNow) {
		t.Fatalf("an unlisted updated_at takes the clock: %v %v", got.args[1], u.UpdatedAt)
	}
}

func TestWithoutStampsUpdateWhitelistMayNameCreatedAt(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	at := testNow.Add(-time.Hour)
	u := &User{ID: 5, Email: "w@x", Version: 1, CreatedAt: at}

	err := Update(ctx, db, u, "created_at")
	if err == nil || !strings.Contains(err.Error(), `"created_at" is maintained by rio`) {
		t.Fatalf("stamped handle: %v", err)
	}

	f.queueExec(0, 1)
	if err := Update(ctx, db.WithoutStamps(), u, "email", "created_at"); err != nil {
		t.Fatalf("WithoutStamps: %v", err)
	}
	got := f.loggedContaining("UPDATE")[0]
	if !strings.HasPrefix(got.sql, `UPDATE "users" SET "email" = $1, "created_at" = $2, "version"`) {
		t.Fatalf("sql: %s", got.sql)
	}
	if bound, ok := got.args[1].(time.Time); !ok || !bound.Equal(at) {
		t.Fatalf("created_at binds the struct's value: %v", got.args[1])
	}
}
