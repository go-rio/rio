package rio

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestLockStrengthsAndTargets(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	pg := f.open()
	for i, tc := range []struct {
		q    Query[Org]
		want string
	}{
		{From[Org]().Where("id = ?", 1).ForNoKeyUpdate(), " FOR NO KEY UPDATE"},
		{From[Org]().Where("id = ?", 1).ForKeyShare(), " FOR KEY SHARE"},
		{From[Org]().Where("id = ?", 1).ForUpdate(LockOf("orgs"), SkipLocked), " FOR UPDATE OF orgs SKIP LOCKED"},
		{
			From[Org]().Join("JOIN users u ON u.org_id = orgs.id").Where("u.id = ?", 1).ForShare(LockOf("orgs", "u"), NoWait),
			" FOR SHARE OF orgs, u NOWAIT",
		},
	} {
		f.queueRows(orgCols)
		if _, err := tc.q.All(ctx, pg); err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if got := f.logged()[i]; !strings.HasSuffix(got, tc.want) {
			t.Fatalf("%d: %s (want suffix %q)", i, got, tc.want)
		}
	}

	fm := newFakeDB()
	my := fm.open(MySQL)
	fm.queueRows(orgCols)
	_, _ = From[Org]().Where("id = ?", 1).ForNoKeyUpdate(LockOf("orgs")).All(ctx, my)
	fm.queueRows(orgCols)
	_, _ = From[Org]().Where("id = ?", 1).ForKeyShare(SkipLocked).All(ctx, my)
	if logs := fm.logged(); !strings.HasSuffix(logs[0], " FOR UPDATE OF orgs") || !strings.HasSuffix(logs[1], " FOR SHARE SKIP LOCKED") {
		t.Fatalf("mysql takes the next stronger lock: %v", logs)
	}

	fs := newFakeDB()
	lite := fs.open(SQLite)
	fs.queueRows(orgCols)
	_, _ = From[Org]().Where("id = ?", 1).ForKeyShare(LockOf("orgs")).All(ctx, lite)
	if got := fs.logged()[0]; strings.Contains(got, " FOR ") {
		t.Fatalf("sqlite elides the lock: %s", got)
	}

	ch := newFakeDB().open(ClickHouse)
	if _, err := From[Org]().ForKeyShare().All(ctx, ch); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("clickhouse rejects: %v", err)
	}

	f.queueRows([]string{"id"})
	if _, err := From[Org]().Where("id = ?", 1).ForKeyShare().Pluck[int64](ctx, pg, "id"); err != nil {
		t.Fatalf("Pluck: %v", err)
	}
	if got := f.logged()[4]; !strings.HasSuffix(got, " FOR KEY SHARE") {
		t.Fatalf("Pluck carries the lock: %s", got)
	}
}

func TestWhereAllAppendsConditions(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	conds := []Condition{Cond("name ILIKE ?", "%acme%"), Cond("id > ?"), Cond("id IN (?)")}
	q := From[Org]().WhereAll(conds...).OrderBy("id").Must()

	f.queueRows(orgCols)
	if _, err := q.All(ctx, db, 5, []int64{7, 8}); err != nil {
		t.Fatalf("All: %v", err)
	}
	got := f.loggedContaining("SELECT")[0]
	want := `SELECT "orgs"."id", "orgs"."name" FROM "orgs" WHERE (name ILIKE $1) AND (id > $2) AND (id IN ($3, $4)) ORDER BY id`
	if got.sql != want || len(got.args) != 4 || got.args[0] != "%acme%" || got.args[1] != int64(5) {
		t.Fatalf("sql: %s %v", got.sql, got.args)
	}
	if err := From[Org]().WhereAll(Cond("a = ? AND b = ?", 1)).Validate(); err == nil {
		t.Fatal("inline arity is checked per condition")
	}
}

func TestTableOverridesRenderedTable(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	archived := From[Org]().Table("orgs_archive").Where("id = ?").Must()

	f.queueRows(orgCols)
	if _, err := From[Org]().Where("id = ?", 1).All(ctx, db); err != nil {
		t.Fatalf("All: %v", err)
	}
	f.queueRows(orgCols)
	if _, err := archived.All(ctx, db, 1); err != nil {
		t.Fatalf("archived All: %v", err)
	}
	f.queueRows([]string{"count"}, []driver.Value{int64(2)})
	if _, err := archived.Count(ctx, db, 1); err != nil {
		t.Fatalf("Count: %v", err)
	}
	f.queueRows([]string{"name"})
	if _, err := archived.Pluck[string](ctx, db, "name", 1); err != nil {
		t.Fatalf("Pluck: %v", err)
	}
	f.queueExec(0, 1)
	if _, err := archived.UpdateAll(ctx, db, Set{"name": "x"}, 1); err != nil {
		t.Fatalf("UpdateAll: %v", err)
	}
	f.queueExec(0, 1)
	if _, err := archived.DeleteAll(ctx, db, 1); err != nil {
		t.Fatalf("DeleteAll: %v", err)
	}
	logs := f.logged()
	want := []string{
		`SELECT "orgs"."id", "orgs"."name" FROM "orgs" WHERE (id = $1)`,
		`SELECT "orgs_archive"."id", "orgs_archive"."name" FROM "orgs_archive" WHERE (id = $1)`,
		`SELECT count(*) FROM "orgs_archive" WHERE (id = $1)`,
		`SELECT "orgs_archive"."name" FROM "orgs_archive" WHERE (id = $1)`,
		`UPDATE "orgs_archive" SET "name" = $1 WHERE (id = $2)`,
		`DELETE FROM "orgs_archive" WHERE (id = $1)`,
	}
	for i, w := range want {
		if logs[i] != w {
			t.Fatalf("statement %d:\n got: %s\nwant: %s", i, logs[i], w)
		}
	}
	f.queueRows(orgCols)
	if _, err := From[Org]().Where("id = ?", 1).All(ctx, db); err != nil {
		t.Fatalf("All: %v", err)
	}
	if got := f.logged()[6]; got != want[0] {
		t.Fatalf("the override must not poison the cached head: %s", got)
	}
}

func TestLimitOffsetBindAndShareOneShape(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	q := From[Org]().Where("id > ?").OrderBy("id").Limit(20).Offset(40).Must()

	f.queueRows(orgCols)
	if _, err := q.All(ctx, db, 5); err != nil {
		t.Fatalf("All: %v", err)
	}
	f.queueRows(orgCols)
	if _, err := q.All(ctx, db, 6); err != nil {
		t.Fatalf("cached All: %v", err)
	}
	f.queueRows(orgCols)
	if _, err := q.Offset(60).All(ctx, db, 5); err != nil {
		t.Fatalf("next page: %v", err)
	}
	want := `SELECT "orgs"."id", "orgs"."name" FROM "orgs" WHERE (id > $1) ORDER BY id LIMIT $2 OFFSET $3`
	stmts := f.loggedContaining("SELECT")
	for i, stmt := range stmts {
		if stmt.sql != want {
			t.Fatalf("statement %d: %s", i, stmt.sql)
		}
	}
	if stmts[0].args[0] != int64(5) || stmts[1].args[0] != int64(6) || stmts[1].args[1] != int64(20) || stmts[2].args[2] != int64(60) {
		t.Fatalf("args: %v %v %v", stmts[0].args, stmts[1].args, stmts[2].args)
	}
	entries := 0
	q.cache.entries.Range(func(_, _ any) bool { entries++; return true })
	if entries != 1 {
		t.Fatalf("cache entries = %d", entries)
	}
}
