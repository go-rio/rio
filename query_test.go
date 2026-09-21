package rio

import (
	"context"
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
