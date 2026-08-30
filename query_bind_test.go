package rio

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestQueryDeferredArgs(t *testing.T) {
	ctx := context.Background()
	adults := From[User]().
		Where("age > ?").
		OrderBy("created_at DESC").
		Limit(10).
		Must()

	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"))
	f.queueRows(userCols)

	users, err := adults.All(ctx, db, 18)
	if err != nil || len(users) != 1 {
		t.Fatalf("All: %v %d", err, len(users))
	}
	if _, err := adults.All(ctx, db, 21); err != nil {
		t.Fatalf("second run: %v", err)
	}

	logs := f.logged()
	if logs[0] != logs[1] {
		t.Fatalf("fixed Query SQL must be identical across runs:\n%s\n%s", logs[0], logs[1])
	}
	if !strings.Contains(logs[0], "(age > $1)") || !strings.Contains(logs[0], "LIMIT 10") {
		t.Fatalf("sql: %s", logs[0])
	}
	stmt := f.loggedContaining("age >")[0]
	if stmt.args[0] != int64(18) {
		t.Fatalf("first run args: %v", stmt.args)
	}

	_, err = adults.All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "needs at least 1 deferred argument(s), got 0") {
		t.Fatalf("arity: %v", err)
	}
	if got := len(f.logged()); got != 2 {
		t.Fatalf("arity error sent SQL: %d statement(s)", got)
	}
}

func TestQueryInlineArgsRejectExecArgs(t *testing.T) {
	ctx := context.Background()
	q := From[User]().Where("age > ?", 18).Must()

	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols)

	if _, err := q.All(ctx, db); err != nil {
		t.Fatalf("inline run: %v", err)
	}
	if _, err := q.All(ctx, db, 21); err == nil || !strings.Contains(err.Error(), "takes 0 deferred argument(s), got 1") {
		t.Fatalf("inline query accepted exec args: %v", err)
	}
}

func TestQueryMixesInlineAndDeferredFragments(t *testing.T) {
	ctx := context.Background()
	q := From[User]().
		Where("email <> ?", "blocked@example.com").
		Where("active").
		Where("age >= ?").
		Must()

	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols)
	if _, err := q.All(ctx, db, 18); err != nil {
		t.Fatalf("All: %v", err)
	}
	stmt := f.loggedContaining("email <>")[0]
	if len(stmt.args) != 2 || stmt.args[0] != "blocked@example.com" || stmt.args[1] != int64(18) {
		t.Fatalf("mixed args = %#v", stmt.args)
	}
}

func TestQueryValidateRelationPaths(t *testing.T) {
	if err := From[User]().With("Posts.Author").Validate(); err != nil {
		t.Fatalf("valid path: %v", err)
	}
	err := From[User]().With("Posts.Nope").Validate()
	if err == nil || !strings.Contains(err.Error(), `no relation "Nope"`) {
		t.Fatalf("invalid path: %v", err)
	}
}

func TestQueryReusesAcrossDialects(t *testing.T) {
	ctx := context.Background()
	q := From[User]().Where("age > ?").Must()

	fpg := newFakeDB()
	pg := fpg.open(Postgres)
	flite := newFakeDB()
	lite := flite.open(SQLite)

	fpg.queueRows(userCols)
	flite.queueRows(userCols)
	if _, err := q.All(ctx, pg, 1); err != nil {
		t.Fatalf("pg: %v", err)
	}
	if _, err := q.All(ctx, lite, 1); err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	if !strings.Contains(fpg.logged()[0], "$1") {
		t.Fatalf("pg form: %s", fpg.logged()[0])
	}
	if !strings.Contains(flite.logged()[0], "age > ?") {
		t.Fatalf("sqlite form: %s", flite.logged()[0])
	}
}

func TestQueryDeferredFirstAndCount(t *testing.T) {
	ctx := context.Background()
	q := From[User]().Where("age > ?").Must()

	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols)
	if _, err := q.First(ctx, db, 18); !errors.Is(err, ErrNotFound) {
		t.Fatalf("First miss: %v", err)
	}

	f.queueRows([]string{"count"}, []driver.Value{int64(5)})
	n, err := q.Count(ctx, db, 18)
	if err != nil || n != 5 {
		t.Fatalf("Count: %v n=%d", err, n)
	}
	logs := f.logged()
	last := logs[len(logs)-1]
	if !strings.Contains(last, "SELECT count(*)") || !strings.Contains(last, "(age > $1)") {
		t.Fatalf("count sql: %s", last)
	}
}

func TestQueryValidateRejectsPlaceholdersInNoArgClauses(t *testing.T) {
	cases := []struct {
		name   string
		q      Query[User]
		clause string
	}{
		{"orderby", From[User]().Where("age > ?").OrderBy("CASE WHEN name = ? THEN 0 ELSE 1 END"), "OrderBy"},
		{"groupby", From[User]().GroupBy("substr(email, ?, 3)"), "GroupBy"},
		{"join", From[User]().Join("INNER JOIN orgs ON orgs.plan = ?"), "Join"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.q.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.clause+"(") ||
				!strings.Contains(err.Error(), "no argument channel") {
				t.Fatalf("err = %v", err)
			}
		})
	}

	defer func() {
		if recover() == nil {
			t.Fatal("Must must panic on a placeholder in OrderBy")
		}
	}()
	From[User]().Where("age > ?").OrderBy("CASE WHEN name = ? THEN 0 ELSE 1 END").Must()
}

func TestQueryValidateAllowsPlaceholderLookalikes(t *testing.T) {
	ctx := context.Background()
	q := From[User]().
		Where("age > ?").
		OrderBy("CASE WHEN email = '?' THEN 0 ELSE 1 END").
		Must()

	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols)
	if _, err := q.All(ctx, db, 18); err != nil {
		t.Fatalf("All: %v", err)
	}
	f.queueRows([]string{"count"}, []driver.Value{int64(0)})
	if _, err := q.Count(ctx, db, 18); err != nil {
		t.Fatalf("Count: %v", err)
	}

	if err := From[User]().Where("age > ?").OrderBy("data ?? 'k' DESC").Validate(); err != nil {
		t.Fatalf("?? escape: %v", err)
	}
}

func TestQueryNoArgPlaceholderFailsBeforeExecution(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	_, err := From[User]().Where("age > ?", 18).OrderBy("CASE WHEN name = ? THEN 0 ELSE 1 END").All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "no argument channel") {
		t.Fatalf("err = %v", err)
	}
	if got := len(f.logged()); got != 0 {
		t.Fatalf("invalid query sent %d statement(s)", got)
	}
}

func TestQueryDeferredSliceArgs(t *testing.T) {
	ctx := context.Background()
	q := From[User]().Where("id IN (?)").Must()

	f := newFakeDB()
	db := f.open()
	// Warm the scalar SQL shape; a later slice must bypass that cache and expand.
	f.queueRows(userCols)
	if _, err := q.All(ctx, db, int64(1)); err != nil {
		t.Fatalf("scalar All: %v", err)
	}
	f.queueRows(userCols)
	if _, err := q.All(ctx, db, []int64{1, 2}); err != nil {
		t.Fatalf("All: %v", err)
	}
	f.queueRows([]string{"count"}, []driver.Value{int64(2)})
	if n, err := q.Count(ctx, db, []int64{1, 2}); err != nil || n != 2 {
		t.Fatalf("Count: %v n=%d", err, n)
	}
	f.queueRows([]string{"1"}, []driver.Value{int64(1)})
	if ok, err := q.Exists(ctx, db, []int64{1, 2}); err != nil || !ok {
		t.Fatalf("Exists: %v %v", err, ok)
	}
	for _, stmt := range f.loggedContaining("id IN") {
		if len(stmt.args) == 1 {
			continue
		}
		if !strings.Contains(stmt.sql, "IN ($1, $2)") || len(stmt.args) != 2 {
			t.Fatalf("slice did not expand: %s %#v", stmt.sql, stmt.args)
		}
	}
	if _, err := q.All(ctx, db, []int64{}); err == nil || !strings.Contains(err.Error(), "empty slice") {
		t.Fatalf("empty slice: %v", err)
	}
}

func TestQueryCacheDoesNotFreezeInlineSliceArgs(t *testing.T) {
	ctx := context.Background()
	ids := []int64{1, 2}
	q := From[User]().Where("id IN (?)", ids).Must()

	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols)
	if _, err := q.All(ctx, db); err != nil {
		t.Fatalf("first All: %v", err)
	}
	ids[0] = 9
	f.queueRows(userCols)
	if _, err := q.All(ctx, db); err != nil {
		t.Fatalf("second All: %v", err)
	}

	stmts := f.loggedContaining("id IN")
	if got := stmts[0].args[0]; got != int64(1) {
		t.Fatalf("first execution arg = %#v", got)
	}
	if got := stmts[1].args[0]; got != int64(9) {
		t.Fatalf("cached Query froze inline slice arg: %#v", got)
	}
}

func TestQueryBuilderInvalidatesTemplateCache(t *testing.T) {
	base := From[User]().Where("age >= ?").Must()
	if base.cache == nil {
		t.Fatal("Must did not enable the private template cache")
	}
	derived := base.OrderBy("id DESC")
	if derived.cache != nil {
		t.Fatal("derived Query retained a stale template cache")
	}
	if base.cache == nil || len(base.s.orders) != 0 || len(derived.s.orders) != 1 {
		t.Fatalf("builder mutation: base=%+v derived=%+v", base.s, derived.s)
	}
}

func TestQueryConcurrentDeferredArgs(t *testing.T) {
	ctx := context.Background()
	q := From[User]().Where("age = ?").Must()
	f := newFakeDB()
	db := f.open()

	const runs = 16
	for range runs {
		f.queueRows(userCols)
	}
	var wg sync.WaitGroup
	for age := range runs {
		wg.Go(func() {
			if _, err := q.All(ctx, db, age); err != nil {
				t.Errorf("All(%d): %v", age, err)
			}
		})
	}
	wg.Wait()
	if got := len(f.logged()); got != runs {
		t.Fatalf("logged %d statements, want %d", got, runs)
	}
	if got := q.s.wheres[0].args; got != nil {
		t.Fatalf("reused Query was mutated: %#v", got)
	}
}

func TestQueryTerminalArityErrorsDoNotExecute(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		run  func(context.Context, Queryer, Query[User]) error
	}{
		{"All", func(ctx context.Context, db Queryer, q Query[User]) error { _, err := q.All(ctx, db); return err }},
		{"First", func(ctx context.Context, db Queryer, q Query[User]) error { _, err := q.First(ctx, db); return err }},
		{"Sole", func(ctx context.Context, db Queryer, q Query[User]) error { _, err := q.Sole(ctx, db); return err }},
		{"Rows", func(ctx context.Context, db Queryer, q Query[User]) error {
			for _, err := range q.Rows(ctx, db) {
				return err
			}
			return nil
		}},
		{"Count", func(ctx context.Context, db Queryer, q Query[User]) error { _, err := q.Count(ctx, db); return err }},
		{"Exists", func(ctx context.Context, db Queryer, q Query[User]) error { _, err := q.Exists(ctx, db); return err }},
		{"Pluck", func(ctx context.Context, db Queryer, q Query[User]) error {
			_, err := q.Pluck[string](ctx, db, "email")
			return err
		}},
		{"UpdateAll", func(ctx context.Context, db Queryer, q Query[User]) error {
			_, err := q.UpdateAll(ctx, db, Set{"age": 1})
			return err
		}},
		{
			"DeleteAll",
			func(ctx context.Context, db Queryer, q Query[User]) error {
				_, err := q.DeleteAll(ctx, db)
				return err
			},
		},
		{"ForceDeleteAll", func(ctx context.Context, db Queryer, q Query[User]) error {
			_, err := q.ForceDeleteAll(ctx, db)
			return err
		}},
		{"RestoreAll", func(ctx context.Context, db Queryer, q Query[User]) error {
			_, err := q.RestoreAll(ctx, db)
			return err
		}},
		{"FirstOrCreate", func(ctx context.Context, db Queryer, q Query[User]) error {
			row := User{Email: "a@x"}
			return q.FirstOrCreate(ctx, db, &row)
		}},
		{"CreateOrFirst", func(ctx context.Context, db Queryer, q Query[User]) error {
			row := User{Email: "a@x"}
			return q.CreateOrFirst(ctx, db, &row)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeDB()
			hook := &recordingHook{}
			db := f.openWith(Postgres, WithQueryHook(hook))
			err := tt.run(ctx, db, From[User]().Where("age > ?"))
			if err == nil || !strings.Contains(err.Error(), "deferred argument") {
				t.Fatalf("arity error = %v", err)
			}
			if got := len(f.logged()); got != 0 {
				t.Fatalf("sent %d statement(s)", got)
			}
			if got := len(hook.events); got != 0 {
				t.Fatalf("emitted %d hook event(s)", got)
			}
		})
	}
}

func TestQueryDeferredSetOperations(t *testing.T) {
	ctx := context.Background()

	t.Run("UpdateAll", func(t *testing.T) {
		f := newFakeDB()
		db := f.open()
		f.queueExec(0, 2)
		n, err := From[User]().Where("age = ?").UpdateAll(ctx, db, Set{"email": "new@x"}, 18)
		if err != nil || n != 2 {
			t.Fatalf("UpdateAll: %v n=%d", err, n)
		}
		stmt := f.loggedContaining("UPDATE")[0]
		if len(stmt.args) < 2 || stmt.args[len(stmt.args)-1] != int64(18) {
			t.Fatalf("args = %#v", stmt.args)
		}
	})

	t.Run("DeleteAll", func(t *testing.T) {
		f := newFakeDB()
		db := f.open()
		f.queueExec(0, 1)
		if _, err := From[User]().Where("age = ?").DeleteAll(ctx, db, 18); err != nil {
			t.Fatalf("DeleteAll: %v", err)
		}
		if stmt := f.loggedContaining("UPDATE")[0]; stmt.args[len(stmt.args)-1] != int64(18) {
			t.Fatalf("args = %#v", stmt.args)
		}
	})

	t.Run("ForceDeleteAll", func(t *testing.T) {
		f := newFakeDB()
		db := f.open()
		f.queueExec(0, 1)
		if _, err := From[User]().Where("age = ?").ForceDeleteAll(ctx, db, 18); err != nil {
			t.Fatalf("ForceDeleteAll: %v", err)
		}
		if stmt := f.loggedContaining("DELETE")[0]; len(stmt.args) != 1 || stmt.args[0] != int64(18) {
			t.Fatalf("args = %#v", stmt.args)
		}
	})

	t.Run("RestoreAll", func(t *testing.T) {
		f := newFakeDB()
		db := f.open()
		f.queueExec(0, 1)
		if _, err := From[User]().Where("age = ?").RestoreAll(ctx, db, 18); err != nil {
			t.Fatalf("RestoreAll: %v", err)
		}
		if stmt := f.loggedContaining("UPDATE")[0]; stmt.args[len(stmt.args)-1] != int64(18) {
			t.Fatalf("args = %#v", stmt.args)
		}
	})
}

func TestQueryUsesDialectLexerForDeferredFragments(t *testing.T) {
	ctx := context.Background()
	const expr = "age = ? # ?"

	t.Run("mysql hash comment", func(t *testing.T) {
		f := newFakeDB()
		db := f.open(MySQL)
		f.queueRows(userCols)
		if _, err := From[User]().Where(expr).All(ctx, db, 18); err != nil {
			t.Fatalf("All: %v", err)
		}
		if got := len(f.loggedContaining("age =")[0].args); got != 1 {
			t.Fatalf("got %d args, want 1", got)
		}
	})

	t.Run("postgres hash operator text", func(t *testing.T) {
		f := newFakeDB()
		db := f.open(Postgres)
		f.queueRows(userCols)
		if _, err := From[User]().Where(expr).All(ctx, db, 18, 21); err != nil {
			t.Fatalf("All: %v", err)
		}
		if got := len(f.loggedContaining("age =")[0].args); got != 2 {
			t.Fatalf("got %d args, want 2", got)
		}
	})

	t.Run("dialect-sensitive inline mismatch", func(t *testing.T) {
		q := From[User]().Where(expr, 18)
		if err := q.Validate(); err != nil {
			t.Fatalf("dialect-independent validation must defer: %v", err)
		}
		f := newFakeDB()
		db := f.open(Postgres)
		if _, err := q.All(ctx, db); err == nil || !strings.Contains(err.Error(), "under the postgres dialect") {
			t.Fatalf("All: %v", err)
		}
		if got := len(f.logged()); got != 0 {
			t.Fatalf("mismatch sent %d statement(s)", got)
		}
	})
}

func BenchmarkQueryDeferredTemplate(b *testing.B) {
	q := From[User]().
		Where("tenant_id = ?", int64(7)).
		Where("active").
		Where("age >= ?").
		OrderBy("created_at DESC").
		Limit(10).
		Must()
	g := newFakeDB().open(Postgres).gram()
	execArgs := []any{18}

	b.ReportAllocs()
	for b.Loop() {
		key := queryCacheKey{grammar: g.weakSelf, op: queryCacheAll}
		_, _, sqlText, args, err := prepareCachedSelect[User](q.cache, key, g, &q.s, selectRows, execArgs)
		if err != nil {
			b.Fatal(err)
		}
		if len(sqlText) == 0 || len(args) != 2 {
			b.Fatalf("invalid render: %q %#v", sqlText, args)
		}
	}
}
