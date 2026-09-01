package rio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"testing"
)

func TestAggregatesGolden(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	q := From[User]().Where("age > ?", 18)

	f.queueRows([]string{"sum"}, []driver.Value{int64(42)})
	sum, err := q.Sum[int64](ctx, db, "age")
	if err != nil || sum != 42 {
		t.Fatalf("Sum: %v %d", err, sum)
	}
	want := `SELECT sum("users"."age") FROM "users" WHERE (age > $1) AND "users"."deleted_at" IS NULL`
	if got := f.logged()[0]; got != want {
		t.Fatalf("sum sql:\n got: %s\nwant: %s", got, want)
	}

	f.queueRows([]string{"avg"}, []driver.Value{float64(2.5)})
	avg, err := q.Distinct().Avg[float64](ctx, db, "age")
	if err != nil || avg != 2.5 {
		t.Fatalf("Avg: %v %v", err, avg)
	}
	if got := f.logged()[1]; !strings.HasPrefix(got, `SELECT avg(DISTINCT "users"."age") FROM`) {
		t.Fatalf("avg honors Distinct: %s", got)
	}

	f.queueRows([]string{"min"}, []driver.Value{"a@x"})
	if v, err := q.Distinct().Min[string](ctx, db, "email"); err != nil || v != "a@x" {
		t.Fatalf("Min: %v %q", err, v)
	}
	if got := f.logged()[2]; !strings.HasPrefix(got, `SELECT min("users"."email") FROM`) {
		t.Fatalf("min ignores Distinct: %s", got)
	}

	f.queueRows([]string{"max"}, []driver.Value{int64(9)})
	if v, err := q.Max[int64](ctx, db, "age"); err != nil || v != 9 {
		t.Fatalf("Max: %v %d", err, v)
	}
}

// Over no rows the aggregate row is NULL: a plain V reads its zero value,
// sql.Null[V] reports the difference.
func TestAggregatesOverNoRows(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	f.queueRows([]string{"sum"}, []driver.Value{nil})
	if sum, err := From[User]().Sum[int64](ctx, db, "age"); err != nil || sum != 0 {
		t.Fatalf("Sum over nothing: %v %d", err, sum)
	}
	f.queueRows([]string{"sum"}, []driver.Value{nil})
	n, err := From[User]().Sum[sql.Null[int64]](ctx, db, "age")
	if err != nil || n.Valid {
		t.Fatalf("Null sum over nothing: %v %+v", err, n)
	}
	f.queueRows([]string{"sum"}, []driver.Value{int64(7)})
	n, err = From[User]().Sum[sql.Null[int64]](ctx, db, "age")
	if err != nil || !n.Valid || n.V != 7 {
		t.Fatalf("Null sum: %v %+v", err, n)
	}
}

func TestAggregatesRejectShapes(t *testing.T) {
	ctx := context.Background()
	db := newFakeDB().open()
	for name, err := range map[string]error{
		"GroupBy": func() error { _, e := From[User]().GroupBy("age").Sum[int64](ctx, db, "age"); return e }(),
		"Limit":   func() error { _, e := From[User]().Limit(3).Max[int64](ctx, db, "age"); return e }(),
		"column":  func() error { _, e := From[User]().Min[int64](ctx, db, "nope"); return e }(),
	} {
		if err == nil {
			t.Fatalf("%s must be rejected", name)
		}
	}
}

func TestAggregateMustCaches(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	q := From[User]().Where("age > ?").Must()
	f.queueRows([]string{"sum"}, []driver.Value{int64(1)})
	f.queueRows([]string{"sum"}, []driver.Value{int64(2)})
	if _, err := q.Sum[int64](ctx, db, "age", 18); err != nil {
		t.Fatal(err)
	}
	if v, err := q.Sum[int64](ctx, db, "age", 21); err != nil || v != 2 {
		t.Fatalf("second Sum: %v %d", err, v)
	}
	stmts := f.loggedContaining("sum(")
	if len(stmts) != 2 || stmts[0].sql != stmts[1].sql || stmts[1].args[0] != int64(21) {
		t.Fatalf("cached shape must rebind: %+v", stmts)
	}
}

func TestDistinctGolden(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	f.queueRows(userCols)
	if _, err := From[User]().Distinct().All(ctx, db); err != nil {
		t.Fatal(err)
	}
	if got := f.logged()[0]; !strings.HasPrefix(got, `SELECT DISTINCT "users"."id", "users"."email"`) {
		t.Fatalf("distinct rows: %s", got)
	}

	f.queueRows([]string{"count"}, []driver.Value{int64(3)})
	if n, err := From[User]().Distinct().Join(`JOIN posts ON posts.user_id = users.id`).Count(ctx, db); err != nil || n != 3 {
		t.Fatalf("Count: %v %d", err, n)
	}
	if got := f.logged()[1]; !strings.HasPrefix(got, `SELECT count(DISTINCT "users"."id") FROM "users" JOIN posts`) {
		t.Fatalf("distinct count: %s", got)
	}

	f.queueRows([]string{"email"}, []driver.Value{"a@x"})
	if _, err := From[User]().Distinct().Pluck[string](ctx, db, "email"); err != nil {
		t.Fatal(err)
	}
	if got := f.logged()[2]; !strings.HasPrefix(got, `SELECT DISTINCT "users"."email" FROM "users"`) {
		t.Fatalf("distinct pluck: %s", got)
	}

	if _, err := From[Grant]().Distinct().Count(ctx, db); err == nil || !strings.Contains(err.Error(), "single-column primary key") {
		t.Fatalf("composite key distinct count: %v", err)
	}
}
