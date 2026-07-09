package rio

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestCompiledExecMode(t *testing.T) {
	ctx := context.Background()
	adults := MustCompile[User](
		From[User]().Where("age > ?").OrderBy("created_at DESC").Limit(10),
	)

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
		t.Fatalf("compiled SQL must be identical across runs:\n%s\n%s", logs[0], logs[1])
	}
	if !strings.Contains(logs[0], "(age > $1)") || !strings.Contains(logs[0], "LIMIT 10") {
		t.Fatalf("sql: %s", logs[0])
	}
	stmt := f.loggedContaining("age >")[0]
	if stmt.args[0] != int64(18) {
		t.Fatalf("first run args: %v", stmt.args)
	}

	if _, err := adults.All(ctx, db); err == nil || !strings.Contains(err.Error(), "takes 1 argument(s), got 0") {
		t.Fatalf("arity: %v", err)
	}
	if _, err := adults.All(ctx, db, []int{1, 2}); err == nil || !strings.Contains(err.Error(), "cannot expand slice") {
		t.Fatalf("slices need inline values: %v", err)
	}
}

func TestCompiledInlineMode(t *testing.T) {
	ctx := context.Background()
	q := MustCompile[User](From[User]().Where("age > ?", 18))

	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols)

	if _, err := q.All(ctx, db); err != nil {
		t.Fatalf("inline run: %v", err)
	}
	if _, err := q.All(ctx, db, 21); err == nil || !strings.Contains(err.Error(), "fully inline") {
		t.Fatalf("inline queries take no exec args: %v", err)
	}
}

func TestCompileRejectsMixedArgs(t *testing.T) {
	_, err := Compile[User](From[User]().Where("age > ?", 18).Where("email = ?"))
	if err == nil || !strings.Contains(err.Error(), "fully inline or fully exec-parameterized") {
		t.Fatalf("mixed args: %v", err)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("MustCompile must panic on mixed args")
		}
	}()
	MustCompile[User](From[User]().Where("age > ?", 18).Where("email = ?"))
}

func TestCompileValidatesRelationPaths(t *testing.T) {
	if _, err := Compile[User](From[User]().With("Posts.Author")); err != nil {
		t.Fatalf("valid path: %v", err)
	}
	_, err := Compile[User](From[User]().With("Posts.Nope"))
	if err == nil || !strings.Contains(err.Error(), `no relation "Nope"`) {
		t.Fatalf("invalid path: %v", err)
	}
}

func TestCompiledPerGrammarCache(t *testing.T) {
	ctx := context.Background()
	q := MustCompile[User](From[User]().Where("age > ?"))

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

func TestCompiledFirstAndCount(t *testing.T) {
	ctx := context.Background()
	q := MustCompile[User](From[User]().Where("age > ?"))

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
