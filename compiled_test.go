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

// AUDIT M3 regression: Compile's placeholder accounting only looked at
// Where/Having, so a ? in OrderBy/GroupBy/Join slipped the inline/exec
// classification — MustCompile did not panic and every execution failed (or
// the arity contract split between All and Count). Those clauses have no
// argument channel, so a placeholder there is now a structural error.
func TestCompileRejectsPlaceholdersInNoArgClauses(t *testing.T) {
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
		_, err := Compile[User](tc.q)
		if err == nil || !strings.Contains(err.Error(), tc.clause+"(") ||
			!strings.Contains(err.Error(), "no argument channel") {
			t.Errorf("%s: err = %v", tc.name, err)
		}
	}

	defer func() {
		if recover() == nil {
			t.Fatal("MustCompile must panic on a placeholder in OrderBy")
		}
	}()
	MustCompile[User](From[User]().Where("age > ?").OrderBy("CASE WHEN name = ? THEN 0 ELSE 1 END"))
}

// Placeholder lookalikes in those clauses stay legal: a ? inside a string
// literal and the ?? escape are not placeholders under any dialect's lexer.
// With the holes closed, All and Count agree on the exec arity again.
func TestCompileAllowsPlaceholderLookalikesInOrderBy(t *testing.T) {
	ctx := context.Background()
	q := MustCompile[User](
		From[User]().Where("age > ?").OrderBy("CASE WHEN email = '?' THEN 0 ELSE 1 END"),
	)

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

	if _, err := Compile[User](From[User]().Where("age > ?").OrderBy("data ?? 'k' DESC")); err != nil {
		t.Fatalf("?? escape: %v", err)
	}
}

// Uncompiled queries keep their behavior: a ? in OrderBy still fails loudly
// at the statement-level render check, never silently.
func TestUncompiledOrderByPlaceholderFailsAtRender(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	_, err := From[User]().Where("age > ?", 18).OrderBy("CASE WHEN name = ? THEN 0 ELSE 1 END").All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "has no argument") {
		t.Fatalf("err = %v", err)
	}
}

// AUDIT M4 regression: Count/Exists accepted and expanded slice exec
// arguments that All/First/Rows reject — the same compiled object carried
// two contracts. Slice expansion is inline-only on all execution points.
func TestCompiledCountExistsRejectSliceArgs(t *testing.T) {
	ctx := context.Background()
	q := MustCompile[User](From[User]().Where("id IN (?)"))

	f := newFakeDB()
	db := f.open()
	if _, err := q.Count(ctx, db, []int64{1, 2}); err == nil || !strings.Contains(err.Error(), "cannot expand slice") {
		t.Fatalf("Count: %v", err)
	}
	if _, err := q.Exists(ctx, db, []int64{1, 2}); err == nil || !strings.Contains(err.Error(), "cannot expand slice") {
		t.Fatalf("Exists: %v", err)
	}

	// Inline slices expand at compile time; all four execution points agree.
	inline := MustCompile[User](From[User]().Where("id IN (?)", []int64{1, 2}))
	f.queueRows(userCols)
	if _, err := inline.All(ctx, db); err != nil {
		t.Fatalf("inline All: %v", err)
	}
	f.queueRows([]string{"count"}, []driver.Value{int64(2)})
	if n, err := inline.Count(ctx, db); err != nil || n != 2 {
		t.Fatalf("inline Count: %v n=%d", err, n)
	}
	f.queueRows([]string{"1"}, []driver.Value{int64(1)})
	if ok, err := inline.Exists(ctx, db); err != nil || !ok {
		t.Fatalf("inline Exists: %v %v", err, ok)
	}
}
