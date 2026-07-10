package rio

import (
	"context"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
)

type Tag struct {
	ID   int64
	Name string
}

type Account struct {
	ID    int64
	OrgID *int64

	Org  BelongsTo[Org]  `rio:",fk:org_id"`
	Tags ManyToMany[Tag] `rio:",join:account_tags"`
}

type Org struct {
	ID   int64
	Name string
}

var (
	postCols = []string{"id", "user_id", "title"}
	orgCols  = []string{"id", "name"}
)

func TestPreloadHasMany(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"), userRow(2, "b@x"))
	f.queueRows(postCols,
		[]driver.Value{int64(10), int64(1), "first"},
		[]driver.Value{int64(11), int64(2), "second"},
		[]driver.Value{int64(12), int64(1), "third"},
	)

	users, err := From[User]().With("Posts").All(context.Background(), db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	rel := f.logged()[1]
	want := `SELECT "posts"."id", "posts"."user_id", "posts"."title" FROM "posts" WHERE "posts"."user_id" IN ($1, $2)`
	if rel != want {
		t.Fatalf("preload sql:\n got: %s\nwant: %s", rel, want)
	}
	if got := users[0].Posts.Rows(); len(got) != 2 || got[0].Title != "first" || got[1].Title != "third" {
		t.Fatalf("user1 posts: %+v", got)
	}
	if got := users[1].Posts.Rows(); len(got) != 1 || got[0].ID != 11 {
		t.Fatalf("user2 posts: %+v", got)
	}
}

func TestPreloadLoadedEmpty(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"))
	f.queueRows(postCols) // no children

	users, err := From[User]().With("Posts").All(context.Background(), db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if !users[0].Posts.Loaded() {
		t.Fatal("no children must still mark the relation loaded")
	}
	if rows := users[0].Posts.Rows(); rows == nil || len(rows) != 0 {
		t.Fatalf("loaded-empty must be an empty slice, got %#v", rows)
	}
}

func TestPreloadBelongsToAndNullFK(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	org := int64(7)
	f.queueRows([]string{"id", "org_id"},
		[]driver.Value{int64(1), int64(7)},
		[]driver.Value{int64(2), nil}, // NULL FK
		[]driver.Value{int64(3), int64(7)},
	)
	f.queueRows(orgCols, []driver.Value{int64(7), "acme"})

	accounts, err := From[Account]().With("Org").All(context.Background(), db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	rel := f.logged()[1]
	if !strings.Contains(rel, `"orgs"."id" IN ($1)`) {
		t.Fatalf("dedup to one key: %s", rel)
	}
	if got := accounts[0].Org.Row(); got == nil || got.Name != "acme" {
		t.Fatalf("account1 org: %+v", got)
	}
	// NULL FK: loaded-nil — Row() stays safe, never panics after With.
	if !accounts[1].Org.Loaded() || accounts[1].Org.Row() != nil {
		t.Fatalf("NULL FK must be loaded-nil: %+v", accounts[1].Org)
	}
	// Shared parents are value copies, not aliases.
	accounts[0].Org.Row().Name = "mutated"
	if accounts[2].Org.Row().Name != "acme" {
		t.Fatal("parents must not share one instance")
	}
	_ = org
}

func TestPreloadNested(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"))
	f.queueRows(postCols, []driver.Value{int64(10), int64(1), "post"})
	f.queueRows(userCols, userRow(1, "a@x")) // Posts.Author

	users, err := From[User]().With("Posts.Author").All(context.Background(), db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	posts := users[0].Posts.Rows()
	if len(posts) != 1 {
		t.Fatalf("posts: %+v", posts)
	}
	author := posts[0].Author.Row()
	if author == nil || author.Email != "a@x" {
		t.Fatalf("nested author must be assembled before the copy into parents: %+v", author)
	}
}

func TestPreloadRelOptions(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"))
	f.queueRows(postCols)

	_, err := From[User]().
		With("Posts", RelWhere("title <> ?", ""), RelOrder("id DESC")).
		All(context.Background(), db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	rel := f.logged()[1]
	if !strings.Contains(rel, `AND (title <> $2) ORDER BY id DESC`) {
		t.Fatalf("rel options: %s", rel)
	}
}

func TestPreloadManyToMany(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id", "org_id"}, []driver.Value{int64(1), nil}, []driver.Value{int64(2), nil})
	f.queueRows([]string{"id", "name", "user_id"},
		[]driver.Value{int64(100), "go", int64(1)},
		[]driver.Value{int64(100), "go", int64(2)}, // shared tag
		[]driver.Value{int64(101), "db", int64(1)},
	)

	accounts, err := From[Account]().With("Tags").All(context.Background(), db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	rel := f.logged()[1]
	want := `SELECT "tags"."id", "tags"."name", "account_tags"."account_id" FROM "tags" INNER JOIN "account_tags" ON "account_tags"."tag_id" = "tags"."id" WHERE "account_tags"."account_id" IN ($1, $2)`
	if rel != want {
		t.Fatalf("m2m sql:\n got: %s\nwant: %s", rel, want)
	}
	if got := accounts[0].Tags.Rows(); len(got) != 2 || got[0].Name != "go" || got[1].Name != "db" {
		t.Fatalf("account1 tags: %+v", got)
	}
	if got := accounts[1].Tags.Rows(); len(got) != 1 || got[0].ID != 100 {
		t.Fatalf("account2 tags: %+v", got)
	}
}

// The panic must name the Go field ("Posts"), not the target type ("Post"):
// With resolves relations by field name, so the old type-name hint sent
// users straight into a second error (AUDIT M12).
func TestUnloadedRelationPanics(t *testing.T) {
	if _, err := planOf[User](); err != nil { // the built plan teaches the field name
		t.Fatal(err)
	}
	defer func() {
		p := recover()
		if p == nil {
			t.Fatal("Rows on an unloaded relation must panic")
		}
		msg := p.(string)
		if !strings.Contains(msg, "HasMany[Post]") || !strings.Contains(msg, `With("Posts")`) {
			t.Fatalf("panic must explain the fix: %s", msg)
		}
	}()
	var u User
	_ = u.Posts.Rows()
}

type orphanTarget struct{ ID int64 }

// Without a built plan the field name is unknowable — the panic falls back
// to generic wording instead of guessing (the type name would be wrong for
// every pluralized field).
func TestUnloadedPanicWithoutPlanStaysGeneric(t *testing.T) {
	defer func() {
		msg, _ := recover().(string)
		if !strings.Contains(msg, `With("<the Go field name of this HasMany[orphanTarget] field>")`) {
			t.Fatalf("unregistered container must get the generic hint: %s", msg)
		}
	}()
	var r HasMany[orphanTarget]
	_ = r.Rows()
}

type ambigTarget struct{ ID int64 }

type ambigOwnerA struct {
	ID    int64
	Items HasMany[ambigTarget]
}

type ambigOwnerB struct {
	ID    int64
	Stuff HasMany[ambigTarget]
}

// Two models declaring the same container type under different field names:
// naming either would be wrong for the other, so the hint goes generic.
func TestUnloadedPanicAmbiguousFieldNameStaysGeneric(t *testing.T) {
	if _, err := planOf[ambigOwnerA](); err != nil {
		t.Fatal(err)
	}
	if _, err := planOf[ambigOwnerB](); err != nil {
		t.Fatal(err)
	}
	defer func() {
		msg, _ := recover().(string)
		if strings.Contains(msg, `With("Items")`) || strings.Contains(msg, `With("Stuff")`) {
			t.Fatalf("ambiguous container must not pick a side: %s", msg)
		}
		if !strings.Contains(msg, "the Go field name of this") {
			t.Fatalf("ambiguous container must fall back to the generic hint: %s", msg)
		}
	}()
	var r HasMany[ambigTarget]
	_ = r.Rows()
}

func TestManualSetIsLegitimate(t *testing.T) {
	var u User
	u.Posts.Set([]Post{{ID: 1}})
	if !u.Posts.Loaded() || len(u.Posts.Rows()) != 1 {
		t.Fatal("manual assembly must work")
	}
	u.Posts.Set(nil)
	if u.Posts.Rows() == nil {
		t.Fatal("Set(nil) normalizes to loaded-empty")
	}
}

func TestPreloadUnknownRelation(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"))

	_, err := From[User]().With("Nope").All(context.Background(), db)
	if err == nil || !strings.Contains(err.Error(), `no relation "Nope"`) {
		t.Fatalf("unknown relation: %v", err)
	}
}

// Pointer keys group by value, never by address — a *int64 owner PK must
// match the int64 keys scanned back from join rows and count queries.
func TestCanonKeyDereferencesPointers(t *testing.T) {
	n := int64(7)
	if canonKey(reflect.ValueOf(&n)) != canonKey(reflect.ValueOf(int64(7))) {
		t.Fatal("pointer key must group with its value")
	}
	var nilp *int64
	if canonKey(reflect.ValueOf(nilp)) != nil {
		t.Fatal("nil pointer key must canonicalize to nil")
	}
}

// With/WithCount typos must fail on every execution, not only when the
// result set happens to be non-empty (AUDIT M13: the old data-dependent
// check let empty-fixture tests ship misspelled relation names).
func TestWithValidationDoesNotDependOnData(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	if _, err := From[User]().With("Bogus").All(ctx, db); err == nil || !strings.Contains(err.Error(), `no relation "Bogus"`) {
		t.Fatalf("With typo on empty result: %v", err)
	}
	if _, err := From[User]().WithCount("Bogus").All(ctx, db); err == nil || !strings.Contains(err.Error(), `no relation "Bogus"`) {
		t.Fatalf("WithCount typo on empty result: %v", err)
	}
	if _, err := From[User]().With("Posts.Bogus").All(ctx, db); err == nil || !strings.Contains(err.Error(), `no relation "Bogus" (path "Posts.Bogus")`) {
		t.Fatalf("nested typo on empty result: %v", err)
	}
	// First must report the typo, not hide it behind ErrNotFound.
	if _, err := From[User]().With("Bogus").First(ctx, db); err == nil || !strings.Contains(err.Error(), `no relation "Bogus"`) {
		t.Fatalf("First with typo: %v", err)
	}
	if got := f.logged(); len(got) != 0 {
		t.Fatalf("validation is metadata-only and must not reach the database: %v", got)
	}
}

type XFParent struct {
	ID        int64
	Kids      HasMany[XFChild] `rio:",fk:parent_id"`
	KidsCount int64            `rio:",countof:Kids"`
}

type XFChild struct {
	ID       int64
	ParentID string `rio:"parent_id"`
}

// A string FK against an int64 PK groups by keys that can never be equal:
// the rows come back from the database and are then silently dropped during
// assembly (AUDIT M11). resolveRel must refuse the pair instead.
func TestPreloadRefusesCrossFamilyKeys(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})

	_, err := From[XFParent]().With("Kids").All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "never compare equal") {
		t.Fatalf("cross-family keys must refuse loudly: %v", err)
	}
	for _, frag := range []string{"XFParent.ID (int64)", "XFChild.ParentID (string)"} {
		if !strings.Contains(err.Error(), frag) {
			t.Fatalf("error must name both sides, missing %q: %v", frag, err)
		}
	}

	// WithCount walks the same resolution and must fail identically — the
	// audit's smoking gun was PostsCount=1 next to a loaded-empty Posts.
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	_, err = From[XFParent]().WithCount("Kids").All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "never compare equal") {
		t.Fatalf("WithCount must refuse the same pair: %v", err)
	}
}

type XOKParent struct {
	ID   int64
	Kids HasMany[XOKChild] `rio:",fk:parent_id"`
}

type XOKChild struct {
	ID       int64
	ParentID int32 `rio:"parent_id"`
}

// Same-family pairs stay legal: canonKey folds all integer kinds together,
// so an int32 FK loads against an int64 PK (pointer FKs are covered by
// TestPreloadBelongsToAndNullFK's *int64).
func TestPreloadAllowsSameFamilyKeys(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	f.queueRows([]string{"id", "parent_id"}, []driver.Value{int64(10), int64(1)})

	parents, err := From[XOKParent]().With("Kids").All(ctx, db)
	if err != nil {
		t.Fatalf("int32 FK against int64 PK must load: %v", err)
	}
	if kids := parents[0].Kids.Rows(); len(kids) != 1 || kids[0].ID != 10 {
		t.Fatalf("kids: %+v", kids)
	}
}

type CompositeSide struct {
	A int64 `rio:",pk"`
	B int64 `rio:",pk"`
}

type CompositeOwner struct {
	ID   int64
	Side ManyToMany[CompositeSide]
}

// The old advice "set ref: explicitly" cannot work on ManyToMany — there
// ref: names a join-table column, not a key (AUDIT LB6). The error must
// state the v1 limitation and a path that actually exists.
func TestManyToManyCompositePKError(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})

	_, err := From[CompositeOwner]().With("Side").All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "composite primary keys is not supported in v1") {
		t.Fatalf("composite-PK m2m must state the limitation: %v", err)
	}
	if !strings.Contains(err.Error(), "surrogate key") {
		t.Fatalf("error must point at a workable path: %v", err)
	}
	if strings.Contains(err.Error(), "set ref:") {
		t.Fatalf("ref: cannot fix an m2m composite PK, the hint must be gone: %v", err)
	}
}
