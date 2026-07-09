package rio

import (
	"context"
	"database/sql/driver"
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

func TestUnloadedRelationPanics(t *testing.T) {
	defer func() {
		p := recover()
		if p == nil {
			t.Fatal("Rows on an unloaded relation must panic")
		}
		msg := p.(string)
		if !strings.Contains(msg, "HasMany[Post]") || !strings.Contains(msg, `With("Post")`) {
			t.Fatalf("panic must explain the fix: %s", msg)
		}
	}()
	var u User
	_ = u.Posts.Rows()
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
