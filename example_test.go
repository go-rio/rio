package rio_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-rio/rio"
)

// db stands for a handle opened by a driver module, for example
// sqlite.Open("file:app.db"). The core has no driver, so these examples
// compile but do not run.
var db *rio.DB

// User is a model: ID is the auto-increment primary key by convention,
// CreatedAt is maintained on insert, PostCount is the WithCount("Posts")
// target, and Posts loads only through With.
type User struct {
	ID         int64
	Email      string
	Age        int
	Active     bool
	LoginCount int64
	PostCount  int64 `rio:",countof:Posts"`
	CreatedAt  time.Time

	Posts rio.HasMany[Post]
}

// Post belongs to a User through the conventional user_id foreign key.
type Post struct {
	ID        int64
	UserID    int64
	Title     string
	Published bool
	Score     int64
	CreatedAt time.Time
}

// adults is a package-level template: validated once by Must, reused
// concurrently, and executed with its deferred argument per call.
var adults = rio.From[User]().
	Where("age >= ?").
	OrderBy("created_at DESC").
	Limit(10).
	Must()

func ExampleFrom() {
	ctx := context.Background()
	users, err := rio.From[User]().
		Where("age >= ?", 18).
		Where("active").
		OrderBy("created_at DESC").
		Limit(10).
		All(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	for _, u := range users {
		fmt.Println(u.Email)
	}
}

func ExampleQuery_With() {
	ctx := context.Background()
	users, err := rio.From[User]().
		With("Posts",
			rio.RelWhere("published = ?", true),
			rio.RelOrderBy("created_at DESC"),
			rio.RelLimit(3)).
		WithCount("Posts", rio.RelWhere("published = ?", true)).
		All(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	for _, u := range users {
		// Rows would panic had Posts not been loaded with With.
		fmt.Println(u.Email, u.PostCount, len(u.Posts.Rows()))
	}
}

func ExampleQuery_Must() {
	ctx := context.Background()
	users, err := adults.All(ctx, db, 18)
	if err != nil {
		log.Fatal(err)
	}
	emails, err := adults.Pluck[string](ctx, db, "email", 21)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(users), len(emails))
}

func ExampleInsert() {
	ctx := context.Background()
	user := User{Email: "alice@example.com", Age: 30, Active: true}
	if err := rio.Insert(ctx, db, &user); err != nil {
		log.Fatal(err)
	}
	// ID is backfilled where the dialect generates it; CreatedAt is stamped.
	fmt.Println(user.ID, user.CreatedAt.IsZero())
}

func ExampleUpsert() {
	ctx := context.Background()
	user := User{Email: "alice@example.com", Age: 31, Active: true}
	err := rio.Upsert(ctx, db, &user,
		rio.OnConflict("email"),
		rio.DoUpdate("age", "active"),
		rio.DoUpdateSet(rio.Set{"login_count": rio.Expr("users.login_count + ?", 1)}),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(user.ID)
}

// A conflict update guarded by DoUpdateWhere: the stored row survives when it
// is newer, and Upsert reports that as ErrStaleObject.
func ExampleDoUpdateWhere() {
	ctx := context.Background()
	user := User{Email: "alice@example.com", Age: 32}
	err := rio.Upsert(ctx, db.WithoutStamps(), &user,
		rio.OnConflict("email"),
		rio.DoUpdate("age"),
		rio.DoUpdateWhere("users.created_at < excluded.created_at"),
	)
	if errors.Is(err, rio.ErrStaleObject) {
		fmt.Println("stored row is newer")
	} else if err != nil {
		log.Fatal(err)
	}
}

// userPosts is a projection: Raw scans it by column name.
type userPosts struct {
	UserID int64 `rio:"user_id"`
	Posts  int64
}

// A Raw template: the head stops at FROM, rio appends the rest, and Must
// caches the shape like Query.Must.
var postCounts = rio.Raw[userPosts]("SELECT user_id, count(*) AS posts FROM posts").
	Where("published = ?").
	GroupBy("user_id").
	OrderBy("posts DESC").
	Must()

func ExampleRaw() {
	ctx := context.Background()
	rows, err := postCounts.Limit(10).All(ctx, db, true)
	if err != nil {
		log.Fatal(err)
	}
	authors, err := postCounts.Count(ctx, db, true)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(rows), authors)
}

// One filter, expressed as conditions, drives the entity list and a raw
// aggregate over the same rows.
func ExampleQuery_WhereAll() {
	ctx := context.Background()
	filter := []rio.Condition{rio.Cond("active"), rio.Cond("age >= ?", 18)}
	users, err := rio.From[User]().WhereAll(filter...).OrderBy("id").Limit(20).All(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	total, err := rio.Raw[int64]("SELECT count(*) FROM users").WhereAll(filter...).Value(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(users), total)
}

// Chunk walks a raw projection by keyset; Expr names the SQL behind the
// ordering column because the head joins another table.
func ExampleRawQuery_Chunk() {
	ctx := context.Background()
	export := rio.Raw[userPosts]("SELECT u.id AS user_id, count(p.id) AS posts FROM users u JOIN posts p ON p.user_id = u.id").
		GroupBy("u.id").
		OrderKeys(rio.SortKey{Column: "user_id", Expr: "u.id"})
	for page, err := range export.Chunk(ctx, db, 500) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(len(page))
	}
}

func ExampleRawQuery_Value() {
	ctx := context.Background()
	n, err := rio.Raw[int64]("SELECT count(*) FROM users").
		Where("age >= ?").
		Value(ctx, db, 18)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(n)
}

// One business time for every row an operation writes.
func ExampleDB_At() {
	ctx := context.Background()
	at := time.Now().UTC()
	err := db.Tx(ctx, func(tx *rio.Tx) error {
		user := User{Email: "bob@example.com"}
		if err := rio.Insert(ctx, tx.At(at), &user); err != nil {
			return err
		}
		return rio.Insert(ctx, tx.At(at), &Post{UserID: user.ID, Title: "hello"})
	})
	if err != nil {
		log.Fatal(err)
	}
}

// LockOf restricts a lock to one side of a join; ForKeyShare only blocks
// deletes and key changes.
func ExampleLockOf() {
	ctx := context.Background()
	posts, err := rio.From[Post]().
		Join("JOIN users u ON u.id = posts.user_id").
		Where("u.active").
		ForKeyShare(rio.LockOf("u")).
		All(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(posts))
}

func ExampleQuery_OrderKeys() {
	ctx := context.Background()
	q := rio.From[Post]().
		Where("published = ?", true).
		OrderKeys(
			rio.SortKey{Column: "score", Desc: true},
			rio.SortKey{Column: "created_at"},
		) // "id" is appended as the tie-breaker

	page, err := q.Limit(20).All(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	if len(page) == 0 {
		return
	}

	// Next page: the cursor at the last row.
	last, err := q.CursorAt(&page[len(page)-1])
	if err != nil {
		log.Fatal(err)
	}
	next, err := q.After(last).Limit(20).All(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	// Previous page: the cursor at the first row; Before reads backwards
	// and turns the page around, so it arrives in OrderKeys order.
	first, err := q.CursorAt(&page[0])
	if err != nil {
		log.Fatal(err)
	}
	prev, err := q.Before(first).Limit(20).All(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	// Tokens round-trip through URL-safe strings.
	token := last.String()
	parsed, err := rio.ParseCursor(token)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(next), len(prev), parsed.IsZero())
}

func ExampleQuery_Chunk() {
	ctx := context.Background()
	// One bounded query per page, in primary-key order, the connection
	// released between pages.
	for posts, err := range rio.From[Post]().Where("published = ?", true).Chunk(ctx, db, 500) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(len(posts))
	}
}

func ExampleQuery_Sub() {
	ctx := context.Background()
	// The subquery renders in place of the ? with its own arguments spliced
	// in; the caller writes the parentheses.
	authors := rio.From[Post]().Where("published = ?", true).Sub("user_id")
	users, err := rio.From[User]().Where("id IN (?)", authors).All(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(users))
}

func ExampleDB_Tx() {
	ctx := context.Background()
	err := db.Tx(ctx, func(tx *rio.Tx) error {
		users, err := rio.From[User]().
			Where("active").
			ForUpdate(rio.SkipLocked).
			Limit(10).
			All(ctx, tx)
		if err != nil {
			return err // rolls back
		}
		for i := range users {
			users[i].Age++
			if err := rio.Update(ctx, tx, &users[i], "age"); err != nil {
				return err
			}
		}
		return nil // commits
	})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleQuery_UpdateAllReturning() {
	ctx := context.Background()
	// Set-based writes need a condition (or AllRows); the returning form
	// hands the affected rows back on dialects with RETURNING.
	deactivated, err := rio.From[User]().
		Where("age < ?", 18).
		UpdateAllReturning(ctx, db, rio.Set{"active": false})
	if err != nil {
		log.Fatal(err)
	}
	for _, u := range deactivated {
		fmt.Println(u.Email, u.Active)
	}
}

// RETURNING only the columns the DTO names.
func ExampleUpdateAllInto() {
	ctx := context.Background()
	type stamped struct {
		ID        int64
		UpdatedAt time.Time
	}
	rows, err := rio.UpdateAllInto[stamped](ctx, db,
		rio.From[User]().Where("age < ?", 18), rio.Set{"active": false})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(rows))
}

// A raw head embeds in an entity query the way Query.Sub does.
func ExampleRawQuery_Sub() {
	ctx := context.Background()
	paid := rio.Raw[int64]("SELECT user_id FROM orders").Where("status = ?", "paid").Sub()
	users, err := rio.From[User]().Where("id IN (?)", paid).All(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(users))
}
