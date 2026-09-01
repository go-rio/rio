package rio_test

import (
	"context"
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
		rio.DoUpdateSet(rio.Set{"login_count": rio.Expr("users.login_count + 1")}),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(user.ID)
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
