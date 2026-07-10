# rio

[![Go Reference](https://pkg.go.dev/badge/github.com/go-rio/rio.svg)](https://pkg.go.dev/github.com/go-rio/rio)
[![Go](https://img.shields.io/github/go-mod/go-version/go-rio/rio)](https://go.dev/)
[![Release](https://img.shields.io/github/release/go-rio/rio.svg)](https://github.com/go-rio/rio/releases)
[![Test](https://github.com/go-rio/rio/actions/workflows/test.yml/badge.svg)](https://github.com/go-rio/rio/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/go-rio/rio)](https://opensource.org/license/MIT)

**The zero-surprise ORM for Go.** Generic, connection-free query values;
writes that do exactly what they say; relations that fail loudly instead of
lazily; and the speed to have nothing to hide — faster than GORM everywhere,
within arm's reach of hand-written `database/sql`.

rio's core has **zero dependencies**. Drivers are separate modules:
[go-rio/postgres](https://github.com/go-rio/postgres) (pgx),
[go-rio/mysql](https://github.com/go-rio/mysql) (go-sql-driver),
[go-rio/sqlite](https://github.com/go-rio/sqlite) (modernc, pure Go).

```go
db, _ := postgres.Open(dsn)

users, err := rio.From[User]().
    Where("age > ?", 18).
    OrderBy("created_at DESC").Limit(10).
    With("Posts", rio.RelWhere("published = ?", true)).
    All(ctx, db)
```

## Why rio

Twenty years of ORM evolution — Rails, Laravel, Django, Hibernate, SQLAlchemy,
EF Core, Ecto, Prisma — converged on one lesson: **implicit magic loses to
explicit declaration**. rio starts where they ended up:

- **Queries are immutable values.** Builders carry no connection and no
  state; deriving from a shared base — concurrently, at package level — can
  never cross-contaminate (the foot-gun GORM documents as "condition
  pollution" is structurally impossible).
- **Zero values are real values.** `Update` writes what you give it,
  including `0`, `false`, and `""` — never silently skipping fields (GORM
  issue #6860 is a design decision here, not a trap). Partial updates are an
  explicit column whitelist; DB defaults are an explicit `omitzero` tag.
- **No hidden queries, ever.** Relations are typed containers
  (`rio.HasMany[Post]`) that know whether they were loaded. Accessing an
  unloaded relation panics with instructions instead of returning silently
  empty data — and lazy loading, the one source of invisible N+1, does not
  exist.
- **Errors behave like Go.** `ErrNotFound` wraps `sql.ErrNoRows` (both
  `errors.Is` checks work), unique violations translate to
  `rio.ErrDuplicateKey` with the driver error intact in the chain, and
  nothing is ever logged for you.
- **Set-based writes refuse to run without a WHERE** unless you call
  `AllRows()` — on every dialect.
- **SQL you can predict.** One builder call, one SQL fragment; `?`
  placeholders everywhere (with `IN (?)` slice expansion); the escape hatch
  `rio.Raw[T]` shares the same scanner, hooks, and transactions.

## Install

```bash
go get github.com/go-rio/rio
go get github.com/go-rio/postgres   # or go-rio/mysql, go-rio/sqlite
```

## Quickstart

```go
package main

import (
    "context"
    "time"

    "github.com/go-rio/rio"
    "github.com/go-rio/sqlite"
)

type User struct {
    ID        int64
    Email     string
    Age       int
    Bio       *string    // pointer = nullable
    Version   int64      `rio:",version"`    // opt-in optimistic locking
    DeletedAt *time.Time `rio:",softdelete"` // opt-in soft delete
    CreatedAt time.Time  // maintained automatically
    UpdatedAt time.Time  // maintained on every write path

    Posts rio.HasMany[Post]
}

type Post struct {
    ID     int64
    UserID int64
    Title  string

    Author rio.BelongsTo[User] `rio:",fk:user_id"`
}

func main() {
    ctx := context.Background()
    db, _ := sqlite.Open("file:app.db")

    u := &User{Email: "alice@example.com", Age: 30}
    _ = rio.Insert(ctx, db, u) // u.ID, timestamps, version filled in

    u.Age = 31
    _ = rio.Update(ctx, db, u)            // full row, version-checked
    _ = rio.Update(ctx, db, u, "age")     // just age (+updated_at)

    user, _ := rio.Find[User](ctx, db, u.ID)
    users, _ := rio.From[User]().
        Where("age BETWEEN ? AND ?", 18, 65).
        With("Posts").
        All(ctx, db)

    _ = db.Tx(ctx, func(tx *rio.Tx) error { // nested Tx = savepoints
        return rio.Insert(ctx, tx, &Post{UserID: u.ID, Title: "hello"})
    })

    _, _ = user, users
}
```

Schema migrations are deliberately out of scope — rio pairs with
[go-rio/migrate](https://github.com/go-rio/migrate), where migrations are Go
code compiled into your binary.

## Model mapping

| Declaration | Meaning |
|---|---|
| `ID int64` | primary key + auto-increment, by convention; a rename or other role-neutral tag keeps it — an explicit `,pk` anywhere in the model opts out |
| `rio:"col_name"` | rename the column (default: snake_case, `UserID` → `user_id`) |
| `rio:",pk"` | primary key (tag several fields for composite keys) |
| `rio:",noautoincr"` | integer single PK that rio must not treat as auto-increment |
| `rio:",version"` | optimistic locking: `UPDATE … SET version = version+1 WHERE … AND version = ?`; a lost race returns `ErrStaleObject` |
| `rio:",softdelete"` | on `time.Time`/`*time.Time`: `Delete` becomes an UPDATE, default queries filter, `WithTrashed`/`OnlyTrashed`/`ForceDelete` opt out |
| `rio:",json"` | (de)serialize the field as JSON |
| `rio:",countof:Posts"` | `int64` target that `WithCount("Posts")` fills with the related row count (HasMany/ManyToMany) |
| `rio:",omitzero"` | skip the column when zero so DB defaults apply (and RETURNING fills it back); a single-row `Upsert` conflict then leaves the existing value untouched |
| `rio:"-"` | not a column |
| `CreatedAt`, `UpdatedAt` | maintained automatically when present (`rio:",nostamp"` opts out) |
| `TableName() string` | override the pluralized table name (`User` → `users`, `Person` → `people`) |

Relations: `rio.HasMany[T]`, `rio.HasOne[T]`, `rio.BelongsTo[T]`,
`rio.ManyToMany[T]` — with `fk:`, `ref:`, and `join:` tag overrides.
`With` takes the Go **field name** (`With("Posts")`), while every column API
(`Update` whitelists, `rio.Set`, `OnConflict`, `DoUpdate`) takes **database
column names** — relations are Go-side concepts, columns are SQL-side.
Preloading always uses per-relation `WHERE … IN` split queries (the strategy
ActiveRecord, Eloquent, Ecto, and SQLAlchemy converged on): no cartesian
explosion, pagination stays correct, identical behavior on all three
dialects. Paths nest: `With("Posts.Comments")`.

## Semantics that refuse to surprise

| Operation | Behavior |
|---|---|
| `First/Find/Sole` miss | `rio.ErrNotFound` (wraps `sql.ErrNoRows`), never logged |
| `All` miss | empty slice, `nil` error |
| `Sole` with 2+ rows | `rio.ErrMultipleRows` |
| version conflict | `rio.ErrStaleObject` |
| `Update/Delete` matching no row (no version column) | `rio.ErrNotFound` |
| `UpdateAll/DeleteAll` without WHERE | `rio.ErrMissingWhere` |
| unique / FK violation | `rio.ErrDuplicateKey` / `rio.ErrForeignKeyViolated` |
| NULL into a non-pointer field | error naming the column — not a silent zero |
| MySQL `Insert` | fills the auto-increment ID; rio never issues a hidden second SELECT |
| `Upsert` onto a soft-deleted row | DoUpdate revives it — a successful DoUpdate upsert is never invisible (`rio.KeepTrashed()` opts out); DoNothing never revives |
| times | stored UTC, microsecond precision — insert-then-reload compares `Equal` |

`Upsert` ships complete: conflict target, update whitelist, RETURNING
backfill, timestamp maintenance:

```go
err := rio.Upsert(ctx, db, &user, rio.OnConflict("email"), rio.DoUpdate("name"))
```

On MySQL the DoUpdate branch uses the 8.0.19+ row-alias syntax (`VALUES()`
is deprecated); MySQL before 8.0.19 and MariaDB support `DoNothing` only.

Batch writes chunk automatically to each dialect's bind limit; backfill
promises only what dialects can keep (documented per dialect on `InsertAll`).

## Compiled queries

Most application SQL is fixed in shape. Entity CRUD is cached invisibly;
hand-built queries compile once, `regexp.MustCompile` style:

```go
var adults = rio.MustCompile[User](
    rio.From[User]().Where("age > ?").OrderBy("created_at DESC").Limit(10),
)

users, err := adults.All(ctx, db, 18) // binds parameters only
```

Structural problems panic at startup; SQL renders lazily per dialect and is
cached. `rio.WithStmtCache()` additionally caches prepared statements
(off by default — PgBouncer transaction pooling breaks server-side prepared
statements; enable it only when talking to the database directly).

## Column constants, without a tool chain

`rio.WriteColumns` generates typo-proof column references from rio's own
mapping plans — no source parsing, no binary to install, and the output can
never drift from runtime behavior:

```go
//go:generate sh -c "go run ./internal/gencols > cols_gen.go"
// internal/gencols: rio.WriteColumns(os.Stdout, "models", User{}, Post{})

users, err := rio.From[User]().Where(UserCols.Email+" = ?", e).All(ctx, db)
```

## Pagination that scales

Offset pagination degrades linearly; keyset pagination is a WHERE clause,
and rio deliberately ships the pattern instead of an API:

```go
// Page 1: rio.From[Post]().OrderBy("created_at DESC, id DESC").Limit(20)
// Next page, keyed by the last row you handed out:
next, err := rio.From[Post]().
    Where("(created_at, id) < (?, ?)", last.CreatedAt, last.ID).
    OrderBy("created_at DESC, id DESC").Limit(20).
    All(ctx, db)
```

Row-value comparison works on PostgreSQL, MySQL, and SQLite 3.15+. For
result sets too large to page at all, stream with `Rows`.

## Observability

```go
db, _ := postgres.Open(dsn, rio.WithQueryHook(myHook))
```

`QueryHook` sees every statement — op, model, SQL, args (redactable with
`rio.WithoutArgs()`), duration, rows affected — and cannot alter any of them.
There are no model hooks: side effects belong in visible application code,
invariants in database constraints.

## Performance

Benchmarked against GORM and hand-written `database/sql` on the same pure-Go
SQLite driver (Apple M4; `rio/bench`, reproducible with
`go test -bench . -benchmem ./...`):

| Scenario | rio | hand-written | GORM |
|---|---|---|---|
| read one (compiled) | 8.8 µs | 7.8 µs | 11.6 µs |
| read one (`Find`) | 8.8 µs | 7.8 µs | 11.6 µs |
| read 100 rows | 119 µs | 120 µs | 158 µs |
| insert | 11.0 µs | 7.8 µs | 19.9 µs |
| update | 6.7 µs | 6.2 µs | 15.5 µs |
| insert 100 (batch) | 262 µs | — | 294 µs |

Reads beat GORM by ~25% and land within ~12% of hand-written scanning
(scanning 100 rows is a dead heat); inserts are ~45% faster than GORM,
updates ~57% faster and within 7% of hand-written SQL. Batch inserts are
driver-dominated — both sides send one multi-VALUES statement — so the ~11%
edge there is mostly allocation discipline. The techniques: per-type mapping plans,
per-grammar SQL caches, offset-based scanning with a reflect fallback, and
`[]byte`-appended rendering — no code generation anywhere.

## What rio deliberately does not have

No model hooks. No implicit lazy loading. No dirty tracking, unit of work, or
identity map. No AutoMigrate. No second-level cache. No association
auto-writes. No client-side evaluation. Each refusal is a researched decision
with the receipts in [DESIGN.md](DESIGN.md).

## License

rio is released under the [MIT License](LICENSE), © 2026-now TreeNewBee.
