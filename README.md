# rio

[![Go Reference](https://pkg.go.dev/badge/github.com/go-rio/rio.svg)](https://pkg.go.dev/github.com/go-rio/rio)
[![Go](https://img.shields.io/github/go-mod/go-version/go-rio/rio)](https://go.dev/)
[![Release](https://img.shields.io/github/release/go-rio/rio.svg)](https://github.com/go-rio/rio/releases)
[![Test](https://github.com/go-rio/rio/actions/workflows/test.yml/badge.svg)](https://github.com/go-rio/rio/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/go-rio/rio)](https://opensource.org/license/MIT)

Generic ORM for Go. Query values carry no connection; writes are explicit;
relations are typed and never lazy-load. Faster than GORM; close to
hand-written `database/sql`.

rio's core has **zero dependencies**. Drivers are separate modules:

| Module | Driver |
|---|---|
| [go-rio/postgres](https://github.com/go-rio/postgres) | pgx |
| [go-rio/mysql](https://github.com/go-rio/mysql) | go-sql-driver |
| [go-rio/sqlite](https://github.com/go-rio/sqlite) | modernc, pure Go |
| [go-rio/clickhouse](https://github.com/go-rio/clickhouse) | clickhouse-go v2 |

```go
db, _ := postgres.Open(dsn)

users, err := rio.From[User]().
    Where("age > ?", 18).
    OrderBy("created_at DESC").Limit(10).
    With("Posts", rio.RelWhere("published = ?", true)).
    All(ctx, db)
```

## Why rio

| Guarantee | Detail |
|---|---|
| Immutable query values | Builders carry no connection or state; concurrent, package-level derivation from a shared base cannot cross-contaminate (GORM's "condition pollution"). |
| Zero values are real | `Update` writes what you give it, including `0`, `false`, and `""`; it never silently skips fields (GORM issue #6860). Partial updates are an explicit column whitelist; DB defaults are an explicit `omitzero` tag. |
| No hidden queries | Typed containers (`rio.HasMany[Post]`) know whether they loaded; accessing an unloaded relation panics with instructions. No lazy loading, no invisible N+1. |
| Go-style errors | `ErrNotFound` wraps `sql.ErrNoRows` (both `errors.Is` checks pass); unique violations translate to `rio.ErrDuplicateKey` with the driver error intact in the chain. Nothing is logged for you. |
| Guarded set-based writes | `UpdateAll`/`DeleteAll` refuse to run without a WHERE unless you call `AllRows()`, on every dialect. |
| Predictable SQL | One builder call, one SQL fragment; `?` placeholders everywhere, with `IN (?)` slice expansion. The escape hatch `rio.Raw[T]` shares the scanner, hooks, and transactions. |

## Install

```bash
go get github.com/go-rio/rio
go get github.com/go-rio/postgres   # or go-rio/mysql, go-rio/sqlite, go-rio/clickhouse
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
    Bio       *string    // nullable
    Version   int64      `rio:",version"`
    DeletedAt *time.Time `rio:",softdelete"`
    CreatedAt time.Time
    UpdatedAt time.Time

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

    user, _ := rio.Find[User](ctx, db, u.ID) // by primary key

    _ = db.Tx(ctx, func(tx *rio.Tx) error { // nested Tx = savepoints
        return rio.Insert(ctx, tx, &Post{UserID: u.ID, Title: "hello"})
    })

    _ = user
}
```

Schema migrations are out of scope. rio pairs with
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

Relation types: `rio.HasMany[T]`, `rio.HasOne[T]`, `rio.BelongsTo[T]`,
`rio.ManyToMany[T]`, with `fk:`, `ref:`, and `join:` tag overrides. `With`
takes the Go field name (`With("Posts")`); column APIs (`Update` whitelists,
`rio.Set`, `OnConflict`, `DoUpdate`) take database column names. Preloading
always uses per-relation `WHERE … IN` split queries: no cartesian explosion,
pagination stays correct, identical behavior on all dialects. Paths nest:
`With("Posts.Comments")`.

## Semantics

| Operation | Behavior |
|---|---|
| `First/Find/Sole` miss | `rio.ErrNotFound` (wraps `sql.ErrNoRows`), never logged |
| `All` miss | empty slice, `nil` error |
| `Sole` with 2+ rows | `rio.ErrMultipleRows` |
| version conflict | `rio.ErrStaleObject` |
| `Update/Delete` matching no row (no version column) | `rio.ErrNotFound` |
| `UpdateAll/DeleteAll` without WHERE | `rio.ErrMissingWhere` |
| unique / FK violation | `rio.ErrDuplicateKey` / `rio.ErrForeignKeyViolated` |
| NULL into a non-pointer field | error naming the column, not a silent zero |
| MySQL `Insert` | fills the auto-increment ID; no hidden second SELECT |
| `Upsert` onto a soft-deleted row | DoUpdate revives it (`rio.KeepTrashed()` opts out); DoNothing never revives |
| times | stored UTC, microsecond precision; insert-then-reload compares `Equal` |

`Upsert` supports conflict target, update whitelist, RETURNING backfill, and
timestamp maintenance:

```go
err := rio.Upsert(ctx, db, &user, rio.OnConflict("email"), rio.DoUpdate("name"))
```

On MySQL the DoUpdate branch uses the 8.0.19+ row-alias syntax (`VALUES()` is
deprecated); MySQL before 8.0.19 and MariaDB support `DoNothing` only.

Batch writes chunk automatically to each dialect's bind limit; backfill
promises only what dialects can keep (per-dialect notes on `InsertAll`).

## Compiled queries

Entity CRUD is cached automatically; hand-built queries compile once,
`regexp.MustCompile` style:

```go
var adults = rio.MustCompile[User](
    rio.From[User]().Where("age > ?").OrderBy("created_at DESC").Limit(10),
)

users, err := adults.All(ctx, db, 18) // binds parameters only
```

Structural problems panic at startup; SQL renders lazily per dialect and is
cached. `rio.WithStmtCache()` also caches prepared statements (off by default:
PgBouncer transaction pooling breaks server-side prepared statements; enable
only when talking to the database directly).

## Column constants

`rio.WriteColumns` generates column references from rio's own mapping plans:
no source parsing, no binary to install, output cannot drift from runtime
behavior:

```go
//go:generate sh -c "go run ./internal/gencols > cols_gen.go"
// internal/gencols: rio.WriteColumns(os.Stdout, "models", User{}, Post{})

users, err := rio.From[User]().Where(UserCols.Email+" = ?", e).All(ctx, db)
```

## Pagination

Offset pagination degrades linearly. Keyset pagination is a WHERE clause; rio
provides the pattern, not an API:

```go
// Page 1: rio.From[Post]().OrderBy("created_at DESC, id DESC").Limit(20)
// Next page, keyed by the last row you handed out:
next, err := rio.From[Post]().
    Where("(created_at, id) < (?, ?)", last.CreatedAt, last.ID).
    OrderBy("created_at DESC, id DESC").Limit(20).
    All(ctx, db)
```

Row-value comparison works on PostgreSQL, MySQL, and SQLite 3.15+. For result
sets too large to page, stream with `Rows`.

## Observability

`QueryHook` sees every statement rio runs — op, model, SQL, args (redactable
with `rio.WithoutArgs()`), duration, rows affected — and cannot alter them.
There are no model hooks.

```go
type SlogHook struct {
    Log  *slog.Logger
    Slow time.Duration // statements at or over this log at WARN
}

func (SlogHook) BeforeQuery(ctx context.Context, e *rio.QueryEvent) context.Context {
    return ctx
}

func (h SlogHook) AfterQuery(ctx context.Context, e *rio.QueryEvent) {
    level := slog.LevelDebug
    switch {
    case e.Err != nil:
        level = slog.LevelError
    case h.Slow > 0 && e.Duration >= h.Slow:
        level = slog.LevelWarn
    }
    h.Log.LogAttrs(ctx, level, "query",
        slog.String("op", e.Op), slog.String("model", e.Model),
        slog.Duration("dur", e.Duration), slog.Int64("rows", e.RowsAffected),
        slog.String("sql", e.Query), slog.Any("err", e.Err))
}

db, _ := postgres.Open(dsn, rio.WithQueryHook(SlogHook{
    Log: slog.Default(), Slow: 200 * time.Millisecond,
}))
```

The context `BeforeQuery` returns is the execution context rio hands the
driver, so tracing spans and deadlines you attach there flow into the query,
and `AfterQuery` receives that same context. To emit OpenTelemetry spans, start
a span in `BeforeQuery` (return its context) and `End()` it in `AfterQuery`,
recording `Op` and `Model` as span attributes.

## Security

rio sorts every string argument into two kinds:

| Argument | APIs | Treatment |
|---|---|---|
| Column names | `Update` whitelist, `rio.Set` keys, `Pluck`, `OnConflict`/`DoUpdate`, `With` paths | validated against the model's mapped columns (relation names for `With`) and emitted as escaped identifiers; an unknown name errors, never injects SQL |
| SQL fragments | `Where`, `OrderBy`, `GroupBy`, `Having`, `Join`, `RelWhere`, `Expr`, `Raw` | rendered verbatim — build them from constants, never from untrusted input |

Values bind as `?` parameters everywhere; only fragment *text* is verbatim. For
an identifier chosen at runtime, map user input to a column constant
(`rio.WriteColumns`) instead of concatenating it:

```go
allowed := map[string]string{"newest": UserCols.CreatedAt, "name": UserCols.Name}
col, ok := allowed[userInput]; if !ok { col = UserCols.ID } // reject unknown keys
users, err := rio.From[User]().OrderBy(col + " DESC").All(ctx, db)
```

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

- Reads: ~25% faster than GORM, within ~12% of hand-written scanning; the
  100-row scan is a dead heat.
- Inserts: ~45% faster than GORM. Updates: ~57% faster than GORM, within 7%
  of hand-written SQL.
- Batch inserts are driver-dominated (both sides send one multi-VALUES
  statement), so the ~11% edge is mostly allocation discipline.
- Techniques: per-type mapping plans, per-grammar SQL caches, offset-based
  scanning with a reflect fallback, `[]byte`-appended rendering. No code
  generation.

## What rio does not have

- No model hooks
- No implicit lazy loading
- No dirty tracking, unit of work, or identity map
- No AutoMigrate
- No second-level cache
- No association auto-writes
- No client-side evaluation

Rationale for each is in [DESIGN.md](DESIGN.md).

## License

rio is released under the [MIT License](LICENSE), © 2026-now TreeNewBee.
