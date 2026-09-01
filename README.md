# rio

<p align="center">
  <img src="assets/logo.png" alt="rio gopher logo" width="360">
</p>

[![Go Reference](https://pkg.go.dev/badge/github.com/go-rio/rio.svg)](https://pkg.go.dev/github.com/go-rio/rio)
[![Go](https://img.shields.io/github/go-mod/go-version/go-rio/rio)](https://go.dev/)
[![Release](https://img.shields.io/github/release/go-rio/rio.svg)](https://github.com/go-rio/rio/releases)
[![Test](https://github.com/go-rio/rio/actions/workflows/test.yml/badge.svg)](https://github.com/go-rio/rio/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/go-rio/rio)](https://opensource.org/license/MIT)

A generic ORM for Go with zero third-party dependencies in the core.

- **Immutable queries.** `Query[T]` is a value: build once, validate, reuse
  concurrently, run against any DB or transaction.
- **Explicit everything.** No lazy loading, no dirty tracking, no hidden
  transactions, no callbacks. What you call is what runs.
- **Typed relations.** `HasMany[T]` and friends load in batched `IN` queries
  when you ask, and panic with guidance when you forgot to.
- **Fast paths where the driver has them.** The PostgreSQL module runs on pgx
  natively: preloads share one round trip per layer and bulk inserts stream
  over `COPY`.

```go
users, err := rio.From[User]().
    Where("age >= ?", 18).
    OrderBy("created_at DESC").
    With("Posts", rio.RelWhere("published = ?", true)).
    All(ctx, db)
```

## Getting started

Requires Go 1.27+. Install the core and one driver module:

```bash
go get github.com/go-rio/rio
go get github.com/go-rio/sqlite # or postgres, mysql, clickhouse
```

| Module | Driver |
|---|---|
| [go-rio/postgres](https://github.com/go-rio/postgres) | pgx (database/sql or native) |
| [go-rio/mysql](https://github.com/go-rio/mysql) | go-sql-driver/mysql |
| [go-rio/sqlite](https://github.com/go-rio/sqlite) | modernc.org/sqlite, pure Go |
| [go-rio/clickhouse](https://github.com/go-rio/clickhouse) | native protocol, zero deps |

```go
package main

import (
    "context"
    "log"

    "github.com/go-rio/rio"
    "github.com/go-rio/sqlite"
)

type User struct {
    ID    int64
    Email string
    Age   int
}

func main() {
    ctx := context.Background()
    db, err := sqlite.Open("file:app.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    user := User{Email: "alice@example.com", Age: 30}
    if err := rio.Insert(ctx, db, &user); err != nil {
        log.Fatal(err)
    }

    loaded, err := rio.Find[User](ctx, db, user.ID)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("loaded %s", loaded.Email)
}
```

Schema migrations live in [go-rio/migrate](https://github.com/go-rio/migrate).

## API overview

| Area | API |
|---|---|
| Query construction | `From[T]`, `Where`, `Having`, `Join`, `OrderBy`, `GroupBy`, `Distinct`, `Limit`, `Offset`, `Scope` |
| Query modifiers | `ForUpdate`, `ForShare`, `Final`, `WithTrashed`, `OnlyTrashed`, `AllRows` |
| Query execution | `All`, `First`, `Sole`, `Find`, `Rows`, `Chunk`, `Count`, `Exists`, `Pluck[V]`, `Sum/Min/Max/Avg[V]`, `SQL` |
| Direct lookup and SQL | `Find[T]`, `Raw[T]`, `Exec`, `Query.Sub` |
| Entity writes | `Insert`, `Update`, `Delete`, `ForceDelete`, `Restore`, `Upsert`, `FirstOrCreate`, `CreateOrFirst` |
| Batch and set writes | `InsertAll`, `UpsertAll`, `UpdateAll`, `UpdateAllReturning`, `DeleteAll`, `DeleteAllReturning`, `ForceDeleteAll`, `RestoreAll` |
| Relations | `With`, `WithCount`, `WhereHas`, `WhereHasNot`, `Attach`, `Detach`, `SyncRelation`, `ClearRelation` |
| Validation and reuse | `Query.Validate`, `Query.Must`, `WithStmtCache`, `WithoutStmtCache` |

## Queries

`Query[T]` carries no connection. Terminal methods take the context, a
`Queryer` (`*DB` or `*Tx`), and any deferred arguments:

```go
var adults = rio.From[User]().
    Where("age >= ?").
    OrderBy("created_at DESC").
    Limit(10).
    Must()

users, err := adults.All(ctx, db, 18)
emails, err := adults.Pluck[string](ctx, db, "email", 18)
total, err := adults.Sum[int64](ctx, db, "age", 18)
user, err := adults.With("Posts").Find(ctx, db, 42) // by primary key, under the query's clauses
```

`Validate` returns structural errors; `Must` panics instead and returns the
query, for package variables. Neither touches the database. `SQL(db, args...)`
renders the statement `All` would run.

`Sum`, `Min`, `Max`, and `Avg` aggregate a mapped column and return the zero
value over no rows (`sql.Null[V]` tells the two apart). `Distinct` applies to
entity rows, `Pluck`, `Count` (distinct primary keys), `Sum`, and `Avg`.

Parameters per fragment:

- `Where("age >= ?", 18)` — inline, owns its arguments.
- `Where("age >= ?")` — deferred, consumes terminal arguments in SQL order.
- `Where("active")` — no placeholders.

Slices expand inside `IN (?)`; an empty slice is an error. A one-column
query embeds the same way: `Where("id IN (?)", banned.Sub("user_id"))` splices
the subquery and its arguments in place. Missing or excess arguments fail
before the driver sees the query. `??` emits a literal `?`.
`Join`/`OrderBy`/`GroupBy` take no placeholders, and `RelWhere` arguments are
always inline.

`Rows` streams without materializing the slice; it rejects `With`/`WithCount`.
`WithStmtCache` caches prepared statements per DB and per transaction; the
sqlite and mysql modules turn it on by default, and `WithoutStmtCache` opts
out behind transaction- or statement-mode poolers.

### Cursor pagination

`OrderKeys` declares ordering over mapped NOT NULL columns, so rio can read
key values back out of a row and issue a keyset cursor. A missing primary-key
column is appended as tie-breaker — pages never skip or repeat:

```go
q := rio.From[Post]().OrderKeys(
    rio.SortKey{Column: "score", Desc: true},
    rio.SortKey{Column: "created_at"},
) // + "id" appended automatically

page, err := q.Limit(20).All(ctx, db)
cur, err := q.CursorAt(&page[len(page)-1])
next, err := q.After(cur).Limit(20).All(ctx, db)
prev, err := q.Before(first).Limit(20).All(ctx, db) // first = CursorAt(&page[0])
```

`Before` runs the reversed query and turns the page around, so it always
reads in `OrderKeys` order. `Chunk(ctx, db, 500)` walks the whole result in
keyset pages (`iter.Seq2[[]T, error]`), releasing the connection between
pages and applying `With`/`WithCount` per page.

`Cursor.String`/`rio.ParseCursor` round-trip a URL-safe token. Tokens carry
values (bound as parameters) plus an ordering fingerprint — a forged token
moves the window, never the query, and a cursor from different `OrderKeys`
fails loudly.

### Schema-drift lint

The read-only `lint` subpackage diffs model expectations against the live
schema (PostgreSQL, MySQL, SQLite): missing tables and columns, nullability,
primary keys, and type mismatches in known equivalence classes. Run it in CI
or a startup probe:

```go
report, err := lint.Check(ctx, db, User{}, Post{})
for _, f := range report.Findings {
    log.Printf("%s: %s", f.Severity, f.Message)
}
```

## Transactions

`*DB` and `*Tx` both implement `Queryer`, so repository code runs unchanged in
or out of a transaction:

```go
err := db.Tx(ctx, func(tx *rio.Tx) error {
    users, err := adults.ForUpdate().All(ctx, tx, 21)
    if err != nil {
        return err
    }
    return updateUsers(ctx, tx, users)
})
```

Error rolls back, `nil` commits, `Tx` on a `*Tx` opens a savepoint. Batch
operations never start hidden transactions — wrap them in `DB.Tx` when all
chunks must land together.

`ForUpdate` and `ForShare` take `rio.NoWait` or `rio.SkipLocked`; the
queue-worker idiom is `Where("state = ?", "queued").ForUpdate(rio.SkipLocked).Limit(1)`.
SQLite elides row locks, ClickHouse rejects them.

## Models and relations

| Declaration | Meaning |
|---|---|
| `ID int64` | conventional auto-increment primary key |
| `rio:"column"` | column name; default is snake_case |
| `rio:",pk"` | explicit primary key; repeat for composite |
| `rio:",noautoincr"` | integer key without auto-generation |
| `rio:",version"` | optimistic locking; conflicts return `ErrStaleObject` |
| `rio:",softdelete"` | deletion timestamp driving soft-delete operations |
| `rio:",json"` | encode and scan as JSON |
| `rio:",omitzero"` | skip zero value on single-row insert so defaults apply |
| `rio:",readonly"` | database-computed column: scanned and loaded back after `Insert`/`Upsert`, never written |
| `rio:",countof:Posts"` | `int64` target for `WithCount("Posts")` |
| `rio:",nostamp"` | opt out of `CreatedAt`/`UpdatedAt` maintenance |
| `rio:"-"` | ignored field |
| `TableName() string` | override the pluralized table name |

Relations are `HasMany[T]`, `HasOne[T]`, `BelongsTo[T]`, `ManyToMany[T]`;
`fk:`/`ref:`/`join:` tags override conventions. Relation APIs take Go field
names (`With("Posts.Comments")`), column APIs take column names. Preloads run
as separate key-set queries (one array parameter on PostgreSQL, an `IN` list
elsewhere), never JOINs, and never lazily. `WithCount` takes `RelWhere` and
`RelWithTrashed` to count a subset.

## Writes and errors

`Update` writes every eligible field — zero values included — unless given a
column whitelist. Set-based writes require a condition (`AllRows()` opts out);
`UpdateAllReturning` and `DeleteAllReturning` hand the affected rows back
where the dialect has `RETURNING`. `Upsert` supports conflict targets, update
whitelists, `DoUpdateSet` for expressions (`rio.Expr("hits + excluded.hits")`)
or bound values, `DoNothing`, and `KeepTrashed`. `InsertAll`/`UpsertAll`
chunk at the dialect bind limit; batch writes share one column list, so
`omitzero` doesn't apply.

| Condition | Result |
|---|---|
| `First`/`Find`/`Sole` miss | `ErrNotFound` (wraps `sql.ErrNoRows`) |
| `All` finds nothing | empty slice, `nil` error |
| `Sole` finds several | `ErrMultipleRows` |
| optimistic-lock conflict | `ErrStaleObject` |
| set write without condition | `ErrMissingWhere` |
| unique / FK violation | `ErrDuplicateKey` / `ErrForeignKeyViolated`, driver error retained |
| NULL into non-nullable field | error naming the column |

Times are normalized to UTC, microsecond precision.

## Dialect differences

| Dialect | Behavior |
|---|---|
| PostgreSQL | `RETURNING` everywhere, including `InsertAll` backfill; row locks with `NoWait`/`SkipLocked`; preload key sets bind as one array (`= ANY`). The driver module adds a pgx-native channel: batched preload round trips and `COPY`-backed bulk inserts. |
| MySQL | `Insert` backfills via `LastInsertId`; batch inserts don't backfill; no `RETURNING`. `DoUpdate` needs MySQL 8.0.19+ (no MariaDB); `DoNothing` works everywhere. Statement cache on by default. |
| SQLite | Pure-Go driver. `RETURNING` where backfill needs it; row locks are no-ops. Statement cache, UTC time binding, and `BEGIN IMMEDIATE` on by default. |
| ClickHouse | Reads, preloads, `Insert`, `InsertAll`. Rejects row locks, transactions, statement caching, synchronous update/delete, and conflict upserts — use `ReplacingMergeTree` + `Final`. No backfill; supply IDs yourself. |

## Security

Values always bind as parameters. SQL fragments do not:

| Input | APIs | Rule |
|---|---|---|
| Mapped columns | `Update` columns, `Set` keys, `Pluck`, aggregates, `Sub`, `OnConflict`, `DoUpdate`, `DoUpdateSet` | validated against the model, quoted as identifiers |
| Relation paths | `With`, `WithCount`, `WhereHas`, relation writes | validated against the model's relations |
| SQL text | `Where`, `Having`, `Join`, `OrderBy`, `GroupBy`, `RelWhere`, `Expr`, `Raw`, `Exec` | rendered verbatim — constants only, never untrusted input |

For runtime-selected identifiers, map external values onto generated
constants: `rio.WriteColumns(os.Stdout, "models", User{}, Post{})` emits
table and column constants from the model mapping.

## Observability

`WithQueryHook` observes every statement: operation, model, SQL, arguments,
duration, row counts, error. Hooks cannot alter SQL. The context returned by
`BeforeQuery` flows through the driver into `AfterQuery`, so tracing spans and
deadlines propagate. `WithoutArgs` strips arguments from events.

See [DESIGN.md](DESIGN.md) for architecture and scope decisions.

## Contributing

```bash
go test ./...
go test -race ./...
go vet ./...
```

## License

rio is released under the [MIT License](LICENSE), © 2026-now TreeNewBee.

The rio gopher logo is inspired by the [Go gopher](https://go.dev/blog/gopher),
created by Renée French and licensed under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
