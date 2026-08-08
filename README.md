# rio

<p align="center">
  <img src="assets/logo.png" alt="rio gopher logo" width="360">
</p>

[![Go Reference](https://pkg.go.dev/badge/github.com/go-rio/rio.svg)](https://pkg.go.dev/github.com/go-rio/rio)
[![Go](https://img.shields.io/github/go-mod/go-version/go-rio/rio)](https://go.dev/)
[![Release](https://img.shields.io/github/release/go-rio/rio.svg)](https://github.com/go-rio/rio/releases)
[![Test](https://github.com/go-rio/rio/actions/workflows/test.yml/badge.svg)](https://github.com/go-rio/rio/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/go-rio/rio)](https://opensource.org/license/MIT)

rio is a generic ORM for Go. Queries are immutable values that carry no
connection; writes are explicit; and typed relations load only when requested.
The core module has no third-party dependencies.

```go
users, err := rio.From[User]().
    Where("age >= ?", 18).
    OrderBy("created_at DESC").
    With("Posts", rio.RelWhere("published = ?", true)).
    All(ctx, db)
```

## Getting started

rio requires Go 1.27 or newer. Install the core and one driver module:

```bash
go get github.com/go-rio/rio
go get github.com/go-rio/sqlite # or postgres, mysql, clickhouse
```

| Module | Driver |
|---|---|
| [go-rio/postgres](https://github.com/go-rio/postgres) | pgx |
| [go-rio/mysql](https://github.com/go-rio/mysql) | go-sql-driver/mysql |
| [go-rio/sqlite](https://github.com/go-rio/sqlite) | modernc.org/sqlite, pure Go |
| [go-rio/clickhouse](https://github.com/go-rio/clickhouse) | clickhouse-go v2 |

Minimal SQLite example:

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

Schema migrations are provided separately by
[go-rio/migrate](https://github.com/go-rio/migrate).

## API overview

| Area | API |
|---|---|
| Query construction | `From[T]`, `Where`, `Having`, `Join`, `OrderBy`, `GroupBy`, `Limit`, `Offset`, `Scope` |
| Query modifiers | `ForUpdate`, `Final`, `WithTrashed`, `OnlyTrashed`, `AllRows` |
| Query execution | `All`, `First`, `Sole`, `Rows`, `Count`, `Exists`, `Query[T].Pluck[V]` |
| Direct lookup and SQL | `Find[T]`, `Raw[T]`, `Exec` |
| Entity writes | `Insert`, `Update`, `Delete`, `ForceDelete`, `Restore`, `Upsert`, `FirstOrCreate`, `CreateOrFirst` |
| Batch and set writes | `InsertAll`, `UpsertAll`, `UpdateAll`, `DeleteAll`, `ForceDeleteAll`, `RestoreAll` |
| Relations | `With`, `WithCount`, `WhereHas`, `WhereHasNot`, `Attach`, `Detach`, `SyncRelation` |
| Validation and reuse | `Query.Validate`, `Query.Must`, `WithStmtCache` |

## Queries and parameters

`Query[T]` is immutable, connection-free, and safe to reuse concurrently. A
terminal method receives the `context.Context`, a `Queryer`, and any deferred
arguments:

```go
var adults = rio.From[User]().
    Where("age >= ?").
    OrderBy("created_at DESC").
    Limit(10).
    Must()

users, err := adults.All(ctx, db, 18)
emails, err := adults.Pluck[string](ctx, db, "email", 18)
```

`Validate` checks connection-independent structure and returns an error. `Must`
performs the same check, panics on failure, and returns the query for package
variables or repository fields. Neither method accesses a database.

Each `Where` or `Having` fragment follows one parameter mode:

- Inline: `Where("age >= ?", 18)` owns its arguments.
- Deferred: `Where("age >= ?")` consumes terminal arguments.
- No placeholders: `Where("active")` consumes nothing.

Inline and deferred fragments may be mixed. Deferred arguments are consumed in
final SQL order, and missing or excess arguments fail before the driver and
query hooks run. Slice arguments expand in `IN (?)`, including deferred slices;
an empty slice is an error.

`Join`, `OrderBy`, and `GroupBy` have no parameter channel and must not contain
placeholders. `RelWhere` arguments in `With` and `WhereHas` must be inline
because preloads execute independently and nested `EXISTS` argument order must
remain explicit. `Raw[T]` receives its arguments when constructed and does not
accept a second terminal argument list.

Use `?` placeholders with every driver. rio expands slices, applies the
dialect's placeholder syntax, normalizes supported values, and checks the
dialect bind limit at execution. `??` emits a literal question mark;
placeholder-like text inside strings, quoted identifiers, and comments is not
bound. `Rows` streams without materializing the result and rejects `With` or
`WithCount`, which require the full parent set.

`WithStmtCache` enables a bounded DB-level cache and a cache local to each
transaction. It is off by default and is unsuitable for poolers in transaction
or statement mode.

## Databases and transactions

`*DB` and `*Tx` both implement `rio.Queryer`, so repository code can accept one
interface and run unchanged in or out of a transaction:

```go
func listAdults(ctx context.Context, db rio.Queryer, age int) ([]User, error) {
    return adults.All(ctx, db, age)
}

err := db.Tx(ctx, func(tx *rio.Tx) error {
    users, err := adults.ForUpdate().All(ctx, tx, 21)
    if err != nil {
        return err
    }
    return updateUsers(ctx, tx, users)
})
```

Returning an error rolls the transaction back; returning `nil` commits it.
Calling `Tx` on an existing `*Tx` uses a savepoint. rio does not start hidden
transactions around batch operations, so wrap a batch in `DB.Tx` when all
chunks must be atomic.

## Models and relations

| Declaration | Meaning |
|---|---|
| `ID int64` | conventional auto-increment primary key |
| `rio:"column"` | database column name; the default is snake_case |
| `rio:",pk"` | explicit primary key; repeat for a composite key |
| `rio:",noautoincr"` | disable automatic generation for a single integer key |
| `rio:",version"` | optimistic-lock counter; conflicts return `ErrStaleObject` |
| `rio:",softdelete"` | deletion timestamp used by soft-delete operations and filters |
| `rio:",json"` | encode and scan the field as JSON |
| `rio:",omitzero"` | omit a zero field from single-row inserts so a database default applies |
| `rio:",countof:Posts"` | `int64` target for `WithCount("Posts")` |
| `rio:",nostamp"` | opt out of `CreatedAt` or `UpdatedAt` maintenance |
| `rio:"-"` | ignore the field |
| `TableName() string` | override the conventional pluralized table name |

Relations use `HasMany[T]`, `HasOne[T]`, `BelongsTo[T]`, and `ManyToMany[T]`.
Use `fk:`, `ref:`, and `join:` tag options when conventions do not match the
schema. Relation APIs take Go field names, such as `With("Posts.Comments")`;
column APIs take database column names.

Preloading uses separate `WHERE ... IN` queries rather than JOINs. It never
lazy-loads: accessing an unloaded relation container panics with guidance.

## Writes and errors

`Update` writes all eligible fields, including zero values, unless given an
explicit database-column whitelist. Set-based writes require a condition; call
`AllRows()` to opt into an unfiltered write. They do not perform optimistic
locking.

`Upsert` supports conflict targets, update whitelists, `DoNothing`, and
`KeepTrashed`. A conflict update restores a soft-deleted row unless
`KeepTrashed` is set. `InsertAll` and `UpsertAll` chunk at the dialect bind
limit and do not create a transaction automatically. Batch writes use one
column list, so `omitzero` does not apply; `UpsertAll` does not backfill.

| Condition | Error or result |
|---|---|
| `First`, `Find`, or `Sole` finds no row | `ErrNotFound`, which wraps `sql.ErrNoRows` |
| `All` finds no rows | empty slice and `nil` error |
| `Sole` finds multiple rows | `ErrMultipleRows` |
| optimistic-lock conflict | `ErrStaleObject` |
| guarded set write lacks a condition | `ErrMissingWhere` |
| translated unique or foreign-key violation | `ErrDuplicateKey` or `ErrForeignKeyViolated`, with the driver error retained |
| NULL scanned into a non-nullable field | error naming the column |

Times written through rio are normalized to UTC and microsecond precision.
Timestamp and version fields may be changed before a write executes, so a
failed write can still modify the in-memory model.

## Dialect differences

| Dialect | Important behavior |
|---|---|
| PostgreSQL | Uses `RETURNING`, including auto-increment-key backfill for `InsertAll`, and supports row locks. The driver module also exposes pgx pool and native execution modes. |
| MySQL | `Insert` backfills the auto-increment ID without a hidden SELECT; batch inserts do not backfill. Conflict updates cannot backfill a server-incremented version. `DoUpdate` requires MySQL 8.0.19 or newer and is not supported by MariaDB; `DoNothing` remains supported. |
| SQLite | `Insert` gets a lone auto-increment key from `LastInsertId`; it keeps `RETURNING` when omitted default columns also need backfill. `InsertAll` uses sorted `RETURNING` backfill. `ForUpdate` is a no-op because SQLite does not provide row locks. The supported driver is pure Go. |
| ClickHouse | Supports reads, relation preloads, `Insert`, and `InsertAll`. It rejects row locks, transactions, prepared-statement caching, synchronous update/delete APIs, and conflict upserts. Model replacements use inserts into `ReplacingMergeTree` and `Final` reads. Generated values are not backfilled, and conventional IDs must be supplied by the caller. |

See each driver module for connection options and additional driver-specific
behavior.

## Migrating to the Go 1.27 API

This is a breaking API change with no compatibility wrappers:

- The minimum version is Go 1.27.
- `Compiled[T]`, `Compile`, and `MustCompile` were removed. Keep the original
  `Query[T]`, call `Validate` or `Must`, and pass values to terminal methods.
- Package-level `Pluck[V, T](ctx, db, q, column)` became
  `q.Pluck[V](ctx, db, column, args...)`.
- Query terminal methods now accept deferred `args ...any`; set operations and
  `FirstOrCreate`/`CreateOrFirst` follow the same parameter contract.
- `RawQuery[T]` is unchanged: arguments still belong to `Raw` construction.

| Before | After |
|---|---|
| `rio.MustCompile(q)` | `q.Must()` |
| `compiled.All(ctx, db, args...)` | `q.All(ctx, db, args...)` |
| `rio.Pluck[V](ctx, db, q, column)` | `q.Pluck[V](ctx, db, column, args...)` |

## Security

Values always bind as parameters. SQL fragments do not:

| Input | APIs | Rule |
|---|---|---|
| Mapped columns | `Update` columns, `Set` keys, `Pluck`, `OnConflict`, `DoUpdate` | validated against model metadata and quoted as identifiers |
| Relation paths | `With`, `WithCount`, `WhereHas`, relation writes | validated against the model's relation fields |
| SQL text | `Where`, `Having`, `Join`, `OrderBy`, `GroupBy`, `RelWhere`, `Expr`, `Raw`, `Exec` | rendered verbatim; use constants, never untrusted input |

For a runtime-selected identifier, map the external value to an allowed
constant instead of concatenating it into SQL. `WriteColumns` can generate
column constants from rio's model mapping. Reject values absent from the
allowlist.

## Observability

`WithQueryHook` installs a read-only `QueryHook` for SQL execution and
transaction control. Events include the operation, model, SQL, arguments,
duration, affected rows, and error. `WithoutArgs` removes arguments before
hooks receive them. The context returned by `BeforeQuery` flows through the
driver to `AfterQuery`, allowing tracing spans and deadlines. rio does not log
errors on the caller's behalf.

## Column constants

`WriteColumns` generates table and column constants from rio's model mapping:

```go
err := rio.WriteColumns(os.Stdout, "models", User{}, Post{})
```

Use the generated constants in identifier allowlists and verbatim SQL
fragments. See [DESIGN.md](DESIGN.md) for architecture and scope.

## Contributing

Use Go 1.27 or newer, then run:

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
