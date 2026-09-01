# rio

<p align="center">
  <img src="assets/logo.png" alt="rio gopher logo" width="360">
</p>

[![Go Reference](https://pkg.go.dev/badge/github.com/go-rio/rio.svg)](https://pkg.go.dev/github.com/go-rio/rio)
[![Go](https://img.shields.io/github/go-mod/go-version/go-rio/rio)](https://go.dev/)
[![Release](https://img.shields.io/github/release/go-rio/rio.svg)](https://github.com/go-rio/rio/releases)
[![Test](https://github.com/go-rio/rio/actions/workflows/test.yml/badge.svg)](https://github.com/go-rio/rio/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/go-rio/rio)](https://opensource.org/license/MIT)

A generic ORM for Go with zero third-party dependencies in the core. Queries
are immutable values, every write is an explicit call, and relations load
only when asked.

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
Compile-only examples of the main entry points are in
[example_test.go](example_test.go).

## Features

### Principles

- **Immutable queries.** `Query[T]` is a value: build once, validate, reuse
  concurrently, run against any DB or transaction.
- **Explicit everything.** No lazy loading, no dirty tracking, no hidden
  transactions, no callbacks. What you call is what runs.
- **Typed relations.** `HasMany[T]` and friends load in batched `IN` queries
  when you ask, and panic with guidance when you forgot to.
- **Fast paths where the driver has them.** The PostgreSQL module runs on pgx
  natively: preloads share one round trip per layer and bulk inserts stream
  over `COPY`.

[DESIGN.md](DESIGN.md) records the architecture, the operation-semantics
table, and what rio deliberately leaves out.

### API surface

| Area | API |
|---|---|
| Query construction | `From[T]`, `Where`, `Having`, `Join`, `OrderBy`, `GroupBy`, `Distinct`, `Limit`, `Offset`, `Scope` |
| Query modifiers | `ForUpdate`, `ForShare`, `Final`, `WithTrashed`, `OnlyTrashed`, `AllRows` |
| Query execution | `All`, `First`, `Sole`, `Find`, `Rows`, `Chunk`, `Count`, `Exists`, `Pluck[V]`, `Sum/Min/Max/Avg[V]`, `SQL` |
| Cursor pagination | `OrderKeys`, `After`, `Before`, `CursorAt`, `Cursor.String`, `ParseCursor` |
| Direct lookup and SQL | `Find[T]`, `Raw[T]`, `Exec`, `Query.Sub` |
| Entity writes | `Insert`, `Update`, `Delete`, `ForceDelete`, `Restore`, `Upsert`, `FirstOrCreate`, `CreateOrFirst` |
| Batch and set writes | `InsertAll`, `UpsertAll`, `UpdateAll`, `UpdateAllReturning`, `DeleteAll`, `DeleteAllReturning`, `ForceDeleteAll`, `RestoreAll` |
| Relations | `With`, `WithCount`, `WhereHas`, `WhereHasNot`, `Attach`, `Detach`, `SyncRelation`, `ClearRelation` |
| Validation and reuse | `Query.Validate`, `Query.Must`, `WithStmtCache`, `WithoutStmtCache` |
| Handles and options | `New`, `NewNative`, `DB.Tx`, `DB.TxWith`, `Tx.Tx`, `WithQueryHook`, `WithoutArgs`, `WithClock`, `WithErrorTranslator`, `WithTableNamer`, `WithDriverHandle`, `DB.DescribeModel`, `DB.Dialect`, `WriteColumns` |

### Queries

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
renders the statement `All` would run, with its bound arguments, without
executing it.

`First` adds `LIMIT 1` only when no limit is set and never adds an order;
`Sole` returns `ErrMultipleRows` past one row. `Query.Find` is the keyed
`First`: `WithTrashed`, `With`, `WithCount`, and inline `Where` all apply, and
composite key parts follow field declaration order. `Limit` and `Offset`
render into the SQL, not as parameters.

`Sum`, `Min`, `Max`, and `Avg` aggregate a mapped column and return the zero
value over no rows (`sql.Null[V]` tells the two apart). `Distinct` applies to
entity rows, `Pluck`, `Count` (distinct primary keys), `Sum`, and `Avg`.
`Count`, `Pluck`, and the aggregates refuse `GroupBy`/`Having` — projections
go through `Raw`; `Count` and the aggregates also refuse `Limit`/`Offset`,
while `Exists` honors them.

Parameters per fragment:

- `Where("age >= ?", 18)` — inline, owns its arguments.
- `Where("age >= ?")` — deferred, consumes terminal arguments in SQL order.
- `Where("active")` — no placeholders.

Slices expand inside `IN (?)`; an empty slice is an error. A one-column
query embeds the same way: `Where("id IN (?)", banned.Sub("user_id"))` splices
the subquery and its arguments in place — the caller writes the parentheses,
and the subquery's own `Where` arguments must be inline. Missing or excess
arguments fail before the driver sees the query. `??` emits a literal `?`
where the rendered SQL can carry one (PostgreSQL, ClickHouse); on MySQL and
SQLite the rendered `?` is the bind marker. `Join`/`OrderBy`/`GroupBy` take no
placeholders, and `RelWhere` arguments are always inline.

`Must` caches stable scalar shapes per handle; slices, subqueries, and cursors
bypass the cache. `Rows` streams without materializing the slice; it rejects
`With`/`WithCount` and `Before`. `WithStmtCache` caches prepared statements
per DB and per transaction (default 512 entries); the sqlite and mysql
modules turn it on by default, and `WithoutStmtCache` opts out behind
transaction- or statement-mode poolers. `New` panics on `WithStmtCache` with
ClickHouse, which cannot prepare general queries.

### Cursor pagination

`OrderKeys` declares ordering over mapped NOT NULL scalar columns, so rio can
read key values back out of a row and issue a keyset cursor. A missing
primary-key column is appended as tie-breaker — pages never skip or repeat.
`OrderKeys` cannot mix with verbatim `OrderBy`:

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
reads in `OrderKeys` order; `After` and `Before` cannot combine.
`Chunk(ctx, db, 500)` walks the whole result in keyset pages
(`iter.Seq2[[]T, error]`), releasing the connection between pages and
applying `With`/`WithCount` per page; it follows `OrderKeys`, defaulting to
the primary key, refuses `Limit`, `Offset`, `After`, and `Before`, and stops
at the first short page.

`Cursor.String`/`rio.ParseCursor` round-trip a URL-safe token. Tokens carry
values (bound as parameters) plus an ordering fingerprint — a forged token
moves the window, never the query, and a cursor from different `OrderKeys`
fails loudly. The zero `Cursor` is rejected; omit `After` for the first page.

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

### Transactions and row locks

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

Error rolls back, `nil` commits, a panic rolls back and re-panics, `Tx` on a
`*Tx` opens a savepoint, and `TxWith` takes `*sql.TxOptions` (isolation
level, read-only). Batch operations never start hidden transactions — wrap
them in `DB.Tx` when all chunks must land together.

`ForUpdate` and `ForShare` take `rio.NoWait` or `rio.SkipLocked`; the
queue-worker idiom is `Where("state = ?", "queued").ForUpdate(rio.SkipLocked).Limit(1)`.
SQLite elides row locks, ClickHouse rejects them.

### Models and relations

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

Table names are the snake_case plural of the struct name (`User` → `users`,
`APIKey` → `api_keys`); `rio.TableName` exposes the derivation and
`WithTableNamer` overrides it per handle. Embedded structs flatten by value;
pointer embedding is rejected.

Relations are `HasMany[T]`, `HasOne[T]`, `BelongsTo[T]`, `ManyToMany[T]`;
`fk:`/`ref:`/`join:` tags override conventions. Relation APIs take Go field
names (`With("Posts.Comments")`), column APIs take column names. Preloads run
as separate key-set queries (one array parameter on PostgreSQL, an `IN` list
elsewhere), never JOINs, and never lazily: `Rows`/`Row` on an unloaded
container panic naming the `With` argument to add, `Loaded` reports the
state, `Set` assembles one by hand, and JSON encodes an unloaded relation as
`null`.

`With` takes `RelWhere`, `RelOrderBy`, `RelLimit` (per parent; needs window
functions: PostgreSQL, MySQL 8+, SQLite 3.25+), and `RelWithTrashed`; options
apply to the leaf of a dotted path. `WithCount` fills the `countof` target in
one `GROUP BY` query and takes `RelWhere` and `RelWithTrashed` to count a
subset; a filtered count never reuses a preload. `WhereHas`/`WhereHasNot`
keep rows whose relation path has (no) matching row, through nested `EXISTS`.

`Attach`, `Detach`, `SyncRelation`, and `ClearRelation` write the join table
of a `ManyToMany` relation and never upsert related entities: `Attach` is
idempotent, `Detach` needs ids, `SyncRelation` makes the relation match its
ids exactly inside a transaction, and `ClearRelation` unlinks every row.

### Writes and errors

`Insert` backfills generated columns where the dialect can and stamps
`CreatedAt`/`UpdatedAt` and a zero `version` before execution. `Update` writes
every eligible field — zero values included — unless given a column
whitelist, and checks the version column. `Delete` becomes an `UPDATE` of the
`softdelete` stamp on soft-delete models, `ForceDelete` deletes, `Restore`
clears the stamp; queries hide trashed rows unless `WithTrashed` or
`OnlyTrashed`. `FirstOrCreate`/`CreateOrFirst` re-read after `ErrDuplicateKey`.

Set-based writes require a condition (`AllRows()` opts out) and refuse
`Limit`/`Offset`, `GroupBy`/`Having`, `Join`, ordering, preloads, and row
locks — select the target rows in `Where`. `UpdateAllReturning` and
`DeleteAllReturning` hand the affected rows back where the dialect has
`RETURNING` (a soft delete returns the trashed state); MySQL rejects them.

`Upsert` supports conflict targets (`OnConflict`), update whitelists
(`DoUpdate`), `DoUpdateSet` for expressions (`rio.Expr("hits + excluded.hits")`)
or bound values, `DoNothing`, and `KeepTrashed`. In `DoUpdateSet` the incoming
row is `excluded` on PostgreSQL and SQLite and `_rio_new` on MySQL;
rio-maintained and `readonly` columns, and columns also named in `DoUpdate`,
are rejected, and `DoNothing` cannot combine with either. A successful upsert
leaves the row visible unless `KeepTrashed`.

`InsertAll`/`UpsertAll` chunk at the dialect bind limit; batch writes share
one column list, so `omitzero` doesn't apply, and a batch mixing zero and
explicit generated keys is refused. `InsertAll` backfills keys only where
ordering is reliable (PostgreSQL by position, SQLite sorted by key, MySQL
never); `UpsertAll` never backfills.

| Condition | Result |
|---|---|
| `First`/`Find`/`Sole` miss | `ErrNotFound` (wraps `sql.ErrNoRows`) |
| `All` finds nothing | empty slice, `nil` error |
| `Sole` finds several | `ErrMultipleRows` |
| optimistic-lock conflict | `ErrStaleObject` |
| set write without condition | `ErrMissingWhere` |
| keyed operation on a model without a primary key | `ErrNoPrimaryKey` |
| unique / FK violation | `ErrDuplicateKey` / `ErrForeignKeyViolated`, driver error retained |
| operation the dialect cannot honor | error matching `errors.ErrUnsupported` |
| NULL into non-nullable field | error naming the column |

Times are normalized to UTC, microsecond precision, and written back to the
struct as they bind.

### Dialect differences

| Dialect | Behavior |
|---|---|
| PostgreSQL | `RETURNING` everywhere, including `InsertAll` backfill; row locks with `NoWait`/`SkipLocked`; preload key sets bind as one array (`= ANY`). The driver module adds a pgx-native channel: batched preload round trips and `COPY`-backed bulk inserts. |
| MySQL | `Insert` backfills via `LastInsertId`; batch inserts don't backfill; no `RETURNING`. `DoUpdate` needs MySQL 8.0.19+ (no MariaDB); `DoNothing` works everywhere. Statement cache on by default. |
| SQLite | Pure-Go driver. `RETURNING` where backfill needs it; row locks are no-ops. Statement cache, UTC time binding, and `BEGIN IMMEDIATE` on by default. |
| ClickHouse | Reads, preloads, `Insert`, `InsertAll`. Rejects row locks, transactions, statement caching, synchronous update/delete, and conflict upserts — use `ReplacingMergeTree` + `Final`. No backfill; supply IDs yourself. |

### Handles and options

`rio.New(*sql.DB, dialect, opts...)` wraps a pool you configure; rio never
tunes it. Dialects are the opaque built-in values `rio.Postgres`, `rio.MySQL`,
`rio.SQLite`, and `rio.ClickHouse`; driver modules pick one, never implement
one. `Unwrap` returns the `*sql.DB`, `Dialect` the dialect value,
`DescribeModel` the resolved table and column mapping, and `Close` closes the
statement cache and the pool.

| Option | Effect |
|---|---|
| `WithQueryHook(h)` | observe statements (see Observability) |
| `WithoutArgs()` | strip bind arguments from hook events |
| `WithClock(fn)` | time source for stamps and soft deletes, for tests |
| `WithErrorTranslator(fn)` | map driver errors to sentinels; driver modules install one |
| `WithTableNamer(fn)` | rename tables per handle; must be pure and stable, since SQL caches per handle |
| `WithDriverHandle(h)` | attach a driver-owned handle, read back by `DB.DriverHandle` |
| `WithStmtCache(cap)` / `WithoutStmtCache()` | prepared-statement caches (see Queries) |

`NewNative` builds a handle on a driver-native channel (`NativeDB`,
`NativeTx`, `NativeRows`, with optional `NativeBatcher` and `NativeCopier`
capabilities discovered by type assertion). It is driver-module SPI;
applications construct through the driver module (`postgres.OpenNative`).

### Security

Values always bind as parameters. SQL fragments do not:

| Input | APIs | Rule |
|---|---|---|
| Mapped columns | `Update` columns, `Set` keys, `Pluck`, aggregates, `Sub`, `OrderKeys`, `DoUpdate`, `DoUpdateSet` | validated against the model, quoted as identifiers |
| Conflict targets | `OnConflict` | quoted as identifiers |
| Relation paths | `With`, `WithCount`, `WhereHas`, relation writes | validated against the model's relations |
| SQL text | `Where`, `Having`, `Join`, `OrderBy`, `GroupBy`, `RelWhere`, `RelOrderBy`, `Expr`, `Raw`, `Exec` | rendered verbatim — constants only, never untrusted input |

For runtime-selected identifiers, map external values onto generated
constants: `rio.WriteColumns(os.Stdout, "models", User{}, Post{})` emits
table and column constants from the model mapping.

### Observability

`WithQueryHook` observes every statement: operation, model, SQL, arguments,
duration, row counts, error. Hooks cannot alter SQL. The context returned by
`BeforeQuery` flows through the driver into `AfterQuery`, so tracing spans and
deadlines propagate. `WithoutArgs` strips arguments from events.

`QueryEvent.Op` is a stable label (`select`, `insert`, `update`, `delete`,
`upsert`, `copy`, `raw`, `exec`, `begin`, `commit`, `rollback`, `savepoint`);
`Phase` marks statements rio derives itself (`preload`, `count`, `probe`). For
row-returning queries `AfterQuery` fires once the rows are consumed, and a
`First`/`Find`/`Sole` miss reports `Err = nil`.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) for setup, the test suites, and the
commit and comment conventions. The short version:

```bash
go test ./...
go test -race ./...
go vet ./...
```

## Contributors

Thanks to everyone who has contributed.

[![Contributors](https://contrib.rocks/image?repo=go-rio/rio)](https://github.com/go-rio/rio/graphs/contributors)

## License

rio is released under the [MIT License](LICENSE), © 2026-now TreeNewBee.

The rio gopher logo is inspired by the [Go gopher](https://go.dev/blog/gopher),
created by Renée French and licensed under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
