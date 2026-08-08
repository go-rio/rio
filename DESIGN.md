# rio Design

rio is an explicit, stateless ORM for Go. This document records its design
invariants and intentional omissions; usage belongs in the README and godoc.

## Positioning

- **Explicit operations:** structs are data; writes are visible function calls.
  There are no model sessions, identity maps, or hidden flushes.
- **SQL correspondence:** one builder call maps to one SQL fragment, so the
  generated statement stays predictable.
- **Stateless execution:** writes execute immediately. There is no change
  tracking or unit of work.
- **Queries are values:** builders are immutable and connection-free; terminal
  methods take `(ctx, db, ...)` and accept either `*DB` or `*Tx` through
  `Queryer`.
- **Zero dependencies** in the core. Drivers live in separate modules
  (`github.com/go-rio/sqlite|mysql|postgres|clickhouse`) and carry the real
  driver deps.

## Architecture

One pipeline, one parameter contract:

> **build** (connection-free immutable value) →
> **validate + bind** (structure plus this execution's deferred arguments) →
> **render** (actual dialect; entity CRUD shapes cache per grammar) →
> **prepare** (optional `*sql.Stmt` cache, keyed by SQL text, opt-in) →
> **exec** (database/sql or native engine, hooks, error translation)

Four layers inside `github.com/go-rio/rio`:

1. **Execution kernel** — the default engine wraps `database/sql`; the
   PostgreSQL module can supply a pgx-native engine. Both preserve the same rio
   semantics behind `Queryer`. rio owns neither pool policy nor pool tuning;
   callers configure the `database/sql` or pgx pool they construct.
2. **SQL layer** — per-statement renderers, dialect grammars, the `?`
   placeholder rebinder with per-dialect lexer profiles, `IN (?)` slice
   expansion (expand first, renumber second). Dialects are an *opaque*
   interface built into the core (`rio.Postgres`, `rio.MySQL`, `rio.SQLite`,
   `rio.ClickHouse`); capability flags replace type switches: returning,
   conflict target, max bind params, FOR UPDATE render/elide/reject, mutations,
   transactions, unique keys, generated PKs, statement prepare, FINAL.
   The dialect interface stays internal so capabilities can evolve without
   freezing a cross-module implementation contract.
3. **Mapping layer** — reflection-based struct↔table plans, computed once per
   type and cached forever (plans are immutable once published). Scanning has a
   reflect slow path and an unsafe fast path (see Performance).
4. **ORM facade** — generic entry points (`rio.From[T]`, `rio.Insert`, …),
   relations with explicit preloading, optimistic locking, timestamps, explicit
   soft delete, and connection-free reusable query templates.

Driver modules contain constructors, DSN hygiene, and error translation while
preserving the driver error chain. SQL grammar stays in the core.

| Driver | DSN hygiene |
|---|---|
| mysql | forces `parseTime=true`; rejects an explicit `parseTime=false` |
| sqlite | defaults `foreign_keys=on` (without it SQLite never raises FK violations) and `busy_timeout` |

## Reusable query templates and caching

Three mechanisms cover structure, rendering, and preparation:

1. **Entity CRUD is cached automatically** — Insert/Update/Delete/Find SQL
   depends only on the column set, so each grammar caches it by model and
   statement shape.
2. **`Query[T]` is the reusable hand-built template.** It owns no connection or
   prepared statement. `Must` may attach a private, concurrency-safe SQL-shape
   cache keyed by grammar and terminal:

   ```go
   var adults = rio.From[User]().
       Where("tenant_id = ?", fixedTenantID).
       Where("active").
       Where("age > ?").
       OrderBy("created_at DESC").
       Limit(10).
       Must()

   users, err := adults.All(ctx, db, 18)
   emails, err := adults.Pluck[string](ctx, tx, "email", 18)
   ```

   - `Validate` returns the first connection-independent model, builder, or
     relation error. `Must` panics on the same error and otherwise preserves the
     query description while enabling its private cache.
   - Deferred main-query placeholders pass validation. At execution, the
     actual dialect lexer classifies each Where/Having fragment independently.
     Each fragment is either fully inline or fully deferred; the two forms may
     be mixed across fragments.
   - Missing and excess arguments fail before driver execution and before query
     hooks. Runtime slices expand in `IN (?)` on every terminal path.
   - `WhereHas` and `With` relation conditions stay fully inline because nested
     EXISTS and preload statements do not share the main query's argument
     order.
   - SQL renders under the executing handle's grammar. Stable scalar shapes on
     a `Must` query reuse a grammar-and-terminal-specific internal render;
     runtime slices and function-valued relation options bypass it. The same
     Query can run across DBs, transactions, dialects, table namers, and
     argument values. Dialect capabilities and driver execution remain runtime
     checks.
   - Limit/Offset take int values and are not parameterizable; rebuild paged
     queries per page (builder cost is negligible).
3. **`rio.WithStmtCache()`** (opt-in, **default off**) caches `*sql.Stmt` per
   SQL text on the `*rio.DB` and within each transaction. Both caches are
   LRU-bounded because each expanded slice length creates a distinct statement.
   Schema-change errors evict and propagate without an automatic retry. It is
   off by default for transaction-pooler compatibility and independent of Query
   reuse. The native channel uses pgx's query execution mode instead.

## Model mapping

```go
type User struct {
    ID        int64     // convention: "ID" is the auto-increment primary key
    Email     string    `rio:"email_addr"`        // rename
    Age       int       // zero value is a real value, always written
    Bio       *string   // pointer = nullable
    Settings  Prefs     `rio:",json"`             // JSON (de)serialization
    Secret    string    `rio:"-"`                 // ignored
    Version   int64     `rio:",version"`          // opt-in optimistic locking (insert writes 1)
    DeletedAt time.Time `rio:",softdelete"`       // opt-in soft delete (NULL↔zero-time exception)
    CreatedAt time.Time // maintained on insert
    UpdatedAt time.Time // maintained on every write path, including UpdateAll

    Posts   rio.HasMany[Post]      // relations are typed containers, not bare slices
    Org     rio.BelongsTo[Org]     `rio:",fk:org_id"`
    Profile rio.HasOne[Profile]
    Tags    rio.ManyToMany[Tag]    `rio:",join:tag_user"`
}
```

- Table names: snake_case plural via a built-in inflector (`User` → `users`,
  `APIKey` → `api_keys`); override per-model with `TableName() string`, or
  per-DB with `rio.WithTableNamer`. Column names: snake_case with initialism
  handling (`UserID` → `user_id`).
- **Convention-vs-explicit rule**: harmless conventions (timestamps, ID,
  naming) are automatic; anything that changes query semantics or error paths
  (optimistic locking, soft delete) requires an explicit tag.
- `rio:",omitzero"`: skip the column when the field is zero, letting the DB
  default apply. Auto-increment PKs get this implicitly. On a single-row
  `Upsert`, a skipped column also leaves the default conflict update set (the
  existing row's value survives a conflict), and naming it in `DoUpdate` is an
  error (the statement inserts no value to reference). `UpsertAll` binds every
  column (one statement, one column list), so batch zeros are written on both
  branches. Every other column writes its zero value by default.
- Composite primary keys: tag multiple fields `rio:",pk"`. `Find` takes all
  parts in field-declaration order. Models without a PK return
  `ErrNoPrimaryKey` from Find/Update/Delete.
- Embedding flattens exported fields and follows Go shadowing rules. Two fields
  at the same depth are a plan-time error. An unexported embedded type may only
  flatten; pointer embedding is rejected because offset-based scanning cannot
  cross nil pointers.
- Structs containing relation containers are not comparable (they hold slices),
  and `cmp.Diff` panics on the containers' unexported state: pass
  `cmpopts.IgnoreUnexported(rio.HasMany[Post]{}, ...)` and compare relation
  contents through the exported accessors (`Rows`/`Row`), or diff those
  accessors directly.

### Relations are containers, not slices

`rio.HasMany[T]` / `HasOne[T]` / `BelongsTo[T]` / `ManyToMany[T]` know whether
they were loaded. Accessing an unloaded relation panics with instructions (add
`.With("Posts")`) instead of silently returning empty data. `Loaded()` reports
the state; JSON emits `null` when unloaded, and `Set` supports manual assembly.
Implicit lazy loading does not exist. Foreign keys resolve by convention
(`Post.UserID` ↔ `users.id`); tags override `fk:`, `ref:`, and `join:`. On a
many-to-many relation, `fk:` and `ref:` name the join table columns. Resolution
is lazy to allow mutually referencing models. A self-referential many-to-many
relation must name both join columns explicitly.

Preloading uses per-relation `WHERE fk IN (...)` queries, avoiding cartesian
products and preserving parent pagination. Keys are deduplicated and chunked
by the dialect's bind limit. Nested paths and relation options are explicit.
After preloading, containers are resolved to loaded-empty or loaded-nil when no
row matches. Many-to-many relations across composite keys are unsupported.

## Operation semantics

| Operation | Behavior |
|---|---|
| `First/Find/Sole` miss | `rio.ErrNotFound`, which wraps `sql.ErrNoRows` — both `errors.Is` checks work; never logged |
| `All` miss | empty slice, `nil` error |
| `Sole` with 2+ rows | `rio.ErrMultipleRows` |
| `Update/Delete` with `version` mismatch | `rio.ErrStaleObject` (0 rows affected) |
| `UpdateAll/DeleteAll` without WHERE | `rio.ErrMissingWhere`; `.AllRows()` opts in explicitly |
| Set-based write with `Limit/Offset/GroupBy/Having` | refused — silently ignoring a Limit would turn "delete ten" into "delete all matching" |
| Idempotent `Update/Restore` (values already identical) | succeeds on PostgreSQL, MySQL, and SQLite — MySQL counts changed rows, so rio issues one PK probe on the ambiguous zero-affected path instead of misreporting `ErrNotFound` |
| All-defaults insert (every column skipped) | renders `DEFAULT VALUES` (PG/SQLite) / `() VALUES ()` (MySQL); the equivalent Upsert is refused — SQLite cannot attach a conflict clause to DEFAULT VALUES |
| `Update` column whitelist | rendered and bound in canonical field order regardless of caller order — the SQL cache keys on an order-free column bitmap |
| Unique violation | `rio.ErrDuplicateKey` (translated by driver modules, driver error stays in chain) |
| FK violation | `rio.ErrForeignKeyViolated` |
| NULL into non-pointer field | error naming the column — sole exception: the `softdelete` column reads NULL as zero time |
| MySQL insert | fills the auto-increment ID only and never issues a hidden second SELECT |
| SQLite insert | uses `LastInsertId` when only the auto-increment key needs backfill; uses `RETURNING` when omitted default columns must also be loaded |
| Batch backfill | InsertAll backfills auto-inc PKs only (PG by position; SQLite sorted-by-PK since RETURNING order is documented as undefined; MySQL none — interleaved autoinc); UpsertAll never backfills (DoNothing shrinks the row set) |
| ClickHouse writes | **never backfilled** — no RETURNING, no generated IDs, the driver's LastInsertId always errors: after Insert/InsertAll the struct holds exactly what you set. A zero conventional `ID` errors instead of silently storing constraint-less `0` duplicates; `rio:",noautoincr"` stays the "zero is a real value" escape hatch |
| Soft-deleted model queries | filtered by default *because the tag is explicit*; `WithTrashed()` / `OnlyTrashed()`; `Delete` becomes UPDATE, `ForceDelete` is real |
| Upsert on a soft-deleted row | **invariant: a successful Upsert leaves the row visible** — DoUpdate automatically sets `deleted_at = NULL` (+updated_at); `rio.KeepTrashed()` opts out; DoNothing never revives |
| Upsert `updated_at` | reset to the clock on every non-DoNothing upsert, even when nonzero — the conflict branch applies the would-be inserted row's stamp, so it must be this call's now (entity Update's unconditional rule) |
| Zero `omitzero` column in `Upsert` | skipped from the INSERT list **and** the default conflict update set — a conflict preserves the existing value instead of resetting it to the DB DEFAULT; naming it in `DoUpdate` errors; `UpsertAll` binds every column and writes zeros on conflict |
| MySQL Upsert version floor | the DoUpdate branch names the new row with the 8.0.19+ row alias (`VALUES()` is deprecated); MySQL <8.0.19 and MariaDB reject that syntax — `DoNothing` renders alias-free and runs everywhere |
| `First` ordering | no implicit ORDER BY — LIMIT 1 (unless the caller set an explicit Limit, which is respected) over whatever order the DB returns; add OrderBy for determinism |
| Placeholders | always `?`, rebound by a dialect lexer; `??` escapes a literal `?`; `IN (?)` expands slices |
| Scan priority | `rio:"-"` → `json` tag (beats Scanner, documented) → `sql.Scanner` (NULL handed to Scan(nil)) → pointer fields (NULL→nil) → `[]byte` (NULL→nil) → basic conversions (overflow-checked; MySQL unsigned BIGINT > MaxInt64 arrives as bytes and is parsed) → NULL into anything else errors with the column name |
| Times | written as UTC, monotonic-stripped, truncated to microseconds (PG/MySQL precision — otherwise reload-and-Equal never holds), and the normalized value is written back to the struct as it binds, so the struct holds exactly what the database stores; trigger-rewritten columns are not read back; SQLite text format is rio's own, not the driver's |
| Failed `Insert/Update/Upsert` | the struct may already carry this attempt's stamps (CreatedAt/UpdatedAt filled, a zero version set to 1) — stamping happens before execution, the database is untouched, and retrying with the same struct is safe |
| Partial scans | `Raw[T]` into an entity must cover every mapped column; partial projections use a DTO |

### ClickHouse: the read + append dialect

ClickHouse supports analytical reads and append-oriented writes. APIs whose
contracts depend on row locks, synchronous mutations, affected-row counts, or
unique constraints return an error instead of a weaker approximation.

- The server floor is **26.x**, which supports rio's offset-carrying time
  encoding, correlated EXISTS, and correct pre-1970 fractional timestamps.
- Reads support the full builder, relations, `WithCount`, `RelLimit`,
  `WhereHas`, soft-delete filtering, Raw, and reusable Query templates.
  `Query.Final` exposes the ReplacingMergeTree `FINAL` modifier.
- Writes are limited to `Insert`, `InsertAll`, and explicit `Exec` mutations.
  UPDATE/DELETE/Upsert families, transactions, `ForUpdate`, and rio's statement
  cache are rejected.
- clickhouse-go interpolates arguments client-side. rio therefore binds time as
  offset-carrying microsecond text, rejects values ClickHouse would clamp, and
  binds `[]byte` as String instead of `Array(UInt8)`.
- The dialect lexer follows ClickHouse quoting, heredoc, and comment rules.
  `??` produces the driver's literal-question-mark escape; regions its binder
  cannot parse are rejected on argument-carrying statements.
- Duplicate-key and foreign-key sentinels do not apply because ClickHouse has
  neither constraint. Integration probes pin the upstream driver behavior rio
  relies on.

## Performance

Performance follows a few enforceable rules:

- Model plans are immutable and cached by type; stable SQL shapes cache by
  grammar. Rendering appends to byte buffers instead of formatting strings.
- Scanners write into the final result slice. Destination arrays are pooled,
  and nullable pointer values use chunked backing storage instead of one
  reflection allocation per cell.
- Fixed-layout fields use offset-based typed stores. Pointer embedding is
  rejected, byte slices are copied, and race/checkptr tests cover the unsafe
  boundary. Entity queries verify plan-order columns once per result set; Raw
  maps by name.
- The pgx-native channel bypasses `driver.Value` conversion while sharing the
  same store helpers and semantic tests as `database/sql`.
- Deterministic fake-driver allocation budgets guard rio's own overhead. Real
  SQLite, MySQL, PostgreSQL, and native pgx benchmarks measure end-to-end cost
  under the documented [benchmark methodology](bench/README.md). Benchmark
  numbers belong in benchmark output, not as permanent design claims.

## What rio does not have

Each of these is a decision, not a gap:

- **No model hooks/callbacks.** Side effects stay in application code and
  invariants in database constraints. `QueryHook` observes execution but cannot
  alter SQL; a context returned by `BeforeQuery` reaches the driver and the
  matching `AfterQuery`.
- **No implicit lazy loading.** Unloaded relations fail instead of issuing
  invisible N+1 queries.
- **No dirty tracking, identity map, or flush.** Explicit `Update`, with an
  optional column whitelist, makes every write visible.
- **No struct update that silently skips zero values.** Full-column writes,
  explicit whitelists, and `omitzero` have distinct contracts.
- **No default scope of any kind** — soft delete is the single, explicitly
  tagged exception, and even it is a per-model declaration, never a global.
- **No AutoMigrate.** Schema changes require a versioned migration tool.
- **No second-level cache.** Cross-process invalidation belongs to the
  application.
- **No association auto-writes.** Attach, Detach, and SyncRelation perform
  explicit join-table writes and never upsert related entities.
- **No client-side evaluation.** Unsupported expressions return an error.
- **No expression-tree query language.** Go can't introspect closures; any
  simulation leaks. Type safety lives in result generics and the optional
  `WriteColumns` generator.
- **No `.Select()` column pruning on entity queries.** Partial columns never
  produce entity values; projections use `Raw[T]` with a dedicated target type.
- **No read/write splitting or multi-DB routing.** Builders are connection-free
  values — construct two `*rio.DB` (primary, replica) and choose one at the call
  site; routing is a caller decision, not ORM state.
- **No retry or circuit breaking.** That belongs to the platform layer; rio
  never retries a statement.
- **No global or default timeouts.** The context is the caller's — every
  execution takes `ctx`, and rio imposes no deadline of its own.
- **No named parameters.** The single `?`-placeholder pipeline, rebound per
  dialect, is deliberate: one binding convention, one lexer.

## Testing & engineering

- A fake `database/sql` driver asserts SQL, arguments, hooks, and transaction
  boundaries; golden tests cover each dialect.
- Differential fuzzing checks placeholder lexers and slice expansion. Race and
  scan matrices cover immutable query derivation, typed stores, reflection
  fallbacks, and unsafe pointer rules.
- The integration module runs SQLite by default and gates PostgreSQL, MySQL,
  and ClickHouse on DSNs. Driver modules keep adapter-specific suites and probes
  for upstream behavior on which rio depends.
- The core remains dependency-free, requires Go 1.27, and uses functional
  options. Published tags are immutable.

## Shipped scope

The core ships mapping, immutable queries, entity and set writes, upsert and
batch paths, four relation types, nested preloading and counts, optimistic
locking, soft delete, timestamps, Raw/Exec, reusable validated Query templates,
column generation, opt-in statement caching, transactions/savepoints, hooks,
error translation, and composite keys. ClickHouse implements the read-and-append
subset plus `Query.Final`.

Not shipped yet: cursor pagination and schema-drift lint.
