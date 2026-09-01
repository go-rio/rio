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
   The dialect interface stays internal so capabilities can evolve freely.
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

   - `Validate` returns the first connection-independent error; `Must` panics
     on it and enables the private cache.
   - Each Where/Having fragment is fully inline or fully deferred; forms mix
     across fragments. Missing and excess arguments fail before the driver and
     hooks. Slices expand in `IN (?)` on every terminal path.
   - `WhereHas` and `With` conditions stay inline: nested EXISTS and preloads
     do not share the main query's argument order.
   - SQL renders under the executing handle's grammar; one Query runs across
     DBs, transactions, dialects, and namers. Stable scalar shapes reuse a
     cached render; slices and function-valued options bypass it. Cache
     entries key handles weakly and die with the grammar.
   - Limit/Offset are ints, not parameters; rebuild paged queries per page.
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
  `APIKey` → `api_keys`); override with `TableName() string` or
  `rio.WithTableNamer`. Namers must be pure and stable — SQL caches per
  handle, so dynamic tenancy means one `*DB` per tenant scheme. Column names:
  snake_case with initialism handling (`UserID` → `user_id`).
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
- Structs holding relation containers are not comparable; for `cmp.Diff`,
  pass `cmpopts.IgnoreUnexported(rio.HasMany[Post]{}, ...)` and diff through
  `Rows`/`Row`.

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
| Idempotent `Update/Restore` (values already identical) | succeeds everywhere — MySQL counts changed rows, so the ambiguous zero-affected path gets one PK probe |
| `UpdateAll` affected count | the driver's number, undoctored: MySQL counts changed rows, PostgreSQL/SQLite count matched |
| All-defaults insert (every column skipped) | `DEFAULT VALUES` (PG/SQLite) / `() VALUES ()` (MySQL); the equivalent Upsert is refused |
| `Update` column whitelist | rendered and bound in canonical field order regardless of caller order — the SQL cache keys on an order-free column bitmap |
| Unique violation | `rio.ErrDuplicateKey` (translated by driver modules, driver error stays in chain) |
| FK violation | `rio.ErrForeignKeyViolated` |
| NULL into non-pointer field | error naming the column — sole exception: the `softdelete` column reads NULL as zero time |
| MySQL insert | fills the auto-increment ID only and never issues a hidden second SELECT |
| SQLite insert | uses `LastInsertId` when only the auto-increment key needs backfill; uses `RETURNING` when omitted default columns must also be loaded |
| Batch backfill | InsertAll backfills auto-inc PKs only (PG by position, SQLite sorted by PK, MySQL none); UpsertAll never backfills |
| ClickHouse writes | never backfilled: the struct holds exactly what you set. A zero conventional `ID` errors; `rio:",noautoincr"` marks zero as a real value |
| Soft-deleted model queries | filtered by default *because the tag is explicit*; `WithTrashed()` / `OnlyTrashed()`; `Delete` becomes UPDATE, `ForceDelete` is real |
| Entity writes on a soft-deleted row | `Update` addresses by PK and writes rows reads hide. `Delete`/`Restore` are idempotent: repeats keep the stored stamp and version (the zero-matched path probes by PK) |
| Upsert on a soft-deleted row | **invariant: a successful Upsert leaves the row visible** — DoUpdate automatically sets `deleted_at = NULL` (+updated_at); `rio.KeepTrashed()` opts out; DoNothing never revives |
| Upsert `updated_at` | reset to the clock on every non-DoNothing upsert — the conflict branch applies this call's stamp |
| Zero `omitzero` column in `Upsert` | skipped from the INSERT list and the default conflict update set, so a conflict preserves the existing value; naming it in `DoUpdate` errors; `UpsertAll` binds every column |
| MySQL Upsert version floor | `DoUpdate` uses the 8.0.19+ row alias, rejected by older MySQL and MariaDB; `DoNothing` renders alias-free and runs everywhere |
| `First` ordering | no implicit ORDER BY; add OrderBy for determinism |
| Placeholders | always `?`, rebound per dialect; `IN (?)` expands slices. `??` escapes a literal `?` where the rendered SQL can carry one (PG, ClickHouse); on MySQL/SQLite the rendered `?` is the bind marker and `??` cannot produce a literal |
| `Count`/`Exists` with Limit/Offset | `Count` refuses them (COUNT aggregates before LIMIT); `Exists` honors them — `Limit(0)` is false, `Offset(n)` probes row n+1 |
| Scan priority | `rio:"-"` → `json` tag (beats Scanner, documented) → `sql.Scanner` (NULL handed to Scan(nil); `sql.NullTime`/`sql.Null[time.Time]` additionally parse rio's own text form first — a TEXT or expression column round-trips without a decltype) → pointer fields (NULL→nil) → `[]byte` (NULL→nil) → basic conversions (overflow-checked; MySQL unsigned BIGINT > MaxInt64 arrives as bytes and is parsed) → NULL into anything else errors with the column name |
| Times | UTC, monotonic-stripped, microsecond precision; the normalized value is written back to the struct as it binds, so the struct holds what the database stores. SQLite text format is rio's own |
| Failed `Insert/Update/Upsert` | stamping happens before execution, so the struct may carry this attempt's stamps while the database stays untouched; retrying the same struct is safe. Exception: a failure scanning RETURNING back names a row the database kept |
| Partial scans | `Raw[T]` into an entity must cover every mapped column; partial projections use a DTO |

### ClickHouse: the read + append dialect

ClickHouse supports analytical reads and append-oriented writes. APIs whose
contracts depend on row locks, synchronous mutations, affected-row counts, or
unique constraints return an error instead of a weaker approximation.

- Server floor **26.x**: rio's offset-carrying time encoding, correlated
  EXISTS, pre-1970 fractional timestamps.
- Reads: full builder, relations, `WithCount`, `RelLimit`, `WhereHas`,
  soft-delete filters, Raw, Query templates, `Query.Final`.
- Writes: `Insert`, `InsertAll`, explicit `Exec`. UPDATE/DELETE/Upsert,
  transactions, `ForUpdate`, and the statement cache are rejected.
- The ClickHouse channel interpolates client-side, so rio binds time as
  offset-carrying microsecond text, rejects values the server would clamp,
  and binds `[]byte` as String (`Array(UInt8)` corrupts).
- The lexer follows ClickHouse quoting and comment rules; `??` produces the
  driver's literal escape; unparseable regions reject argument-carrying
  statements.
- No duplicate-key or FK sentinels — the constraints don't exist. Integration
  probes pin the upstream behaviors rio relies on.

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
- Native drivers extend past the frozen `NativeDB`/`NativeTx`/`NativeRows` trio
  through optional interfaces, discovered by type assertion the way
  `database/sql/driver` grows: a `NativeBatcher` collapses each preload layer's
  statements into one round trip, and a `NativeCopier` streams explicit-key
  `InsertAll` batches through the driver's bulk-load protocol. Channels without
  a capability keep the per-statement path; hook events fire per statement
  either way.
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
cursor pagination (`OrderKeys`/`After`/`CursorAfter`: keyset predicates with
an automatic primary-key tie-breaker and fingerprinted value-only tokens),
column generation,
opt-in statement caching, transactions/savepoints, hooks, error translation,
and composite keys. ClickHouse implements the read-and-append subset plus
`Query.Final`. The `lint` subpackage reports decidable drift between models and the live
schema (PostgreSQL, MySQL, SQLite); unknown types stay silent.
