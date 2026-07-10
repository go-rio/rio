# rio Design

rio is a zero-surprise ORM for Go. This document records the design decisions,
the research they came from, and — just as importantly — the features rio
deliberately refuses to have.

The research base: GORM/ent/Bun/sqlc/SQLBoiler/sqlx/Jet/XORM (Go),
ActiveRecord/Eloquent/Django (Active Record family), SQLAlchemy/Hibernate/EF
Core/Doctrine (Data Mapper family), Prisma/Drizzle/Kysely/Ecto/Diesel/SeaORM/jOOQ
(modern type-safe family), plus the collected complaints of the Go community.
Twenty years of ORM evolution across every ecosystem converge on one direction:
**from implicit magic to explicit declaration**. Go starts where they ended up.

## Positioning

> GORM's ergonomics + sqlc's honesty + type safety through generics,
> minus the weight of all three. Fast enough to have nothing to hide.

- **Repo-style explicitness** (Ecto): structs are pure data; every database
  operation is a visible call. No `user.Save()`, no global handles, no
  sessions, no identity map.
- **SQL correspondence** (Drizzle): one builder call ≈ one SQL fragment. The
  generated SQL is predictable and printable. If you know SQL, you know rio.
- **Stateless by default** (Hibernate 7's own conclusion): insert/update/delete
  execute immediately. No unit of work, no change tracking, no flush ordering.
- **Queries are values** (SQLAlchemy 2.0 statement/session split): builders are
  immutable, connection-free values; execution points take the `Queryer`.
  Every execution signature reads `(ctx, db, ...)`.
- **Zero dependencies** in the core. Drivers live in separate modules
  (`github.com/go-rio/sqlite|mysql|postgres|clickhouse`) and carry the real
  driver deps.

## Architecture

One pipeline with an explicit cache key per stage:

> **build** (connection-free pure value, no cache) →
> **render** (SQL text, cached per *grammar* — the identity frozen per DB:
> dialect + table namer + SQL-affecting options) →
> **prepare** (optional `*sql.Stmt` cache, keyed by SQL text, opt-in) →
> **exec** (database/sql, hooks, error translation)

Four layers inside `github.com/go-rio/rio`:

1. **Execution kernel** — wraps `database/sql`. `*rio.DB`, `*rio.Tx`, and the
   `rio.Queryer` interface both implement (DAO code works inside and outside
   transactions unchanged). `Unwrap()` on both exposes the underlying handle;
   rio never wraps or replaces the connection pool (Prisma spent 2025 undoing
   its self-built engine and pool).
2. **SQL layer** — per-statement renderers, dialect grammars, the `?`
   placeholder rebinder with per-dialect lexer profiles, `IN (?)` slice
   expansion (expand first, renumber second). Dialects are an *opaque*
   interface built into the core (`rio.Postgres`, `rio.MySQL`, `rio.SQLite`,
   `rio.ClickHouse`); capability flags (returning, conflict target, max bind
   params, FOR UPDATE render/elide/reject, mutations, transactions, unique
   keys, generated PKs, statement prepare, FINAL) instead of type switches.
   Cross-module dialect implementations would freeze the interface — the same
   argument, validated repeatedly, in go-rio/migrate.
3. **Mapping layer** — reflection-based struct↔table plans, computed once per
   type and cached forever (plans are immutable once published). Scanning has
   a reflect slow path and an unsafe fast path (see Performance).
4. **ORM facade** — generic entry points (`rio.From[T]`, `rio.Insert`, …),
   relations with explicit preloading, optimistic locking, timestamps,
   explicit soft delete, compiled queries.

Driver modules contain: `Open(dsn)` / `New(*sql.DB)` constructors, an error
translator (driver error → `rio.ErrDuplicateKey` / `ErrForeignKeyViolated`,
keeping the driver error in the chain), and DSN hygiene (mysql forces
`parseTime=true` and rejects an explicit `parseTime=false`; sqlite defaults
`foreign_keys=on` — without it SQLite would never raise FK violations — and
`busy_timeout`). Nothing else — grammar lives in the core.

## API at a glance

```go
db, err := postgres.Open(dsn)            // *rio.DB

// Queries: From reads like SQL. Builders are immutable, connection-free
// values — package-level base queries and concurrent derivation are safe by
// construction (GORM's "condition pollution" is structurally impossible).
users, err := rio.From[User]().
    Where("age > ?", 18).
    OrderBy("created_at DESC").
    Limit(10).
    With("Posts", rio.RelWhere("published = ?", true)).
    All(ctx, db)

user, err := rio.Find[User](ctx, db, 42)             // by PK; composite PKs pass all parts in declared order
user, err := rio.From[User]().Where("email = ?", e).First(ctx, db)
n, err    := rio.From[User]().Where(...).Count(ctx, db)

// Writes: immediate, explicit, zero values are real values.
err = rio.Insert(ctx, db, &user)                     // fills ID + skipped omitzero columns via RETURNING on PG/SQLite
err = rio.InsertAll(ctx, db, users)                  // single multi-VALUES statement, auto-chunked
err = rio.Update(ctx, db, &user)                     // full-column UPDATE by PK (honest), or:
err = rio.Update(ctx, db, &user, "name", "email")    // explicit column whitelist
err = rio.Delete(ctx, db, &user)

// Set-based writes: refuse to run without WHERE.
n, err = rio.From[User]().Where("last_login < ?", cutoff).
    UpdateAll(ctx, db, rio.Set{"status": "inactive"})    // maintains updated_at; rio.Expr for "count + 1"
n, err = rio.From[Session]().Where("expired").DeleteAll(ctx, db)

// Upsert: SQL-shaped, all four elements from day one
// (conflict target, update whitelist, returning, timestamps).
err = rio.Upsert(ctx, db, &user, rio.OnConflict("email"), rio.DoUpdate("name"))

// Escape hatch: same scanner, same transactions, any shape.
stats, err := rio.Raw[UserStat](
    "SELECT org_id, count(*) AS n FROM users GROUP BY org_id").All(ctx, db)
n, err := rio.Raw[int64]("SELECT count(*) FROM users").First(ctx, db)

// Transactions: closure + automatic SAVEPOINT on nesting.
err = db.Tx(ctx, func(tx *rio.Tx) error {
    if err := rio.Insert(ctx, tx, &order); err != nil { return err }
    return rio.Insert(ctx, tx, &payment)
})
```

## Compiled queries

Most application SQL is fixed in shape; only parameters vary. Three tiers:

1. **Entity CRUD is cached invisibly** — Insert/Update/Delete/Find SQL depends
   only on the column set, so it is rendered once per grammar and cached on
   the plan.
2. **`rio.MustCompile[T]`** for hand-built queries, `regexp.MustCompile`
   style:

   ```go
   var adults = rio.MustCompile[User](
       rio.From[User]().Where("age > ?").OrderBy("created_at DESC").Limit(10),
   )
   users, err := adults.All(ctx, db, 18)   // binds parameters only
   ```

   Package-level declaration runs before any DB exists: the eager phase does
   structural validation only (tags, plan, relation paths — panic fits Must
   semantics) and renders nothing grammar-dependent. SQL is rendered lazily
   per grammar and cached; placeholder arity is verified precisely at first
   render (`?` counting is lexer- and therefore dialect-dependent).
   **MustCompile passing does not mean execution cannot fail.** Inline args
   and exec-time args must not mix: a compiled query is either fully inline
   (frozen constant query) or fully exec-parameterized — mixing panics.
   `RelWhere` args are preload subqueries, inherently inline, exempt.
   Limit/Offset take int values and are not parameterizable — rebuild paged
   queries per page (build cost is negligible; only render is cached).
   Compiled execution still runs every QueryHook. `rio.Compile` is the
   error-returning variant.
3. **`rio.WithStmtCache()`** (opt-in, **default off**) caches `*sql.Stmt` per
   SQL text on the `*rio.DB` only — transactions bypass it. LRU-bounded
   (`IN` expansion makes each slice length a distinct statement), evicted
   statements are closed correctly, schema-change errors ("cached plan must
   not change result type") evict and propagate — never auto-retry. Off by
   default because of PgBouncer transaction pooling (GORM #5908's lesson);
   independent of MustCompile so compiled queries never smuggle prepared
   statements past a pooler.

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
- **Convention vs. surprise rule**: harmless conventions (timestamps, ID,
  naming) are automatic; anything that changes query semantics or error paths
  (optimistic locking, soft delete) requires an explicit tag.
- `rio:",omitzero"`: skip the column when the field is zero, letting the DB
  default apply. Auto-increment PKs get this implicitly. On a single-row
  `Upsert`, a skipped column also leaves the default conflict update set —
  the existing row's value survives a conflict — and naming it in `DoUpdate`
  is an error (the statement inserts no value to reference). `UpsertAll`
  binds every column (one statement, one column list), so batch zeros are
  written on both branches. The default for every other column is to write
  the zero value — Go zero values are values (go-pg's `NULL`-by-default is
  the canonical counterexample).
- Composite primary keys: tag multiple fields `rio:",pk"`. `Find` takes all
  parts in field-declaration order. Models without a PK return
  `ErrNoPrimaryKey` from Find/Update/Delete.
- Embedding: an anonymous struct field flattens its exported fields into the
  parent — a shared `type Timestamps struct { CreatedAt, UpdatedAt time.Time }`
  maps the same as if written inline, and this holds even when the embedded
  type's own name is unexported (`encoding/json` flattens these too; silently
  dropping mapped columns is the surprise rio refuses). Same-named fields
  follow Go's shadowing rules: the shallowest declaration wins — even one
  excluded with `rio:"-"` or renamed — and deeper ones do not map; two at the
  same depth are a plan-time error, not a silent drop. An unexported embedded
  type can only flatten: tagging it into a column of its own is refused at
  plan time (binding cannot address unexported fields). Embed by value:
  pointer embedding is rejected because offset-based scanning cannot hop a nil.
- Structs containing relation containers are not comparable (they hold
  slices), and cmp.Diff panics on the containers' unexported state: pass
  cmpopts.IgnoreUnexported(rio.HasMany[Post]{}, ...) and compare relation
  contents through the exported accessors (Rows/Row), or diff those
  accessors directly.

### Relations are containers, not slices

`rio.HasMany[T]` / `HasOne[T]` / `BelongsTo[T]` / `ManyToMany[T]` know whether
they were loaded. Accessing an unloaded relation panics with instructions
(add `.With("Posts")`) instead of silently returning empty data. This is
Ecto's `NotLoaded` + Rails/Laravel strict-loading — built in, not bolted on.
`Loaded()` reports the state; JSON marshalling emits `null` when unloaded.
`Set` is exported — manual assembly is a legitimate use. Implicit lazy loading
does not exist: rio never issues a query you didn't ask for. Foreign keys
resolve by convention (`Post.UserID` ↔ `users.id`), tags override (`fk:`,
`ref:`, `join:` — on ManyToMany, `fk:`/`ref:` name the join table's owner-side
and target-side columns); resolution is lazy (first preload) to allow mutually
referencing models. A self-referential ManyToMany must name its two join
columns explicitly — the convention would derive the same name for both, so
rio errors with the fix instead of emitting a broken join.

Preloading always uses per-relation `WHERE fk IN (...)` split queries
(selectin): no cartesian explosion, pagination-safe, identical behavior on all
three dialects — the strategy ActiveRecord, Eloquent, Ecto, and SQLAlchemy all
converged on. Parent keys are deduplicated and chunked by the dialect's bind
limit. `With("Posts.Comments")` nests; `RelWhere`/`RelOrder`/`RelWithTrashed`
customize. After preloading, containers are always resolved: no children →
loaded-empty; a NULL FK on BelongsTo → loaded-nil. m2m across composite PKs
returns a clear error in v1.

## Semantics that refuse to surprise

| Operation | Behavior |
|---|---|
| `First/Find/Sole` miss | `rio.ErrNotFound`, which wraps `sql.ErrNoRows` — both `errors.Is` checks work; never logged |
| `All` miss | empty slice, `nil` error |
| `Sole` with 2+ rows | `rio.ErrMultipleRows` |
| `Update/Delete` with `version` mismatch | `rio.ErrStaleObject` (0 rows affected) |
| `UpdateAll/DeleteAll` without WHERE | `rio.ErrMissingWhere`; `.AllRows()` opts in explicitly |
| Set-based write with `Limit/Offset/GroupBy/Having` | refused loudly — silently ignoring a Limit would turn "delete ten" into "delete all matching" |
| Idempotent `Update/Restore` (values already identical) | succeeds on all three dialects — MySQL counts changed rows, so rio issues one PK probe on the ambiguous zero-affected path instead of misreporting `ErrNotFound` |
| All-defaults insert (every column skipped) | renders `DEFAULT VALUES` (PG/SQLite) / `() VALUES ()` (MySQL); the equivalent Upsert is refused — SQLite cannot attach a conflict clause to DEFAULT VALUES |
| `Update` column whitelist | rendered and bound in canonical field order regardless of caller order — the SQL cache keys on an order-free column bitmap |
| Unique violation | `rio.ErrDuplicateKey` (translated by driver modules, driver error stays in chain) |
| FK violation | `rio.ErrForeignKeyViolated` |
| NULL into non-pointer field | error naming the column — sole exception: the `softdelete` column reads NULL as zero time |
| MySQL insert | fills auto-increment ID only; rio never issues a hidden second SELECT (`SupportsReturning` capability, Ecto's honest route) |
| Batch backfill | InsertAll backfills auto-inc PKs only (PG by position; SQLite sorted-by-PK since RETURNING order is documented as undefined; MySQL none — interleaved autoinc); UpsertAll never backfills (DoNothing shrinks the row set) |
| ClickHouse writes | **never backfilled** — no RETURNING, no generated IDs, the driver's LastInsertId always errors: after Insert/InsertAll the struct holds exactly what you set. The flip side: a zero conventional `ID` errors loudly instead of silently storing constraint-less `0` duplicates; `rio:",noautoincr"` stays the "zero is a real value" escape hatch |
| Soft-deleted model queries | filtered by default *because the tag is explicit*; `WithTrashed()` / `OnlyTrashed()`; `Delete` becomes UPDATE, `ForceDelete` is real |
| Upsert on a soft-deleted row | **invariant: a successful Upsert leaves the row visible** — DoUpdate automatically sets `deleted_at = NULL` (+updated_at); `rio.KeepTrashed()` opts out; DoNothing never revives |
| Upsert `updated_at` | reset to the clock on every non-DoNothing upsert, even when nonzero — the conflict branch applies the would-be inserted row's stamp, so it must be this call's now (entity Update's unconditional rule) |
| Zero `omitzero` column in `Upsert` | skipped from the INSERT list **and** the default conflict update set — a conflict preserves the existing value instead of resetting it to the DB DEFAULT; naming it in `DoUpdate` errors; `UpsertAll` binds every column and writes zeros on conflict |
| MySQL Upsert version floor | the DoUpdate branch names the new row with the 8.0.19+ row alias (`VALUES()` is deprecated); MySQL <8.0.19 and MariaDB reject that syntax — `DoNothing` renders alias-free and runs everywhere |
| `First` ordering | no implicit ORDER BY — LIMIT 1 (unless the caller set an explicit Limit, which is respected) over whatever order the DB returns; add OrderBy for determinism (SQL correspondence) |
| Placeholders | always `?`, rebound per dialect with a per-dialect lexer; `??` escapes a literal `?` (PostgreSQL JSONB operators); `IN (?)` expands slices — sqlx/Bun's established conventions |
| Scan priority | `rio:"-"` → `json` tag (beats Scanner, documented) → `sql.Scanner` (NULL handed to Scan(nil), no second-guessing) → pointer fields (NULL→nil) → `[]byte` (NULL→nil) → basic conversions (overflow-checked; MySQL unsigned BIGINT > MaxInt64 arrives as bytes and is parsed) → NULL into anything else errors with the column name |
| Times | written as UTC, monotonic-stripped, truncated to microseconds (PG/MySQL precision — otherwise reload-and-Equal never holds), and the normalized value is written back to the struct as it binds, so the struct holds exactly what the database stores; trigger-rewritten columns are not read back; SQLite text format is rio's own, not the driver's |
| Failed `Insert/Update/Upsert` | the struct may already carry this attempt's stamps (CreatedAt/UpdatedAt filled, a zero version set to 1) — stamping happens before execution, the database is untouched, and retrying with the same struct is safe |
| Partial scans | `Raw[T]` into an entity requires the result to cover every mapped column — a partial scan errors (naming the missing columns and pointing at a DTO) rather than letting a later `rio.Update` write zeros to the unscanned ones (mirror image of GORM #6860) |

### ClickHouse: the read + append dialect

ClickHouse is the fourth built-in dialect and the first that does not speak
the full surface — deliberately. It is an honest subset, not a degraded port:
the supported half is exactly OLAP's real usage shape (analytical reads +
batched appends), and every rejected API returns an error naming the
ClickHouse-native way out. "If rio can't compile it, it returns an error"
applies to semantics too: an API whose contract cannot hold does not get a
lookalike that silently means something weaker.

- **Reads are complete**: the whole query builder, all four relation
  preloads, `WithCount`, window-function `RelLimit`, `WhereHas` (server ≥
  25.8), soft-delete read filtering, `Raw`/`Exec`/compiled queries. Plus one
  dialect-gated addition, `Query.Final()` — reads through the `FINAL` table
  modifier, the read-side companion of ReplacingMergeTree deduplication.
  It exists because the Upsert/Update rejection messages point at
  ReplacingMergeTree: pointing there without a read-side closure would send
  users straight back out to Raw.
- **Writes are Insert/InsertAll (+ `rio.Exec` mutations)**. The
  UPDATE/DELETE/Upsert families, transactions, `ForUpdate`, and the stmt
  cache are rejected at the render/execution layer — each on a double
  ground: server semantics (asynchronous mutations, no unique constraints,
  no row locks) *and* driver fact (clickhouse-go's `Begin` is a no-op, its
  `RowsAffected` is unconditionally 0 — the count `ErrNotFound`,
  `ErrStaleObject`, and the idempotence probe are built on cannot be
  implemented, not merely "is awkward").
- **Every argument is interpolated client-side** — the driver's
  `database/sql` path has no parameter binding. Two dialect rules follow:
  times bind as fixed-format text (`2006-01-02 15:04:05.000000+00:00` — the
  driver would silently truncate a `time.Time` to whole seconds; the
  explicit offset overrides column timezones, and the out-of-range values
  ClickHouse would silently *clamp* are rejected client-side, zero
  `time.Time` included), and `[]byte` binds as `String` (the driver renders
  it as an `Array(UInt8)` literal otherwise — a String column then stores
  the literal's text form, silently).
- **The lexer is pinned to the server's Lexer.cpp** (heredocs with empty and
  digit-leading tags where unterminated ones do not lex, `//` comments, the
  `# ` space rule, backslash escapes in every quote flavor), and `??` emits
  the driver's `\?` escape so a literal `?` (ClickHouse's ternary operator)
  survives the driver's own rewriting. The two regions the driver's scanner
  cannot see — heredocs and `//` comments — are rejected on
  argument-carrying statements instead of silently corrupted; a bare `\?`
  passes through as the driver's literal-? escape, consuming nothing.
- **`ErrDuplicateKey` and `ErrForeignKeyViolated` never fire** — ClickHouse
  has no constraints to violate. The go-rio/clickhouse module accordingly
  installs no error translator: this is a documented dialect fact, not a gap.
- The upstream behaviors all of this leans on (quote-aware binding —
  clickhouse-go ≥ v2.47.0, enforced in the driver module's go.mod — the
  second-truncation, the Array rendering, RowsAffected 0, the fake Begin,
  INSERT-only Prepare) are each pinned by a named integration probe: if
  upstream changes one, the matching probe fails before any user does.

## Performance

Target, set by the user: **fastest full-featured Go ORM; at minimum, beat
GORM everywhere by ≥25%** (research baseline, reads: raw/sqlc ~148k ns/op <
Bun/ent ~176k < GORM 223k < sqlx 319k). The gap is never reflection itself —
it is missing plan caches and sloppy string assembly.

- Per-type plans cached forever; per-grammar SQL caches (see pipeline).
- Rendering appends to `[]byte` (no fmt in hot paths).
- Scanning appends a zero value and scans into `&s[len(s)-1]` in place;
  dest slices are pooled across queries (zero steady-state allocations) and
  reused per row. Non-NULL pointer cells (`*T` fields) come out of chunked
  per-column backing arrays (1→4→16→64, then 128-cell chunks) instead of one
  `reflect.New` per cell: a hundred nullable rows cost five allocations per
  column, not a hundred; a surviving `*T` keeps at most its chunk alive,
  never the whole column.
- **Unsafe fast path**: plans record field offsets; fixed-layout kinds
  (ints/uints/floats/bool/string/[]byte/time.Time) are written via
  `unsafe.Add` directly, skipping reflect.Value.Set (Bun/sonic-class
  technique). Discipline: no uintptr variables; pointer embedding is rejected
  at plan time (offset-based scanning cannot hop a nil); []byte is always
  cloned; the per-kind scan test matrix — fast stores and the reflect slow
  paths alike — runs under -race, which enables checkptr.
  Entity SELECTs render columns in plan order and verify the result set
  matches once per query; Raw always matches by column name.
- Entity CRUD ≤2 extra allocs per call over hand-written database/sql on the
  same driver — measured Find +1, Insert +0 (RETURNING) / +1 (exec),
  Update +2, Delete +1 — asserted with testing.AllocsPerRun
  (TestCRUDAllocBudget). Upsert adds its conflict-shape machinery on top:
  +5 (PostgreSQL) / +5 (MySQL), asserted at those budgets. ClickHouse rides
  the same paths (Find +1, Insert +1): its capability checks are early-exit
  branches, so the three older dialects' budgets did not move when it landed.
- Benchmarks in-repo against hand-written database/sql (fake driver in
  perf_test.go isolates rio's own overhead; bench/ adds real SQLite and a
  GORM comparison, the source of the README numbers). Honest methodology or
  no numbers.

## What rio deliberately does not have

Each of these is a decision, not a gap:

- **No model hooks/callbacks.** BeforeSave-style hooks are where invariants go
  to die (Rails' callback hell, reproduced by GORM). Side effects belong in
  visible application code; invariants belong in database constraints.
  Observability gets `QueryHook` (before/after, ctx-deriving, with
  op/model/duration/args — `WithoutArgs` for redaction) which cannot alter
  queries.
- **No implicit lazy loading.** The only source of invisible N+1. Unloaded
  relations fail loudly instead.
- **No dirty tracking / unit of work / identity map / flush.** Go has no
  property interception; snapshot diffing doubles memory and hides what a save
  will write (Hibernate/Doctrine's admitted black holes; Hibernate 7 promoted
  StatelessSession to first class). Explicit `Update` with optional column
  whitelist replaces all of it.
- **No struct-update-skips-zero-values.** GORM's most famous foot-gun
  (issue #6860). Full-column by default, whitelist when partial, `omitzero`
  when you want DB defaults. All three are visible at the call site.
- **No default scope of any kind** — soft delete is the single, explicitly
  tagged exception, and even it is a per-model declaration, never a global.
- **No AutoMigrate.** Half a migration tool is worse than none (schema drift,
  repeated ALTERs, silent data loss). Use a real versioned migration tool —
  rio pairs with `github.com/go-rio/migrate` (Go-code migrations, compiled
  into your binary, no dirty state).
- **No second-level cache.** Invalidation across nodes/out-of-band writes is
  an operations trap (Hibernate's own lesson; EF Core never shipped one).
  Caching belongs to the application.
- **No association auto-writes.** GORM's `Association.Replace` upserting rows
  behind your back (#3462) is the anti-pattern. Writing relations means
  visible inserts/deletes; the shipped helpers — Attach, Detach, SyncRelation
  — are exactly that: explicit join-table writes, never entity upserts.
- **No client-side evaluation, ever.** If rio can't compile it to SQL, it
  returns an error (EF Core 3.0's most expensive lesson: fail, don't degrade).
- **No expression-tree query language.** Go can't introspect closures; any
  simulation leaks. Type safety lives in generics (result types) and the
  optional column-constant generator (WriteColumns) — never required for full
  ergonomics (jOOQ's cliff is the counterexample).
- **No `.Select()` column pruning on entity queries.** Partial columns never
  produce entity values (Doctrine deleted partial objects for a reason, and
  Go's zero values make it worse); projections go through `Raw[T]` with any
  target shape.

## Testing & engineering

- Core: fake `database/sql` driver asserting exact SQL sequences, args, and
  transaction boundaries (the go-rio/migrate pattern) + golden SQL tests per
  dialect. Inflector goldens freeze before model.go exists.
- Differential fuzz on the rebind lexer (fast vs naive-correct, four dialect
  profiles × three bind styles, scalar and slice-expansion arguments), a
  per-kind scan-path test matrix (fast stores and the reflect slow paths),
  concurrent query derivation under -race, savepoint failure paths (PG
  aborted state; MySQL 1213 kills all savepoints — tolerate ROLLBACK TO
  failing via errors.Join).
- `integration/` sub-module drives real databases through the core only
  (modernc SQLite always — including a probe test pinning the driver's
  time/type behavior; PG/MySQL/ClickHouse gated by env DSNs, provided as CI
  services or local docker; the ClickHouse leg adds named probes pinning
  every clickhouse-go behavior the dialect design depends on). Driver
  modules carry their own small suites.
- Family conventions: MIT (TreeNewBee), zero-dependency core, go 1.25,
  functional options, CI matrix (oldstable/stable × linux/macos/windows),
  never move a published tag.

## Shipped scope

Core + four drivers: full mapping/tags, From/Find/First/Sole/Count/Exists,
immutable connection-free builder (Where/OrderBy/Limit/Offset/GroupBy/Having/
Join-for-filtering/ForUpdate), Insert/InsertAll/Update/Delete/Upsert/UpsertAll,
set-based UpdateAll/DeleteAll with rio.Expr, FirstOrCreate/CreateOrFirst
(race-honest, documented), four relation kinds with nested preloading,
per-parent preload limits (RelLimit, window functions), WithCount aggregate
preloads, explicit relation write helpers (Attach/Detach/SyncRelation),
optimistic locking, soft delete, timestamps, Raw[T]/Exec with strict
column-completeness checks for Raw-into-entity, MustCompile/Compile,
column-constant generation (WriteColumns), opt-in stmt cache, transactions
with savepoints, QueryHook, error translation, composite PKs. ClickHouse
speaks the read + append subset of all of this plus `Query.Final()` (see the
dialect section above).

Not shipped yet: cursor pagination (the README documents the keyset WHERE
pattern instead of an API), schema-drift lint.
