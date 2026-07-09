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
  (`github.com/go-rio/sqlite|mysql|postgres`) and carry the real driver deps.

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
   interface built into the core (`rio.Postgres`, `rio.MySQL`, `rio.SQLite`);
   capability flags (returning, conflict target, max bind params, FOR UPDATE)
   instead of type switches. Cross-module dialect implementations would freeze
   the interface — the same argument, validated repeatedly, in go-rio/migrate.
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
err = rio.Insert(ctx, db, &user)                     // fills ID (+ full row via RETURNING on PG/SQLite)
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
  default apply. Auto-increment PKs get this implicitly. The default for every
  other column is to write the zero value — Go zero values are values
  (go-pg's `NULL`-by-default is the canonical counterexample).
- Composite primary keys: tag multiple fields `rio:",pk"`. `Find` takes all
  parts in field-declaration order. Models without a PK return
  `ErrNoPrimaryKey` from Find/Update/Delete.
- Structs containing relation containers are not comparable (they hold
  slices); use cmp.Diff in tests.

### Relations are containers, not slices

`rio.HasMany[T]` / `HasOne[T]` / `BelongsTo[T]` / `ManyToMany[T]` know whether
they were loaded. Accessing an unloaded relation panics with instructions
(add `.With("Posts")`) instead of silently returning empty data. This is
Ecto's `NotLoaded` + Rails/Laravel strict-loading — built in, not bolted on.
`Loaded()` reports the state; JSON marshalling emits `null` when unloaded.
`Set` is exported — manual assembly is a legitimate use. Implicit lazy loading
does not exist: rio never issues a query you didn't ask for. Foreign keys
resolve by convention (`Post.UserID` ↔ `users.id`), tags override (`fk:`,
`ref:`, `join:`); resolution is lazy (first preload) to allow mutually
referencing models.

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
| Unique violation | `rio.ErrDuplicateKey` (translated by driver modules, driver error stays in chain) |
| FK violation | `rio.ErrForeignKeyViolated` |
| NULL into non-pointer field | error naming the column — sole exception: the `softdelete` column reads NULL as zero time |
| MySQL insert | fills auto-increment ID only; rio never issues a hidden second SELECT (`SupportsReturning` capability, Ecto's honest route) |
| Batch backfill | InsertAll backfills auto-inc PKs only (PG by position; SQLite sorted-by-PK since RETURNING order is documented as undefined; MySQL none — interleaved autoinc); UpsertAll never backfills (DoNothing shrinks the row set) |
| Soft-deleted model queries | filtered by default *because the tag is explicit*; `WithTrashed()` / `OnlyTrashed()`; `Delete` becomes UPDATE, `ForceDelete` is real |
| Upsert on a soft-deleted row | **invariant: a successful Upsert leaves the row visible** — DoUpdate automatically sets `deleted_at = NULL` (+updated_at); `rio.KeepTrashed()` opts out; DoNothing never revives |
| `First` ordering | no implicit ORDER BY — LIMIT 1 over whatever order the DB returns; add OrderBy for determinism (SQL correspondence) |
| Placeholders | always `?`, rebound per dialect with a per-dialect lexer; `??` escapes a literal `?` (PostgreSQL JSONB operators); `IN (?)` expands slices — sqlx/Bun's established conventions |
| Scan priority | `rio:"-"` → `json` tag (beats Scanner, documented) → `sql.Scanner` (NULL handed to Scan(nil), no second-guessing) → pointer fields (NULL→nil) → `[]byte` (NULL→nil) → basic conversions (overflow-checked; MySQL unsigned BIGINT > MaxInt64 arrives as bytes and is parsed) → NULL into anything else errors with the column name |
| Times | written as UTC, monotonic-stripped, truncated to microseconds (PG/MySQL precision — otherwise reload-and-Equal never holds); SQLite text format is rio's own, not the driver's |
| Partial scans | `Raw[User]` filling half an entity then `rio.Update` writes zero values to the unscanned columns — documented loudly (mirror image of GORM #6860) |

## Performance

Target, set by the user: **fastest full-featured Go ORM; at minimum, beat
GORM everywhere by ≥25%** (research baseline, reads: raw/sqlc ~148k ns/op <
Bun/ent ~176k < GORM 223k < sqlx 319k). The gap is never reflection itself —
it is missing plan caches and sloppy string assembly.

- Per-type plans cached forever; per-grammar SQL caches (see pipeline).
- Rendering appends to `[]byte` (no fmt in hot paths).
- Scanning appends a zero value and scans into `&s[len(s)-1]` in place;
  dest slices and null-scanners are allocated once per query, reused per row.
- **Unsafe fast path**: plans record field offsets; fixed-layout kinds
  (ints/uints/floats/bool/string/[]byte/time.Time) are written via
  `unsafe.Add` directly, skipping reflect.Value.Set (Bun/sonic-class
  technique). Discipline: no uintptr variables; embedded *pointer* structs
  take the slow path; []byte is always cloned; fast and slow paths are
  fuzz-compared for equality and the whole matrix runs under -race/checkptr.
  Entity SELECTs render columns in plan order and verify the result set
  matches once per query; Raw always matches by column name.
- Entity CRUD ≤4 extra allocs per call, asserted with testing.AllocsPerRun.
- Benchmarks in-repo against hand-written database/sql (fake driver + real
  SQLite), plus a local reproduction of efectn/go-orm-benchmarks for the
  README. Honest methodology or no numbers.

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
  visible inserts/deletes; helpers may come later, and they will be explicit.
- **No client-side evaluation, ever.** If rio can't compile it to SQL, it
  returns an error (EF Core 3.0's most expensive lesson: fail, don't degrade).
- **No expression-tree query language.** Go can't introspect closures; any
  simulation leaks. Type safety lives in generics (result types) and, later,
  an optional column-constant generator — never required for full ergonomics
  (jOOQ's cliff is the counterexample).
- **No `.Select()` column pruning on entity queries.** Partial columns never
  produce entity values (Doctrine deleted partial objects for a reason, and
  Go's zero values make it worse); projections go through `Raw[T]` with any
  target shape.

## Testing & engineering

- Core: fake `database/sql` driver asserting exact SQL sequences, args, and
  transaction boundaries (the go-rio/migrate pattern) + golden SQL tests per
  dialect. Inflector goldens freeze before model.go exists.
- Differential fuzz on the rebind lexer (fast vs naive-correct, three dialect
  profiles), fast/slow scan-path comparison fuzz, concurrent query derivation
  under -race, savepoint failure paths (PG aborted state; MySQL 1213 kills
  all savepoints — tolerate ROLLBACK TO failing via errors.Join).
- `integration/` sub-module drives real databases through the core only
  (modernc SQLite always — including a probe test pinning the driver's
  time/type behavior; PG/MySQL gated by env DSNs, provided as CI services).
  Driver modules carry their own small suites.
- Family conventions: MIT (TreeNewBee), zero-dependency core, go 1.25,
  functional options, CI matrix (oldstable/stable × linux/macos/windows),
  never move a published tag.

## Phase 1 scope (v0.1.0)

Core + three drivers: full mapping/tags, From/Find/First/Sole/Count/Exists,
immutable connection-free builder (Where/OrderBy/Limit/Offset/GroupBy/Having/
Join-for-filtering/ForUpdate), Insert/InsertAll/Update/Delete/Upsert/UpsertAll,
set-based UpdateAll/DeleteAll with rio.Expr, FirstOrCreate/CreateOrFirst
(race-honest, documented), four relation kinds with nested preloading,
optimistic locking, soft delete, timestamps, Raw[T]/Exec, MustCompile/Compile,
opt-in stmt cache, transactions with savepoints, QueryHook, error translation,
composite PKs.

Later phases (not v0.1): per-parent preload limits (window functions),
WithCount aggregate preloads, cursor pagination, explicit relation write
helpers (Attach/Detach), optional column-constant codegen, schema-drift lint,
strict column-completeness checks for Raw-into-entity.
