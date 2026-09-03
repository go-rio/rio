# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) with 0.x semantics: minor versions may break the API.

## [Unreleased]

## [0.18.1] - 2026-09-02

### Fixed

- Under `WithoutStamps`, a full-column `Update` and the conflict branch of `Upsert` dropped the `UpdatedAt` assignment, so the row kept whatever stale value the database held — neither the caller's nor a fresh one, and at odds with the insert branch of the same `Upsert`. Both now bind the struct's value. Statements rio composes without a caller row (a column-list `Update`, `UpdateAll`, `Delete`, `Restore`) still drop it.

## [0.18.0] - 2026-09-02

### Added

- `DB.WithoutStamps` and `Tx.WithoutStamps`: handles that generate no `CreatedAt`/`UpdatedAt` value. Statements carrying the caller's row bind the struct's values as they are; the ones rio composes itself drop the assignment. Versions and softdelete stamps are unaffected, and the handle shares its parent's pool and caches.

## [0.17.0] - 2026-09-02

### Added

- `NativeLastInserter`, an optional native-channel capability: `ExecLastInsert` also reports the last inserted row id, which the SQLite dialect uses to backfill a lone auto-increment key. rio prefers it over `Exec`; the sqlite module's native channel implements it.

### Changed

- The `rio.WithStmtCache` panic on the native channel names both driver modules' caching (pgx's `default_query_exec_mode`, sqlite's per-connection cache).

## [0.16.1] - 2026-09-02

### Added

- `CONTRIBUTING.md`, `CHANGELOG.md`, `llms.txt`, and compile-only examples for the query builder, writes, relations, cursor paging, `Chunk`, `Sub`, and transactions.

### Changed

- README restructured (summary, demo, getting started, features by area, contributing, license); every v0.16.0 addition is documented.
- Source files follow one declaration order (constants, types with their constructors and methods, helpers); every exported identifier carries a doc comment stating its contract. No behavior or API change; allocation counts are unchanged.

### Fixed

- README no longer claims `OnConflict` columns are validated against the model; they are quoted as identifiers, while `DoUpdate` and `DoUpdateSet` columns are validated.

## [0.16.0] - 2026-09-02

### Added

- `ForShare`, and the `LockOption` values `NoWait` and `SkipLocked` for `ForUpdate`/`ForShare`; ClickHouse rejects every row lock with one message.
- `Distinct`: entity rows deduplicate, as do `Pluck` values; `Count` counts distinct primary keys; `Sum` and `Avg` aggregate distinct values.
- `Sum`, `Min`, `Max`, and `Avg` — generic terminals over a mapped column, returning the zero value over no rows.
- `rio:",readonly"`: a database-computed column is scanned and loaded back by `RETURNING` after `Insert`/`Upsert`, never written; `Update`, `UpdateAll`, `DoUpdate`, and `DoUpdateSet` reject it by name.
- `Before(cursor)` selects the page ending at a cursor; `CursorAt` issues the cursor for either edge of a page.
- `Chunk(ctx, db, size, args...)` walks a result in keyset pages of a fixed size (`iter.Seq2[[]T, error]`), defaulting to primary-key order and releasing the connection between pages.
- `UpdateAllReturning` and `DeleteAllReturning` scan the affected rows back on dialects with `RETURNING`.
- `DoUpdateSet(rio.Set{...})` assigns conflict columns: an `Expr` renders verbatim, any other value binds after the row values.
- `Query.Find(ctx, db, key...)` looks a row up by primary key under the query's clauses (`WithTrashed`, `With`, `WithCount`, inline `Where`), cached by `Must`.
- `Query.Sub(column)` embeds a one-column query as a `?` argument in `Where`, `Having`, `RelWhere`, `Raw`, and `Exec` on every dialect.
- `Query.SQL(db, args...)` renders the statement `All` would run, without executing it.
- `WithCount(relation, opts...)` takes `RelWhere` and `RelWithTrashed`; a filtered count never reuses a preload.
- `WithoutStmtCache()` opts out of a driver module's default prepared-statement cache.
- `ClearRelation` unlinks every row of a `ManyToMany` relation.

### Changed

- **Breaking:** `RelOrder` is `RelOrderBy`.
- **Breaking:** `CursorAfter` is `CursorAt`.
- **Breaking:** `SyncRelation` takes ids variadically (`ids ...K`); no ids clears the relation.
- **Breaking:** the ClickHouse dialect binds `uint64` values above `MaxInt64` as-is instead of as decimal text.
- Preloads regroup through the typed container into one slab with a capped sub-slice per owner. On PostgreSQL, preload key sets bind as one array parameter (`= ANY(?)`), so the statement text no longer varies with the key count and bind-limit chunking disappears there. PreloadHasMany 357 → 60 allocs/op, PreloadNested 1904 → 354, PreloadManyToMany 367 → 71, WithCount 149 → 49.
- Relation options are evaluated when `With`, `WhereHas`, and `WithCount` are called: repeated requests for one relation merge, `Must` caches shapes that carry them, and `WhereHas` leaf arguments bind inline after `Where`.
- `[]any`, `[]int64`, `[]int`, `[]uint64`, and `[]string` arguments expand in `IN (?)` without reflection.

## [0.15.0] - 2026-09-01

### Added

- `rio.TableName(structName)` exports the struct-to-table derivation (`User` → `users`, `APIKey` → `api_keys`, `Person` → `people`) so code generators agree with rio at runtime.

## [0.14.0] - 2026-09-01

### Fixed

- ClickHouse: time arguments bind as `time.Time` and the go-rio/clickhouse driver renders them as epoch-microsecond expressions, so comparisons against a sorting-key time column (`created_at >= ?`) no longer fail with `TYPE_MISMATCH`.

## [0.13.3] - 2026-09-01

### Changed

- Relation-count queries scan through rio's typed cells, so a native driver serves `WithCount` through its one scan path with no plain-pointer destination to special-case.

## [0.13.2] - 2026-08-31

### Changed

- No user-visible changes: the preload pipeline is reorganized by layer.

## [0.13.1] - 2026-08-31

### Fixed

- Preload and `WithCount` keys whose type implements `driver.Valuer` bind their original value again, so `Value()` runs and the `IN` list matches the stored form.
- A named `uint64` key with the high bit set binds; a NULL `GROUP BY` key row no longer credits parents with a NULL foreign key.
- The unknown-result-column error says how to fix it (a rio tag or an SQL alias).

### Changed

- Relation grouping runs in typed key spaces (`int64`, `uint64`, `string`, `any`): 15–19% less time per preload benchmark, and keys past the runtime's small-integer cache cost the same as small ones (PreloadHasMany 957 → 357 allocs/op).

## [0.13.0] - 2026-08-31

### Added

- Native SPI capabilities discovered by type assertion: `NativeBatcher` (`QueryBatch`) runs a relation layer's statements in one round trip, and `NativeCopier` (`CopyIn`) streams an explicit-key `InsertAll` through the driver's bulk-load protocol; `BatchStatement` and `NativeBatchResults` carry the batch. Hooks keep one event per logical statement; the copy path reports `Op: "copy"`.
- `ColumnSchema.Scanner` marks columns delegated to a custom `sql.Scanner`; `lint` treats them as undecidable instead of reporting a type mismatch.

### Changed

- **Breaking:** `DB.Dialect()` returns the opaque `Dialect` value and replaces `DB.DialectName()`; `lint` dispatches on `rio.Postgres`, `rio.MySQL`, and `rio.SQLite`.
- **Breaking:** `TableSchema.PKs` is removed; derive the key from `Columns`.
- `NewNative` honors `WithDriverHandle` like any later option.
- Preloading allocates 56–74% less per relation benchmark and runs 15–19% faster; on real SQLite, `ReadHundredWithPosts` drops from 2816 to 1818 allocs/op.
- Cursor checks (zero cursor, ordering fingerprint, value count) run in `Validate`.

### Fixed

- A paginated `Pluck` carried the keyset predicate but lost its `ORDER BY`; it now pages through the same machinery as entity queries.

## [0.12.0] - 2026-08-30

### Added

- Cursor pagination, the `lint` schema-drift subpackage, and driver handles; the `Must` render cache is bounded, relation benchmarks and hook observability are added.

## [0.11.0] - 2026-08-30

### Fixed

- Write-semantics hardening; lexer and cache fixes.

## [0.10.1] - 2026-08-20

### Changed

- Go 1.27 GA and ClickHouse 26.7; bench and integration track the released driver modules.

## [0.10.0] - 2026-08-09

### Changed

- Generic methods, deferred query arguments, Go 1.27.

## [0.9.0] - 2026-07-11

### Changed

- Pre-1.0 hardening batch; the benchmark GORM comparison runs on the modernc SQLite driver.

## [0.8.0] - 2026-07-11

### Added

- Native execution SPI; the pgx-native PostgreSQL channel lands in go-rio/postgres.

## [0.7.2] - 2026-07-10

### Changed

- ClickHouse floors at server 26: plain time binds, insert +1 alloc.

## [0.7.1] - 2026-07-10

### Fixed

- ClickHouse time arguments inline as `parseDateTime64BestEffort`, so comparisons work on 25.8 LTS.

## [0.7.0] - 2026-07-10

### Added

- ClickHouse dialect (the read-and-append OLAP subset with `Final()`); savepoint, `Close`-error, and `Valuer` correctness fixes.

## [0.6.0] - 2026-07-10

### Changed

- Profile-driven hot-path work: chunked pointer cells, pooled row scanners, +0-alloc `Insert`; real PostgreSQL and MySQL benchmarks and driver guidance.

## [0.5.0] - 2026-07-10

### Fixed

- Pre-release audit hardening: 38 findings fixed across plan guards, write-path symmetry, relations, compile checks, and hot paths.

## [0.4.0] - 2026-07-09

### Fixed

- Four-round audit hardening.

## [0.3.0] - 2026-07-09

### Added

- Column constants, relation sync, scopes, streaming.

## [0.2.0] - 2026-07-09

### Added

- Relations that filter, count, and stream.

## [0.1.1] - 2026-07-09

### Fixed

- Adversarial-review hardening.

## [0.1.0] - 2026-07-09

### Added

- Initial release.

[Unreleased]: https://github.com/go-rio/rio/compare/v0.18.1...HEAD
[0.18.1]: https://github.com/go-rio/rio/compare/v0.18.0...v0.18.1
[0.18.0]: https://github.com/go-rio/rio/compare/v0.17.0...v0.18.0
[0.17.0]: https://github.com/go-rio/rio/compare/v0.16.1...v0.17.0
[0.16.1]: https://github.com/go-rio/rio/compare/v0.16.0...v0.16.1
[0.16.0]: https://github.com/go-rio/rio/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/go-rio/rio/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/go-rio/rio/compare/v0.13.3...v0.14.0
[0.13.3]: https://github.com/go-rio/rio/compare/v0.13.2...v0.13.3
[0.13.2]: https://github.com/go-rio/rio/compare/v0.13.1...v0.13.2
[0.13.1]: https://github.com/go-rio/rio/compare/v0.13.0...v0.13.1
[0.13.0]: https://github.com/go-rio/rio/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/go-rio/rio/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/go-rio/rio/compare/v0.10.1...v0.11.0
[0.10.1]: https://github.com/go-rio/rio/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/go-rio/rio/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/go-rio/rio/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/go-rio/rio/compare/v0.7.2...v0.8.0
[0.7.2]: https://github.com/go-rio/rio/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/go-rio/rio/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/go-rio/rio/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/go-rio/rio/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/go-rio/rio/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/go-rio/rio/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/go-rio/rio/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/go-rio/rio/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/go-rio/rio/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/go-rio/rio/releases/tag/v0.1.0
