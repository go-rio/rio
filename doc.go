// Package rio is a zero-surprise ORM: GORM's ergonomics, sqlc's honesty,
// type safety through generics, and the weight of none of them.
//
// Structs are pure data and every database operation is a visible call —
// there is no user.Save(), no session, no change tracking, and rio never
// issues a query you didn't ask for. Queries are immutable, connection-free
// builder values; nothing touches the database until an execution method
// takes the (ctx, db) pair:
//
//	users, err := rio.From[User]().
//		Where("age > ?", 18).
//		OrderBy("created_at DESC").
//		Limit(10).
//		With("Posts").
//		All(ctx, db)
//
// db is any rio.Queryer — the *rio.DB returned by a driver module
// (go-rio/postgres, go-rio/mysql, go-rio/sqlite) or by rio.New, and equally
// the *rio.Tx inside DB.Tx, so data-access code runs unchanged in and out of
// transactions (nested Tx calls become savepoints). Placeholders are always
// ?, rebound per dialect; IN (?) expands slices. On PostgreSQL the driver
// module offers three execution tiers behind this same API — database/sql
// (Open), pgxpool with database/sql queries (OpenPool), and fully pgx-native
// (OpenNative, the fastest read path); see go-rio/postgres for the table.
//
// Reading starts at From, Find, and the builder's All, First, Sole, Count,
// and Exists. Writes are immediate and explicit: Insert, InsertAll, Update
// (full-column by default, whitelist when partial), Delete, Upsert, and the
// set-based UpdateAll/DeleteAll, which refuse to run without a WHERE.
// Relations declared as typed containers (HasMany, HasOne, BelongsTo,
// ManyToMany) load only on request — With preloads, WithCount aggregates,
// Attach/Detach/SyncRelation write join rows — and panic loudly when
// accessed unloaded instead of returning silently empty data.
//
// Escape hatches and hot paths: Raw scans any SQL into any shape with the
// same scanner and transactions, Exec runs bare statements, MustCompile
// renders a fixed-shape query once per dialect, and WithStmtCache opts into
// prepared-statement reuse. QueryHook observes every statement (it cannot
// rewrite them); sentinel errors (ErrNotFound, ErrDuplicateKey, …) answer
// errors.Is with the driver's error kept in the chain.
//
// Struct tags map fields and assign column roles (rio:"col,opt,…"):
//
//   - "col_name": rename the column (default snake_case, UserID → user_id).
//   - ",pk": primary key; tag several fields for a composite key.
//   - ",noautoincr": a single integer primary key rio must not auto-increment.
//   - ",version": optimistic-lock counter (integer, int64 recommended); a lost race returns ErrStaleObject.
//   - ",softdelete": on time.Time/*time.Time, Delete becomes a timestamp UPDATE and default reads filter the row out.
//   - ",json": store the field as JSON.
//   - ",countof:Rel": int64 target that WithCount("Rel") fills with a HasMany/ManyToMany row count.
//   - ",omitzero": skip the column while the field is zero so the database default applies.
//   - ",nostamp": opt a time field out of CreatedAt/UpdatedAt maintenance.
//   - ",fk:col" / ",ref:col" / ",join:table": relation-container overrides — foreign key, referenced key, join table.
//   - "-": not a column.
//
// Conventions need no tag: an ID field is the primary key and, as a single
// integer, auto-increments — a rename/omitzero/noautoincr keeps that, an
// explicit pk anywhere in the model opts out; CreatedAt and UpdatedAt
// (time.Time or *time.Time) are stamped on write; a TableName() string method
// overrides the pluralized table name (User → users, Person → people).
//
// The full design rationale — including the list of features rio refuses to
// have — lives in DESIGN.md; schema migrations live in go-rio/migrate.
package rio
