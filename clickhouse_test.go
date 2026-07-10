package rio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// This file freezes the ClickHouse dialect's public behavior: the supported
// surface byte for byte, and the rejected surface message for message with
// proof that no SQL was sent. The three older dialects' goldens live in their
// existing tests and must never move because of anything here.

// chTS is testNow under chTimeFormat — what every stamped column binds.
const chTS = "2026-07-09 12:00:00.000000+00:00"

// --- supported surface: golden SQL ---

func TestClickHouseSelectGolden(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows(userCols, userRow(1, "a@x"))

	users, err := From[User]().Where("age > ?", 18).OrderBy("created_at DESC").Limit(10).All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(users) != 1 || users[0].Email != "a@x" {
		t.Fatalf("scan: %+v", users)
	}
	want := "SELECT `users`.`id`, `users`.`email`, `users`.`age`, `users`.`bio`, `users`.`version`, `users`.`deleted_at`, `users`.`created_at`, `users`.`updated_at` FROM `users` WHERE (age > ?) AND `users`.`deleted_at` IS NULL ORDER BY created_at DESC LIMIT 10"
	if got := f.logged()[0]; got != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", got, want)
	}
}

func TestClickHouseFindGolden(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows(userCols, userRow(42, "found@x"))

	u, err := Find[User](ctx, db, 42)
	if err != nil || u.ID != 42 {
		t.Fatalf("Find: %v %+v", err, u)
	}
	want := "SELECT `users`.`id`, `users`.`email`, `users`.`age`, `users`.`bio`, `users`.`version`, `users`.`deleted_at`, `users`.`created_at`, `users`.`updated_at` FROM `users` WHERE `users`.`id` = ? AND `users`.`deleted_at` IS NULL LIMIT 1"
	if got := f.logged()[0]; got != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", got, want)
	}
}

// A bare OFFSET is native ClickHouse (like PostgreSQL): no LIMIT synthesized.
func TestClickHouseBareOffset(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows(orgCols)
	_, _ = From[Org]().Offset(5).All(ctx, db)
	if got := f.logged()[0]; !strings.HasSuffix(got, "FROM `orgs` OFFSET 5") {
		t.Fatalf("bare OFFSET must not synthesize a LIMIT: %s", got)
	}
}

// Insert on ClickHouse: every column binds (the explicit ID included), no
// RETURNING, and nothing backfills — the fake's LastInsertId of 1 must not
// leak into the struct.
func TestClickHouseInsertGolden(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueExec(1, 1) // a LastInsertId the driver could never really report

	u := &User{ID: 7, Email: "a@x", Age: 30}
	if err := Insert(ctx, db, u); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	stmt := f.loggedContaining("INSERT")[0]
	want := "INSERT INTO `users` (`id`, `email`, `age`, `bio`, `version`, `deleted_at`, `created_at`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	if stmt.sql != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", stmt.sql, want)
	}
	if u.ID != 7 {
		t.Fatalf("nothing may backfill on clickhouse: %d", u.ID)
	}
	if u.Version != 1 || !u.CreatedAt.Equal(normalizeTime(testNow)) {
		t.Fatalf("client-side stamps still apply: %+v", u)
	}
	// Time columns bind rio's fixed-format text, offset included.
	if stmt.args[6] != chTS || stmt.args[7] != chTS {
		t.Fatalf("time args must bind chTimeFormat text: %#v", stmt.args)
	}
}

func TestClickHouseInsertAllGolden(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueExec(0, 0)

	rows := []Org{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}}
	if err := InsertAll(ctx, db, rows); err != nil {
		t.Fatalf("InsertAll: %v", err)
	}
	want := "INSERT INTO `orgs` (`id`, `name`) VALUES (?, ?), (?, ?)"
	if got := f.logged()[0]; got != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", got, want)
	}
	if rows[0].ID != 1 || rows[1].ID != 2 {
		t.Fatalf("no backfill may touch explicit ids: %+v", rows)
	}
}

// maxBindParams is 8192 on ClickHouse: a 1025-row insert of the 8-column User
// (8192/8 = 1024 rows per statement) must split into exactly two statements.
func TestClickHouseInsertAllChunksAt8192(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)

	rows := make([]User, 1025)
	for i := range rows {
		rows[i] = User{ID: int64(i + 1), Email: "u@x"}
	}
	if err := InsertAll(ctx, db, rows); err != nil {
		t.Fatalf("InsertAll: %v", err)
	}
	stmts := f.loggedContaining("INSERT")
	if len(stmts) != 2 {
		t.Fatalf("1025 rows × 8 cols must chunk into 2 statements, got %d", len(stmts))
	}
	if len(stmts[0].args) != 1024*8 || len(stmts[1].args) != 1*8 {
		t.Fatalf("chunk sizes: %d, %d", len(stmts[0].args), len(stmts[1].args))
	}
}

// The noautoincr escape hatch keeps working: zero is a real value there, and
// the zero-ID rejection must not fire.
func TestClickHouseNoAutoIncrZeroIDInserts(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueExec(0, 0)

	type Ledger struct {
		ID   int64 `rio:"id,pk,noautoincr"`
		Note string
	}
	if err := Insert(ctx, db, &Ledger{Note: "zero id is real"}); err != nil {
		t.Fatalf("noautoincr zero id must insert: %v", err)
	}
	stmt := f.loggedContaining("INSERT")[0]
	if !strings.Contains(stmt.sql, "(`id`, `note`)") || stmt.args[0] != int64(0) {
		t.Fatalf("zero id must be written as a real value: %s %v", stmt.sql, stmt.args)
	}
}

func TestClickHousePreloadGolden(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows(userCols, userRow(1, "a@x"))
	f.queueRows(postCols, []driver.Value{int64(10), int64(1), "first"})

	users, err := From[User]().With("Posts").All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(users[0].Posts.Rows()) != 1 {
		t.Fatalf("preload rows: %+v", users)
	}
	want := "SELECT `posts`.`id`, `posts`.`user_id`, `posts`.`title` FROM `posts` WHERE `posts`.`user_id` IN (?)"
	if got := f.logged()[1]; got != want {
		t.Fatalf("preload sql:\n got: %s\nwant: %s", got, want)
	}
}

func TestClickHouseManyToManyPreloadGolden(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows([]string{"id", "org_id"}, []driver.Value{int64(1), nil})
	f.queueRows([]string{"id", "name", "__rio_key"}, []driver.Value{int64(100), "go", int64(1)})

	accounts, err := From[Account]().With("Tags").All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if got := accounts[0].Tags.Rows(); len(got) != 1 || got[0].Name != "go" {
		t.Fatalf("tags: %+v", got)
	}
	rel := f.logged()[1]
	want := "SELECT `tags`.`id`, `tags`.`name`, `account_tags`.`account_id` FROM `tags` INNER JOIN `account_tags` ON `account_tags`.`tag_id` = `tags`.`id` WHERE `account_tags`.`account_id` IN (?)"
	if rel != want {
		t.Fatalf("m2m sql:\n got: %s\nwant: %s", rel, want)
	}
}

func TestClickHouseWithCountGolden(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	f.queueRows([]string{"board_id", "count"}, []driver.Value{int64(1), int64(3)})

	boards, err := From[Board]().WithCount("Posts").All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if boards[0].PostsCount != 3 {
		t.Fatalf("counts: %+v", boards)
	}
	want := "SELECT `board_posts`.`board_id`, count(*) FROM `board_posts` WHERE `board_posts`.`board_id` IN (?) GROUP BY `board_posts`.`board_id`"
	if got := f.logged()[1]; got != want {
		t.Fatalf("count sql:\n got: %s\nwant: %s", got, want)
	}
}

func TestClickHouseRelLimitWindowGolden(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows(userCols, userRow(1, "a@x"))
	f.queueRows(postCols, []driver.Value{int64(10), int64(1), "first"})

	_, err := From[User]().With("Posts", RelOrder("id DESC"), RelLimit(1)).All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	rel := f.logged()[1]
	for _, frag := range []string{
		"SELECT `id`, `user_id`, `title` FROM (SELECT `posts`.`id`, `posts`.`user_id`, `posts`.`title`, ROW_NUMBER() OVER (PARTITION BY `posts`.`user_id` ORDER BY id DESC) AS `__rio_rn` FROM `posts` WHERE `posts`.`user_id` IN (?)",
		") AS `rio_w` WHERE `rio_w`.`__rio_rn` <= 1",
	} {
		if !strings.Contains(rel, frag) {
			t.Fatalf("missing %q in:\n%s", frag, rel)
		}
	}
}

func TestClickHouseWhereHasGolden(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows(userCols)

	_, err := From[User]().WhereHas("Posts", RelWhere("title <> ?", "")).All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	want := "EXISTS (SELECT 1 FROM `posts` AS `rio_h1` WHERE `rio_h1`.`user_id` = `users`.`id` AND (title <> ?))"
	if got := f.logged()[0]; !strings.Contains(got, want) {
		t.Fatalf("sql:\n got: %s\nwant fragment: %s", got, want)
	}
}

// --- Final(): the one new API ---

func TestClickHouseFinalGolden(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)

	f.queueRows(userCols)
	if _, err := From[User]().Final().Where("age > ?", 1).All(ctx, db); err != nil {
		t.Fatalf("All: %v", err)
	}
	f.queueRows([]string{"count"}, []driver.Value{int64(0)})
	if _, err := From[User]().Final().Count(ctx, db); err != nil {
		t.Fatalf("Count: %v", err)
	}
	f.queueRows([]string{"1"})
	if _, err := From[User]().Final().Exists(ctx, db); err != nil {
		t.Fatalf("Exists: %v", err)
	}
	f.queueRows([]string{"email"})
	if _, err := Pluck[string](ctx, db, From[User]().Final(), "email"); err != nil {
		t.Fatalf("Pluck: %v", err)
	}

	logs := f.logged()
	for i, want := range []string{
		"SELECT `users`.`id`, `users`.`email`, `users`.`age`, `users`.`bio`, `users`.`version`, `users`.`deleted_at`, `users`.`created_at`, `users`.`updated_at` FROM `users` FINAL WHERE (age > ?) AND `users`.`deleted_at` IS NULL",
		"SELECT count(*) FROM `users` FINAL WHERE `users`.`deleted_at` IS NULL",
		"SELECT 1 FROM `users` FINAL WHERE `users`.`deleted_at` IS NULL LIMIT 1",
		"SELECT `users`.`email` FROM `users` FINAL WHERE `users`.`deleted_at` IS NULL",
	} {
		if logs[i] != want {
			t.Fatalf("shape %d:\n got: %s\nwant: %s", i, logs[i], want)
		}
	}
}

// Final does not propagate into preload statements: those are independent
// SELECTs with no propagation rule worth guessing (RelFinal is a deliberate
// non-feature for now).
func TestClickHouseFinalDoesNotPropagateToPreload(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows(userCols, userRow(1, "a@x"))
	f.queueRows(postCols)

	if _, err := From[User]().Final().With("Posts").All(ctx, db); err != nil {
		t.Fatalf("All: %v", err)
	}
	if rel := f.logged()[1]; strings.Contains(rel, "FINAL") {
		t.Fatalf("preload must not inherit FINAL: %s", rel)
	}
}

func TestClickHouseFinalCompiled(t *testing.T) {
	ctx := context.Background()
	q := MustCompile[User](From[User]().Final().Where("age > ?"))

	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows(userCols)
	if _, err := q.All(ctx, db, 1); err != nil {
		t.Fatalf("compiled All on clickhouse: %v", err)
	}
	if got := f.logged()[0]; !strings.Contains(got, "FROM `users` FINAL WHERE") {
		t.Fatalf("compiled exec-mode must render FINAL: %s", got)
	}

	// The same compiled value renders per grammar: on postgres it errors at
	// first use, consistent with "MustCompile passing ≠ execution cannot fail".
	fpg := newFakeDB()
	pg := fpg.open(Postgres)
	_, err := q.All(ctx, pg, 1)
	requireRejected(t, fpg, err, "rio: Final() requires a dialect with the FINAL table modifier (clickhouse); remove it on postgres")
}

func TestFinalRejectedPerDialect(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		d    Dialect
		want string
	}{
		{Postgres, "rio: Final() requires a dialect with the FINAL table modifier (clickhouse); remove it on postgres"},
		{MySQL, "rio: Final() requires a dialect with the FINAL table modifier (clickhouse); remove it on mysql"},
		{SQLite, "rio: Final() requires a dialect with the FINAL table modifier (clickhouse); remove it on sqlite"},
	} {
		f := newFakeDB()
		db := f.open(tc.d)
		_, err := From[User]().Final().All(ctx, db)
		requireRejected(t, f, err, tc.want)
		_, err = Pluck[string](ctx, db, From[User]().Final(), "email")
		requireRejected(t, f, err, tc.want)
	}
}

// --- rejected surface: exact messages, zero SQL sent ---

// requireRejected asserts the exact rejection text and that nothing reached
// the driver — the rejection layer must sit before any SQL is sent.
func requireRejected(t *testing.T, f *fakeDB, err error, want string) {
	t.Helper()
	if err == nil || err.Error() != want {
		t.Fatalf("error:\n got: %v\nwant: %s", err, want)
	}
	if logs := f.logged(); len(logs) != 0 {
		t.Fatalf("no SQL may be sent on a rejected call, got %v", logs)
	}
}

func TestClickHouseRejectionMatrix(t *testing.T) {
	ctx := context.Background()
	u := &User{ID: 5, Email: "a@x", Version: 1}
	acc := &Account{ID: 7}

	for name, tc := range map[string]struct {
		call func(db *DB) error
		want string
	}{
		"Tx": {
			func(db *DB) error { return db.Tx(ctx, func(tx *Tx) error { return nil }) },
			"rio: transactions are not supported on clickhouse (the driver's Begin is a no-op and statements would commit independently); group rows into one InsertAll for per-statement atomicity, or use db.Unwrap() with clickhouse-go's native batch API",
		},
		"TxWith": {
			func(db *DB) error { return db.TxWith(ctx, &sql.TxOptions{}, func(tx *Tx) error { return nil }) },
			"rio: transactions are not supported on clickhouse (the driver's Begin is a no-op and statements would commit independently); group rows into one InsertAll for per-statement atomicity, or use db.Unwrap() with clickhouse-go's native batch API",
		},
		"Update": {
			func(db *DB) error { return Update(ctx, db, u) },
			`rio: Update is not supported on clickhouse (no synchronous UPDATE with an affected-row count); ClickHouse updates are asynchronous mutations — issue one explicitly with rio.Exec(ctx, db, "ALTER TABLE users UPDATE ... WHERE ...") or model updates as inserts into a ReplacingMergeTree table`,
		},
		"Delete": {
			func(db *DB) error { return Delete(ctx, db, u) },
			`rio: Delete is not supported on clickhouse; use rio.Exec with a lightweight DELETE ("DELETE FROM users WHERE ...", ClickHouse 23.3+) or ALTER TABLE ... DELETE, both asynchronous mutations`,
		},
		"ForceDelete": {
			func(db *DB) error { return ForceDelete(ctx, db, u) },
			`rio: Delete is not supported on clickhouse; use rio.Exec with a lightweight DELETE ("DELETE FROM users WHERE ...", ClickHouse 23.3+) or ALTER TABLE ... DELETE, both asynchronous mutations`,
		},
		"Restore": {
			func(db *DB) error { return Restore(ctx, db, u) },
			"rio: Restore is not supported on clickhouse (soft-delete writes are UPDATEs); use rio.Exec with ALTER TABLE ... UPDATE",
		},
		"UpdateAll": {
			func(db *DB) error {
				_, err := From[User]().Where("age > ?", 1).UpdateAll(ctx, db, Set{"age": 2})
				return err
			},
			`rio: UpdateAll is not supported on clickhouse (no synchronous UPDATE with an affected-row count); ClickHouse updates are asynchronous mutations — issue one explicitly with rio.Exec(ctx, db, "ALTER TABLE users UPDATE ... WHERE ...") or model updates as inserts into a ReplacingMergeTree table`,
		},
		"DeleteAll": {
			func(db *DB) error {
				_, err := From[User]().Where("age > ?", 1).DeleteAll(ctx, db)
				return err
			},
			`rio: DeleteAll is not supported on clickhouse; use rio.Exec with a lightweight DELETE ("DELETE FROM users WHERE ...", ClickHouse 23.3+) or ALTER TABLE ... DELETE, both asynchronous mutations`,
		},
		"ForceDeleteAll": {
			func(db *DB) error {
				_, err := From[User]().Where("age > ?", 1).ForceDeleteAll(ctx, db)
				return err
			},
			`rio: ForceDeleteAll is not supported on clickhouse; use rio.Exec with a lightweight DELETE ("DELETE FROM users WHERE ...", ClickHouse 23.3+) or ALTER TABLE ... DELETE, both asynchronous mutations`,
		},
		"RestoreAll": {
			func(db *DB) error {
				_, err := From[User]().Where("age > ?", 1).RestoreAll(ctx, db)
				return err
			},
			"rio: RestoreAll is not supported on clickhouse (soft-delete writes are UPDATEs); use rio.Exec with ALTER TABLE ... UPDATE",
		},
		"Upsert": {
			func(db *DB) error { return Upsert(ctx, db, u, OnConflict("email")) },
			"rio: Upsert is not supported on clickhouse (no unique constraints, no conflict clause); insert a new row version into a ReplacingMergeTree table and read with Final() — background merges keep the latest version per sorting key",
		},
		"UpsertAll": {
			func(db *DB) error { return UpsertAll(ctx, db, []User{*u}, OnConflict("email")) },
			"rio: UpsertAll is not supported on clickhouse (no unique constraints, no conflict clause); insert a new row version into a ReplacingMergeTree table and read with Final() — background merges keep the latest version per sorting key",
		},
		"FirstOrCreate": {
			func(db *DB) error { return From[User]().Where("email = ?", "a@x").FirstOrCreate(ctx, db, u) },
			"rio: FirstOrCreate is not supported on clickhouse (no unique constraint to arbitrate the race — concurrent callers would both insert); use ReplacingMergeTree semantics or coordinate in the application",
		},
		"CreateOrFirst": {
			func(db *DB) error { return From[User]().Where("email = ?", "a@x").CreateOrFirst(ctx, db, u) },
			"rio: CreateOrFirst is not supported on clickhouse (no unique constraint to arbitrate the race — concurrent callers would both insert); use ReplacingMergeTree semantics or coordinate in the application",
		},
		"Attach": {
			func(db *DB) error { return Attach(ctx, db, acc, "Tags", 1) },
			"rio: Attach is not supported on clickhouse (idempotency needs a unique key over the join table); insert join rows with rio.Exec or InsertAll on a ReplacingMergeTree join table",
		},
		"Detach": {
			func(db *DB) error { return Detach(ctx, db, acc, "Tags", 1) },
			"rio: Detach is not supported on clickhouse (join-table DELETE is an asynchronous mutation); use rio.Exec",
		},
		"SyncRelation": {
			func(db *DB) error { return SyncRelation(ctx, db, acc, "Tags", []int64{1}) },
			"rio: SyncRelation is not supported on clickhouse (needs a transaction and row locks)",
		},
		"InsertZeroID": {
			func(db *DB) error { return Insert(ctx, db, &User{Email: "zero@x"}) },
			"rio: Insert on clickhouse: User.ID is zero and clickhouse cannot generate it (no auto-increment); assign the ID yourself (UUID/Snowflake/etc), or tag the field `rio:\",noautoincr\"` if zero is a real value you mean to store",
		},
		"InsertAllZeroID": {
			func(db *DB) error {
				return InsertAll(ctx, db, []User{{ID: 1, Email: "a@x"}, {Email: "zero@x"}})
			},
			"rio: InsertAll on clickhouse: User.ID is zero and clickhouse cannot generate it (no auto-increment); assign the ID yourself (UUID/Snowflake/etc), or tag the field `rio:\",noautoincr\"` if zero is a real value you mean to store",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeDB()
			db := f.open(ClickHouse)
			requireRejected(t, f, tc.call(db), tc.want)
		})
	}
}

func TestClickHouseForUpdateRejected(t *testing.T) {
	ctx := context.Background()
	const want = "rio: ForUpdate is not supported on clickhouse (no row locks); remove it — reads there are lock-free snapshots"

	f := newFakeDB()
	db := f.open(ClickHouse)
	_, err := From[User]().ForUpdate().All(ctx, db)
	requireRejected(t, f, err, want)

	f = newFakeDB()
	db = f.open(ClickHouse)
	_, err = Pluck[string](ctx, db, From[User]().ForUpdate(), "email")
	requireRejected(t, f, err, want)

	// Exec-mode compiled queries reject at first render for this grammar.
	f = newFakeDB()
	db = f.open(ClickHouse)
	q := MustCompile[User](From[User]().ForUpdate().Where("age > ?"))
	_, err = q.All(ctx, db, 1)
	requireRejected(t, f, err, want)
}

// All-defaults inserts have no ClickHouse spelling: no DEFAULT VALUES
// statement exists.
func TestClickHouseAllDefaultsInsertRejected(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)

	type Blank struct {
		Note string `rio:",omitzero"`
	}
	err := Insert(ctx, db, &Blank{})
	requireRejected(t, f, err, "rio: clickhouse has no DEFAULT VALUES statement; set at least one column on Blank")
}

func TestClickHouseStmtCachePanics(t *testing.T) {
	defer func() {
		r := recover()
		want := "rio: WithStmtCache is not supported on clickhouse (clickhouse-go implements Prepare only for INSERT batching; a prepared SELECT fails on first use)"
		if r != want {
			t.Fatalf("panic:\n got: %v\nwant: %s", r, want)
		}
	}()
	newFakeDB().openWith(ClickHouse, WithStmtCache())
}

// ForUpdate on the count shape stays elided on every dialect (aggregates
// lock nothing); ClickHouse's rejection therefore applies to row and exists
// shapes only — pinned here so the cross-dialect invariant does not drift.
func TestClickHouseForUpdateCountStillElides(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows([]string{"count"}, []driver.Value{int64(0)})
	if _, err := From[User]().ForUpdate().Count(ctx, db); err != nil {
		t.Fatalf("Count never renders FOR UPDATE, so it cannot reject: %v", err)
	}
	if got := f.logged()[0]; strings.Contains(got, "FOR UPDATE") {
		t.Fatalf("count must not lock: %s", got)
	}
}

// --- lexer behavior: ??, \?, heredocs, //, # ---

func TestClickHouseQuestionEscape(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows([]string{"v"}, []driver.Value{"y"})

	// The ternary operator's ? must reach the server as a literal: the user
	// writes ??, rio emits the driver's \? escape, the driver un-escapes.
	_, err := Raw[string]("SELECT age > ? ?? 'y' : 'n' FROM t", 1).All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	want := `SELECT age > ? \? 'y' : 'n' FROM t`
	if got := f.logged()[0]; got != want {
		t.Fatalf("?? must emit the driver escape:\n got: %s\nwant: %s", got, want)
	}
}

// A hand-written \? is already the driver's literal-? escape: it passes
// through, consumes no argument, and the accounting matches the driver's.
func TestClickHouseBackslashQuestionPassthrough(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueRows([]string{"v"}, []driver.Value{int64(1)})

	_, err := Raw[int64](`SELECT \? , ?`, 5).All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if got := f.logged()[0]; got != `SELECT \? , ?` {
		t.Fatalf("bare backslash-question must pass through: %s", got)
	}
	if args := f.loggedContaining("SELECT")[0].args; len(args) != 1 || args[0] != int64(5) {
		t.Fatalf("exactly the real placeholder binds: %v", args)
	}
}

func TestClickHouseDriverBlindRegionsRejected(t *testing.T) {
	ctx := context.Background()

	f := newFakeDB()
	db := f.open(ClickHouse)
	_, err := Raw[int64]("SELECT $$a?b$$, ?", 1).All(ctx, db)
	requireRejected(t, f, err,
		"rio: a ? inside a $...$ heredoc (byte 10) would be rewritten by clickhouse-go's client-side binder; use '...' string syntax or bind the value as an argument")

	f = newFakeDB()
	db = f.open(ClickHouse)
	_, err = Raw[int64]("SELECT ? // is ? ok\n", 1).All(ctx, db)
	requireRejected(t, f, err,
		"rio: a ? inside a // comment (byte 15) would be rewritten by clickhouse-go's client-side binder; use a -- comment")

	// Exec is the same funnel.
	f = newFakeDB()
	db = f.open(ClickHouse)
	_, err = Exec(ctx, db, "INSERT INTO t VALUES ($$?$$, ?)", 1)
	requireRejected(t, f, err,
		"rio: a ? inside a $...$ heredoc (byte 24) would be rewritten by clickhouse-go's client-side binder; use '...' string syntax or bind the value as an argument")
}

// Argument-free statements pass those same regions untouched: the driver
// skips its binder entirely when no arguments ride along.
func TestClickHouseDriverBlindRegionsPassWithoutArgs(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)

	f.queueRows([]string{"v"}, []driver.Value{"he?llo"})
	if _, err := Raw[string]("SELECT $$he?llo$$").All(ctx, db); err != nil {
		t.Fatalf("argument-free heredoc: %v", err)
	}
	f.queueRows([]string{"v"}, []driver.Value{int64(1)})
	if _, err := Raw[int64]("SELECT 1 // trailing ?\n").All(ctx, db); err != nil {
		t.Fatalf("argument-free // comment: %v", err)
	}
	logs := f.logged()
	if logs[0] != "SELECT $$he?llo$$" || logs[1] != "SELECT 1 // trailing ?\n" {
		t.Fatalf("blind regions must pass through byte for byte: %v", logs)
	}
}

// ClickHouse's # comments only exist before a space or '!' — `#x` is a
// server-side lexer error, so its trailing ? stays a live placeholder.
func TestClickHouseHashCommentRules(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)

	// `# ` comments: the trailing ? is dead, so one argument over-supplies.
	_, err := Raw[int64]("SELECT 1 # dead ?", 1).All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "0 placeholder(s) but 1 argument(s)") {
		t.Fatalf("a ? inside '# ' comment is not a placeholder: %v", err)
	}
	// `#x` is not a comment: the ? is live and binds.
	f.queueRows([]string{"v"}, []driver.Value{int64(1)})
	if _, err := Raw[int64]("SELECT 1 #x ?", 9).All(ctx, db); err != nil {
		t.Fatalf("#x is not a comment, its ? must bind: %v", err)
	}
	stmt := f.loggedContaining("#x")[0]
	if len(stmt.args) != 1 || stmt.args[0] != int64(9) {
		t.Fatalf("live placeholder after #x: %v", stmt.args)
	}
}

// Backslashes escape inside every ClickHouse quote flavor — a ? behind an
// escaped quote must stay quoted, not become a placeholder.
func TestClickHouseQuotedRegionEscapes(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)

	for _, q := range []string{
		`SELECT '\' ?' , ?`,        // backslash-escaped quote inside a string
		"SELECT `a\\` ?` , ?",      // ...inside a backtick identifier
		`SELECT "a\" ?" , ?`,       // ...inside a double-quoted identifier
		"SELECT /* /* ? */ ? */ ?", // nested block comment
	} {
		f.queueRows([]string{"v"}, []driver.Value{int64(1)})
		if _, err := Raw[int64](q, 1).All(ctx, db); err != nil {
			t.Fatalf("%q must carry exactly one live placeholder: %v", q, err)
		}
	}
}

// --- identifier quoting ---

func TestClickHouseQuoteEscapesBackticksAndBackslashes(t *testing.T) {
	for ident, want := range map[string]string{
		"users":        "`users`",
		"analytics.ev": "`analytics`.`ev`",
		"we`ird":       "`we``ird`",
		`back\slash`:   "`back\\\\slash`",
		"mix`.\\seg":   "`mix```.`\\\\seg`",
	} {
		if got := string(ClickHouse.quote(nil, ident)); got != want {
			t.Fatalf("quote(%q):\n got: %s\nwant: %s", ident, got, want)
		}
	}
}

// --- time binding ---

func TestClickHouseTimeFormatRoundTrip(t *testing.T) {
	at := time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.UTC)
	bound := ClickHouse.bindTime(normalizeTime(at)).(string)
	if bound != "2024-01-02 03:04:05.123456+00:00" {
		t.Fatalf("bindTime: %s", bound)
	}
	// The emitted text parses back to the same instant through rio's own
	// scan formats — a full write/read round trip stays Equal.
	parsed, err := parseTime(bound, &field{column: "at"})
	if err != nil {
		t.Fatalf("parse back: %v", err)
	}
	if !parsed.Equal(at.Truncate(time.Microsecond)) {
		t.Fatalf("round trip drifted: %v != %v", parsed, at)
	}
	// Zoned inputs normalize to the same UTC text.
	zoned := at.In(time.FixedZone("CST", 8*3600))
	if got := ClickHouse.bindTime(normalizeTime(zoned)).(string); got != bound {
		t.Fatalf("zoned input must bind identical text: %s", got)
	}
}

func TestClickHouseTimeRangeChecked(t *testing.T) {
	ctx := context.Background()

	type Sample struct {
		ID int64 `rio:"id,pk,noautoincr"`
		At time.Time
	}
	insertAt := func(at time.Time) (*fakeDB, error) {
		f := newFakeDB()
		db := f.open(ClickHouse)
		f.queueExec(0, 0)
		return f, Insert(ctx, db, &Sample{ID: 1, At: at})
	}

	// In range: both boundaries store.
	for _, ok := range []time.Time{
		time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2299, 12, 31, 23, 59, 59, 999999999, time.UTC), // truncates to .999999
	} {
		if _, err := insertAt(ok); err != nil {
			t.Fatalf("boundary %v must bind: %v", ok, err)
		}
	}

	// Out of range: entity funnel (Insert) rejects before sending.
	f, err := insertAt(time.Date(1899, 12, 31, 23, 59, 59, 999999999, time.UTC))
	requireRejected(t, f, err,
		"rio: time 1899-12-31T23:59:59.999999Z is outside ClickHouse's DateTime64 range [1900-01-01, 2299-12-31] and would be silently clamped")
	f, err = insertAt(time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC))
	requireRejected(t, f, err,
		"rio: time 2300-01-01T00:00:00Z is outside ClickHouse's DateTime64 range [1900-01-01, 2299-12-31] and would be silently clamped")

	// The zero time gets the dedicated message — the most common accident.
	f, err = insertAt(time.Time{})
	requireRejected(t, f, err,
		`rio: a zero time.Time is outside ClickHouse's DateTime64 range [1900-01-01, 2299-12-31] and would be silently clamped; use a *time.Time (Nullable column) for "no value"`)

	// User-argument funnel (normalizeArgs) applies the same rule.
	fq := newFakeDB()
	db := fq.open(ClickHouse)
	_, err = From[User]().Where("created_at > ?", time.Time{}).All(ctx, db)
	requireRejected(t, fq, err,
		`rio: a zero time.Time is outside ClickHouse's DateTime64 range [1900-01-01, 2299-12-31] and would be silently clamped; use a *time.Time (Nullable column) for "no value"`)

	// The other dialects are untouched by the range rule.
	fpg := newFakeDB()
	pg := fpg.open(Postgres)
	fpg.queueRows(userCols)
	if _, err := From[User]().Where("created_at > ?", time.Time{}).All(ctx, pg); err != nil {
		t.Fatalf("postgres must accept the zero time: %v", err)
	}
}

// sql.NullTime and *time.Time route through the same range check.
func TestClickHouseTimeRangeCoversNullableForms(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)

	_, err := From[User]().Where("created_at > ?", sql.NullTime{Time: time.Time{}, Valid: true}).All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "zero time.Time") {
		t.Fatalf("valid NullTime zero must hit the zero message: %v", err)
	}
	// Invalid NullTime is NULL, not a range violation.
	f2 := newFakeDB()
	db2 := f2.open(ClickHouse)
	f2.queueRows(userCols)
	if _, err := From[User]().Where("deleted_at IS NULL OR deleted_at > ?", sql.NullTime{}).All(ctx, db2); err != nil {
		t.Fatalf("invalid NullTime binds NULL: %v", err)
	}
}

// --- []byte and uint64 funnels ---

func TestClickHouseByteFunnel(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueExec(0, 0)

	type Doc2 struct {
		ID     int64 `rio:"id,pk,noautoincr"`
		Body   []byte
		Raw    json.RawMessage
		Config *Prefs `rio:",json"`
	}
	d := &Doc2{ID: 1, Body: []byte("bytes"), Raw: json.RawMessage(`{"k":1}`), Config: &Prefs{Theme: "dark"}}
	if err := Insert(ctx, db, d); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	args := f.loggedContaining("INSERT")[0].args
	if s, ok := args[1].(string); !ok || s != "bytes" {
		t.Fatalf("[]byte field must bind as string on clickhouse: %#v", args[1])
	}
	if s, ok := args[2].(string); !ok || s != `{"k":1}` {
		t.Fatalf("named byte slice must bind as string: %#v", args[2])
	}
	if s, ok := args[3].(string); !ok || s != `{"theme":"dark"}` {
		t.Fatalf("json column must bind as string: %#v", args[3])
	}

	// User-argument funnel.
	f.queueRows([]string{"v"}, []driver.Value{int64(1)})
	if _, err := Raw[int64]("SELECT 1 FROM t WHERE body = ?", []byte("q")).All(ctx, db); err != nil {
		t.Fatalf("raw arg: %v", err)
	}
	stmt := f.loggedContaining("WHERE body")[0]
	if s, ok := stmt.args[0].(string); !ok || s != "q" {
		t.Fatalf("[]byte argument must bind as string: %#v", stmt.args[0])
	}

	// Other dialects keep binding []byte as-is.
	fpg := newFakeDB()
	pg := fpg.open(Postgres)
	fpg.queueRows([]string{"v"}, []driver.Value{int64(1)})
	if _, err := Raw[int64]("SELECT 1 FROM t WHERE body = ?", []byte("q")).All(ctx, pg); err != nil {
		t.Fatalf("pg raw arg: %v", err)
	}
	if _, ok := fpg.loggedContaining("WHERE body")[0].args[0].([]byte); !ok {
		t.Fatal("postgres []byte binding must stay []byte")
	}
}

func TestClickHouseHugeUint64Binds(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)
	f.queueExec(0, 0)

	type Big struct {
		ID uint64 `rio:"id,pk,noautoincr"`
		N  uint64
	}
	huge := uint64(1) << 63
	if err := Insert(ctx, db, &Big{ID: 1, N: huge}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	args := f.loggedContaining("INSERT")[0].args
	if s, ok := args[1].(string); !ok || s != "9223372036854775808" {
		t.Fatalf("huge uint64 must bind as decimal string: %#v", args[1])
	}

	f.queueRows([]string{"v"}, []driver.Value{int64(1)})
	if _, err := Raw[int64]("SELECT 1 FROM t WHERE n = ?", huge).All(ctx, db); err != nil {
		t.Fatalf("raw huge arg: %v", err)
	}
	stmt := f.loggedContaining("WHERE n")[0]
	if s, ok := stmt.args[0].(string); !ok || s != "9223372036854775808" {
		t.Fatalf("huge uint64 argument must bind as decimal string: %#v", stmt.args[0])
	}
}

// --- Raw and Exec pass everything else through ---

func TestClickHouseRawAndExecFullPass(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(ClickHouse)

	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	if _, err := Raw[int64]("SELECT id FROM t FINAL SAMPLE 0.1 WHERE x = ? SETTINGS max_threads = 2", 1).All(ctx, db); err != nil {
		t.Fatalf("raw clickhouse SQL: %v", err)
	}
	if _, err := Exec(ctx, db, "ALTER TABLE t UPDATE x = ? WHERE id = ?", 1, 2); err != nil {
		t.Fatalf("exec mutation escape hatch: %v", err)
	}
	logs := f.logged()
	if logs[0] != "SELECT id FROM t FINAL SAMPLE 0.1 WHERE x = ? SETTINGS max_threads = 2" {
		t.Fatalf("raw must pass through: %s", logs[0])
	}
	if logs[1] != "ALTER TABLE t UPDATE x = ? WHERE id = ?" {
		t.Fatalf("exec must pass through: %s", logs[1])
	}
}
