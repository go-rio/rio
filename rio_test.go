package rio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"
)

type User struct {
	ID        int64
	Email     string
	Age       int
	Bio       *string
	Version   int64      `rio:",version"`
	DeletedAt *time.Time `rio:",softdelete"`
	CreatedAt time.Time
	UpdatedAt time.Time

	Posts HasMany[Post]
}

type Post struct {
	ID     int64
	UserID int64
	Title  string

	Author BelongsTo[User] `rio:",fk:user_id"`
}

// userCols matches the plan's column order for scripting fake results.
var userCols = []string{"id", "email", "age", "bio", "version", "deleted_at", "created_at", "updated_at"}

func userRow(id int64, email string) []driver.Value {
	return []driver.Value{id, email, int64(30), nil, int64(1), nil, testNow, testNow}
}

func TestAllRendersQualifiedSelect(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"), userRow(2, "b@x"))

	users, err := From[User]().Where("age > ?", 18).OrderBy("created_at DESC").Limit(10).All(context.Background(), db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(users) != 2 || users[0].Email != "a@x" || users[1].ID != 2 {
		t.Fatalf("scanned %+v", users)
	}
	want := `SELECT "users"."id", "users"."email", "users"."age", "users"."bio", "users"."version", "users"."deleted_at", "users"."created_at", "users"."updated_at" FROM "users" WHERE (age > $1) AND "users"."deleted_at" IS NULL ORDER BY created_at DESC LIMIT 10`
	if got := f.logged()[0]; got != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", got, want)
	}
}

func TestFirstNotFoundWrapsErrNoRows(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols)

	_, err := From[User]().Where("email = ?", "missing").First(context.Background(), db)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("ErrNotFound must wrap sql.ErrNoRows")
	}
}

func TestSoleMultipleRows(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"), userRow(2, "b@x"))

	_, err := From[User]().Sole(context.Background(), db)
	if !errors.Is(err, ErrMultipleRows) {
		t.Fatalf("want ErrMultipleRows, got %v", err)
	}
	if !strings.Contains(f.logged()[0], "LIMIT 2") {
		t.Fatal("Sole should probe with LIMIT 2")
	}
}

func TestFindByPrimaryKey(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(42, "found@x"))

	u, err := Find[User](context.Background(), db, 42)
	if err != nil || u.ID != 42 {
		t.Fatalf("Find: %v %+v", err, u)
	}
	got := f.logged()[0]
	if !strings.Contains(got, `"users"."id" = $1`) || !strings.Contains(got, "LIMIT 1") {
		t.Fatalf("sql: %s", got)
	}
	if !strings.Contains(got, `"deleted_at" IS NULL`) {
		t.Fatalf("Find must respect soft delete: %s", got)
	}

	if _, err := Find[User](context.Background(), db, 1, 2); err == nil || !strings.Contains(err.Error(), "1 key part(s)") {
		t.Fatalf("composite arity should be checked: %v", err)
	}
}

func TestInsertReturningBackfillsGenerated(t *testing.T) {
	f := newFakeDB()
	db := f.open() // postgres
	f.queueRows([]string{"id"}, []driver.Value{int64(7)})

	u := &User{Email: "a@x", Age: 30}
	if err := Insert(context.Background(), db, u); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// RETURNING covers exactly what the database generates — here only the
	// auto-increment PK; timestamps and version are client-set and known.
	want := `INSERT INTO "users" ("email", "age", "bio", "version", "deleted_at", "created_at", "updated_at") VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING "id"`
	if got := f.logged()[0]; got != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", got, want)
	}
	if u.ID != 7 {
		t.Fatalf("id backfill: %d", u.ID)
	}
	if !u.CreatedAt.Equal(normalizeTime(testNow)) {
		t.Fatal("timestamps are client-generated")
	}
	if u.Version != 1 {
		t.Fatalf("version initialized to %d", u.Version)
	}
}

func TestInsertMySQLLastInsertID(t *testing.T) {
	f := newFakeDB()
	db := f.open(MySQL)
	f.queueExec(99, 1)

	u := &User{Email: "a@x"}
	if err := Insert(context.Background(), db, u); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	got := f.logged()[0]
	if strings.Contains(got, "RETURNING") {
		t.Fatalf("mysql must not render RETURNING: %s", got)
	}
	if !strings.HasPrefix(got, "INSERT INTO `users` (`email`,") {
		t.Fatalf("sql: %s", got)
	}
	if u.ID != 99 {
		t.Fatalf("LastInsertId backfill: %d", u.ID)
	}
}

func TestInsertZeroValuesAreWritten(t *testing.T) {
	f := newFakeDB()
	db := f.open(MySQL)
	f.queueExec(1, 1)

	u := &User{Email: "", Age: 0} // all zero values
	if err := Insert(context.Background(), db, u); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	stmt := f.loggedContaining("INSERT")[0]
	if !strings.Contains(stmt.sql, "`email`") || !strings.Contains(stmt.sql, "`age`") {
		t.Fatalf("zero-valued columns must be written: %s", stmt.sql)
	}
	if stmt.args[0] != "" || stmt.args[1] != int64(0) {
		t.Fatalf("zero values must bind as real values: %v", stmt.args)
	}
}

func TestUpdateFullColumnWithOptimisticLock(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueExec(0, 1)

	u := &User{ID: 5, Email: "new@x", Age: 31, Version: 3}
	if err := Update(context.Background(), db, u); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got := f.logged()[0]
	// created_at is never updated; version renders as an atomic increment;
	// deleted_at is owned by Delete/Restore/ForceDelete and never rides along
	// (a stale live struct would silently resurrect a tombstoned row).
	want := `UPDATE "users" SET "email" = ?, "age" = ?, "bio" = ?, "updated_at" = ?, "version" = "version" + 1 WHERE "id" = ? AND "version" = ?`
	if got != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", got, want)
	}
	if u.Version != 4 {
		t.Fatalf("version must bump after success: %d", u.Version)
	}
	if !u.UpdatedAt.Equal(normalizeTime(testNow)) {
		t.Fatal("UpdatedAt must be maintained")
	}
}

func TestUpdateColumnWhitelist(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueExec(0, 1)

	u := &User{ID: 5, Email: "w@x", Version: 1}
	if err := Update(context.Background(), db, u, "email"); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got := f.logged()[0]
	want := `UPDATE "users" SET "email" = ?, "updated_at" = ?, "version" = "version" + 1 WHERE "id" = ? AND "version" = ?`
	if got != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", got, want)
	}

	if err := Update(context.Background(), db, u, "no_such"); err == nil || !strings.Contains(err.Error(), `no column "no_such"`) {
		t.Fatalf("unknown column: %v", err)
	}
	if err := Update(context.Background(), db, u, "id"); err == nil || !strings.Contains(err.Error(), "maintained by rio") {
		t.Fatalf("pk in whitelist: %v", err)
	}
}

func TestUpdateStaleObject(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueExec(0, 0) // no rows matched: version conflict

	u := &User{ID: 5, Version: 3}
	err := Update(context.Background(), db, u)
	if !errors.Is(err, ErrStaleObject) {
		t.Fatalf("want ErrStaleObject, got %v", err)
	}
	if u.Version != 3 {
		t.Fatal("version must not bump on failure")
	}
}

func TestDeleteSoftAndForce(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueExec(0, 1)

	u := &User{ID: 5, Version: 1}
	if err := Delete(context.Background(), db, u); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got := f.logged()[0]
	if !strings.HasPrefix(got, `UPDATE "users" SET "deleted_at" = ?`) {
		t.Fatalf("soft delete must UPDATE: %s", got)
	}
	if u.DeletedAt == nil {
		t.Fatal("DeletedAt must be written back")
	}

	f.queueExec(0, 1)
	if err := ForceDelete(context.Background(), db, u); err != nil {
		t.Fatalf("ForceDelete: %v", err)
	}
	if !strings.HasPrefix(f.logged()[1], `DELETE FROM "users" WHERE`) {
		t.Fatalf("force delete must DELETE: %s", f.logged()[1])
	}
}

func TestSoftDeleteFilterModes(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols)
	f.queueRows(userCols)
	f.queueRows(userCols)

	_, _ = From[User]().All(ctx, db)
	_, _ = From[User]().WithTrashed().All(ctx, db)
	_, _ = From[User]().OnlyTrashed().All(ctx, db)

	logs := f.logged()
	if !strings.Contains(logs[0], `"deleted_at" IS NULL`) {
		t.Fatalf("default filters trashed: %s", logs[0])
	}
	if strings.Contains(logs[1], `"deleted_at" IS`) {
		t.Fatalf("WithTrashed must not filter: %s", logs[1])
	}
	if !strings.Contains(logs[2], `"deleted_at" IS NOT NULL`) {
		t.Fatalf("OnlyTrashed: %s", logs[2])
	}
}

func TestUpdateAllRequiresWhere(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(SQLite)

	if _, err := From[User]().UpdateAll(ctx, db, Set{"age": 1}); !errors.Is(err, ErrMissingWhere) {
		t.Fatalf("want ErrMissingWhere, got %v", err)
	}
	if _, err := From[User]().DeleteAll(ctx, db); !errors.Is(err, ErrMissingWhere) {
		t.Fatalf("want ErrMissingWhere, got %v", err)
	}

	f.queueExec(0, 3)
	n, err := From[User]().AllRows().UpdateAll(ctx, db, Set{"age": Expr("age + 1"), "email": "x"})
	if err != nil || n != 3 {
		t.Fatalf("UpdateAll: %v n=%d", err, n)
	}
	got := f.logged()[0]
	want := `UPDATE "users" SET "age" = age + 1, "email" = ?, "updated_at" = ? WHERE "users"."deleted_at" IS NULL`
	if got != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", got, want)
	}
}

func TestDeleteAllSoftDeletes(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueExec(0, 2)

	n, err := From[User]().Where("age < ?", 10).DeleteAll(context.Background(), db)
	if err != nil || n != 2 {
		t.Fatalf("DeleteAll: %v n=%d", err, n)
	}
	got := f.logged()[0]
	if !strings.HasPrefix(got, `UPDATE "users" SET "deleted_at" = ?`) || !strings.Contains(got, `"deleted_at" IS NULL`) {
		t.Fatalf("soft DeleteAll updates only kept rows: %s", got)
	}
}

func TestInsertAllChunksAndBackfills(t *testing.T) {
	f := newFakeDB()
	db := f.open() // postgres
	f.queueRows([]string{"id"}, []driver.Value{int64(11)}, []driver.Value{int64(12)})

	rows := []User{{Email: "a@x"}, {Email: "b@x"}}
	if err := InsertAll(context.Background(), db, rows); err != nil {
		t.Fatalf("InsertAll: %v", err)
	}
	got := f.logged()[0]
	if !strings.Contains(got, "VALUES ($1, $2, $3, $4, $5, $6, $7), ($8, $9, $10, $11, $12, $13, $14)") {
		t.Fatalf("multi-VALUES: %s", got)
	}
	if !strings.HasSuffix(got, `RETURNING "id"`) {
		t.Fatalf("backfill returning: %s", got)
	}
	if rows[0].ID != 11 || rows[1].ID != 12 {
		t.Fatalf("pk backfill: %+v", rows)
	}
}

func TestInsertAllMixedIDsRefused(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	rows := []User{{ID: 1, Email: "a@x"}, {Email: "b@x"}}
	err := InsertAll(context.Background(), db, rows)
	if err == nil || !strings.Contains(err.Error(), "mix zero and explicit") {
		t.Fatalf("mixed ids: %v", err)
	}
}

func TestUpsertAllMySQLUsesRowAlias(t *testing.T) {
	f := newFakeDB()
	db := f.open(MySQL)
	f.queueExec(0, 2)

	rows := []User{{ID: 1, Email: "a@x"}, {ID: 2, Email: "b@x"}}
	if err := UpsertAll(context.Background(), db, rows, OnConflict("email"), DoUpdate("age")); err != nil {
		t.Fatalf("UpsertAll: %v", err)
	}
	got := f.logged()[0]
	for _, frag := range []string{
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?) AS _rio_new ON DUPLICATE KEY UPDATE",
		"`age` = _rio_new.`age`",
	} {
		if !strings.Contains(got, frag) {
			t.Fatalf("missing %q in:\n%s", frag, got)
		}
	}
	if strings.Contains(got, "VALUES(`") {
		t.Fatalf("mysql upsert must not use deprecated VALUES(col): %s", got)
	}
}

func TestUpsertRestoresSoftDeleted(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueRows(userCols, userRow(1, "a@x"))

	u := &User{Email: "a@x"}
	if err := Upsert(context.Background(), db, u, OnConflict("email")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got := f.logged()[0]
	for _, frag := range []string{
		`ON CONFLICT ("email") DO UPDATE SET`,
		`"age" = excluded."age"`,
		`"updated_at" = excluded."updated_at"`,
		`"version" = "users"."version" + 1`,
		`"deleted_at" = NULL`, // restore-on-upsert invariant
	} {
		if !strings.Contains(got, frag) {
			t.Fatalf("missing %q in:\n%s", frag, got)
		}
	}

	f.queueRows(userCols, userRow(1, "a@x"))
	if err := Upsert(context.Background(), db, u, OnConflict("email"), KeepTrashed()); err != nil {
		t.Fatalf("Upsert KeepTrashed: %v", err)
	}
	if strings.Contains(f.logged()[1], `"deleted_at" = NULL`) {
		t.Fatal("KeepTrashed must not restore")
	}
}

func TestUpsertMySQLForm(t *testing.T) {
	f := newFakeDB()
	db := f.open(MySQL)
	f.queueExec(5, 1) // affected=1: fresh insert

	u := &User{Email: "a@x"}
	if err := Upsert(context.Background(), db, u, OnConflict("email"), DoUpdate("age")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got := f.logged()[0]
	if strings.Contains(got, "ON CONFLICT") {
		t.Fatalf("mysql renders ON DUPLICATE KEY: %s", got)
	}
	for _, frag := range []string{
		"AS _rio_new ON DUPLICATE KEY UPDATE `age` = _rio_new.`age`",
		"`version` = `users`.`version` + 1",
		"`deleted_at` = NULL",
	} {
		if !strings.Contains(got, frag) {
			t.Fatalf("missing %q in:\n%s", frag, got)
		}
	}
	if u.ID != 5 {
		t.Fatalf("insert path backfills LastInsertId: %d", u.ID)
	}
}

func TestUpsertClearsSoftDeleteBeforeInsertValues(t *testing.T) {
	ctx := context.Background()
	deleted := testNow.Add(-time.Hour)

	f := newFakeDB()
	db := f.open(MySQL)
	f.queueExec(5, 1)
	row := &User{Email: "a@x", DeletedAt: &deleted}
	if err := Upsert(ctx, db, row, OnConflict("email"), DoUpdate("age")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if row.DeletedAt != nil && !row.DeletedAt.IsZero() {
		t.Fatalf("non-KeepTrashed upsert must clear in-memory deleted_at before bind, got %v", row.DeletedAt)
	}
	args := f.loggedContaining("INSERT")[0].args
	if args[4] != nil {
		t.Fatalf("non-KeepTrashed upsert must bind deleted_at as NULL, got %#v", args[4])
	}

	f = newFakeDB()
	db = f.open(MySQL)
	f.queueExec(5, 1)
	row = &User{Email: "a@x", DeletedAt: &deleted}
	if err := Upsert(ctx, db, row, OnConflict("email"), DoUpdate("age"), KeepTrashed()); err != nil {
		t.Fatalf("Upsert KeepTrashed: %v", err)
	}
	if row.DeletedAt == nil || row.DeletedAt.IsZero() {
		t.Fatal("KeepTrashed upsert must preserve in-memory deleted_at")
	}
	args = f.loggedContaining("INSERT")[0].args
	if args[4] == nil {
		t.Fatal("KeepTrashed upsert must bind the deleted_at value")
	}
}

func TestUpsertDoNothing(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id"}) // conflict: RETURNING yields no row

	u := &User{Email: "a@x"}
	if err := Upsert(context.Background(), db, u, OnConflict("email"), DoNothing()); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if !strings.Contains(f.logged()[0], "ON CONFLICT (\"email\") DO NOTHING") {
		t.Fatalf("sql: %s", f.logged()[0])
	}
}

func TestTxCommitAndRollback(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(SQLite)

	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	err := db.Tx(ctx, func(tx *Tx) error {
		return Insert(ctx, tx, &Post{UserID: 1, Title: "hi"})
	})
	if err != nil {
		t.Fatalf("Tx: %v", err)
	}

	boom := errors.New("boom")
	err = db.Tx(ctx, func(tx *Tx) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("Tx must return fn's error: %v", err)
	}

	logs := f.logged()
	joined := strings.Join(logs, " | ")
	if !strings.Contains(joined, "BEGIN") || !strings.Contains(joined, "COMMIT") || !strings.Contains(joined, "ROLLBACK") {
		t.Fatalf("tx boundaries: %s", joined)
	}
}

func TestNestedTxUsesSavepoints(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(SQLite)

	boom := errors.New("inner boom")
	err := db.Tx(ctx, func(tx *Tx) error {
		if err := tx.Tx(ctx, func(tx2 *Tx) error { return boom }); !errors.Is(err, boom) {
			t.Fatalf("inner error: %v", err)
		}
		return tx.Tx(ctx, func(tx2 *Tx) error { return nil })
	})
	if err != nil {
		t.Fatalf("outer Tx: %v", err)
	}
	logs := f.logged()
	var sps []string
	for _, l := range logs {
		if strings.Contains(l, "SAVEPOINT") {
			sps = append(sps, l)
		}
	}
	want := []string{
		"SAVEPOINT rio_sp_1",
		"ROLLBACK TO SAVEPOINT rio_sp_1",
		"RELEASE SAVEPOINT rio_sp_1",
		"SAVEPOINT rio_sp_2", // monotonic: names never reused
		"RELEASE SAVEPOINT rio_sp_2",
	}
	if len(sps) != len(want) {
		t.Fatalf("savepoint sequence: %v", sps)
	}
	for i := range want {
		if sps[i] != want[i] {
			t.Fatalf("savepoint[%d] = %q, want %q", i, sps[i], want[i])
		}
	}
}

// DESIGN.md, savepoint failure paths: ROLLBACK TO SAVEPOINT can itself fail,
// and its error must be joined to the cause, never allowed to mask it.
func TestSavepointRollbackFailureJoinsErrors(t *testing.T) {
	ctx := context.Background()

	// PostgreSQL flavor: the failed statement aborts the transaction; if the
	// ROLLBACK TO then fails too (dead connection, missing savepoint), rio
	// reports both errors and skips the RELEASE that could only fail as well.
	t.Run("postgres aborted state", func(t *testing.T) {
		f := newFakeDB()
		db := f.open(Postgres)
		insertErr := errors.New(`pq: duplicate key value violates unique constraint "posts_pkey"`)
		rbErr := errors.New("pq: current transaction is aborted, commands ignored until end of transaction block")
		f.failContaining("INSERT", insertErr)
		f.failContaining("ROLLBACK TO", rbErr)

		err := db.Tx(ctx, func(tx *Tx) error {
			return tx.Tx(ctx, func(tx2 *Tx) error {
				return Insert(ctx, tx2, &Post{Title: "x", UserID: 1})
			})
		})
		if !errors.Is(err, insertErr) {
			t.Fatalf("the original cause must survive the failed rollback: %v", err)
		}
		if !errors.Is(err, rbErr) {
			t.Fatalf("the ROLLBACK TO failure must be joined, not swallowed: %v", err)
		}
		logs := strings.Join(f.logged(), " | ")
		if strings.Contains(logs, "RELEASE") {
			t.Fatalf("no RELEASE after a failed ROLLBACK TO: %s", logs)
		}
		if !strings.Contains(logs, "ROLLBACK TO SAVEPOINT rio_sp_1") {
			t.Fatalf("rollback must be attempted before giving up: %s", logs)
		}
		// The outer transaction saw the inner error and rolled back whole.
		if f.logged()[len(f.logged())-1] != "ROLLBACK" {
			t.Fatalf("outer transaction must roll back: %s", logs)
		}
	})

	// MySQL flavor: a deadlock (1213) rolls back the entire transaction and
	// destroys every savepoint, so the ROLLBACK TO fails with 1305. Both
	// errors must reach the caller — the deadlock is the one worth retrying.
	t.Run("mysql 1213 kills savepoints", func(t *testing.T) {
		f := newFakeDB()
		db := f.open(MySQL)
		deadlock := errors.New("Error 1213 (40001): Deadlock found when trying to get lock; try restarting transaction")
		spGone := errors.New("Error 1305 (42000): SAVEPOINT rio_sp_1 does not exist")
		f.failContaining("INSERT", deadlock)
		f.failContaining("ROLLBACK TO", spGone)

		err := db.Tx(ctx, func(tx *Tx) error {
			return tx.Tx(ctx, func(tx2 *Tx) error {
				return Insert(ctx, tx2, &Post{Title: "x", UserID: 1})
			})
		})
		if !errors.Is(err, deadlock) {
			t.Fatalf("the deadlock must stay visible for retry logic: %v", err)
		}
		if !errors.Is(err, spGone) {
			t.Fatalf("the ROLLBACK TO failure must be joined: %v", err)
		}
		if logs := strings.Join(f.logged(), " | "); strings.Contains(logs, "RELEASE") {
			t.Fatalf("no RELEASE after a failed ROLLBACK TO: %s", logs)
		}
	})
}

func TestTxBeginHookPanicRollsBack(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	hook := hookFunc(func(_ context.Context, e *QueryEvent) {
		if e.Op == "begin" {
			panic("telemetry exploded")
		}
	})
	db := f.openWith(SQLite, WithQueryHook(hook))

	ran := false
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = db.Tx(ctx, func(tx *Tx) error {
			ran = true
			return nil
		})
	}()
	if recovered != "telemetry exploded" {
		t.Fatalf("the hook panic must propagate unchanged, got %v", recovered)
	}
	if ran {
		t.Fatal("fn must not run when the BEGIN hook panics")
	}
	// The transaction was already open when AfterQuery fired; the connection
	// must go back to the pool via ROLLBACK, not leak.
	joined := strings.Join(f.logged(), " | ")
	if !strings.Contains(joined, "BEGIN") || !strings.Contains(joined, "ROLLBACK") {
		t.Fatalf("BEGIN hook panic must roll back the open transaction: %s", joined)
	}
}

func TestErrorTranslatorMapsDuplicate(t *testing.T) {
	f := newFakeDB()
	dup := errors.New("driver: unique violation")
	db := f.openWith(SQLite, WithErrorTranslator(func(err error) error {
		if errors.Is(err, dup) {
			return ErrDuplicateKey
		}
		return nil
	}))
	f.failContaining("INSERT", dup)

	err := Insert(context.Background(), db, &Post{Title: "x"})
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("want ErrDuplicateKey, got %v", err)
	}
	if !errors.Is(err, dup) {
		t.Fatal("driver error must stay in the chain")
	}
}

func TestRawScansScalarsAndStructs(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	f.queueRows([]string{"n"}, []driver.Value{int64(42)})
	n, err := Raw[int64]("SELECT count(*) FROM users WHERE age > ?", 18).First(ctx, db)
	if err != nil || *n != 42 {
		t.Fatalf("scalar: %v %v", err, n)
	}
	if got := f.logged()[0]; got != "SELECT count(*) FROM users WHERE age > $1" {
		t.Fatalf("rebind: %s", got)
	}

	type stat struct {
		Email string
		N     int64
	}
	f.queueRows([]string{"email", "n"}, []driver.Value{"a@x", int64(3)})
	stats, err := Raw[stat]("SELECT email, count(*) AS n FROM posts GROUP BY email").All(ctx, db)
	if err != nil || len(stats) != 1 || stats[0].N != 3 {
		t.Fatalf("dto: %v %+v", err, stats)
	}

	f.queueRows([]string{"email", "mystery"}, []driver.Value{"a@x", int64(1)})
	_, err = Raw[stat]("SELECT ...").All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), `"mystery"`) {
		t.Fatalf("unknown columns must error: %v", err)
	}
}

func TestNullIntoNonPointerErrors(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	row := userRow(1, "a@x")
	row[1] = nil // email NULL
	f.queueRows(userCols, row)

	_, err := From[User]().All(context.Background(), db)
	if err == nil || !strings.Contains(err.Error(), `column "email" is NULL`) {
		t.Fatalf("NULL honesty: %v", err)
	}
}

func TestQueryImmutability(t *testing.T) {
	base := From[User]().Where("age > ?", 18)
	q1 := base.Where("email LIKE ?", "%a%").OrderBy("id")
	q2 := base.Where("email LIKE ?", "%b%").Limit(5)

	if len(base.s.wheres) != 1 {
		t.Fatalf("base mutated: %+v", base.s.wheres)
	}
	if len(q1.s.wheres) != 2 || len(q2.s.wheres) != 2 {
		t.Fatal("derived queries must extend independently")
	}
	if q1.s.wheres[1].args[0] == q2.s.wheres[1].args[0] {
		t.Fatal("cross-contamination between siblings")
	}
	if q1.s.limitSet || len(q2.s.orders) != 0 {
		t.Fatal("options leaked between siblings")
	}
}

func TestQueryConcurrentDerivation(t *testing.T) {
	base := From[User]().Where("age > ?", 18)
	done := make(chan Query[User], 64)
	for i := 0; i < 64; i++ {
		go func(i int) {
			done <- base.Where("id > ?", i).OrderBy("id").Limit(i)
		}(i)
	}
	for i := 0; i < 64; i++ {
		q := <-done
		if len(q.s.wheres) != 2 {
			t.Fatalf("derived query has %d conditions", len(q.s.wheres))
		}
	}
	if len(base.s.wheres) != 1 {
		t.Fatal("base mutated under concurrency")
	}
}
