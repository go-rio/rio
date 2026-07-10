package rio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

// loopDB is fakeDB's measurement counterpart: it serves the same scripted
// rows for every query and a fixed (1, 1) result for every exec, with no
// statement log and no locking — fakeDB's per-statement log append would
// pollute AllocsPerRun and benchmark numbers.
type loopDB struct {
	cols []string
	rows [][]driver.Value
}

func (l *loopDB) open(d Dialect, opts ...Option) *DB {
	db := New(l.raw(), d, append([]Option{WithClock(fixedClock)}, opts...)...)
	return db
}

func (l *loopDB) raw() *sql.DB {
	db := sql.OpenDB(loopConnector{l})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db
}

type loopConnector struct{ l *loopDB }

func (c loopConnector) Connect(context.Context) (driver.Conn, error) { return loopConn(c), nil }
func (c loopConnector) Driver() driver.Driver                        { return fakeDriver{} }

type loopConn struct{ l *loopDB }

func (loopConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("loopdb: no prepare") }
func (loopConn) Close() error                        { return nil }
func (loopConn) Begin() (driver.Tx, error)           { return nil, errors.New("loopdb: no tx") }

func (c loopConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &fakeRowsIter{data: fakeRows{cols: c.l.cols, rows: c.l.rows}}, nil
}

func (c loopConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return fakeExecResult{fakeResult{lastID: 1, affected: 1}}, nil
}

// perfUser is the entity-CRUD measurement model: five plain columns, the
// shape of the audit's isolated fake-driver runs.
type perfUser struct {
	ID        int64
	Email     string
	Age       int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

var perfUserCols = []string{"id", "email", "age", "created_at", "updated_at"}

func perfUserRow() []driver.Value {
	return []driver.Value{int64(1), "u@example.com", int64(30), testNow, testNow}
}

// perfPlain and perfPtr share one column shape; only nullability differs, so
// the benchmark pair isolates the scanPtr path's per-cell cost.
type perfPlain struct {
	ID int64
	A  int64
	B  string
	C  float64
}

type perfPtr struct {
	ID int64
	A  *int64
	B  *string
	C  *float64
}

func scanBenchDB() (*loopDB, [][]driver.Value) {
	rows := make([][]driver.Value, 100)
	for i := range rows {
		rows[i] = []driver.Value{int64(i + 1), int64(i), "value-string", float64(i) / 3}
	}
	return &loopDB{cols: []string{"id", "a", "b", "c"}, rows: rows}, rows
}

func BenchmarkScan100Plain(b *testing.B) {
	l, _ := scanBenchDB()
	db := l.open(SQLite)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := From[perfPlain]().All(ctx, db)
		if err != nil || len(out) != 100 {
			b.Fatal(err, len(out))
		}
	}
}

func BenchmarkScan100Ptr(b *testing.B) {
	l, _ := scanBenchDB()
	db := l.open(SQLite)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := From[perfPtr]().All(ctx, db)
		if err != nil || len(out) != 100 {
			b.Fatal(err, len(out))
		}
	}
}

func BenchmarkInsertSQLite(b *testing.B) {
	l := &loopDB{cols: []string{"id"}, rows: [][]driver.Value{{int64(1)}}}
	db := l.open(SQLite)
	ctx := context.Background()
	u := &perfUser{Email: "u@example.com", Age: 30}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u.ID, u.CreatedAt, u.UpdatedAt = 0, time.Time{}, time.Time{}
		if err := Insert(ctx, db, u); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertMySQL(b *testing.B) {
	l := &loopDB{}
	db := l.open(MySQL)
	ctx := context.Background()
	u := &perfUser{Email: "u@example.com", Age: 30}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u.ID, u.CreatedAt, u.UpdatedAt = 0, time.Time{}, time.Time{}
		if err := Insert(ctx, db, u); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFindPostgres(b *testing.B) {
	l := &loopDB{cols: perfUserCols, rows: [][]driver.Value{perfUserRow()}}
	db := l.open(Postgres)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Find[perfUser](ctx, db, int64(1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdatePostgres(b *testing.B) {
	l := &loopDB{}
	db := l.open(Postgres)
	ctx := context.Background()
	u := &perfUser{ID: 1, Email: "u@example.com", Age: 30, CreatedAt: testNow, UpdatedAt: testNow}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Update(ctx, db, u); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeletePostgres(b *testing.B) {
	l := &loopDB{}
	db := l.open(Postgres)
	ctx := context.Background()
	u := &perfUser{ID: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Delete(ctx, db, u); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpsertPostgres(b *testing.B) {
	l := &loopDB{cols: perfUserCols, rows: [][]driver.Value{perfUserRow()}}
	db := l.open(Postgres)
	ctx := context.Background()
	u := &perfUser{ID: 1, Email: "u@example.com", Age: 30, CreatedAt: testNow, UpdatedAt: testNow}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Upsert(ctx, db, u, OnConflict("email"), DoUpdate("age")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpsertSQLite(b *testing.B) {
	l := &loopDB{cols: perfUserCols, rows: [][]driver.Value{perfUserRow()}}
	db := l.open(SQLite)
	ctx := context.Background()
	u := &perfUser{ID: 1, Email: "u@example.com", Age: 30, CreatedAt: testNow, UpdatedAt: testNow}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Upsert(ctx, db, u, OnConflict("email"), DoUpdate("age")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpsertMySQL(b *testing.B) {
	l := &loopDB{}
	db := l.open(MySQL)
	ctx := context.Background()
	u := &perfUser{ID: 1, Email: "u@example.com", Age: 30, CreatedAt: testNow, UpdatedAt: testNow}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Upsert(ctx, db, u, DoUpdate("age")); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUpsertAllSQLiteChunk upserts exactly one full SQLite chunk
// (999/5 = 199 rows with explicit IDs), the shape the batch SQL cache keys.
func BenchmarkUpsertAllSQLiteChunk(b *testing.B) {
	l := &loopDB{}
	db := l.open(SQLite)
	ctx := context.Background()
	rows := make([]perfUser, 199)
	for i := range rows {
		rows[i] = perfUser{ID: int64(i + 1), Email: "u@example.com", Age: 30, CreatedAt: testNow, UpdatedAt: testNow}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := UpsertAll(ctx, db, rows, OnConflict("email"), DoUpdate("age")); err != nil {
			b.Fatal(err)
		}
	}
}

// TestAllocDiagnostics prints AllocsPerRun for each CRUD op next to a
// hand-written database/sql equivalent on the same loop driver. Run with
// -run TestAllocDiagnostics -v; the pinned budget assertions live in
// TestCRUDAllocBudget.
func TestAllocDiagnostics(t *testing.T) {
	if testing.Short() {
		t.Skip("diagnostic only")
	}
	ctx := context.Background()

	for name, m := range allocMeasurements(ctx) {
		rio := testing.AllocsPerRun(200, m.rio)
		std := testing.AllocsPerRun(200, m.std)
		t.Logf("%-16s rio=%.0f std=%.0f extra=%+.0f", name, rio, std, rio-std)
	}
}

// TestCRUDAllocBudget pins DESIGN.md's allocation contract: entity CRUD pays
// at most 4 extra allocations per call over a hand-written database/sql
// equivalent on the same driver, and Upsert at most its conflict-shape
// machinery on top. Budgets are the values measured when this test was
// written — a regression fails loudly; an improvement is the cue to tighten
// them. Deltas, not absolute counts: database/sql's own allocations shift
// across Go releases and cancel out of the difference.
func TestCRUDAllocBudget(t *testing.T) {
	budgets := map[string]float64{
		"find/pg":           3,
		"insert/sqlite":     3, // RETURNING path
		"insert/mysql":      2, // exec + LastInsertId path
		"update/pg":         2,
		"delete/pg":         1,
		"upsert/pg":         8, // conflict shape: spec, option appends, update set, cache key
		"upsert/pg-hoisted": 8,
		"upsert/mysql":      6,
	}
	ctx := context.Background()
	pairs := allocMeasurements(ctx)
	for name, budget := range budgets {
		m, ok := pairs[name]
		if !ok {
			t.Fatalf("no measurement named %q", name)
		}
		rio := testing.AllocsPerRun(300, m.rio)
		std := testing.AllocsPerRun(300, m.std)
		if extra := rio - std; extra > budget {
			t.Errorf("%s: %.0f allocs/op vs %.0f hand-written — %+.0f extra exceeds the %+.0f budget",
				name, rio, std, extra, budget)
		}
	}
}

type allocPair struct {
	rio func()
	std func()
}

// allocMeasurements builds the rio-vs-stdlib pairs on identical loop
// drivers. Every std closure executes the byte-identical SQL rio renders,
// with equivalent argument preparation (time formatting included), so the
// difference is rio's own overhead.
func allocMeasurements(ctx context.Context) map[string]allocPair {
	fatal := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	pairs := map[string]allocPair{}

	{ // Find, Postgres
		l := &loopDB{cols: perfUserCols, rows: [][]driver.Value{perfUserRow()}}
		db, raw := l.open(Postgres), l.raw()
		const q = `SELECT "perf_users"."id", "perf_users"."email", "perf_users"."age", "perf_users"."created_at", "perf_users"."updated_at" FROM "perf_users" WHERE "perf_users"."id" = $1`
		pairs["find/pg"] = allocPair{
			rio: func() {
				_, err := Find[perfUser](ctx, db, int64(1))
				fatal(err)
			},
			std: func() {
				rows, err := raw.QueryContext(ctx, q, int64(1))
				fatal(err)
				var u perfUser
				if !rows.Next() {
					panic("no row")
				}
				fatal(rows.Scan(&u.ID, &u.Email, &u.Age, &u.CreatedAt, &u.UpdatedAt))
				fatal(rows.Close())
			},
		}
	}

	{ // Insert, SQLite RETURNING path
		l := &loopDB{cols: []string{"id"}, rows: [][]driver.Value{{int64(1)}}}
		db, raw := l.open(SQLite), l.raw()
		u := &perfUser{Email: "u@example.com", Age: 30}
		const q = `INSERT INTO "perf_users" ("email", "age", "created_at", "updated_at") VALUES (?, ?, ?, ?) RETURNING "id"`
		pairs["insert/sqlite"] = allocPair{
			rio: func() {
				u.ID, u.CreatedAt, u.UpdatedAt = 0, time.Time{}, time.Time{}
				fatal(Insert(ctx, db, u))
			},
			std: func() {
				now := time.Now().UTC().Truncate(time.Microsecond)
				ts := now.Format(sqliteTimeFormat)
				rows, err := raw.QueryContext(ctx, q, "u@example.com", int64(30), ts, ts)
				fatal(err)
				var id int64
				if !rows.Next() {
					panic("no row")
				}
				fatal(rows.Scan(&id))
				fatal(rows.Close())
			},
		}
	}

	{ // Insert, MySQL exec path
		l := &loopDB{}
		db, raw := l.open(MySQL), l.raw()
		u := &perfUser{Email: "u@example.com", Age: 30}
		const q = "INSERT INTO `perf_users` (`email`, `age`, `created_at`, `updated_at`) VALUES (?, ?, ?, ?)"
		pairs["insert/mysql"] = allocPair{
			rio: func() {
				u.ID, u.CreatedAt, u.UpdatedAt = 0, time.Time{}, time.Time{}
				fatal(Insert(ctx, db, u))
			},
			std: func() {
				now := time.Now().UTC().Truncate(time.Microsecond)
				res, err := raw.ExecContext(ctx, q, "u@example.com", int64(30), now, now)
				fatal(err)
				id, err := res.LastInsertId()
				fatal(err)
				_ = id
			},
		}
	}

	{ // Update, Postgres
		l := &loopDB{}
		db, raw := l.open(Postgres), l.raw()
		u := &perfUser{ID: 1, Email: "u@example.com", Age: 30, CreatedAt: testNow, UpdatedAt: testNow}
		const q = `UPDATE "perf_users" SET "email" = $1, "age" = $2, "updated_at" = $3 WHERE "id" = $4`
		pairs["update/pg"] = allocPair{
			rio: func() { fatal(Update(ctx, db, u)) },
			std: func() {
				now := time.Now().UTC().Truncate(time.Microsecond)
				res, err := raw.ExecContext(ctx, q, "u@example.com", int64(30), now, int64(1))
				fatal(err)
				n, err := res.RowsAffected()
				fatal(err)
				_ = n
			},
		}
	}

	{ // Delete (hard), Postgres
		l := &loopDB{}
		db, raw := l.open(Postgres), l.raw()
		u := &perfUser{ID: 1}
		const q = `DELETE FROM "perf_users" WHERE "id" = $1`
		pairs["delete/pg"] = allocPair{
			rio: func() { fatal(Delete(ctx, db, u)) },
			std: func() {
				res, err := raw.ExecContext(ctx, q, int64(1))
				fatal(err)
				n, err := res.RowsAffected()
				fatal(err)
				_ = n
			},
		}
	}

	{ // Upsert, Postgres RETURNING full row
		l := &loopDB{cols: perfUserCols, rows: [][]driver.Value{perfUserRow()}}
		db, raw := l.open(Postgres), l.raw()
		u := &perfUser{ID: 1, Email: "u@example.com", Age: 30, CreatedAt: testNow, UpdatedAt: testNow}
		const q = `INSERT INTO "perf_users" ("id", "email", "age", "created_at", "updated_at") VALUES ($1, $2, $3, $4, $5) ON CONFLICT ("email") DO UPDATE SET "age" = excluded."age", "updated_at" = excluded."updated_at" RETURNING "perf_users"."id", "perf_users"."email", "perf_users"."age", "perf_users"."created_at", "perf_users"."updated_at"`
		pairs["upsert/pg"] = allocPair{
			rio: func() { fatal(Upsert(ctx, db, u, OnConflict("email"), DoUpdate("age"))) },
			std: func() {
				now := time.Now().UTC().Truncate(time.Microsecond)
				rows, err := raw.QueryContext(ctx, q, int64(1), "u@example.com", int64(30), now, now)
				fatal(err)
				var out perfUser
				if !rows.Next() {
					panic("no row")
				}
				fatal(rows.Scan(&out.ID, &out.Email, &out.Age, &out.CreatedAt, &out.UpdatedAt))
				fatal(rows.Close())
			},
		}
	}

	{ // Upsert, Postgres, options hoisted by the caller (isolates rio's own
		// per-call overhead from the functional-option construction cost).
		l := &loopDB{cols: perfUserCols, rows: [][]driver.Value{perfUserRow()}}
		db := l.open(Postgres)
		u := &perfUser{ID: 1, Email: "u@example.com", Age: 30, CreatedAt: testNow, UpdatedAt: testNow}
		opts := []UpsertOption{OnConflict("email"), DoUpdate("age")}
		pairs["upsert/pg-hoisted"] = allocPair{
			rio: func() { fatal(Upsert(ctx, db, u, opts...)) },
			std: pairs["upsert/pg"].std,
		}
	}

	{ // Upsert, MySQL exec path
		l := &loopDB{}
		db, raw := l.open(MySQL), l.raw()
		u := &perfUser{ID: 1, Email: "u@example.com", Age: 30, CreatedAt: testNow, UpdatedAt: testNow}
		const q = "INSERT INTO `perf_users` (`id`, `email`, `age`, `created_at`, `updated_at`) VALUES (?, ?, ?, ?, ?) AS _rio_new ON DUPLICATE KEY UPDATE `age` = _rio_new.`age`, `updated_at` = _rio_new.`updated_at`"
		pairs["upsert/mysql"] = allocPair{
			rio: func() { fatal(Upsert(ctx, db, u, DoUpdate("age"))) },
			std: func() {
				now := time.Now().UTC().Truncate(time.Microsecond)
				res, err := raw.ExecContext(ctx, q, int64(1), "u@example.com", int64(30), now, now)
				fatal(err)
				n, err := res.RowsAffected()
				fatal(err)
				_ = n
			},
		}
	}

	return pairs
}
