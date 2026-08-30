package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-rio/postgres"
	"github.com/go-rio/rio"
	"github.com/go-rio/rio/lint"
	"github.com/go-sql-driver/mysql"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

func sqliteDB(t *testing.T) *rio.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", "file:rio_it?mode=memory&cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(1) // shared in-memory DB lives as long as one conn does
	db := rio.New(raw, rio.SQLite)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSQLiteSuite(t *testing.T) {
	runSuite(t, sqliteDB(t), "sqlite")
}

func TestSQLiteV02Suite(t *testing.T) {
	db := sqliteDB(t)
	runV02Suite(t, db, "sqlite")
	runV03Sync(t, db, "sqlite")
	runHardening(t, db, "sqlite")
	runHardDelete(t, db, "sqlite")
}

// sql.NullTime round-trips through a TEXT column: the write side binds rio's
// own text encoding, and without a DATETIME decltype the driver hands the
// text straight back — the read side must parse it, not delegate to
// NullTime.Scan (which rejects strings). Storage parity with time.Time no
// longer depends on the column's declared type.
func TestSQLiteNullTimeTextColumnRoundTrip(t *testing.T) {
	db := sqliteDB(t)
	ctx := context.Background()
	if _, err := rio.Exec(ctx, db, "CREATE TABLE null_time_rows (id INTEGER PRIMARY KEY, maybe TEXT)"); err != nil {
		t.Fatal(err)
	}

	type nullTimeRow struct {
		ID    int64
		Maybe sql.NullTime
	}
	at := time.Date(2026, 7, 9, 3, 4, 5, 123456000, time.UTC)
	if err := rio.Insert(ctx, db, &nullTimeRow{ID: 1, Maybe: sql.NullTime{Time: at, Valid: true}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := rio.Insert(ctx, db, &nullTimeRow{ID: 2}); err != nil {
		t.Fatalf("insert null: %v", err)
	}

	rows, err := rio.Raw[nullTimeRow]("SELECT id, maybe FROM null_time_rows ORDER BY id").All(ctx, db)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !rows[0].Maybe.Valid || !rows[0].Maybe.Time.Equal(at) {
		t.Fatalf("TEXT column must round-trip: %+v", rows[0].Maybe)
	}
	if rows[1].Maybe.Valid {
		t.Fatalf("NULL must stay invalid: %+v", rows[1].Maybe)
	}

	// An expression column has no decltype either; MAX() carries the text.
	got, err := rio.Raw[sql.NullTime]("SELECT MAX(maybe) FROM null_time_rows").First(ctx, db)
	if err != nil || !got.Valid || !got.Time.Equal(at) {
		t.Fatalf("expression column must parse too: %+v err=%v", got, err)
	}
}

// TestSQLiteRawRowsStream is a leak smoke for RawQuery.Rows: stream a few rows,
// break early, and prove the connection was returned by running more work on
// the same single-connection in-memory database afterwards — a leaked cursor
// would wedge it.
func TestSQLiteRawRowsStream(t *testing.T) {
	db := sqliteDB(t)
	ctx := context.Background()
	if _, err := rio.Exec(ctx, db, "CREATE TABLE stream_rows (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := rio.Exec(ctx, db, "INSERT INTO stream_rows (id, n) VALUES (?, ?)", i, i*10); err != nil {
			t.Fatal(err)
		}
	}

	type streamRow struct {
		ID int64
		N  int64
	}
	// Early break after two rows: RawQuery.Rows must close the underlying
	// cursor on break.
	var seen []int64
	for r, err := range rio.Raw[streamRow]("SELECT id, n FROM stream_rows ORDER BY id").Rows(ctx, db) {
		if err != nil {
			t.Fatalf("raw rows: %v", err)
		}
		seen = append(seen, r.ID)
		if len(seen) == 2 {
			break
		}
	}
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 2 {
		t.Fatalf("early-break stream: %v", seen)
	}

	// No leak: on a MaxOpenConns(1) in-memory DB a leaked cursor would block
	// this forever. It returns, so Rows released the connection on break.
	cnt, err := rio.Raw[int64]("SELECT count(*) FROM stream_rows").First(ctx, db)
	if err != nil || cnt == nil || *cnt != 5 {
		t.Fatalf("follow-up query after early break: cnt=%v err=%v", cnt, err)
	}

	// And a full drain still works and reads every row in order.
	full := 0
	for r, err := range rio.Raw[streamRow]("SELECT id, n FROM stream_rows ORDER BY id").Rows(ctx, db) {
		if err != nil {
			t.Fatalf("raw rows (full drain): %v", err)
		}
		full++
		if r.ID != int64(full) || r.N != int64(full*10) {
			t.Fatalf("row %d drifted: %+v", full, r)
		}
	}
	if full != 5 {
		t.Fatalf("full drain saw %d rows, want 5", full)
	}
}

func TestPostgresSuite(t *testing.T) {
	dsn := os.Getenv("RIO_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("RIO_POSTGRES_DSN not set")
	}
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := rio.New(raw, rio.Postgres)
	t.Cleanup(func() { _ = db.Close() })
	runSuite(t, db, "postgres")
	runV02Suite(t, db, "postgres")
	runV03Sync(t, db, "postgres")
	runHardening(t, db, "postgres")
	runHardDelete(t, db, "postgres")
	runPostgresJSONB(t, db)
	runPostgresTextArray(t, db)
}

// TestPostgresNativeSuite replays the entire PostgreSQL suite through the
// pgx-native channel (postgres.OpenNative): same DSN, same schema, same
// assertions. The double run is the design's keystone test — every rio
// semantic the stdlib channel passes must hold natively too.
func TestPostgresNativeSuite(t *testing.T) {
	dsn := os.Getenv("RIO_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("RIO_POSTGRES_DSN not set")
	}
	db, err := postgres.OpenNative(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	runSuite(t, db, "postgres")
	runV02Suite(t, db, "postgres")
	runV03Sync(t, db, "postgres")
	runHardening(t, db, "postgres")
	runHardDelete(t, db, "postgres")
	// jsonb holds natively too; the text[] wrapper is stdlib-only (native
	// hands a bare Scanner the binary array wire format — see the runner).
	runPostgresJSONB(t, db)
}

func TestMySQLSuite(t *testing.T) {
	dsn := os.Getenv("RIO_MYSQL_DSN")
	if dsn == "" {
		t.Skip("RIO_MYSQL_DSN not set")
	}
	raw, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	// MySQL errors carry no probe-able interface, so precise translation is
	// the driver module's job — github.com/go-rio/mysql installs this for
	// you; here the suite drives the core directly and installs its own.
	db := rio.New(raw, rio.MySQL, rio.WithErrorTranslator(func(err error) error {
		var me *mysql.MySQLError
		if !errors.As(err, &me) {
			return nil
		}
		switch me.Number {
		case 1062:
			return rio.ErrDuplicateKey
		case 1451, 1452:
			return rio.ErrForeignKeyViolated
		}
		return nil
	}))
	t.Cleanup(func() { _ = db.Close() })
	runSuite(t, db, "mysql")
	runV02Suite(t, db, "mysql")
	runV03Sync(t, db, "mysql")
	runHardening(t, db, "mysql")
	runHardDelete(t, db, "mysql")
}

// TestModerncTimeProbe pins how the modernc driver round-trips rio's own
// time encoding. If a driver upgrade changes scan types or formats, this
// fails before any user does.
func TestModerncTimeProbe(t *testing.T) {
	db := sqliteDB(t)
	ctx := t.Context()

	if _, err := rio.Exec(ctx, db, "CREATE TABLE probes (id INTEGER PRIMARY KEY, at DATETIME, txt TEXT)"); err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 7, 9, 3, 4, 5, 123456000, time.UTC)

	type probe struct {
		ID  int64
		At  time.Time
		Txt *string
	}
	if err := rio.Insert(ctx, db, &probe{ID: 1, At: want}); err != nil {
		t.Fatal(err)
	}
	got, err := rio.Find[probe](ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !got.At.Equal(want.Truncate(time.Microsecond)) {
		t.Fatalf("time round-trip drifted: wrote %v, read %v", want, got.At)
	}

	// The stored text must be rio's own canonical format, parseable by
	// SQLite's date functions — independent of driver time handling.
	raw, err := rio.Raw[*string]("SELECT datetime(at) FROM probes WHERE id = 1").First(ctx, db)
	if err != nil {
		t.Fatalf("reading datetime(at): %v", err)
	}
	if *raw == nil || **raw == "" {
		t.Fatal("SQLite date functions must parse the stored value; datetime() returned NULL")
	}
}

// Cursor pagination walks the whole set without gaps or repeats — heavy
// ties on the leading key make the PK tie-breaker do real work, and every
// page resumes through the string token round-trip.
func TestSQLiteCursorPaginationWalk(t *testing.T) {
	db := sqliteDB(t)
	ctx := context.Background()
	if _, err := rio.Exec(ctx, db, `CREATE TABLE walk_items (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		score INTEGER NOT NULL,
		name TEXT NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	type walkItem struct {
		ID    int64
		Score int64
		Name  string
	}
	for i := range 100 {
		if _, err := rio.Exec(ctx, db,
			"INSERT INTO walk_items (score, name) VALUES (?, ?)",
			i%7, fmt.Sprintf("n%02d", i%13),
		); err != nil {
			t.Fatal(err)
		}
	}

	q := rio.From[walkItem]().OrderKeys(
		rio.SortKey{Column: "score", Desc: true},
		rio.SortKey{Column: "name"},
	)
	full, err := q.All(ctx, db)
	if err != nil || len(full) != 100 {
		t.Fatalf("full walk: len=%d err=%v", len(full), err)
	}

	var walked []int64
	page := q.Limit(9)
	var token string
	for {
		p := page
		if token != "" {
			cur, err := rio.ParseCursor(token)
			if err != nil {
				t.Fatalf("ParseCursor: %v", err)
			}
			p = page.After(cur)
		}
		rows, err := p.All(ctx, db)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		if len(rows) == 0 {
			break
		}
		for _, r := range rows {
			walked = append(walked, r.ID)
		}
		cur, err := page.CursorAfter(&rows[len(rows)-1])
		if err != nil {
			t.Fatalf("CursorAfter: %v", err)
		}
		token = cur.String()
		if len(rows) < 9 {
			break
		}
	}

	if len(walked) != 100 {
		t.Fatalf("walked %d rows, want 100", len(walked))
	}
	seen := make(map[int64]bool, 100)
	for i, id := range walked {
		if seen[id] {
			t.Fatalf("row %d repeated at position %d", id, i)
		}
		seen[id] = true
		if id != full[i].ID {
			t.Fatalf("position %d: walked %d, full scan %d — the orders drifted", i, id, full[i].ID)
		}
	}
}

// The drift lint reads the live schema and reports exactly the decidable
// disagreements: every finding kind fires once against a deliberately
// skewed table, and a clean table reports nothing.
func TestSQLiteSchemaDriftLint(t *testing.T) {
	db := sqliteDB(t)
	ctx := context.Background()

	type DriftItem struct {
		ID      int64
		Name    string
		Missing string // not in the table
		Wrong   int64  // nullable in the table, non-pointer here
	}
	if _, err := rio.Exec(ctx, db, `CREATE TABLE drift_items (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		wrong INTEGER,
		legacy TEXT
	)`); err != nil {
		t.Fatal(err)
	}

	report, err := lint.Check(ctx, db, DriftItem{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	kinds := map[string]int{}
	for _, f := range report.Findings {
		kinds[f.Kind]++
	}
	if kinds["missing-column"] != 1 || kinds["nullability"] != 1 || kinds["extra-column"] != 1 {
		t.Fatalf("finding kinds drifted: %+v\n%+v", kinds, report.Findings)
	}
	if n := len(report.Errors()); n != 1 { // the missing column
		t.Fatalf("errors = %d, want 1: %+v", n, report.Errors())
	}

	type CleanItem struct {
		ID   int64
		Name string
	}
	if _, err := rio.Exec(ctx, db,
		"CREATE TABLE clean_items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	clean, err := lint.Check(ctx, db, CleanItem{})
	if err != nil {
		t.Fatalf("Check clean: %v", err)
	}
	if len(clean.Findings) != 0 {
		t.Fatalf("a matching schema must report nothing: %+v", clean.Findings)
	}

	missing, err := lint.Check(ctx, db, struct {
		ID int64
	}{})
	if err != nil {
		t.Fatalf("Check missing: %v", err)
	}
	if len(missing.Findings) != 1 || missing.Findings[0].Kind != "missing-table" {
		t.Fatalf("a missing table is one loud error: %+v", missing.Findings)
	}
}

// The soft-delete state machine holds under arbitrary Delete/Restore
// interleavings, stale versions included: a deterministic random walk
// compares every step's error, write-back, and stored row against a pure
// in-memory model of the documented semantics.
func TestSQLiteSoftDeleteStateMachine(t *testing.T) {
	db := sqliteDB(t)
	ctx := context.Background()
	if _, err := rio.Exec(ctx, db, `CREATE TABLE prop_items (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		version INTEGER NOT NULL,
		deleted_at DATETIME,
		updated_at DATETIME NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	type propItem struct {
		ID        int64
		Version   int64      `rio:",version"`
		DeletedAt *time.Time `rio:",softdelete"`
		UpdatedAt time.Time
	}

	for seed := int64(1); seed <= 3; seed++ {
		rng := rand.New(rand.NewSource(seed))
		row := &propItem{}
		if err := rio.Insert(ctx, db, row); err != nil {
			t.Fatal(err)
		}
		// The model mirrors the documented semantics.
		model := struct {
			trashed bool
			version int64
			stamp   time.Time
		}{version: row.Version}

		for step := range 200 {
			// Half the time act on a stale snapshot: the stored version
			// minus one, which never matches a live row.
			attempt := &propItem{ID: row.ID, Version: model.version}
			stale := rng.Intn(2) == 0
			if stale {
				attempt.Version = model.version - 1
			}
			del := rng.Intn(2) == 0

			var err error
			if del {
				err = rio.Delete(ctx, db, attempt)
			} else {
				err = rio.Restore(ctx, db, attempt)
			}

			// What the documented semantics demand of this step:
			// already in the target state → idempotent success adopting the
			// stored stamp and version, stale or not; otherwise a stale
			// version is a conflict, and a fresh one performs the write.
			switch {
			case del && model.trashed:
				if err != nil {
					t.Fatalf("seed %d step %d: repeat Delete: %v", seed, step, err)
				}
				if attempt.Version != model.version || attempt.DeletedAt == nil || !attempt.DeletedAt.Equal(model.stamp) {
					t.Fatalf("seed %d step %d: Delete must adopt stored state: %+v vs %+v", seed, step, attempt, model)
				}
			case !del && !model.trashed:
				if err != nil {
					t.Fatalf("seed %d step %d: repeat Restore: %v", seed, step, err)
				}
				if attempt.Version != model.version || attempt.DeletedAt != nil {
					t.Fatalf("seed %d step %d: Restore must adopt live state: %+v vs %+v", seed, step, attempt, model)
				}
			case stale:
				if !errors.Is(err, rio.ErrStaleObject) {
					t.Fatalf("seed %d step %d: stale write must conflict, got %v", seed, step, err)
				}
			case del:
				if err != nil {
					t.Fatalf("seed %d step %d: Delete: %v", seed, step, err)
				}
				model.trashed = true
				model.version++
				model.stamp = *attempt.DeletedAt
			default:
				if err != nil {
					t.Fatalf("seed %d step %d: Restore: %v", seed, step, err)
				}
				model.trashed = false
				model.version++
			}

			// The stored row must match the model exactly.
			stored, err := rio.From[propItem]().WithTrashed().Where("id = ?", row.ID).First(ctx, db)
			if err != nil {
				t.Fatalf("seed %d step %d: read back: %v", seed, step, err)
			}
			if stored.Version != model.version || (stored.DeletedAt != nil) != model.trashed {
				t.Fatalf("seed %d step %d: stored %+v drifted from model %+v", seed, step, stored, model)
			}
			if model.trashed && !stored.DeletedAt.Equal(model.stamp) {
				t.Fatalf("seed %d step %d: deletion stamp drifted: %v vs %v", seed, step, stored.DeletedAt, model.stamp)
			}
		}
	}
}
