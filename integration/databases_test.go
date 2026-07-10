package integration

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/go-rio/postgres"
	"github.com/go-rio/rio"
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
