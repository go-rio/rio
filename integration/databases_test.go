package integration

import (
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

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
	runV02Suite(t, sqliteDB(t), "sqlite")
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
