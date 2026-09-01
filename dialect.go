package rio

import (
	"errors"
	"time"
)

// sqliteTimeFormat is rio's canonical SQLite time encoding: sortable text,
// accepted by SQLite's date functions; values are already UTC (bindArg).
const sqliteTimeFormat = "2006-01-02 15:04:05.999999+00:00"

// Dialect identifies one of the built-in SQL dialects. All methods are
// unexported: driver modules pick a built-in value, never implement one.
type Dialect interface {
	name() string
	lexer() lexProfile
	style() bindStyle
	caps() dialectCaps
	// quote appends the quoted identifier; dotted names quote per segment.
	quote(b []byte, ident string) []byte
	// translate maps a driver error to a rio sentinel, or nil if unknown;
	// driver modules install precise translators beyond SQLSTATE.
	translate(err error) error
	// bindTime converts a time.Time into the driver-facing bind value.
	bindTime(t time.Time) any
}

// Built-in dialects: driver modules select one, and New and NewNative take it.
var (
	// Postgres renders $n placeholders, RETURNING, and ON CONFLICT (columns),
	// and binds a typed key slice as one array parameter.
	Postgres Dialect = postgresDialect{}
	// MySQL renders ? placeholders and ON DUPLICATE KEY UPDATE; it has no
	// RETURNING and no conflict target.
	MySQL Dialect = mysqlDialect{}
	// SQLite renders ? placeholders, RETURNING, and ON CONFLICT (columns);
	// ForUpdate is elided because the whole database locks, times bind as
	// sqliteTimeFormat text, and statements chunk under 999 parameters.
	SQLite Dialect = sqliteDialect{}
	// ClickHouse is append-only OLAP: no transactions, row locks, unique keys,
	// or generated keys, and every argument interpolates client-side; see
	// clickhouseDialect.caps for the exact surface.
	ClickHouse Dialect = clickhouseDialect{}
)

// forUpdateMode is what a dialect does with Query.ForUpdate.
type forUpdateMode uint8

const (
	forUpdateRender forUpdateMode = iota // render FOR UPDATE (PostgreSQL, MySQL)
	forUpdateElide                       // omit: whole-db locking is equivalent (SQLite)
	forUpdateReject                      // error: no row locks exist at all (ClickHouse)
)

type dialectCaps struct {
	returning      bool          // INSERT/UPDATE/DELETE ... RETURNING
	conflictTarget bool          // ON CONFLICT (cols) vs MySQL's ON DUPLICATE KEY
	forUpdate      forUpdateMode // SELECT ... FOR UPDATE: render, elide, or reject
	maxBindParams  int           // chunk size ceiling for IN expansion and multi-VALUES
	mutations      bool          // UPDATE/DELETE exist with an honest RowsAffected
	transactions   bool          // BEGIN/COMMIT/ROLLBACK are real, not driver shims
	uniqueKeys     bool          // unique constraints exist: upserts and conflict arbitration
	autoIncrPK     bool          // the database can generate the conventional ID
	stmtPrepare    bool          // the driver prepares arbitrary statements (stmt cache)
	finalTable     bool          // FROM t FINAL merges row versions at read (ClickHouse)

	// bindBytesAsString rebinds []byte arguments as strings: the ClickHouse channel
	// would interpolate them as Array(UInt8) literals.
	bindBytesAsString bool
	// bindUint64 passes uint64 values above MaxInt64 through; other channels
	// take them as decimal text because database/sql rejects them.
	bindUint64 bool
	// arrayBind binds a typed key slice as one array parameter (= ANY(?))
	// instead of expanding it: one bind, a key-count-independent statement.
	arrayBind bool
}

type postgresDialect struct{}

func (postgresDialect) name() string             { return "postgres" }
func (postgresDialect) lexer() lexProfile        { return pgLex }
func (postgresDialect) style() bindStyle         { return bindDollar }
func (postgresDialect) bindTime(t time.Time) any { return t }

func (postgresDialect) caps() dialectCaps {
	return dialectCaps{
		returning: true, conflictTarget: true, forUpdate: forUpdateRender, maxBindParams: 65535,
		mutations: true, transactions: true, uniqueKeys: true, autoIncrPK: true, stmtPrepare: true,
		arrayBind: true,
	}
}

func (postgresDialect) quote(b []byte, ident string) []byte {
	return quoteWith(b, ident, '"')
}

func (postgresDialect) translate(err error) error {
	switch sqlState(err) {
	case "23505":
		return ErrDuplicateKey
	case "23503":
		return ErrForeignKeyViolated
	}
	return nil
}

type mysqlDialect struct{}

func (mysqlDialect) name() string             { return "mysql" }
func (mysqlDialect) lexer() lexProfile        { return mysqlLex }
func (mysqlDialect) style() bindStyle         { return bindQuestion }
func (mysqlDialect) bindTime(t time.Time) any { return t }

func (mysqlDialect) caps() dialectCaps {
	return dialectCaps{
		returning: false, conflictTarget: false, forUpdate: forUpdateRender, maxBindParams: 65535,
		mutations: true, transactions: true, uniqueKeys: true, autoIncrPK: true, stmtPrepare: true,
	}
}

func (mysqlDialect) quote(b []byte, ident string) []byte {
	return quoteWith(b, ident, '`')
}

func (mysqlDialect) translate(error) error {
	// MySQL folds unique and FK violations into SQLSTATE 23000; the
	// go-rio/mysql module installs the precise errno-based translator.
	return nil
}

type sqliteDialect struct{}

func (sqliteDialect) name() string      { return "sqlite" }
func (sqliteDialect) lexer() lexProfile { return sqliteLex }
func (sqliteDialect) style() bindStyle  { return bindQuestion }

func (sqliteDialect) bindTime(t time.Time) any {
	return t.Format(sqliteTimeFormat)
}

func (sqliteDialect) caps() dialectCaps {
	// 999 is stock SQLite's bind ceiling (modernc allows 32766); the
	// conservative cap keeps chunked statements valid everywhere.
	return dialectCaps{
		returning: true, conflictTarget: true, forUpdate: forUpdateElide, maxBindParams: 999,
		mutations: true, transactions: true, uniqueKeys: true, autoIncrPK: true, stmtPrepare: true,
	}
}

func (sqliteDialect) quote(b []byte, ident string) []byte {
	return quoteWith(b, ident, '"')
}

func (sqliteDialect) translate(err error) error {
	// modernc.org/sqlite errors expose Code() int; the interface probe
	// keeps the core dependency-free.
	var coder interface{ Code() int }
	if !errors.As(err, &coder) {
		return nil
	}
	switch coder.Code() {
	case 1555, 2067: // SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_UNIQUE
		return ErrDuplicateKey
	case 787: // SQLITE_CONSTRAINT_FOREIGNKEY
		return ErrForeignKeyViolated
	}
	return nil
}

func quoteWith(b []byte, ident string, q byte) []byte {
	b = append(b, q)
	start := 0
	for i := range len(ident) {
		switch ident[i] {
		case q: // double the quote character
			b = append(b, ident[start:i+1]...)
			b = append(b, q)
			start = i + 1
		case '.': // quote each dotted segment separately
			b = append(b, ident[start:i]...)
			b = append(b, q, '.', q)
			start = i + 1
		}
	}
	b = append(b, ident[start:]...)
	return append(b, q)
}
