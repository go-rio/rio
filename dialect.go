package rio

import (
	"errors"
	"time"
)

// Dialect identifies one of the built-in SQL dialects. The interface is
// deliberately opaque (all methods unexported): rio's renderers evolve
// together with the grammars, and a cross-module implementation would freeze
// them. Driver modules pick a built-in value; they never implement this.
type Dialect interface {
	name() string
	lexer() lexProfile
	style() bindStyle
	caps() dialectCaps
	// quote appends the quoted identifier. Dotted names are quoted per
	// segment ("analytics"."events") so schema-qualified tables work.
	quote(b []byte, ident string) []byte
	// translate maps a driver error to a rio sentinel, or nil if unknown.
	// Only SQLSTATE-carrying errors are recognized here; driver modules
	// install precise translators for everything else.
	translate(err error) error
	// bindTime converts a time.Time into the driver-facing bind value.
	bindTime(t time.Time) any
}

type dialectCaps struct {
	returning      bool // INSERT/UPDATE/DELETE ... RETURNING
	conflictTarget bool // ON CONFLICT (cols) vs MySQL's ON DUPLICATE KEY
	forUpdate      bool // SELECT ... FOR UPDATE (SQLite: whole-db locking, no-op)
	maxBindParams  int  // chunk size ceiling for IN expansion and multi-VALUES
}

// Built-in dialects. Driver modules re-export the matching value from their
// Open/New constructors.
var (
	Postgres Dialect = postgresDialect{}
	MySQL    Dialect = mysqlDialect{}
	SQLite   Dialect = sqliteDialect{}
)

// sqliteTimeFormat is rio's own canonical text encoding for SQLite time
// columns: lexicographically sortable, accepted by SQLite's date functions,
// and independent of the driver's optional time handling. Values are always
// UTC by the time they reach a dialect (see bindArg).
const sqliteTimeFormat = "2006-01-02 15:04:05.999999+00:00"

func quoteWith(b []byte, ident string, q byte) []byte {
	b = append(b, q)
	start := 0
	for i := 0; i < len(ident); i++ {
		switch ident[i] {
		case q: // double the quote character inside the identifier
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

// --- PostgreSQL ---

type postgresDialect struct{}

func (postgresDialect) name() string             { return "postgres" }
func (postgresDialect) lexer() lexProfile        { return pgLex }
func (postgresDialect) style() bindStyle         { return bindDollar }
func (postgresDialect) bindTime(t time.Time) any { return t }

func (postgresDialect) caps() dialectCaps {
	return dialectCaps{returning: true, conflictTarget: true, forUpdate: true, maxBindParams: 65535}
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

// --- MySQL ---

type mysqlDialect struct{}

func (mysqlDialect) name() string             { return "mysql" }
func (mysqlDialect) lexer() lexProfile        { return mysqlLex }
func (mysqlDialect) style() bindStyle         { return bindQuestion }
func (mysqlDialect) bindTime(t time.Time) any { return t }

func (mysqlDialect) caps() dialectCaps {
	return dialectCaps{returning: false, conflictTarget: false, forUpdate: true, maxBindParams: 65535}
}

func (mysqlDialect) quote(b []byte, ident string) []byte {
	return quoteWith(b, ident, '`')
}

func (mysqlDialect) translate(error) error {
	// MySQL folds both unique and FK violations into SQLSTATE 23000, so no
	// honest state-based mapping exists, and go-sql-driver errors carry no
	// SQLState accessor anyway; the go-rio/mysql module installs the precise
	// errno-based translator.
	return nil
}

// --- SQLite ---

type sqliteDialect struct{}

func (sqliteDialect) name() string      { return "sqlite" }
func (sqliteDialect) lexer() lexProfile { return sqliteLex }
func (sqliteDialect) style() bindStyle  { return bindQuestion }

func (sqliteDialect) bindTime(t time.Time) any {
	return t.Format(sqliteTimeFormat)
}

func (sqliteDialect) caps() dialectCaps {
	// modernc.org/sqlite compiles with SQLITE_MAX_VARIABLE_NUMBER=32766;
	// stock SQLite builds cap at 999. The conservative ceiling keeps chunked
	// statements valid everywhere and costs a few extra round-trips at most.
	return dialectCaps{returning: true, conflictTarget: true, forUpdate: false, maxBindParams: 999}
}

func (sqliteDialect) quote(b []byte, ident string) []byte {
	return quoteWith(b, ident, '"')
}

func (sqliteDialect) translate(err error) error {
	// modernc.org/sqlite errors expose Code() int; probing the interface
	// keeps the core dependency-free while translating out of the box.
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
