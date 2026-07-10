package rio

import (
	"errors"
	"fmt"
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
}

// Built-in dialects. Driver modules re-export the matching value from their
// Open/New constructors.
var (
	Postgres   Dialect = postgresDialect{}
	MySQL      Dialect = mysqlDialect{}
	SQLite     Dialect = sqliteDialect{}
	ClickHouse Dialect = clickhouseDialect{}
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
	return dialectCaps{
		returning: true, conflictTarget: true, forUpdate: forUpdateRender, maxBindParams: 65535,
		mutations: true, transactions: true, uniqueKeys: true, autoIncrPK: true, stmtPrepare: true,
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

// --- MySQL ---

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
	return dialectCaps{
		returning: true, conflictTarget: true, forUpdate: forUpdateElide, maxBindParams: 999,
		mutations: true, transactions: true, uniqueKeys: true, autoIncrPK: true, stmtPrepare: true,
	}
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

// --- ClickHouse ---

// chTimeFormat is rio's canonical time encoding for ClickHouse: wall-clock
// text with a fixed six-digit fraction and an explicit UTC offset. The offset
// overrides the column's timezone attribute during parsing, so the same
// instant lands in DateTime64(6) and DateTime64(6, 'Asia/Shanghai') alike —
// unlike bare wall-clock text (read in the column's zone) or fractional
// epoch strings (which cannot express pre-1970 instants). Text at all
// because clickhouse-go's client-side binder truncates a time.Time argument
// to whole seconds; the fixed fraction keeps the behavior uniform per column
// type instead of per value.
const chTimeFormat = "2006-01-02 15:04:05.000000+00:00"

// ClickHouse's DateTime64 range. The server silently clamps values outside
// it to the boundary — even on INSERT — so rio's bind funnels reject them
// loudly instead (checkBindTime).
var (
	chTimeMin = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	chTimeMax = time.Date(2299, 12, 31, 23, 59, 59, 999999000, time.UTC)
)

// checkBindTime validates a normalized time against the dialect's storable
// range. Only ClickHouse needs policing: PostgreSQL and MySQL reject
// out-of-range times loudly on their own, ClickHouse clamps silently.
func checkBindTime(d Dialect, nt time.Time) error {
	if d.name() != "clickhouse" || !(nt.Before(chTimeMin) || nt.After(chTimeMax)) {
		return nil
	}
	if nt.IsZero() {
		return errors.New(`rio: a zero time.Time is outside ClickHouse's DateTime64 range [1900-01-01, 2299-12-31] and would be silently clamped; use a *time.Time (Nullable column) for "no value"`)
	}
	return fmt.Errorf("rio: time %s is outside ClickHouse's DateTime64 range [1900-01-01, 2299-12-31] and would be silently clamped", nt.Format(time.RFC3339Nano))
}

type clickhouseDialect struct{}

func (clickhouseDialect) name() string      { return "clickhouse" }
func (clickhouseDialect) lexer() lexProfile { return chLex }
func (clickhouseDialect) style() bindStyle  { return bindQuestionEsc }

func (clickhouseDialect) bindTime(t time.Time) any { return t.Format(chTimeFormat) }

func (clickhouseDialect) caps() dialectCaps {
	// ClickHouse is an append-only OLAP dialect: UPDATE/DELETE are
	// asynchronous mutations without an affected-row count (and the driver
	// reports RowsAffected 0 unconditionally), clickhouse-go's Begin is a
	// no-op shim for its batch API, unique constraints and generated primary
	// keys do not exist, and Prepare only works for INSERT batching.
	// maxBindParams is a text budget, not a protocol limit — every argument
	// is interpolated into the statement client-side: 8192 keeps IN
	// expansions under the server's default 256 KiB max_query_size, while
	// multi-VALUES INSERT data is exempt from that limit entirely.
	return dialectCaps{forUpdate: forUpdateReject, maxBindParams: 8192, finalTable: true}
}

func (clickhouseDialect) quote(b []byte, ident string) []byte {
	// Backticks, ClickHouse's conventional identifier quoting — but not via
	// quoteWith: ClickHouse honors backslash escapes inside quoted
	// identifiers (one lexer routine serves ', " and `), so a literal
	// backslash must be doubled or it would swallow the closing quote.
	b = append(b, '`')
	for i := 0; i < len(ident); i++ {
		switch c := ident[i]; c {
		case '`':
			b = append(b, '`', '`')
		case '\\':
			b = append(b, '\\', '\\')
		case '.': // quote each dotted segment separately
			b = append(b, '`', '.', '`')
		default:
			b = append(b, c)
		}
	}
	return append(b, '`')
}

func (clickhouseDialect) translate(error) error {
	// ClickHouse has no unique or foreign key constraints, so no server
	// error honestly maps to ErrDuplicateKey or ErrForeignKeyViolated — and
	// *clickhouse.Exception carries its code in a struct field, out of reach
	// of a dependency-free interface probe anyway. The go-rio/clickhouse
	// module installs no translator either, for the same reason.
	return nil
}
