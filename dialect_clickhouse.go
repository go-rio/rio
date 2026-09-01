package rio

import (
	"fmt"
	"time"
)

// ClickHouse's DateTime64(6) binding range.
var (
	chTimeMin = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	chTimeMax = time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)
)

type clickhouseDialect struct{}

func (clickhouseDialect) name() string      { return "clickhouse" }
func (clickhouseDialect) lexer() lexProfile { return chLex }
func (clickhouseDialect) style() bindStyle  { return bindQuestionEsc }

// Times pass through; the driver renders the epoch-microsecond form the
// primary-key range analyzer accepts.
func (clickhouseDialect) bindTime(t time.Time) any { return t }

func (clickhouseDialect) caps() dialectCaps {
	// Append-only OLAP: mutations are asynchronous with no affected-row
	// count, Begin is a no-op shim, unique keys and generated primary keys
	// do not exist, and Prepare only covers INSERT batching. maxBindParams
	// is a client-side text budget (every argument is interpolated), sized
	// to keep IN expansions under the default 256 KiB max_query_size.
	return dialectCaps{forUpdate: forUpdateReject, maxBindParams: 8192, finalTable: true, bindBytesAsString: true}
}

func (clickhouseDialect) quote(b []byte, ident string) []byte {
	// Not quoteWith: ClickHouse honors backslash escapes inside quoted
	// identifiers, so a literal backslash must be doubled.
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
	// error maps to a rio sentinel; the go-rio/clickhouse module installs
	// none either.
	return nil
}

// checkBindTime validates a normalized time against the dialect's range.
func checkBindTime(d Dialect, nt time.Time) error {
	if d.name() != "clickhouse" || !(nt.Before(chTimeMin) || nt.After(chTimeMax)) {
		return nil
	}
	return fmt.Errorf(
		"rio: time %s is outside ClickHouse's DateTime64 range [0001-01-01, 9999-12-31]",
		nt.Format(time.RFC3339Nano),
	)
}
