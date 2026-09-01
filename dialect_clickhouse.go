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

// Times pass through; the driver renders toDateTime64 literals, which every
// time column accepts and the primary-key range analyzer folds.
func (clickhouseDialect) bindTime(t time.Time) any { return t }

func (clickhouseDialect) caps() dialectCaps {
	// Async mutations report no row count, Begin is a shim, unique and generated
	// keys do not exist, and Prepare covers only INSERT batching. maxBindParams
	// budgets interpolated argument text under the default 256 KiB max_query_size.
	return dialectCaps{
		forUpdate: forUpdateReject, maxBindParams: 8192, finalTable: true,
		bindBytesAsString: true, bindUint64: true,
	}
}

func (clickhouseDialect) quote(b []byte, ident string) []byte {
	// Not quoteWith: ClickHouse honors backslash escapes inside quoted
	// identifiers, so a literal backslash must be doubled.
	b = append(b, '`')
	for i := range len(ident) {
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
	// No unique or foreign key constraints exist, so no server error maps to a
	// sentinel; the go-rio/clickhouse module installs no translator either.
	return nil
}

// checkBindTime rejects, for ClickHouse only, a normalized time outside the
// DateTime64 range.
func checkBindTime(d Dialect, nt time.Time) error {
	if d.name() != "clickhouse" {
		return nil
	}
	inRange := !nt.Before(chTimeMin) && !nt.After(chTimeMax)
	if inRange {
		return nil
	}
	return fmt.Errorf(
		"rio: time %s is outside ClickHouse's DateTime64 range [0001-01-01, 9999-12-31]",
		nt.Format(time.RFC3339Nano),
	)
}
