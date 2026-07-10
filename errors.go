package rio

import (
	"database/sql"
	"errors"
	"fmt"
)

// Sentinel errors. Every error returned by rio can be inspected with
// errors.Is; none of them are ever logged by rio itself.
var (
	// ErrNotFound is returned by First, Find, Sole and friends when no row
	// matches. It wraps sql.ErrNoRows, so both
	// errors.Is(err, rio.ErrNotFound) and errors.Is(err, sql.ErrNoRows) hold.
	ErrNotFound = fmt.Errorf("rio: record not found (%w)", sql.ErrNoRows)

	// ErrMultipleRows is returned by Sole when more than one row matches.
	ErrMultipleRows = errors.New("rio: expected exactly one row, found more")

	// ErrStaleObject is returned by Update and Delete when the row's version
	// column no longer matches, meaning another writer got there first —
	// or the row is gone entirely.
	ErrStaleObject = errors.New("rio: stale object: version conflict or row deleted")

	// ErrMissingWhere guards set-based writes: UpdateAll and DeleteAll refuse
	// to touch a whole table unless AllRows was called explicitly.
	ErrMissingWhere = errors.New("rio: UpdateAll/DeleteAll without conditions; call AllRows() to affect the whole table")

	// ErrDuplicateKey reports a unique constraint violation, translated from
	// the driver error (which remains in the chain for errors.As).
	ErrDuplicateKey = errors.New("rio: duplicate key violates unique constraint")

	// ErrForeignKeyViolated reports a foreign key constraint violation.
	ErrForeignKeyViolated = errors.New("rio: foreign key constraint violated")

	// ErrNoPrimaryKey is returned when Find, Update, or Delete is called on
	// a model that declares no primary key.
	ErrNoPrimaryKey = errors.New("rio: model has no primary key")
)

// unsupportedError marks a dialect-capability rejection: an operation the
// target dialect structurally cannot honor (not a validation or server-side
// error). Error returns the original message verbatim; Is reports
// errors.ErrUnsupported so callers branch on the stdlib sentinel rather than
// matching message substrings.
type unsupportedError string

func (e unsupportedError) Error() string { return string(e) }

func (unsupportedError) Is(target error) bool { return target == errors.ErrUnsupported }

// unsupportedf builds a capability rejection from a format string, mirroring
// fmt.Errorf. Every dialect-capability rejection funnels through it so all
// satisfy errors.Is(err, errors.ErrUnsupported).
func unsupportedf(format string, args ...any) error {
	return unsupportedError(fmt.Sprintf(format, args...))
}

// translateErr wraps err with the matching sentinel so callers can use
// errors.Is(err, rio.ErrDuplicateKey) while errors.As still reaches the
// driver's own error type. The driver modules install precise translators;
// the dialects provide an SQLSTATE-based fallback.
func translateErr(err error, cfg *config, d Dialect) error {
	if err == nil {
		return nil
	}
	if cfg.translator != nil {
		if sentinel := cfg.translator(err); sentinel != nil {
			return fmt.Errorf("%w (%w)", sentinel, err)
		}
	}
	if sentinel := d.translate(err); sentinel != nil {
		return fmt.Errorf("%w (%w)", sentinel, err)
	}
	return err
}

// sqlState reports the five-character SQLSTATE code if the error exposes one.
// pgx and lib/pq errors both implement SQLState() string.
func sqlState(err error) string {
	var coder interface{ SQLState() string }
	if errors.As(err, &coder) {
		return coder.SQLState()
	}
	return ""
}
