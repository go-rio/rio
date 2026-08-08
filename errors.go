package rio

import (
	"database/sql"
	"errors"
	"fmt"
)

var (
	// ErrNotFound reports that no row matched. It wraps sql.ErrNoRows.
	ErrNotFound = fmt.Errorf("rio: record not found (%w)", sql.ErrNoRows)

	// ErrMultipleRows is returned by Sole when more than one row matches.
	ErrMultipleRows = errors.New("rio: expected exactly one row, found more")

	// ErrStaleObject reports an optimistic-lock conflict or deleted row.
	ErrStaleObject = errors.New("rio: stale object: version conflict or row deleted")

	// ErrMissingWhere reports a set-based write without conditions or AllRows.
	ErrMissingWhere = errors.New("rio: UpdateAll/DeleteAll without conditions; call AllRows() to affect the whole table")

	// ErrDuplicateKey reports a translated unique-constraint violation.
	ErrDuplicateKey = errors.New("rio: duplicate key violates unique constraint")

	// ErrForeignKeyViolated reports a foreign key constraint violation.
	ErrForeignKeyViolated = errors.New("rio: foreign key constraint violated")

	// ErrNoPrimaryKey reports an operation that requires a model primary key.
	ErrNoPrimaryKey = errors.New("rio: model has no primary key")
)

// unsupportedError makes dialect capability errors match errors.ErrUnsupported.
type unsupportedError string

func (e unsupportedError) Error() string { return string(e) }

func (unsupportedError) Is(target error) bool { return target == errors.ErrUnsupported }

func unsupportedf(format string, args ...any) error {
	return unsupportedError(fmt.Sprintf(format, args...))
}

// translateErr adds a sentinel without hiding the driver error.
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

// sqlState returns the SQLSTATE code exposed by pgx and lib/pq errors.
func sqlState(err error) string {
	var coder interface{ SQLState() string }
	if errors.As(err, &coder) {
		return coder.SQLState()
	}
	return ""
}
