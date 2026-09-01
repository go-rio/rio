package rio

import (
	"context"
	"database/sql"
)

// engine is the execution channel behind a Queryer; it receives only fully
// rendered, dialect-form SQL.
type engine interface {
	exec(ctx context.Context, sqlText string, args []any) (sql.Result, error)
	query(ctx context.Context, sqlText string, args []any) (rows, error)
}

// dbEngine opens transactions and owns the channel's resources.
type dbEngine interface {
	engine
	begin(ctx context.Context, opts *sql.TxOptions) (txEngine, error)
	close() error
}

// txEngine is a transaction-level engine; callers on cleanup paths own the
// WithoutCancel discipline (see Tx.Tx).
type txEngine interface {
	engine
	commit(ctx context.Context) error
	rollback(ctx context.Context) error
}

// rows is the *sql.Rows method subset the scan paths consume, so *sql.Rows
// satisfies it unwrapped.
type rows interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// sqlEngine executes through database/sql.
type sqlEngine struct {
	db    *sql.DB
	stmts *stmtCache // nil unless WithStmtCache
}

// stmt returns a cached prepared statement; zero-arg statements never prepare
// (nothing to bind, and multi-command scripts cannot prepare).
func (e *sqlEngine) stmt(ctx context.Context, sqlText string, nargs int) (*sql.Stmt, bool, error) {
	if e.stmts == nil || nargs == 0 {
		return nil, false, nil
	}
	st, err := e.stmts.get(ctx, sqlText)
	if err != nil {
		return nil, false, err
	}
	return st, true, nil
}

func (e *sqlEngine) exec(ctx context.Context, sqlText string, args []any) (sql.Result, error) {
	st, ok, err := e.stmt(ctx, sqlText, len(args))
	if err != nil {
		return nil, err
	}
	if !ok {
		return e.db.ExecContext(ctx, sqlText, args...)
	}
	res, err := st.ExecContext(ctx, args...)
	if isStmtClosed(err) {
		// A concurrent eviction closed the handle before it ran; direct
		// execution is safe.
		return e.db.ExecContext(ctx, sqlText, args...)
	}
	return res, e.evictOnSchemaChange(sqlText, err)
}

func (e *sqlEngine) query(ctx context.Context, sqlText string, args []any) (rows, error) {
	st, ok, err := e.stmt(ctx, sqlText, len(args))
	if err != nil {
		return nil, err
	}
	if !ok {
		return e.directQuery(ctx, sqlText, args)
	}
	rs, err := st.QueryContext(ctx, args...)
	if isStmtClosed(err) {
		return e.directQuery(ctx, sqlText, args)
	}
	if err != nil {
		return nil, e.evictOnSchemaChange(sqlText, err)
	}
	return rs, nil
}

func (e *sqlEngine) directQuery(ctx context.Context, sqlText string, args []any) (rows, error) {
	rs, err := e.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func (e *sqlEngine) begin(ctx context.Context, opts *sql.TxOptions) (txEngine, error) {
	tx, err := e.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	var stmts *stmtCache
	if e.stmts != nil {
		stmts = newStmtCache(tx, e.stmts.cap)
	}
	return &sqlTxEngine{tx: tx, stmts: stmts}, nil
}

func (e *sqlEngine) close() error {
	if e.stmts != nil {
		e.stmts.close()
	}
	return e.db.Close()
}

// evictOnSchemaChange drops a statement invalidated by DDL (SQLSTATE 0A000),
// returning the error unchanged: rio never retries, a write could run twice.
func (e *sqlEngine) evictOnSchemaChange(sqlText string, err error) error {
	if err == nil {
		return nil
	}
	if e.stmts != nil && sqlState(err) == "0A000" {
		e.stmts.evict(sqlText)
	}
	return err
}

// sqlTxEngine executes on one *sql.Tx; its statement cache ends with the
// transaction.
type sqlTxEngine struct {
	tx    *sql.Tx
	stmts *stmtCache
}

func (e *sqlTxEngine) exec(ctx context.Context, sqlText string, args []any) (sql.Result, error) {
	if e.stmts == nil || len(args) == 0 {
		return e.tx.ExecContext(ctx, sqlText, args...)
	}
	st, err := e.stmts.get(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	res, err := st.ExecContext(ctx, args...)
	if isStmtClosed(err) {
		return e.tx.ExecContext(ctx, sqlText, args...)
	}
	if err != nil && sqlState(err) == "0A000" {
		e.stmts.evict(sqlText)
	}
	return res, err
}

func (e *sqlTxEngine) query(ctx context.Context, sqlText string, args []any) (rows, error) {
	if e.stmts == nil || len(args) == 0 {
		return e.directQuery(ctx, sqlText, args)
	}
	st, err := e.stmts.get(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	rs, err := st.QueryContext(ctx, args...)
	if isStmtClosed(err) {
		return e.directQuery(ctx, sqlText, args)
	}
	if err != nil {
		if sqlState(err) == "0A000" {
			e.stmts.evict(sqlText)
		}
		return nil, err
	}
	return rs, nil
}

func (e *sqlTxEngine) directQuery(ctx context.Context, sqlText string, args []any) (rows, error) {
	rs, err := e.tx.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

// commit and rollback ignore ctx: database/sql's finishers take no context
// and always reach the driver.
func (e *sqlTxEngine) commit(context.Context) error {
	err := e.tx.Commit()
	if e.stmts != nil {
		e.stmts.close()
	}
	return err
}

func (e *sqlTxEngine) rollback(context.Context) error {
	err := e.tx.Rollback()
	if e.stmts != nil {
		e.stmts.close()
	}
	return err
}

// isStmtClosed matches database/sql's unexported error; its text is the only
// signal available.
func isStmtClosed(err error) bool {
	return err != nil && err.Error() == "sql: statement is closed"
}
