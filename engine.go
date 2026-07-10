package rio

import (
	"context"
	"database/sql"
)

// engine is the execution channel behind a Queryer. Everything above this
// seam — rendering, hooks, error translation, scanning — is
// channel-independent; an engine only receives fully rendered, dialect-form
// SQL and returns results. database/sql is the one implementation today;
// the seam exists so a driver-native channel can slot in without moving a
// single semantic.
type engine interface {
	exec(ctx context.Context, sqlText string, args []any) (sql.Result, error)
	query(ctx context.Context, sqlText string, args []any) (rows, error)
}

// dbEngine is a DB-level engine: it opens transactions and owns the
// channel's resources.
type dbEngine interface {
	engine
	begin(ctx context.Context, opts *sql.TxOptions) (txEngine, error)
	close() error
}

// txEngine is a transaction-level engine. Both finishers take a context for
// the seam's sake; callers on cleanup paths stay responsible for the
// WithoutCancel discipline (see Tx.Tx).
type txEngine interface {
	engine
	commit(ctx context.Context) error
	rollback(ctx context.Context) error
}

// rows is the minimal result-set surface the scan paths consume. Its method
// set is exactly the *sql.Rows subset rio uses, so the database/sql engine
// hands back *sql.Rows values directly — no wrapper, no per-row
// indirection, and (a pointer being interface-inlinable) no allocation.
type rows interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// sqlEngine executes through database/sql. The prepared-statement cache
// belongs to the channel, not the handle: engines without a prepare concept
// never see it.
type sqlEngine struct {
	db    *sql.DB
	stmts *stmtCache // nil unless WithStmtCache
}

func (e *sqlEngine) stmt(ctx context.Context, sqlText string) (*sql.Stmt, bool, error) {
	if e.stmts == nil {
		return nil, false, nil
	}
	st, err := e.stmts.get(ctx, sqlText)
	if err != nil {
		return nil, false, err
	}
	return st, true, nil
}

func (e *sqlEngine) exec(ctx context.Context, sqlText string, args []any) (sql.Result, error) {
	if st, ok, err := e.stmt(ctx, sqlText); err != nil {
		return nil, err
	} else if ok {
		res, err := st.ExecContext(ctx, args...)
		if isStmtClosed(err) {
			// A concurrent eviction closed the handle between get and use;
			// the statement never ran, so direct execution is safe.
			return e.db.ExecContext(ctx, sqlText, args...)
		}
		return res, e.evictOnSchemaChange(sqlText, err)
	}
	return e.db.ExecContext(ctx, sqlText, args...)
}

func (e *sqlEngine) query(ctx context.Context, sqlText string, args []any) (rows, error) {
	if st, ok, err := e.stmt(ctx, sqlText); err != nil {
		return nil, err
	} else if ok {
		rs, err := st.QueryContext(ctx, args...)
		if isStmtClosed(err) {
			return e.directQuery(ctx, sqlText, args)
		}
		if err != nil {
			return nil, e.evictOnSchemaChange(sqlText, err)
		}
		return rs, nil
	}
	return e.directQuery(ctx, sqlText, args)
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
	return sqlTxEngine{tx: tx}, nil
}

func (e *sqlEngine) close() error {
	if e.stmts != nil {
		e.stmts.close()
	}
	return e.db.Close()
}

// evictOnSchemaChange drops a cached statement invalidated by DDL (Postgres:
// "cached plan must not change result type", SQLSTATE 0A000) and returns the
// error unchanged. rio never retries on its own — retrying writes risks
// double execution.
func (e *sqlEngine) evictOnSchemaChange(sqlText string, err error) error {
	if err == nil {
		return nil
	}
	if e.stmts != nil && sqlState(err) == "0A000" {
		e.stmts.evict(sqlText)
	}
	return err
}

// isStmtClosed matches database/sql's unexported "statement is closed"
// condition — stable text since Go 1.0, and the only signal available.
func isStmtClosed(err error) bool {
	return err != nil && err.Error() == "sql: statement is closed"
}

// sqlTxEngine executes on one *sql.Tx. A single-pointer struct, so the
// txEngine interface holds it directly and beginning a transaction
// allocates nothing beyond what database/sql does. The statement cache
// stays on the DB engine: re-preparing per transaction costs more than it
// saves, so transactions always execute directly.
type sqlTxEngine struct {
	tx *sql.Tx
}

func (e sqlTxEngine) exec(ctx context.Context, sqlText string, args []any) (sql.Result, error) {
	return e.tx.ExecContext(ctx, sqlText, args...)
}

func (e sqlTxEngine) query(ctx context.Context, sqlText string, args []any) (rows, error) {
	rs, err := e.tx.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

// database/sql's transaction finishers take no context and always reach the
// driver — exactly the property rio's cleanup discipline relies on — so the
// seam's context is deliberately unused here.
func (e sqlTxEngine) commit(context.Context) error   { return e.tx.Commit() }
func (e sqlTxEngine) rollback(context.Context) error { return e.tx.Rollback() }
