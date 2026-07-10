package rio

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// executor is the subset of database/sql shared by *sql.DB and *sql.Tx.
type executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Queryer is what every execution point accepts: a *DB or a *Tx. Data-access
// code written against Queryer runs unchanged inside and outside
// transactions. Tx opens a transaction on a DB and a savepoint on a Tx, so
// transactional helpers compose transparently.
type Queryer interface {
	// Tx runs fn inside a transaction (on *DB) or a savepoint (on *Tx),
	// committing when fn returns nil and rolling back when it returns an
	// error or panics.
	Tx(ctx context.Context, fn func(tx *Tx) error) error

	exec() executor
	gram() *grammar
	conf() *config
	stmt(ctx context.Context, query string) (*sql.Stmt, bool, error)
}

// DB wraps a *sql.DB with a dialect. rio never replaces or tunes the
// connection pool — configure pooling on the *sql.DB you pass in.
type DB struct {
	db    *sql.DB
	g     *grammar
	cfg   *config
	stmts *stmtCache
}

// New wraps an existing *sql.DB. Driver modules (go-rio/postgres, go-rio/mysql,
// go-rio/sqlite) call this for you and add driver-specific error translation;
// use New directly when you bring your own driver.
func New(db *sql.DB, dialect Dialect, opts ...Option) *DB {
	if db == nil {
		panic("rio: New: db must not be nil")
	}
	if dialect == nil {
		panic("rio: New: dialect must not be nil")
	}
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.stmtCache && !dialect.caps().stmtPrepare {
		// Construction-time misuse, like a nil db: clickhouse-go implements
		// Prepare only for INSERT batching, so every cached SELECT would fail
		// on first use — there is no configuration under which this works.
		panic("rio: WithStmtCache is not supported on " + dialect.name() +
			" (clickhouse-go implements Prepare only for INSERT batching; a prepared SELECT fails on first use)")
	}
	d := &DB{db: db, g: newGrammar(dialect, cfg), cfg: cfg}
	if cfg.stmtCache {
		d.stmts = newStmtCache(db, cfg.stmtCap)
	}
	return d
}

// Unwrap returns the underlying *sql.DB for anything rio does not cover.
func (d *DB) Unwrap() *sql.DB { return d.db }

// Close closes the prepared-statement cache (if enabled) and the underlying
// *sql.DB.
func (d *DB) Close() error {
	if d.stmts != nil {
		d.stmts.close()
	}
	return d.db.Close()
}

func (d *DB) exec() executor { return d.db }
func (d *DB) gram() *grammar { return d.g }
func (d *DB) conf() *config  { return d.cfg }

func (d *DB) stmt(ctx context.Context, query string) (*sql.Stmt, bool, error) {
	if d.stmts == nil {
		return nil, false, nil
	}
	st, err := d.stmts.get(ctx, query)
	if err != nil {
		return nil, false, err
	}
	return st, true, nil
}

// Tx runs fn in a transaction with default options.
func (d *DB) Tx(ctx context.Context, fn func(tx *Tx) error) error {
	return d.TxWith(ctx, nil, fn)
}

// TxWith runs fn in a transaction with the given options (isolation level,
// read-only).
func (d *DB) TxWith(ctx context.Context, opts *sql.TxOptions, fn func(tx *Tx) error) (err error) {
	if !d.g.d.caps().transactions {
		// clickhouse-go's Begin returns the connection itself and opens
		// nothing: fn would run with every statement committing independently
		// while looking transactional — the heaviest silent surprise there is.
		return fmt.Errorf("rio: transactions are not supported on %s (the driver's Begin is a no-op and statements would commit independently); group rows into one InsertAll for per-statement atomicity, or use db.Unwrap() with clickhouse-go's native batch API", d.g.d.name())
	}
	// Armed before BEGIN: its AfterQuery hook can panic with the transaction
	// already open, and the connection must be rolled back before the panic
	// continues. tx is nil until BeginTx succeeds — nothing to clean up then.
	var tx *sql.Tx
	defer func() {
		if p := recover(); p != nil {
			if tx != nil {
				_ = d.finishTx(ctx, tx, errors.New("panic"))
			}
			panic(p)
		}
	}()
	err = observe(ctx, d.cfg, d.g.d, "begin", "BEGIN", func() error {
		var berr error
		tx, berr = d.db.BeginTx(ctx, opts)
		return berr
	})
	if err != nil {
		return err
	}

	rtx := &Tx{tx: tx, g: d.g, cfg: d.cfg, spSeq: new(int)}
	if err = fn(rtx); err != nil {
		if rbErr := d.finishTx(ctx, tx, err); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		return err
	}
	return observe(ctx, d.cfg, d.g.d, "commit", "COMMIT", tx.Commit)
}

// finishTx rolls the transaction back. Unlike the savepoint statements below,
// a canceled ctx cannot suppress this cleanup: database/sql's Tx.Rollback
// takes no context and always reaches the driver, and BeginTx's own watcher
// already rolls back automatically when the begin context dies (Rollback then
// reports ErrTxDone, tolerated here).
func (d *DB) finishTx(ctx context.Context, tx *sql.Tx, cause error) error {
	err := observe(ctx, d.cfg, d.g.d, "rollback", "ROLLBACK", tx.Rollback)
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		return fmt.Errorf("rio: rollback after %q failed: %w", cause, err)
	}
	return nil
}

// observe wraps transaction-control statements with hooks and error
// translation; without hooks it is a plain call plus translation. COMMIT in
// particular must translate: deferred constraints surface their violations
// at commit time.
func observe(ctx context.Context, cfg *config, d Dialect, op, sqlText string, fn func() error) error {
	if len(cfg.hooks) == 0 {
		return translateErr(fn(), cfg, d)
	}
	ev := &QueryEvent{Op: op, Query: sqlText}
	hctx := cfg.beforeQuery(ctx, ev)
	start := time.Now()
	err := translateErr(fn(), cfg, d)
	cfg.afterQuery(hctx, ev, start, err, -1)
	return err
}

// Tx is a transaction handle. It satisfies Queryer, so every rio entry point
// accepts it in place of a *DB. Like *sql.Tx it is bound to one connection
// and must not be used concurrently.
type Tx struct {
	tx  *sql.Tx
	g   *grammar
	cfg *config
	// spSeq is shared across every Tx wrapper of the same root transaction
	// and increases monotonically, so savepoint names are never reused by
	// siblings or nested levels.
	spSeq *int
}

// Unwrap returns the underlying *sql.Tx.
func (t *Tx) Unwrap() *sql.Tx { return t.tx }

func (t *Tx) exec() executor { return t.tx }
func (t *Tx) gram() *grammar { return t.g }
func (t *Tx) conf() *config  { return t.cfg }

func (t *Tx) stmt(context.Context, string) (*sql.Stmt, bool, error) {
	// The statement cache lives on the DB; re-preparing per transaction
	// costs more than it saves, so transactions always execute directly.
	return nil, false, nil
}

// Tx runs fn inside a savepoint, giving nested transactional code partial
// rollback. Savepoints commit ("RELEASE") when fn returns nil; on error the
// savepoint is rolled back and the error returned, leaving the outer
// transaction usable.
func (t *Tx) Tx(ctx context.Context, fn func(tx *Tx) error) (err error) {
	*t.spSeq++
	name := "rio_sp_" + strconv.Itoa(*t.spSeq)

	if err := t.spExec(ctx, "SAVEPOINT "+name); err != nil {
		return err
	}
	// Cleanup statements run on a cancellation-decoupled context. fn failing
	// *because* its context died is exactly when ROLLBACK TO must still reach
	// the database: on the caller's ctx, database/sql short-circuits before
	// sending it, the savepoint's writes silently survive, and the outer
	// transaction — its own context still live — would commit them. The
	// connection itself is healthy; only the context is dead. SAVEPOINT above
	// keeps the caller's ctx: refusing to *open* work under a canceled
	// context is the correct half of cancellation.
	cleanup := context.WithoutCancel(ctx)
	inner := &Tx{tx: t.tx, g: t.g, cfg: t.cfg, spSeq: t.spSeq}
	defer func() {
		if p := recover(); p != nil {
			_ = t.spExec(cleanup, "ROLLBACK TO SAVEPOINT "+name)
			panic(p)
		}
	}()
	if err = fn(inner); err != nil {
		// Roll back first: after a failed statement PostgreSQL aborts the
		// transaction and accepts nothing but ROLLBACK TO. The rollback can
		// itself fail legitimately — a MySQL deadlock (1213) rolls back the
		// whole transaction and destroys every savepoint — so its error is
		// joined rather than allowed to mask the cause.
		if rbErr := t.spExec(cleanup, "ROLLBACK TO SAVEPOINT "+name); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		_ = t.spExec(cleanup, "RELEASE SAVEPOINT "+name) // keep the stack clean; failure is harmless here
		return err
	}
	return t.spExec(cleanup, "RELEASE SAVEPOINT "+name)
}

func (t *Tx) spExec(ctx context.Context, stmt string) error {
	return observe(ctx, t.cfg, t.g.d, "savepoint", stmt, func() error {
		_, err := t.tx.ExecContext(ctx, stmt)
		return err
	})
}

// run executes a non-row-returning statement through the shared pipeline:
// ClickHouse time inlining, statement cache (DB only), hooks, error
// translation. Without hooks the event is never materialized — the hot path
// allocates nothing here.
func run(ctx context.Context, q Queryer, op, model, sqlText string, args []any) (sql.Result, error) {
	cfg := q.conf()
	sqlText, args, err := inlineTimeArgs(q.gram().d, sqlText, args)
	if err != nil {
		return nil, err
	}
	if len(cfg.hooks) == 0 {
		res, err := execute(ctx, q, sqlText, args)
		return res, translateErr(err, cfg, q.gram().d)
	}
	ev := &QueryEvent{Op: op, Model: model, Query: sqlText, Args: args}
	hctx := cfg.beforeQuery(ctx, ev)
	start := time.Now()
	res, err := execute(ctx, q, sqlText, args)
	err = translateErr(err, cfg, q.gram().d)
	var rows int64 = -1
	hookErr := err
	if err == nil && res != nil {
		if n, aerr := res.RowsAffected(); aerr == nil {
			rows = n
		} else {
			// The write-path callers all consult RowsAffected themselves and
			// will surface this same failure; the hook must not record the
			// statement as a success the caller then reports as an error.
			// run's own return stays (res, nil): which errors abort the call
			// is the caller's contract, not the observer's.
			hookErr = aerr
		}
	}
	cfg.afterQuery(hctx, ev, start, hookErr, rows)
	return res, err
}

// runQuery executes a row-returning statement through the shared pipeline;
// see run for the stages. The returned finish callback (nil without hooks —
// the hot path stays allocation-free) fires AfterQuery once the rows are
// consumed, so hooks see scan errors and a duration that includes row
// consumption.
func runQuery(ctx context.Context, q Queryer, op, model, sqlText string, args []any) (*sql.Rows, func(error), error) {
	cfg := q.conf()
	sqlText, args, err := inlineTimeArgs(q.gram().d, sqlText, args)
	if err != nil {
		return nil, nil, err
	}
	if len(cfg.hooks) == 0 {
		rows, err := query(ctx, q, sqlText, args)
		return rows, nil, translateErr(err, cfg, q.gram().d)
	}
	ev := &QueryEvent{Op: op, Model: model, Query: sqlText, Args: args}
	hctx := cfg.beforeQuery(ctx, ev)
	start := time.Now()
	rows, err := query(ctx, q, sqlText, args)
	err = translateErr(err, cfg, q.gram().d)
	if err != nil {
		cfg.afterQuery(hctx, ev, start, err, -1)
		return nil, nil, err
	}
	finish := func(scanErr error) {
		cfg.afterQuery(hctx, ev, start, scanErr, -1)
	}
	return rows, finish, nil
}

// finishQuery fires a runQuery finish callback, tolerating the no-hook nil.
// A miss (ErrNotFound) is a successfully executed query, not a failure —
// telemetry would otherwise count every First/Find miss as an error.
func finishQuery(finish func(error), err error) {
	if finish == nil {
		return
	}
	if errors.Is(err, ErrNotFound) {
		err = nil
	}
	finish(err)
}

func execute(ctx context.Context, q Queryer, sqlText string, args []any) (sql.Result, error) {
	if st, ok, err := q.stmt(ctx, sqlText); err != nil {
		return nil, err
	} else if ok {
		res, err := st.ExecContext(ctx, args...)
		if isStmtClosed(err) {
			// A concurrent eviction closed the handle between get and use;
			// the statement never ran, so direct execution is safe.
			return q.exec().ExecContext(ctx, sqlText, args...)
		}
		return res, evictOnSchemaChange(q, sqlText, err)
	}
	return q.exec().ExecContext(ctx, sqlText, args...)
}

// isStmtClosed matches database/sql's unexported "statement is closed"
// condition — stable text since Go 1.0, and the only signal available.
func isStmtClosed(err error) bool {
	return err != nil && err.Error() == "sql: statement is closed"
}

func query(ctx context.Context, q Queryer, sqlText string, args []any) (*sql.Rows, error) {
	if st, ok, err := q.stmt(ctx, sqlText); err != nil {
		return nil, err
	} else if ok {
		rows, err := st.QueryContext(ctx, args...)
		if isStmtClosed(err) {
			return q.exec().QueryContext(ctx, sqlText, args...)
		}
		return rows, evictOnSchemaChange(q, sqlText, err)
	}
	return q.exec().QueryContext(ctx, sqlText, args...)
}

// evictOnSchemaChange drops a cached statement invalidated by DDL (Postgres:
// "cached plan must not change result type", SQLSTATE 0A000) and returns the
// error unchanged. rio never retries on its own — retrying writes risks
// double execution.
func evictOnSchemaChange(q Queryer, sqlText string, err error) error {
	if err == nil {
		return nil
	}
	if db, ok := q.(*DB); ok && db.stmts != nil && sqlState(err) == "0A000" {
		db.stmts.evict(sqlText)
	}
	return err
}
