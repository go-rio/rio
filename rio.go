package rio

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Queryer is what every execution point accepts: a *DB or a *Tx. Data-access
// code written against Queryer runs unchanged inside and outside
// transactions. Tx opens a transaction on a DB and a savepoint on a Tx, so
// transactional helpers compose transparently.
type Queryer interface {
	// Tx runs fn inside a transaction (on *DB) or a savepoint (on *Tx),
	// committing when fn returns nil and rolling back when it returns an
	// error or panics.
	Tx(ctx context.Context, fn func(tx *Tx) error) error

	eng() engine
	gram() *grammar
	conf() *config
}

// DB wraps a *sql.DB with a dialect. rio never replaces or tunes the
// connection pool — configure pooling on the *sql.DB you pass in.
type DB struct {
	db     *sql.DB
	e      dbEngine
	g      *grammar
	cfg    *config
	native any // driver-native pool handle (NativeConfig.Handle); nil on database/sql
}

// New wraps an existing *sql.DB. Driver modules (go-rio/postgres, go-rio/mysql,
// go-rio/sqlite) call this for you and add driver-specific error translation;
// use New directly when you bring your own driver.
//
// Panics on a nil db or dialect, and on WithStmtCache with the ClickHouse
// dialect (clickhouse-go prepares only INSERT batches, so a prepared SELECT
// fails on first use).
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
		// Construction-time misuse, like a nil db: no configuration makes this work, so panic.
		panic("rio: WithStmtCache is not supported on " + dialect.name() +
			" (clickhouse-go implements Prepare only for INSERT batching; a prepared SELECT fails on first use)")
	}
	e := &sqlEngine{db: db}
	if cfg.stmtCache {
		e.stmts = newStmtCache(db, cfg.stmtCap)
	}
	return &DB{db: db, e: e, g: newGrammar(dialect, cfg), cfg: cfg}
}

// Unwrap returns the underlying *sql.DB for anything rio does not cover. On
// the native channel it returns the database/sql view the driver module
// supplied over the same pool (NativeConfig.SQLView; go-rio/postgres always
// provides one), or nil when the driver module supplied none. Never tune
// pooling on a native view — the pool belongs to the driver's configuration.
func (d *DB) Unwrap() *sql.DB { return d.db }

// Native returns the driver-native pool handle behind the native channel —
// NativeConfig.Handle, a *pgxpool.Pool under go-rio/postgres — and nil on
// the database/sql channel. Application code goes through the driver
// module's typed accessor (postgres.PoolOf); this is the raw door those
// accessors are built on. Its transaction-scoped sibling is Tx.NativeTx, which
// returns the SPI transaction adapter instead of a pool handle.
func (d *DB) Native() any { return d.native }

// Close closes the prepared-statement cache (if enabled) and the underlying
// *sql.DB.
func (d *DB) Close() error { return d.e.close() }

func (d *DB) eng() engine    { return d.e }
func (d *DB) gram() *grammar { return d.g }
func (d *DB) conf() *config  { return d.cfg }

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
		return unsupportedf("rio: transactions are not supported on %s (the driver's Begin is a no-op and statements would commit independently); group rows into one InsertAll for per-statement atomicity, or use db.Unwrap() with clickhouse-go's native batch API", d.g.d.name())
	}
	// Armed before BEGIN: its AfterQuery hook can panic with the transaction
	// already open, and the connection must be rolled back before the panic
	// continues. te is nil until begin succeeds — nothing to clean up then.
	var te txEngine
	defer func() {
		if p := recover(); p != nil {
			if te != nil {
				_ = d.finishTx(ctx, te, errors.New("panic"))
			}
			panic(p)
		}
	}()
	err = observe(ctx, d.cfg, d.g.d, "begin", "BEGIN", func(ctx context.Context) error {
		var berr error
		te, berr = d.e.begin(ctx, opts)
		return berr
	})
	if err != nil {
		return err
	}

	rtx := &Tx{e: te, g: d.g, cfg: d.cfg, spSeq: new(int)}
	if se, ok := te.(sqlTxEngine); ok {
		rtx.tx = se.tx // Unwrap's view; engines without a *sql.Tx leave it nil
	}
	if err = fn(rtx); err != nil {
		if rbErr := d.finishTx(ctx, te, err); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		return err
	}
	return observe(ctx, d.cfg, d.g.d, "commit", "COMMIT", func(ctx context.Context) error { return te.commit(ctx) })
}

// finishTx rolls the transaction back. Unlike the savepoint statements below,
// a canceled ctx cannot suppress this cleanup: the rollback runs on a
// cancellation-decoupled context — the caller-owned WithoutCancel discipline
// the engine seam documents. database/sql's Tx.Rollback ignores the context
// anyway; a native engine's rollback honors it, and a dead one there would
// strand the transaction (pgx fails fast and tears down the connection).
// Either channel reports a transaction the driver already finished — a begin
// context that died, for one — as sql.ErrTxDone, tolerated here.
func (d *DB) finishTx(ctx context.Context, te txEngine, cause error) error {
	// WithoutCancel wraps the hook context, not the raw ctx: a BeforeQuery span
	// still reaches the rollback, but a canceled caller ctx cannot suppress it.
	err := observe(ctx, d.cfg, d.g.d, "rollback", "ROLLBACK", func(ctx context.Context) error {
		return te.rollback(context.WithoutCancel(ctx))
	})
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		return fmt.Errorf("rio: rollback after %q failed: %w", cause, err)
	}
	return nil
}

// observe wraps transaction-control statements with hooks and error
// translation. fn runs under the context BeforeQuery returned (hooks.go), so
// a hook's span or deadline reaches begin/commit/rollback/savepoint. COMMIT
// in particular must translate: deferred constraints surface their violations
// at commit time.
func observe(ctx context.Context, cfg *config, d Dialect, op, sqlText string, fn func(context.Context) error) error {
	if len(cfg.hooks) == 0 {
		return translateErr(fn(ctx), cfg, d)
	}
	ev := &QueryEvent{Op: op, Query: sqlText}
	hctx := cfg.beforeQuery(ctx, ev)
	start := time.Now()
	err := translateErr(fn(hctx), cfg, d)
	cfg.afterQuery(hctx, ev, start, err, -1)
	return err
}

// Tx is a transaction handle. It satisfies Queryer, so every rio entry point
// accepts it in place of a *DB. Like *sql.Tx it is bound to one connection
// and must not be used concurrently.
type Tx struct {
	tx  *sql.Tx // Unwrap's view of the engine's transaction
	e   txEngine
	g   *grammar
	cfg *config
	// spSeq is shared across every Tx wrapper of the same root transaction
	// and increases monotonically, so savepoint names are never reused by
	// siblings or nested levels.
	spSeq *int
}

// Unwrap returns the underlying *sql.Tx — and nil on the native channel,
// which has no *sql.Tx to give. Use the driver module's typed accessor
// there: postgres.TxOf returns the pgx.Tx this transaction runs on.
func (t *Tx) Unwrap() *sql.Tx { return t.tx }

// NativeTx returns the NativeTx SPI transaction adapter the native channel
// runs this transaction on, and nil on the database/sql channel. Like
// (*DB).Native — which hands back the driver pool handle — it is the raw door:
// application code uses the driver module's typed accessor (postgres.TxOf)
// instead.
func (t *Tx) NativeTx() any {
	if ne, ok := t.e.(nativeTxEngine); ok {
		return ne.nt
	}
	return nil
}

func (t *Tx) eng() engine    { return t.e }
func (t *Tx) gram() *grammar { return t.g }
func (t *Tx) conf() *config  { return t.cfg }

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
	inner := &Tx{tx: t.tx, e: t.e, g: t.g, cfg: t.cfg, spSeq: t.spSeq}
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
	return observe(ctx, t.cfg, t.g.d, "savepoint", stmt, func(ctx context.Context) error {
		_, err := t.e.exec(ctx, stmt, nil)
		return err
	})
}

// run executes a non-row-returning statement through the shared pipeline:
// statement cache (DB only), hooks, error translation. The statement runs
// under the context BeforeQuery returned (hooks.go). Without hooks the event
// is never materialized — the hot path allocates nothing here.
func run(ctx context.Context, q Queryer, op, model, sqlText string, args []any) (sql.Result, error) {
	cfg := q.conf()
	if len(cfg.hooks) == 0 {
		res, err := q.eng().exec(ctx, sqlText, args)
		return res, translateErr(err, cfg, q.gram().d)
	}
	ev := &QueryEvent{Op: op, Model: model, Query: sqlText, Args: args}
	hctx := cfg.beforeQuery(ctx, ev)
	start := time.Now()
	res, err := q.eng().exec(hctx, sqlText, args)
	err = translateErr(err, cfg, q.gram().d)
	var rows int64 = -1
	hookErr := err
	if err == nil && res != nil {
		if n, aerr := res.RowsAffected(); aerr == nil {
			rows = n
		} else {
			// Record the failure for the hook so it never logs a success, but
			// run still returns (res, nil): the write-path callers re-check
			// RowsAffected and decide which errors abort.
			hookErr = aerr
		}
	}
	cfg.afterQuery(hctx, ev, start, hookErr, rows)
	return res, err
}

// runQuery executes a row-returning statement through the shared pipeline.
// The statement — and the row consumption its context governs — runs under
// the context BeforeQuery returned (hooks.go). The returned finish callback
// (nil without hooks — the hot path stays allocation-free) fires AfterQuery
// once the rows are consumed, so hooks see scan errors and a duration that
// includes row consumption.
func runQuery(ctx context.Context, q Queryer, op, model, sqlText string, args []any) (rows, func(error), error) {
	cfg := q.conf()
	if len(cfg.hooks) == 0 {
		rs, err := q.eng().query(ctx, sqlText, args)
		return rs, nil, translateErr(err, cfg, q.gram().d)
	}
	ev := &QueryEvent{Op: op, Model: model, Query: sqlText, Args: args}
	hctx := cfg.beforeQuery(ctx, ev)
	start := time.Now()
	rs, err := q.eng().query(hctx, sqlText, args)
	err = translateErr(err, cfg, q.gram().d)
	if err != nil {
		cfg.afterQuery(hctx, ev, start, err, -1)
		return nil, nil, err
	}
	finish := func(scanErr error) {
		cfg.afterQuery(hctx, ev, start, scanErr, -1)
	}
	return rs, finish, nil
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
