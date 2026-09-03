package rio

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Queryer is the execution target every rio entry point accepts: a *DB or a *Tx.
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
	handle any // driver-owned handle (WithDriverHandle / NativeConfig.Handle)
}

// New wraps an existing *sql.DB. Panics on a nil db or dialect, and on
// WithStmtCache with a dialect that cannot prepare statements (ClickHouse).
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
		panic("rio: WithStmtCache is not supported on " + dialect.name() +
			" (the channel has no prepared statements)")
	}
	e := &sqlEngine{db: db}
	if cfg.stmtCache {
		e.stmts = newStmtCache(db, cfg.stmtCap)
	}
	return &DB{db: db, e: e, g: newGrammar(dialect, cfg), cfg: cfg, handle: cfg.driverHandle}
}

// Unwrap returns the underlying *sql.DB. On the native channel it is the
// driver module's database/sql view over the same pool (NativeConfig.SQLView),
// or nil when none was supplied; never tune pooling on that view.
func (d *DB) Unwrap() *sql.DB { return d.db }

// Native returns NativeConfig.Handle on the native channel and nil on the
// database/sql channel.
func (d *DB) Native() any { return d.native }

// DriverHandle returns the driver-owned handle attached through
// WithDriverHandle or NativeConfig.Handle, or nil when none was attached.
func (d *DB) DriverHandle() any { return d.handle }

// WithoutStamps returns a handle whose writes leave CreatedAt and UpdatedAt
// to the caller: inserts bind the struct's values as they are, zero included,
// and updates neither add nor bump UpdatedAt. Versions and softdelete stamps
// are unaffected. The handle shares its parent's pool and caches, and
// transactions it begins inherit the setting.
func (d *DB) WithoutStamps() *DB {
	cfg := *d.cfg
	cfg.noStamps = true
	c := *d
	c.cfg = &cfg
	return &c
}

// Close closes the prepared-statement cache (if enabled) and the underlying
// *sql.DB.
func (d *DB) Close() error { return d.e.close() }

// Tx runs fn in a transaction with default options.
func (d *DB) Tx(ctx context.Context, fn func(tx *Tx) error) error {
	return d.TxWith(ctx, nil, fn)
}

// TxWith runs fn in a transaction with the given options (isolation level,
// read-only).
func (d *DB) TxWith(ctx context.Context, opts *sql.TxOptions, fn func(tx *Tx) error) (err error) {
	if !d.g.d.caps().transactions {
		return unsupportedf(
			"rio: transactions are not supported on %s "+
				"(every statement commits independently); "+
				"group rows into one InsertAll for per-statement atomicity",
			d.g.d.name(),
		)
	}
	// Armed before BEGIN: a hook may panic with the transaction already open —
	// roll back, then re-panic.
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
	if se, ok := te.(*sqlTxEngine); ok {
		rtx.tx = se.tx
	}
	if err = fn(rtx); err != nil {
		if rbErr := d.finishTx(ctx, te, err); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		return err
	}
	return observe(ctx, d.cfg, d.g.d, "commit", "COMMIT", func(ctx context.Context) error { return te.commit(ctx) })
}

func (d *DB) eng() engine    { return d.e }
func (d *DB) gram() *grammar { return d.g }
func (d *DB) conf() *config  { return d.cfg }

// finishTx rolls the transaction back on a cancellation-decoupled context
// (a dead ctx must not suppress cleanup) and tolerates sql.ErrTxDone.
func (d *DB) finishTx(ctx context.Context, te txEngine, cause error) error {
	err := observeCleanup(ctx, d.cfg, d.g.d, "rollback", "ROLLBACK", func(ctx context.Context) error {
		return te.rollback(ctx)
	})
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		return fmt.Errorf("rio: rollback after %q failed: %w", cause, err)
	}
	return nil
}

// Tx is a transaction handle. Like *sql.Tx it is bound to one connection and
// must not be used concurrently.
type Tx struct {
	tx  *sql.Tx // Unwrap's view; nil without a *sql.Tx
	e   txEngine
	g   *grammar
	cfg *config
	// spSeq is shared by every Tx wrapper of one root transaction and only
	// grows, so savepoint names are never reused.
	spSeq *int
}

// Unwrap returns the underlying *sql.Tx, or nil on the native channel.
func (t *Tx) Unwrap() *sql.Tx { return t.tx }

// NativeTx returns the NativeTx SPI adapter this transaction runs on, or nil
// on the database/sql channel.
func (t *Tx) NativeTx() any {
	if ne, ok := t.e.(nativeTxEngine); ok {
		return ne.nt
	}
	return nil
}

// WithoutStamps returns a view of this transaction that leaves CreatedAt and
// UpdatedAt to the caller; see DB.WithoutStamps.
func (t *Tx) WithoutStamps() *Tx {
	cfg := *t.cfg
	cfg.noStamps = true
	c := *t
	c.cfg = &cfg
	return &c
}

// Tx runs fn inside a savepoint: released when fn returns nil, rolled back
// when fn returns an error or panics, leaving the outer transaction usable.
func (t *Tx) Tx(ctx context.Context, fn func(tx *Tx) error) (err error) {
	*t.spSeq++
	name := "rio_sp_" + strconv.Itoa(*t.spSeq)

	if err := t.spExec(ctx, "SAVEPOINT "+name); err != nil {
		return err
	}
	// Cleanup is cancellation-decoupled: when fn fails because ctx died,
	// ROLLBACK TO must still reach the database or the savepoint's writes commit.
	cleanup := context.WithoutCancel(ctx)
	inner := &Tx{tx: t.tx, e: t.e, g: t.g, cfg: t.cfg, spSeq: t.spSeq}
	defer func() {
		if p := recover(); p != nil {
			_ = t.spRollback(cleanup, name)
			panic(p)
		}
	}()
	if err = fn(inner); err != nil {
		// An aborted transaction (PostgreSQL) accepts nothing but ROLLBACK TO,
		// so roll back before releasing.
		if rbErr := t.spRollback(cleanup, name); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		_ = t.spExec(cleanup, "RELEASE SAVEPOINT "+name) // failure is harmless
		return err
	}
	return t.spExec(cleanup, "RELEASE SAVEPOINT "+name)
}

func (t *Tx) eng() engine    { return t.e }
func (t *Tx) gram() *grammar { return t.g }
func (t *Tx) conf() *config  { return t.cfg }

func (t *Tx) spRollback(ctx context.Context, name string) error {
	stmt := "ROLLBACK TO SAVEPOINT " + name
	return observeCleanup(ctx, t.cfg, t.g.d, "savepoint", stmt, func(ctx context.Context) error {
		_, err := t.e.exec(ctx, stmt, nil)
		return err
	})
}

func (t *Tx) spExec(ctx context.Context, stmt string) error {
	return observe(ctx, t.cfg, t.g.d, "savepoint", stmt, func(ctx context.Context) error {
		_, err := t.e.exec(ctx, stmt, nil)
		return err
	})
}

// relStatement is one derived relation-loading statement; its loader owns
// draining and closing the rows.
type relStatement struct {
	phase   string
	model   string
	sqlText string
	args    []any
	load    relConsumer
}

// relConsumer drains one derived statement's rows into its loader's buffer;
// one loader serves every chunk statement of its relation.
type relConsumer interface {
	consume(rows) (int64, error)
}

// relFinisher assembles a loader's buffered rows once its layer ran.
type relFinisher interface {
	finish(context.Context) error
}

// observeCleanup runs fn even when a BeforeQuery hook panics; the panic then
// propagates.
func observeCleanup(
	ctx context.Context,
	cfg *config,
	d Dialect,
	op string,
	sqlText string,
	fn func(context.Context) error,
) (err error) {
	attempted := false
	defer func() {
		if p := recover(); p != nil {
			if !attempted {
				_ = fn(context.WithoutCancel(ctx))
			}
			panic(p)
		}
	}()
	return observe(ctx, cfg, d, op, sqlText, func(hookCtx context.Context) error {
		attempted = true
		return fn(context.WithoutCancel(hookCtx))
	})
}

// observe wraps transaction-control statements with hooks and error
// translation; fn runs under the context BeforeQuery returned.
func observe(
	ctx context.Context,
	cfg *config,
	d Dialect,
	op string,
	sqlText string,
	fn func(context.Context) error,
) error {
	if len(cfg.hooks) == 0 {
		return translateErr(fn(ctx), cfg, d)
	}
	ev := &QueryEvent{Op: op, Query: sqlText}
	hctx := cfg.beforeQuery(ctx, ev)
	start := time.Now()
	err := translateErr(fn(hctx), cfg, d)
	cfg.afterQuery(hctx, ev, start, err, -1, -1)
	return err
}

// run executes a non-row-returning statement through the shared pipeline:
// statement cache, hooks, error translation.
func run(
	ctx context.Context,
	q Queryer,
	op string,
	model string,
	sqlText string,
	args []any,
) (sql.Result, error) {
	cfg := q.conf()
	if len(cfg.hooks) == 0 {
		res, err := q.eng().exec(ctx, sqlText, args)
		return res, translateErr(err, cfg, q.gram().d)
	}
	ev := &QueryEvent{
		Op:    op,
		Model: model,
		Query: sqlText,
		Args:  args,
	}
	hctx := cfg.beforeQuery(ctx, ev)
	start := time.Now()
	res, err := q.eng().exec(hctx, sqlText, args)
	err = translateErr(err, cfg, q.gram().d)
	rows := int64(-1)
	hookErr := err
	if err == nil && res != nil {
		if n, aerr := res.RowsAffected(); aerr == nil {
			rows = n
		} else {
			// The hook sees the failure; callers re-check RowsAffected themselves.
			hookErr = aerr
		}
	}
	cfg.afterQuery(hctx, ev, start, hookErr, rows, -1)
	return res, err
}

func runAffected(
	ctx context.Context,
	q Queryer,
	op string,
	model string,
	sqlText string,
	args []any,
) (int64, error) {
	res, err := run(ctx, q, op, model, sqlText, args)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// runQuery executes a row-returning statement through the shared pipeline.
// The finish callback (nil without hooks) must fire once the rows are consumed.
func runQuery(
	ctx context.Context,
	q Queryer,
	op string,
	model string,
	sqlText string,
	args []any,
) (rows, func(error, int64), error) {
	return runQueryPhase(ctx, q, "", op, model, sqlText, args)
}

// runQueryPhase is runQuery with a phase label for derived statements
// (preloads, counts, write probes).
func runQueryPhase(
	ctx context.Context,
	q Queryer,
	phase string,
	op string,
	model string,
	sqlText string,
	args []any,
) (rows, func(error, int64), error) {
	cfg := q.conf()
	if len(cfg.hooks) == 0 {
		rs, err := q.eng().query(ctx, sqlText, args)
		return rs, nil, translateErr(err, cfg, q.gram().d)
	}
	ev := &QueryEvent{
		Op:    op,
		Phase: phase,
		Model: model,
		Query: sqlText,
		Args:  args,
	}
	hctx := cfg.beforeQuery(ctx, ev)
	start := time.Now()
	rs, err := q.eng().query(hctx, sqlText, args)
	err = translateErr(err, cfg, q.gram().d)
	if err != nil {
		cfg.afterQuery(hctx, ev, start, err, -1, -1)
		return nil, nil, err
	}
	finish := func(scanErr error, returned int64) {
		cfg.afterQuery(hctx, ev, start, scanErr, -1, returned)
	}
	return rs, finish, nil
}

func oneIf(scanned bool) int64 {
	if scanned {
		return 1
	}
	return 0
}

// missIsSuccess scrubs ErrNotFound from hook reporting: a miss is a
// successfully executed query, not a failure.
func missIsSuccess(err error) error {
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func finishQuery(finish func(error, int64), err error, returned int64) {
	if finish == nil {
		return
	}
	finish(missIsSuccess(err), returned)
}

// runRelLayer runs a relation layer's statements first, then its finishes,
// so nested layers start only once their parents' buffers are complete.
func runRelLayer(ctx context.Context, q Queryer, stmts []relStatement, finishes []relFinisher) error {
	if err := runRelStatements(ctx, q, stmts); err != nil {
		return err
	}
	for _, f := range finishes {
		if err := f.finish(ctx); err != nil {
			return err
		}
	}
	return nil
}

// runRelStatements executes a layer's statements — one round trip on a
// batching channel, sequential otherwise; the first error stops either way.
func runRelStatements(ctx context.Context, q Queryer, stmts []relStatement) error {
	if be, ok := q.eng().(batchEngine); ok && len(stmts) > 1 {
		if b, ok := be.batcher(); ok {
			return runRelBatch(ctx, q, b, stmts)
		}
	}
	for i := range stmts {
		st := &stmts[i]
		rows, finish, err := runQueryPhase(ctx, q, st.phase, "select", st.model, st.sqlText, st.args)
		if err != nil {
			return err
		}
		n, err := st.load.consume(rows)
		finishQuery(finish, err, n)
		if err != nil {
			return err
		}
	}
	return nil
}

// runRelBatch is the batched leg; hooks still get one Before/After pair per
// statement, every BeforeQuery chained before the shared send.
func runRelBatch(ctx context.Context, q Queryer, b NativeBatcher, stmts []relStatement) error {
	cfg := q.conf()
	d := q.gram().d
	batch := make([]BatchStatement, len(stmts))
	for i, st := range stmts {
		batch[i] = BatchStatement{SQL: st.sqlText, Args: st.args}
	}
	var evs []*QueryEvent
	hctx := ctx
	if len(cfg.hooks) > 0 {
		evs = make([]*QueryEvent, len(stmts))
		for i, st := range stmts {
			ev := &QueryEvent{Op: "select", Phase: st.phase, Model: st.model, Query: st.sqlText, Args: st.args}
			hctx = cfg.beforeQuery(hctx, ev)
			evs[i] = ev
		}
	}
	start := time.Now()
	res, err := b.QueryBatch(hctx, batch)
	after := func(i int, err error, n int64) {
		if evs != nil {
			cfg.afterQuery(hctx, evs[i], start, missIsSuccess(err), -1, n)
		}
	}
	if err != nil {
		err = translateErr(err, cfg, d)
		for i := range stmts {
			after(i, err, -1)
		}
		return err
	}
	var firstErr error
	var nrw nativeRows // consume closes it, so one adapter serves every statement
	consumed := 0
	for i := range stmts {
		nr, done, rerr := res.Rows()
		if rerr != nil || done {
			if rerr == nil {
				rerr = errors.New("rio: batch returned fewer results than statements")
			}
			firstErr = translateErr(rerr, cfg, d)
			break
		}
		nrw.nr = nr
		n, cerr := stmts[i].load.consume(&nrw)
		cerr = translateErr(cerr, cfg, d)
		after(i, cerr, n)
		consumed++
		if cerr != nil {
			firstErr = cerr
			break
		}
	}
	closeErr := translateErr(res.Close(), cfg, d)
	if firstErr == nil {
		firstErr = closeErr
	}
	if firstErr != nil {
		// Skipped statements still close their events so hooks stay paired.
		for i := consumed; i < len(stmts); i++ {
			after(i, firstErr, -1)
		}
	}
	return firstErr
}
