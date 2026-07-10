package rio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"
)

// This file is rio's native driver SPI — the equivalent of database/sql/driver
// for driver-native execution channels. A driver module (go-rio/postgres)
// implements these small, std-types-only interfaces around its native client
// (pgxpool) and hands them to NewNative; every rio semantic above the engine
// seam — rendering, hooks, error translation, scanning rules, savepoints —
// stays the channel-independent core. Application code never touches this
// surface: construct through the driver module (postgres.OpenNative) and keep
// writing the same rio calls.

// NativeDB is a driver-native execution channel: what rio needs from a driver
// pool. Driver-module SPI, not application API.
//
// SQL arrives fully rendered in the dialect's placeholder form; args are the
// bind values rio would hand database/sql. Exec returns the statement's
// affected-row count (the driver's command tag). Begin maps *sql.TxOptions
// (possibly nil) onto the driver's transaction options. Close releases the
// channel's resources — for a pooled driver, the pool itself.
type NativeDB interface {
	Query(ctx context.Context, sql string, args []any) (NativeRows, error)
	Exec(ctx context.Context, sql string, args []any) (rowsAffected int64, err error)
	Begin(ctx context.Context, opts *sql.TxOptions) (NativeTx, error)
	Close() error
}

// NativeTx is one driver-native transaction. Driver-module SPI, not
// application API.
//
// Finished-transaction contract: once the transaction has ended — committed,
// rolled back, or destroyed by the driver on its own (a begin context that
// died, a broken connection) — Commit and Rollback must return an error
// satisfying errors.Is(err, sql.ErrTxDone), translating the driver's own
// sentinel (pgx.ErrTxClosed) where needed. rio's cleanup paths tolerate
// exactly sql.ErrTxDone, which keeps rollback semantics identical across
// channels without the core importing any driver.
type NativeTx interface {
	Query(ctx context.Context, sql string, args []any) (NativeRows, error)
	Exec(ctx context.Context, sql string, args []any) (int64, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// NativeRows is a driver-native result set, shaped like pgx.Rows: Close
// returns nothing, and errors — including those Close itself discovers, such
// as the failed statement behind an undrained single-row read — converge in
// Err. rio reads Err after Close, so deferred protocol errors still surface
// (see mergeClose). Driver-module SPI, not application API.
//
// rio passes the same dest slots, in the same order, for every row of one
// result. Each slot is either a NativeCell (rio's per-column typed sink) or a
// plain pointer (rare; scan it the way the driver natively would).
// Implementations may therefore classify the dest list on the first Scan and
// reuse that classification for the remaining rows.
type NativeRows interface {
	Columns() []string
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// NativeScanKind names the plan-time scan strategy of one NativeCell, so a
// NativeRows implementation can pick a typed decode path per column.
// Driver-module SPI, not application API.
//
// The enum can grow. Treat any kind you do not recognize as
// NativeKindScanner: the sql.Scanner fallback is correct for every kind —
// only slower — because the cell's Scan accepts the same driver-canonical
// values database/sql delivers.
type NativeScanKind uint8

const (
	// NativeKindScanner is the fallback: pass the cell itself to the
	// driver's sql.Scanner path. Also the zero value, so an adapter that
	// never learned a newer kind degrades to correct-but-slower.
	NativeKindScanner NativeScanKind = iota
	NativeKindInt
	NativeKindUint
	NativeKindFloat
	NativeKindBool
	NativeKindString
	NativeKindBytes
	NativeKindTime
	NativeKindJSON
)

// NativeCell is the typed sink a NativeRows implementation feeds decoded
// column values into — rio's side of the native scan path, one cell per
// column. Driver-module SPI, not application API.
//
// Every Set method is exactly Scan with the interface boxing removed:
// SetInt64(v) behaves like Scan(int64(v)), SetNull like Scan(nil), and so on
// — same conversion rules, same overflow and NULL handling, same error
// shapes. The equivalence holds by construction: both paths run the same
// store helpers (scan.go). SetBytes never retains v — driver memory is
// copied where it is stored. SetString stores its argument as-is, so hand
// over an owned string, never an unsafe view of driver memory.
//
// ScanKind reports the cell's strategy. Pointer fields report their
// element's kind: the sinks allocate and publish the *T cell internally and
// SetNull stores nil, so pointer-ness never crosses the SPI.
type NativeCell interface {
	sql.Scanner // the fallback path: the cell scans driver-canonical values itself
	ScanKind() NativeScanKind
	SetInt64(int64) error
	SetFloat64(float64) error
	SetBool(bool) error
	SetString(string) error
	SetBytes([]byte) error
	SetTime(time.Time) error
	SetNull() error
}

// NativeConfig carries what a driver module hands NewNative, all wired to
// the same underlying pool.
type NativeConfig struct {
	// DB is the native execution channel. Required.
	DB NativeDB

	// Handle is the driver-native pool handle (a *pgxpool.Pool under
	// go-rio/postgres), returned verbatim by (*DB).Native so the driver
	// module's typed accessors (postgres.PoolOf) can reach it.
	Handle any

	// SQLView is an optional database/sql view over the same pool, returned
	// by (*DB).Unwrap so pool-agnostic helpers (pings, migrations) keep
	// working on the native channel. (*DB).Close closes it before DB.
	// Without one, Unwrap returns nil.
	SQLView *sql.DB
}

// NewNative constructs a *DB on a driver-native execution channel.
// Driver-module SPI, not application API — applications construct through
// the driver module (postgres.OpenNative), which wires the adapter, the pool
// handle, and the database/sql view together.
//
// Like New taking over the *sql.DB's Close, Close on the returned DB closes
// what the config carries: first the SQLView, then the channel (whose
// adapter closes the pool). WithStmtCache panics here — a native channel has
// no database/sql prepared statements; statement caching belongs to the
// driver (with pgx, the DSN parameter default_query_exec_mode, which
// defaults to caching already).
func NewNative(nc NativeConfig, dialect Dialect, opts ...Option) *DB {
	if nc.DB == nil {
		panic("rio: NewNative: NativeConfig.DB must not be nil")
	}
	if dialect == nil {
		panic("rio: NewNative: dialect must not be nil")
	}
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.stmtCache {
		// Construction-time misuse, as in New: there is no configuration
		// under which a native channel serves database/sql prepared
		// statements — the driver owns statement caching there.
		panic("rio: WithStmtCache is not supported on the native channel (no database/sql prepared statements exist here); statement caching belongs to the driver — with pgx, tune the DSN parameter default_query_exec_mode (cache_statement is already its default)")
	}
	return &DB{
		db:     nc.SQLView,
		e:      &nativeEngine{nd: nc.DB, view: nc.SQLView},
		g:      newGrammar(dialect, cfg),
		cfg:    cfg,
		native: nc.Handle,
	}
}

// nativeEngine executes through a NativeDB. It carries no statement cache:
// prepared statements belong to the native driver's own machinery.
type nativeEngine struct {
	nd   NativeDB
	view *sql.DB // Unwrap's view; closed by close() ahead of the channel
}

func (e *nativeEngine) exec(ctx context.Context, sqlText string, args []any) (sql.Result, error) {
	n, err := e.nd.Exec(ctx, sqlText, args)
	if err != nil {
		return nil, err
	}
	// driver.RowsAffected is the exact result pgx's database/sql adapter
	// returns for an Exec: RowsAffected reports the command tag's count and
	// never fails, LastInsertId returns the stdlib's own "not supported"
	// error — the stdlib channel's behavior, verbatim by construction.
	return driver.RowsAffected(n), nil
}

func (e *nativeEngine) query(ctx context.Context, sqlText string, args []any) (rows, error) {
	nr, err := e.nd.Query(ctx, sqlText, args)
	if err != nil {
		return nil, err
	}
	return &nativeRows{nr: nr}, nil
}

func (e *nativeEngine) begin(ctx context.Context, opts *sql.TxOptions) (txEngine, error) {
	nt, err := e.nd.Begin(ctx, opts)
	if err != nil {
		return nil, err
	}
	return nativeTxEngine{nt: nt}, nil
}

// close closes the database/sql view first, then the channel (the driver
// module's adapter closes the pool there) — construction in reverse, and the
// only order that is clean on both sides: closing a pgx view never touches
// the pool, while a pool closed first would fail the view's own close-time
// connection teardown.
func (e *nativeEngine) close() error {
	var verr error
	if e.view != nil {
		verr = e.view.Close()
	}
	return errors.Join(verr, e.nd.Close())
}

// nativeTxEngine executes on one NativeTx. Rollback passes the context it is
// given straight through: rio's cleanup callers (finishTx, spExec) already
// decouple cancellation — the caller-owned WithoutCancel discipline the
// engine seam documents.
type nativeTxEngine struct {
	nt NativeTx
}

func (e nativeTxEngine) exec(ctx context.Context, sqlText string, args []any) (sql.Result, error) {
	n, err := e.nt.Exec(ctx, sqlText, args)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(n), nil
}

func (e nativeTxEngine) query(ctx context.Context, sqlText string, args []any) (rows, error) {
	nr, err := e.nt.Query(ctx, sqlText, args)
	if err != nil {
		return nil, err
	}
	return &nativeRows{nr: nr}, nil
}

func (e nativeTxEngine) commit(ctx context.Context) error   { return e.nt.Commit(ctx) }
func (e nativeTxEngine) rollback(ctx context.Context) error { return e.nt.Rollback(ctx) }

// nativeRows adapts the SPI's pgx-shaped result — Close without a return
// value, errors converging in Err — to the internal rows seam. Close-then-Err
// keeps mergeClose's promise: a deferred protocol error behind an undrained
// result surfaces at close time.
type nativeRows struct {
	nr NativeRows
}

func (r *nativeRows) Columns() ([]string, error) { return r.nr.Columns(), nil }
func (r *nativeRows) Next() bool                 { return r.nr.Next() }
func (r *nativeRows) Scan(dest ...any) error     { return r.nr.Scan(dest...) }
func (r *nativeRows) Err() error                 { return r.nr.Err() }

func (r *nativeRows) Close() error {
	r.nr.Close()
	return r.nr.Err()
}
