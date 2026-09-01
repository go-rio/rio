package rio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"
)

// Driver-module SPI: a driver implements NativeDB, NativeTx, and NativeRows
// around its native client and hands them to NewNative; applications construct
// through the driver module (postgres.OpenNative). As in database/sql/driver,
// those three interfaces are frozen within v1 — new capabilities arrive as
// optional interfaces discovered by type assertion. NativeCell is the sealed,
// rio-implemented counterpart and may grow.

// NativeDB is a driver-native execution channel: what rio needs from a driver
// pool. SQL arrives fully rendered in the dialect's placeholder form; args
// are the bind values rio would hand database/sql. Exec returns the driver's
// affected-row count. Begin maps *sql.TxOptions (possibly nil) onto the
// driver's transaction options. Close releases the channel's resources.
type NativeDB interface {
	Query(ctx context.Context, sql string, args []any) (NativeRows, error)
	Exec(ctx context.Context, sql string, args []any) (rowsAffected int64, err error)
	Begin(ctx context.Context, opts *sql.TxOptions) (NativeTx, error)
	Close() error
}

// NativeTx is one driver-native transaction. Once the transaction has ended —
// committed, rolled back, or destroyed by the driver on its own — Commit and
// Rollback must return an error satisfying errors.Is(err, sql.ErrTxDone),
// translating the driver's own sentinel where needed.
type NativeTx interface {
	Query(ctx context.Context, sql string, args []any) (NativeRows, error)
	Exec(ctx context.Context, sql string, args []any) (int64, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// NativeRows is a driver-native result set. Close returns nothing; errors —
// including those Close itself discovers — converge in Err, which rio reads
// after Close. rio passes the same dest slots, in the same order, for every
// row of one result; each slot is either a NativeCell or a plain pointer
// (scan it as the driver natively would), so implementations may classify
// the dest list on the first Scan and reuse it.
type NativeRows interface {
	Columns() []string
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// NativeScanKind names the plan-time scan strategy of one NativeCell, so a
// NativeRows implementation can pick a typed decode path per column. The
// enum can grow: treat any unrecognized kind as NativeKindScanner — the
// fallback is correct for every kind, only slower.
type NativeScanKind uint8

const (
	// NativeKindScanner is the fallback and zero value: pass the cell
	// itself to the driver's sql.Scanner path.
	NativeKindScanner NativeScanKind = iota
	// NativeKindInt marks an integer field; SetInt64 is its direct path.
	NativeKindInt
	// NativeKindUint marks an unsigned integer field; SetUint64 is its direct path.
	NativeKindUint
	// NativeKindFloat marks a float field; SetFloat64 is its direct path.
	NativeKindFloat
	// NativeKindBool marks a bool field; SetBool is its direct path.
	NativeKindBool
	// NativeKindString marks a string field; SetString is its direct path.
	NativeKindString
	// NativeKindBytes marks a []byte field; SetBytes is its direct path.
	NativeKindBytes
	// NativeKindTime marks a time.Time field; SetTime is its direct path.
	NativeKindTime
	// NativeKindJSON takes the column's raw JSON payload through SetBytes or
	// SetString, not a value decoded driver-side.
	NativeKindJSON
)

// NativeCell is the typed sink a NativeRows implementation feeds decoded
// column values into, one cell per column. Sealed: drivers consume it, never
// implement it, so rio may add Set methods in a minor version without
// breaking a driver.
//
// Every Set method is exactly Scan with the interface boxing removed —
// SetInt64(v) behaves like Scan(int64(v)), SetNull like Scan(nil) — same
// conversion, overflow, and NULL rules, same error shapes, mismatched-kind
// fallback included. SetBytes never retains its argument. SetString stores
// its argument as-is, so hand over an owned string, never an unsafe view of
// driver memory. ScanKind reports the cell's strategy; pointer fields report
// their element's kind, and SetNull stores nil.
type NativeCell interface {
	sql.Scanner // fallback: accepts driver-canonical values
	ScanKind() NativeScanKind
	SetInt64(int64) error
	SetUint64(uint64) error
	SetFloat64(float64) error
	SetBool(bool) error
	SetString(string) error
	SetBytes([]byte) error
	SetTime(time.Time) error
	SetNull() error

	sealedNativeCell()
}

// NativeConfig carries what a driver module hands NewNative, all wired to
// the same underlying pool.
type NativeConfig struct {
	// DB is the native execution channel. Required.
	DB NativeDB

	// Handle is the driver-native pool handle, returned by
	// (*DB).DriverHandle and (*DB).Native.
	Handle any

	// SQLView is an optional database/sql view over the same pool, returned
	// by (*DB).Unwrap (nil without one). (*DB).Close closes it before DB.
	SQLView *sql.DB
}

// NewNative constructs a *DB on a driver-native execution channel.
// Driver-module SPI: applications construct through the driver module
// (postgres.OpenNative). Close on the returned DB closes the SQLView first,
// then the channel. Panics if NativeConfig.DB or dialect is nil, or if opts
// include WithStmtCache — statement caching belongs to the native driver.
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
		panic(
			"rio: WithStmtCache is not supported on the native channel " +
				"(no database/sql prepared statements exist here); statement caching belongs to the driver — " +
				"with pgx, tune the DSN parameter default_query_exec_mode " +
				"(cache_statement is already its default)",
		)
	}
	handle := nc.Handle
	if cfg.driverHandle != nil {
		handle = cfg.driverHandle
	}
	return &DB{
		db:     nc.SQLView,
		e:      &nativeEngine{nd: nc.DB, view: nc.SQLView},
		g:      newGrammar(dialect, cfg),
		cfg:    cfg,
		native: nc.Handle,
		handle: handle,
	}
}

// BatchStatement is one rendered statement of a batch: SQL in the dialect's
// placeholder form and its bind values, as NativeDB.Query receives them.
type BatchStatement struct {
	SQL  string
	Args []any
}

// NativeBatcher is an optional capability of a NativeDB or NativeTx:
// executing a group of independent row-returning statements in one driver
// round trip. Implementations queue every statement and flush once; results
// are consumed strictly in submission order.
type NativeBatcher interface {
	QueryBatch(ctx context.Context, stmts []BatchStatement) (NativeBatchResults, error)
}

// NativeBatchResults yields each batched statement's rows in submission
// order. Rows returns the next statement's result — its NativeRows must be
// fully consumed and closed before the next call — and reports done when
// every statement's result has been handed out. Close releases the batch and
// surfaces any deferred protocol error; it must be called once, after
// consumption stops (early on failure is fine).
type NativeBatchResults interface {
	Rows() (rows NativeRows, done bool, err error)
	Close() error
}

// NativeCopier is an optional capability of a NativeDB or NativeTx:
// bulk-loading rows through the driver's streaming copy protocol (PostgreSQL
// COPY FROM). table is the resolved, unquoted table name split into schema
// segments ([]string{"app", "users"}); the driver quotes each segment. next
// returns one row's bind values in columns order, (nil, nil) when the batch
// is exhausted, or a non-nil error to abort the copy; the returned slice is
// valid only until the next call, so encode it before pulling the next row.
type NativeCopier interface {
	CopyIn(ctx context.Context, table []string, columns []string, next func() ([]any, error)) (int64, error)
}

// batchEngine is the internal seam preload probes for round-trip batching.
type batchEngine interface {
	batcher() (NativeBatcher, bool)
}

// copyEngine is the internal seam InsertAll probes for streaming bulk loads.
type copyEngine interface {
	copier() (NativeCopier, bool)
}

// nativeRows adapts the pgx-shaped NativeRows to the internal rows seam;
// Close returns Err so deferred protocol errors surface at close time.
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

// nativeEngine executes through a NativeDB.
type nativeEngine struct {
	nd   NativeDB
	view *sql.DB // Unwrap's view; closed before the channel
}

func (e *nativeEngine) exec(ctx context.Context, sqlText string, args []any) (sql.Result, error) {
	n, err := e.nd.Exec(ctx, sqlText, args)
	if err != nil {
		return nil, err
	}
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

// close closes the view before the channel: a pool closed first would break
// the view's own close-time connection teardown.
func (e *nativeEngine) close() error {
	var verr error
	if e.view != nil {
		verr = e.view.Close()
	}
	return errors.Join(verr, e.nd.Close())
}

func (e *nativeEngine) batcher() (NativeBatcher, bool) { b, ok := e.nd.(NativeBatcher); return b, ok }
func (e *nativeEngine) copier() (NativeCopier, bool)   { c, ok := e.nd.(NativeCopier); return c, ok }

// nativeTxEngine executes on one NativeTx. Rollback passes ctx through;
// rio's cleanup callers already decouple cancellation.
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

func (e nativeTxEngine) batcher() (NativeBatcher, bool) { b, ok := e.nt.(NativeBatcher); return b, ok }
func (e nativeTxEngine) copier() (NativeCopier, bool)   { c, ok := e.nt.(NativeCopier); return c, ok }
