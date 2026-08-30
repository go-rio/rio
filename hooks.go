package rio

import (
	"context"
	"slices"
	"time"
)

// QueryEvent describes one statement execution. Hooks receive the same event
// pointer in Before and After; After sees Err, Duration, RowsAffected, and
// RowsReturned filled in.
type QueryEvent struct {
	// Op is a stable label usable as a metrics dimension without parsing
	// SQL: "select", "insert", "update", "delete", "upsert", "copy", "raw", "exec",
	// "begin", "commit", "rollback", "savepoint".
	Op string
	// Model is the Go struct name behind the statement, "" for Raw/Exec and
	// transaction control.
	Model string
	// Query is the rendered, dialect-form SQL.
	Query string
	// Args are the bind arguments, nil when the DB was built WithoutArgs.
	Args []any
	// Err is the translated execution error, nil on success. A write whose
	// Result.RowsAffected fails carries that failure here — the caller
	// returns the same error, and the hook must not record the statement as
	// a success. After only.
	Err error
	// Duration is the execution wall time; for row-returning queries it runs
	// through row consumption. After only.
	Duration time.Duration
	// RowsAffected is the driver-reported count for writes, -1 when unknown
	// (row-returning queries, or the driver failed to report it — the
	// failure is then in Err). After only.
	RowsAffected int64
	// RowsReturned is how many rows the statement handed back through row
	// consumption, -1 for statements that return none (writes, transaction
	// control). Count and Exists report their result-set rows (one), not
	// the value they carry. After only.
	RowsReturned int64
	// Phase labels the secondary statements rio itself derives: "preload"
	// and "count" for the relation queries With and WithCount add, "probe"
	// for the internal zero-affected write probes. Every other statement —
	// including the helper reads inside relation writes — carries "".
	Phase string
}

// QueryHook observes statement execution. The context BeforeQuery returns is
// the execution context: rio runs the statement — and, for row-returning
// queries, the row consumption its context governs — under it, so a tracing
// span or deadline the hook installs flows into the driver, and AfterQuery
// receives that same context. Returning nil leaves the incoming context in
// force. Hooks must not retain the event past the call and cannot alter the
// statement (Op, Query, Args) — rio has no mutating middleware by design.
//
// For row-returning queries AfterQuery fires once the rows are consumed:
// Err includes scan and iteration failures, and Duration spans execution
// through row consumption. One exception: a First/Find/Sole miss reports
// Err = nil — ErrNotFound is a successfully executed query, and telemetry
// would otherwise count every miss as an error.
//
// When a native driver batches a relation layer or streams a bulk insert,
// events still fire per logical statement, but the statements share one wire
// exchange: every BeforeQuery fires before the send and the contexts chain
// into the one execution context, and a failure mid-batch reports the
// remaining statements' AfterQuery with the same error.
//
// The method set is fixed: later hook capabilities arrive as optional
// interfaces a hook may also satisfy, discovered by type assertion, never as
// methods added here — so existing hooks keep compiling.
type QueryHook interface {
	BeforeQuery(ctx context.Context, e *QueryEvent) context.Context
	AfterQuery(ctx context.Context, e *QueryEvent)
}

func (c *config) beforeQuery(ctx context.Context, e *QueryEvent) context.Context {
	if !c.logArgs {
		e.Args = nil
	} else {
		e.Args = cloneEventArgs(e.Args)
	}
	for _, h := range c.hooks {
		// nil from a hook keeps the context in force rather than nil-ing the
		// chain: the execution context this returns is never nil.
		if next := h.BeforeQuery(ctx, e); next != nil {
			ctx = next
		}
	}
	return ctx
}

func (c *config) afterQuery(
	ctx context.Context,
	e *QueryEvent,
	start time.Time,
	err error,
	affected, returned int64,
) {
	if len(c.hooks) == 0 {
		return
	}
	e.Err = err
	e.Duration = time.Since(start)
	e.RowsAffected = affected
	e.RowsReturned = returned
	for _, v := range slices.Backward(c.hooks) {
		v.AfterQuery(ctx, e)
	}
}

func cloneEventArgs(args []any) []any {
	if len(args) == 0 {
		return nil
	}
	out := append([]any(nil), args...)
	for i, a := range out {
		if b, ok := a.([]byte); ok && b != nil {
			cp := append([]byte(nil), b...)
			out[i] = cp
		}
	}
	return out
}
