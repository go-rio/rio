package rio

import (
	"context"
	"time"
)

// QueryEvent describes one statement execution. Hooks receive the same event
// pointer in Before and After; After sees Err, Duration, and RowsAffected
// filled in.
type QueryEvent struct {
	// Op is a stable label usable as a metrics dimension without parsing
	// SQL: "select", "insert", "update", "delete", "upsert", "raw", "exec",
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

func (c *config) afterQuery(ctx context.Context, e *QueryEvent, start time.Time, err error, rows int64) {
	if len(c.hooks) == 0 {
		return
	}
	e.Err = err
	e.Duration = time.Since(start)
	e.RowsAffected = rows
	for i := len(c.hooks) - 1; i >= 0; i-- {
		c.hooks[i].AfterQuery(ctx, e)
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
