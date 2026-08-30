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
	// Op is a stable statement label: "select", "insert", "update",
	// "delete", "upsert", "copy", "raw", "exec", "begin", "commit",
	// "rollback", "savepoint".
	Op string
	// Model is the Go struct name behind the statement, "" for Raw/Exec and
	// transaction control.
	Model string
	// Query is the rendered, dialect-form SQL.
	Query string
	// Args are the bind arguments, nil when the DB was built WithoutArgs.
	Args []any
	// Err is the translated execution error, nil on success; a write whose
	// Result.RowsAffected fails carries that failure here. After only.
	Err error
	// Duration is the execution wall time; for row-returning queries it runs
	// through row consumption. After only.
	Duration time.Duration
	// RowsAffected is the driver-reported count for writes, -1 when unknown
	// (row-returning queries, or a report failure carried in Err). After only.
	RowsAffected int64
	// RowsReturned is how many rows the statement handed back, -1 for
	// statements that return none. Count and Exists report their result-set
	// rows (one), not the value they carry. After only.
	RowsReturned int64
	// Phase labels statements rio itself derives — "preload" and "count"
	// for With/WithCount relation queries, "probe" for internal write
	// probes; "" for everything else.
	Phase string
}

// QueryHook observes statement execution. The context BeforeQuery returns is
// the execution context: the statement and its row consumption run under it,
// and AfterQuery receives it; returning nil leaves the incoming context in
// force. Hooks must not retain the event past the call and cannot alter the
// statement.
//
// For row-returning queries AfterQuery fires once the rows are consumed, so
// Err includes scan and iteration failures. Exception: a First/Find/Sole
// miss reports Err = nil — ErrNotFound is a successfully executed query.
// Batched or streamed native execution still fires events per logical
// statement: every BeforeQuery runs before the one wire exchange, contexts
// chaining into the single execution context, and a mid-batch failure
// reports the remaining statements' AfterQuery with the same error.
//
// The method set is fixed: new hook capabilities arrive as optional
// interfaces discovered by type assertion, never as methods added here.
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
