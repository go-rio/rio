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
	// Err is the translated execution error, nil on success. After only.
	Err error
	// Duration is the execution wall time. After only.
	Duration time.Duration
	// RowsAffected is the driver-reported count for writes, -1 when unknown
	// (row-returning queries). After only.
	RowsAffected int64
}

// QueryHook observes statement execution. BeforeQuery may derive a context
// (tracing spans); AfterQuery completes it. Hooks must not retain the event
// past the call and cannot modify the statement — rio has no mutating
// middleware by design.
//
// The event covers statement execution: for row-returning queries, Err and
// Duration describe sending the query, not consuming the rows — scan and
// iteration failures surface only through the call's returned error.
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
		ctx = h.BeforeQuery(ctx, e)
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
