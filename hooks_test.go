package rio

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"
)

// beforeHook is a QueryHook with the wrapped BeforeQuery and a no-op AfterQuery.
type beforeHook func(ctx context.Context, e *QueryEvent) context.Context

func (h beforeHook) BeforeQuery(ctx context.Context, e *QueryEvent) context.Context {
	return h(ctx, e)
}
func (beforeHook) AfterQuery(context.Context, *QueryEvent) {}

type hookCtxKey struct{}

// deriveHookCtx installs a sentinel so a driver-level probe can see the hook context.
func deriveHookCtx(ctx context.Context, _ *QueryEvent) context.Context {
	return context.WithValue(ctx, hookCtxKey{}, "hooked")
}

// The context BeforeQuery returns is the execution context at the driver.
func TestHookContextReachesReadDriver(t *testing.T) {
	f := newFakeDB()
	var saw any
	f.probe = func(ctx context.Context) { saw = ctx.Value(hookCtxKey{}) }
	db := f.openWith(SQLite, WithQueryHook(beforeHook(deriveHookCtx)))

	f.queueRows(userCols)
	if _, err := From[User]().All(context.Background(), db); err != nil {
		t.Fatalf("All: %v", err)
	}
	if saw != "hooked" {
		t.Fatalf("BeforeQuery context must reach QueryContext, got %v", saw)
	}
}

// A write reaches the driver's ExecContext under the hook context.
func TestHookContextReachesWriteDriver(t *testing.T) {
	f := newFakeDB()
	var saw any
	f.probe = func(ctx context.Context) { saw = ctx.Value(hookCtxKey{}) }
	db := f.openWith(SQLite, WithQueryHook(beforeHook(deriveHookCtx)))

	if _, err := Exec(context.Background(), db, "UPDATE users SET age = age + 1"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if saw != "hooked" {
		t.Fatalf("BeforeQuery context must reach ExecContext, got %v", saw)
	}
}

// Transaction control runs under the hook context too (observable only on the native channel).
func TestHookContextReachesTxBeginAndCommit(t *testing.T) {
	nf := newFakeNative()
	var beginVal, commitVal any
	nf.probe = func(sqlText string, ctx context.Context) {
		switch sqlText {
		case "BEGIN":
			beginVal = ctx.Value(hookCtxKey{})
		case "COMMIT":
			commitVal = ctx.Value(hookCtxKey{})
		}
	}
	db := nf.openWith(Postgres, WithQueryHook(beforeHook(deriveHookCtx)))

	if err := db.Tx(context.Background(), func(tx *Tx) error { return nil }); err != nil {
		t.Fatalf("Tx: %v", err)
	}
	if beginVal != "hooked" {
		t.Fatalf("BeforeQuery context must reach BEGIN, got %v", beginVal)
	}
	if commitVal != "hooked" {
		t.Fatalf("BeforeQuery context must reach COMMIT, got %v", commitVal)
	}
}

// A hook returning nil leaves the incoming context in force, without panicking.
func TestHookNilContextFallsBackToIncoming(t *testing.T) {
	f := newFakeDB()
	var saw any
	f.probe = func(ctx context.Context) { saw = ctx.Value(hookCtxKey{}) }
	db := f.openWith(SQLite, WithQueryHook(beforeHook(
		func(context.Context, *QueryEvent) context.Context { return nil })))

	f.queueRows(userCols)
	ctx := context.WithValue(context.Background(), hookCtxKey{}, "incoming")
	if _, err := From[User]().All(ctx, db); err != nil {
		t.Fatalf("All: %v", err)
	}
	if saw != "incoming" {
		t.Fatalf("nil hook must fall back to the incoming context, got %v", saw)
	}
}

// afterHook records every AfterQuery event.
type afterHook struct{ events []QueryEvent }

func (h *afterHook) BeforeQuery(ctx context.Context, _ *QueryEvent) context.Context { return ctx }
func (h *afterHook) AfterQuery(_ context.Context, e *QueryEvent)                    { h.events = append(h.events, *e) }

func (h *afterHook) byPhase(phase string) []QueryEvent {
	var out []QueryEvent
	for _, e := range h.events {
		if e.Phase == phase {
			out = append(out, e)
		}
	}
	return out
}

// Row-returning statements report RowsReturned; preloads are phase-labeled.
func TestHookSeesRowsReturnedAndPreloadPhase(t *testing.T) {
	f := newFakeDB()
	h := &afterHook{}
	db := f.openWith(SQLite, WithQueryHook(h))
	ctx := context.Background()

	f.queueRows(userCols, userRow(1, "a@x"), userRow(2, "b@x"))
	f.queueRows([]string{"id", "user_id", "title"},
		[]driver.Value{int64(1), int64(1), "t"},
		[]driver.Value{int64(2), int64(1), "t"},
		[]driver.Value{int64(3), int64(2), "t"},
	)
	if _, err := From[User]().With("Posts").All(ctx, db); err != nil {
		t.Fatalf("All: %v", err)
	}

	main := h.byPhase("")
	if len(main) != 1 || main[0].RowsReturned != 2 || main[0].RowsAffected != -1 {
		t.Fatalf("main statement event: %+v", main)
	}
	pre := h.byPhase("preload")
	if len(pre) != 1 || pre[0].RowsReturned != 3 || pre[0].Model != "Post" {
		t.Fatalf("preload event: %+v", pre)
	}
}

// Scalar probes report result-set rows; writes stay at RowsReturned -1.
func TestHookRowsReturnedAcrossShapes(t *testing.T) {
	f := newFakeDB()
	h := &afterHook{}
	db := f.openWith(SQLite, WithQueryHook(h))
	ctx := context.Background()

	f.queueRows([]string{"count"}, []driver.Value{int64(7)})
	if _, err := From[User]().Count(ctx, db); err != nil {
		t.Fatalf("Count: %v", err)
	}
	if e := h.events[len(h.events)-1]; e.RowsReturned != 1 {
		t.Fatalf("Count consumes one result-set row, got %+v", e)
	}

	f.queueRows(userCols) // a miss consumed nothing
	if _, err := From[User]().First(ctx, db); err == nil {
		t.Fatal("First on no rows must miss")
	}
	if e := h.events[len(h.events)-1]; e.RowsReturned != 0 || e.Err != nil {
		t.Fatalf("a miss reports zero rows and no error, got %+v", e)
	}

	f.queueExec(0, 3)
	if _, err := From[User]().Where("age > ?", 1).UpdateAll(ctx, db, Set{"age": 2}); err != nil {
		t.Fatalf("UpdateAll: %v", err)
	}
	if e := h.events[len(h.events)-1]; e.RowsReturned != -1 || e.RowsAffected != 3 {
		t.Fatalf("a write keeps RowsReturned at -1, got %+v", e)
	}
}

// The zero-affected soft-delete probe is labeled "probe".
func TestHookSeesProbePhase(t *testing.T) {
	f := newFakeDB()
	h := &afterHook{}
	db := f.openWith(SQLite, WithQueryHook(h))
	ctx := context.Background()

	stored := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	f.queueExec(0, 0)
	f.queueRows([]string{"deleted_at", "version"}, []driver.Value{stored, int64(7)})
	if err := Delete(ctx, db, &User{ID: 5, Version: 6}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	probes := h.byPhase("probe")
	if len(probes) != 1 || probes[0].RowsReturned != 1 {
		t.Fatalf("probe event: %+v", probes)
	}
}
