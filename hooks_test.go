package rio

import (
	"context"
	"testing"
)

// beforeHook is a QueryHook whose BeforeQuery is the wrapped func and whose
// AfterQuery is a no-op — enough to pin the context-propagation contract.
type beforeHook func(ctx context.Context, e *QueryEvent) context.Context

func (h beforeHook) BeforeQuery(ctx context.Context, e *QueryEvent) context.Context {
	return h(ctx, e)
}
func (beforeHook) AfterQuery(context.Context, *QueryEvent) {}

type hookCtxKey struct{}

// deriveHookCtx is a BeforeQuery that installs a sentinel value, so a
// driver-level probe can prove the statement executed under the hook context.
func deriveHookCtx(ctx context.Context, _ *QueryEvent) context.Context {
	return context.WithValue(ctx, hookCtxKey{}, "hooked")
}

// The context BeforeQuery returns is the execution context: a read reaches the
// driver's QueryContext under it.
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

// Transaction control runs under the hook context too. The native channel is
// the observable one here: database/sql's Tx.Commit takes no context, so only
// a native engine can carry the hook's context into the commit.
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

// A hook returning nil must not panic and must leave the incoming context in
// force as the execution context.
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
