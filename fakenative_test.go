package rio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeNative is a zero-dependency NativeDB that records every statement and
// serves scripted results through the NativeCell typed sinks; like a real
// driver it refuses statements on a dead context without sending them.
type fakeNative struct {
	mu      sync.Mutex
	log     []fakeNativeStmt
	results []fakeNativeRowsData
	execs   []fakeNativeExec
	failOn  map[string]error
	closed  bool

	batches     int // QueryBatch flushes, for round-trip assertions
	copies      int // CopyIn streams, for copy-path assertions
	beginErr    error
	rollbackErr error // forced Rollback result (sql.ErrTxDone injection)
	lastTxOpts  *sql.TxOptions
	// probe, when non-nil, sees every statement's SQL and executing context, begin/commit/rollback included.
	probe func(sqlText string, ctx context.Context)
}

type fakeNativeStmt struct {
	sql  string
	args []any
	tx   bool
}

type fakeNativeRowsData struct {
	cols []string
	rows [][]any
	// errAfter surfaces from Err only after the rows are exhausted or closed — pgx's deferred-error shape.
	errAfter error
}

type fakeNativeExec struct {
	affected int64
}

func newFakeNative() *fakeNative {
	return &fakeNative{failOn: map[string]error{}}
}

func (f *fakeNative) open(d ...Dialect) *DB {
	dialect := Dialect(Postgres)
	if len(d) > 0 {
		dialect = d[0]
	}
	return NewNative(NativeConfig{DB: f}, dialect, WithClock(fixedClock))
}

func (f *fakeNative) openWith(dialect Dialect, opts ...Option) *DB {
	return NewNative(NativeConfig{DB: f}, dialect, append([]Option{WithClock(fixedClock)}, opts...)...)
}

func (f *fakeNative) queueRows(cols []string, rows ...[]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results = append(f.results, fakeNativeRowsData{cols: cols, rows: rows})
}

func (f *fakeNative) queueRowsCloseErr(errAfter error, cols []string, rows ...[]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results = append(f.results, fakeNativeRowsData{cols: cols, rows: rows, errAfter: errAfter})
}

// queueExec scripts the next Exec's affected count; unscripted execs report 1.
func (f *fakeNative) queueExec(affected int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs = append(f.execs, fakeNativeExec{affected: affected})
}

func (f *fakeNative) failContaining(sub string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failOn[sub] = err
}

func (f *fakeNative) logged() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.log))
	for i, s := range f.log {
		out[i] = s.sql
	}
	return out
}

func (f *fakeNative) loggedContaining(sub string) []fakeNativeStmt {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeNativeStmt
	for _, s := range f.log {
		if strings.Contains(s.sql, sub) {
			out = append(out, s)
		}
	}
	return out
}

// record logs one statement, refusing dead contexts before "sending", like a real driver stack.
func (f *fakeNative) record(ctx context.Context, sqlText string, args []any, inTx bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.probe != nil {
		f.probe(sqlText, ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, fakeNativeStmt{sql: sqlText, args: args, tx: inTx})
	for sub, err := range f.failOn {
		if strings.Contains(sqlText, sub) {
			return err
		}
	}
	return nil
}

func (f *fakeNative) nextRows() fakeNativeRowsData {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.results) == 0 {
		return fakeNativeRowsData{}
	}
	r := f.results[0]
	f.results = f.results[1:]
	return r
}

func (f *fakeNative) nextExec() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.execs) == 0 {
		return 1
	}
	n := f.execs[0].affected
	f.execs = f.execs[1:]
	return n
}

// --- NativeDB ---

func (f *fakeNative) Query(ctx context.Context, sqlText string, args []any) (NativeRows, error) {
	if err := f.record(ctx, sqlText, args, false); err != nil {
		return nil, err
	}
	return &fakeNativeRows{data: f.nextRows()}, nil
}

func (f *fakeNative) Exec(ctx context.Context, sqlText string, args []any) (int64, error) {
	if err := f.record(ctx, sqlText, args, false); err != nil {
		return 0, err
	}
	return f.nextExec(), nil
}

func (f *fakeNative) Begin(ctx context.Context, opts *sql.TxOptions) (NativeTx, error) {
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	if err := f.record(ctx, "BEGIN", nil, false); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.lastTxOpts = opts
	f.mu.Unlock()
	return &fakeNativeTx{f: f}, nil
}

func (f *fakeNative) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// --- NativeTx ---

type fakeNativeTx struct {
	f    *fakeNative
	done bool
}

func (t *fakeNativeTx) Query(ctx context.Context, sqlText string, args []any) (NativeRows, error) {
	if t.done {
		return nil, sql.ErrTxDone
	}
	if err := t.f.record(ctx, sqlText, args, true); err != nil {
		return nil, err
	}
	return &fakeNativeRows{data: t.f.nextRows()}, nil
}

func (t *fakeNativeTx) Exec(ctx context.Context, sqlText string, args []any) (int64, error) {
	if t.done {
		return 0, sql.ErrTxDone
	}
	if err := t.f.record(ctx, sqlText, args, true); err != nil {
		return 0, err
	}
	return t.f.nextExec(), nil
}

func (t *fakeNativeTx) Commit(ctx context.Context) error {
	if t.done {
		return sql.ErrTxDone
	}
	if err := t.f.record(ctx, "COMMIT", nil, true); err != nil {
		return err
	}
	t.done = true
	return nil
}

// Rollback reports sql.ErrTxDone when finished and refuses dead contexts, per the SPI contract.
func (t *fakeNativeTx) Rollback(ctx context.Context) error {
	if t.done {
		return sql.ErrTxDone
	}
	if t.f.rollbackErr != nil {
		t.done = true
		return t.f.rollbackErr
	}
	if err := t.f.record(ctx, "ROLLBACK", nil, true); err != nil {
		return err
	}
	t.done = true
	return nil
}

// --- NativeRows ---

type fakeNativeRows struct {
	data   fakeNativeRowsData
	pos    int
	done   bool
	closed bool
}

func (r *fakeNativeRows) Columns() []string { return r.data.cols }

func (r *fakeNativeRows) Next() bool {
	if r.closed || r.done {
		return false
	}
	if r.pos >= len(r.data.rows) {
		// Exhaustion makes deferred errors visible to Err, as with a real driver.
		r.done = true
		return false
	}
	r.pos++
	return true
}

func (r *fakeNativeRows) Scan(dest ...any) error {
	if r.pos == 0 || r.pos > len(r.data.rows) {
		return errors.New("fakenative: Scan without a row")
	}
	row := r.data.rows[r.pos-1]
	if len(dest) != len(row) {
		return fmt.Errorf("fakenative: %d dest for %d columns", len(dest), len(row))
	}
	for i, d := range dest {
		if err := assignNativeDest(d, row[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *fakeNativeRows) Err() error {
	if r.done || r.closed {
		return r.data.errAfter
	}
	return nil
}

func (r *fakeNativeRows) Close() { r.closed = true }

// assignNativeDest routes one scripted value like a native codec: typed sinks
// for NativeCell dests, *int64 for WithCount's count column, cell.Scan otherwise.
func assignNativeDest(dest, v any) error {
	cell, ok := dest.(NativeCell)
	if !ok {
		switch d := dest.(type) {
		case *int64:
			n, ok := v.(int64)
			if !ok {
				return fmt.Errorf("fakenative: cannot scan %T into *int64", v)
			}
			*d = n
			return nil
		}
		return fmt.Errorf("fakenative: unsupported dest %T", dest)
	}
	switch tv := v.(type) {
	case nil:
		return cell.SetNull()
	case int64:
		return cell.SetInt64(tv)
	case uint64:
		return cell.SetUint64(tv)
	case float64:
		return cell.SetFloat64(tv)
	case bool:
		return cell.SetBool(tv)
	case string:
		return cell.SetString(tv)
	case []byte:
		return cell.SetBytes(tv)
	case time.Time:
		return cell.SetTime(tv)
	default:
		return cell.Scan(tv)
	}
}

func nativeUserRow(id int64, email string) []any {
	return []any{id, email, int64(30), nil, int64(1), nil, testNow, testNow}
}

// --- behavior: the native channel replays the stdlib channel's contracts ---

// The rendered SQL is channel-independent — the engine seam feeds it through untouched.
func TestNativeAllRendersAndScansTypedRows(t *testing.T) {
	nf := newFakeNative()
	db := nf.open()
	nf.queueRows(userCols, nativeUserRow(1, "a@x"), nativeUserRow(2, "b@x"))

	users, err := From[User]().Where("age > ?", 18).OrderBy("created_at DESC").Limit(10).All(context.Background(), db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(users) != 2 || users[0].Email != "a@x" || users[1].ID != 2 {
		t.Fatalf("scanned %+v", users)
	}
	if users[0].Bio != nil || users[0].DeletedAt != nil {
		t.Fatalf("NULL columns must be nil pointers: %+v", users[0])
	}
	if users[0].Age != 30 || users[0].Version != 1 || !users[0].CreatedAt.Equal(testNow) {
		t.Fatalf("typed values misrouted: %+v", users[0])
	}
	want := `SELECT "users"."id", "users"."email", "users"."age", "users"."bio", "users"."version", "users"."deleted_at", "users"."created_at", "users"."updated_at" FROM "users" WHERE (age > $1) AND "users"."deleted_at" IS NULL ORDER BY created_at DESC LIMIT 10`
	if got := nf.logged()[0]; got != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", got, want)
	}
	// Native args are rio's bind values verbatim — 18 stays an int.
	if args := nf.loggedContaining("SELECT")[0].args; len(args) != 1 || args[0] != 18 {
		t.Fatalf("args = %v", args)
	}
}

func TestNativeInsertReturningBackfills(t *testing.T) {
	nf := newFakeNative()
	db := nf.open()
	nf.queueRows([]string{"id"}, []any{int64(7)})

	u := &User{Email: "a@x", Age: 30}
	if err := Insert(context.Background(), db, u); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if u.ID != 7 {
		t.Fatalf("ID = %d, want backfilled 7", u.ID)
	}
	want := `INSERT INTO "users" ("email", "age", "bio", "version", "deleted_at", "created_at", "updated_at") VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING "id"`
	if got := nf.logged()[0]; got != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", got, want)
	}
}

func TestNativeFindMissReportsNotFound(t *testing.T) {
	nf := newFakeNative()
	db := nf.open()
	nf.queueRows(userCols)

	_, err := Find[User](context.Background(), db, int64(1))
	if !errors.Is(err, ErrNotFound) || !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("want ErrNotFound wrapping sql.ErrNoRows, got %v", err)
	}
}

func TestNativeUpdateAffectedCountSemantics(t *testing.T) {
	ctx := context.Background()

	t.Run("versioned zero affected is stale", func(t *testing.T) {
		nf := newFakeNative()
		db := nf.open()
		nf.queueExec(0)
		u := &User{ID: 1, Email: "a@x", Age: 30, Version: 3, CreatedAt: testNow, UpdatedAt: testNow}
		if err := Update(ctx, db, u); !errors.Is(err, ErrStaleObject) {
			t.Fatalf("want ErrStaleObject, got %v", err)
		}
	})

	t.Run("versionless zero affected is not found, no probe", func(t *testing.T) {
		nf := newFakeNative()
		db := nf.open()
		nf.queueExec(0)
		p := &Post{ID: 9, UserID: 1, Title: "x"}
		if err := Update(ctx, db, p); !errors.Is(err, ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
		// PostgreSQL counts matched rows; the MySQL-only pk probe must not run.
		if logs := nf.logged(); len(logs) != 1 {
			t.Fatalf("expected the UPDATE alone, got %v", logs)
		}
	})

	t.Run("success bumps version", func(t *testing.T) {
		nf := newFakeNative()
		db := nf.open()
		u := &User{ID: 1, Email: "a@x", Age: 30, Version: 3, CreatedAt: testNow, UpdatedAt: testNow}
		if err := Update(ctx, db, u); err != nil {
			t.Fatalf("Update: %v", err)
		}
		if u.Version != 4 {
			t.Fatalf("version = %d, want 4", u.Version)
		}
	})
}

// Exec's sql.Result is driver.RowsAffected, so both channels refuse LastInsertId with the same words.
func TestNativeExecResultShape(t *testing.T) {
	nf := newFakeNative()
	db := nf.open()
	nf.queueExec(5)

	res, err := Exec(context.Background(), db, "UPDATE users SET age = age + 1")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 5 {
		t.Fatalf("RowsAffected = %d, %v", n, err)
	}
	if _, err := res.LastInsertId(); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("LastInsertId must fail like pgx's stdlib driver, got %v", err)
	}
	var std sql.Result = driver.RowsAffected(0)
	_, stdErr := std.LastInsertId()
	_, natErr := res.LastInsertId()
	if natErr.Error() != stdErr.Error() {
		t.Fatalf("error text drifted: %q vs %q", natErr, stdErr)
	}
}

func TestNativeTxBoundariesAndOptions(t *testing.T) {
	ctx := context.Background()
	nf := newFakeNative()
	db := nf.open()
	nf.queueRows([]string{"id"}, []any{int64(1)})

	opts := &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true}
	err := db.TxWith(ctx, opts, func(tx *Tx) error {
		return Insert(ctx, tx, &Post{UserID: 1, Title: "t"})
	})
	if err != nil {
		t.Fatalf("TxWith: %v", err)
	}
	logs := nf.logged()
	if logs[0] != "BEGIN" || logs[len(logs)-1] != "COMMIT" {
		t.Fatalf("boundaries: %v", logs)
	}
	if nf.lastTxOpts == nil || nf.lastTxOpts.Isolation != sql.LevelSerializable || !nf.lastTxOpts.ReadOnly {
		t.Fatalf("TxOptions must reach the channel: %+v", nf.lastTxOpts)
	}
	if ins := nf.loggedContaining("INSERT"); len(ins) != 1 || !ins[0].tx {
		t.Fatalf("the INSERT must run on the transaction, got %+v", ins)
	}

	boom := errors.New("boom")
	err = db.Tx(ctx, func(tx *Tx) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("Tx: %v", err)
	}
	logs = nf.logged()
	if logs[len(logs)-1] != "ROLLBACK" {
		t.Fatalf("failed fn must roll back: %v", logs)
	}
}

func TestNativeSavepointChoreography(t *testing.T) {
	ctx := context.Background()
	nf := newFakeNative()
	db := nf.open()

	boom := errors.New("inner failed")
	err := db.Tx(ctx, func(tx *Tx) error {
		if err := tx.Tx(ctx, func(sp *Tx) error { return nil }); err != nil {
			return err
		}
		if err := tx.Tx(ctx, func(sp *Tx) error { return boom }); !errors.Is(err, boom) {
			return fmt.Errorf("savepoint error lost: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Tx: %v", err)
	}
	want := []string{
		"BEGIN",
		"SAVEPOINT rio_sp_1",
		"RELEASE SAVEPOINT rio_sp_1",
		"SAVEPOINT rio_sp_2",
		"ROLLBACK TO SAVEPOINT rio_sp_2",
		"RELEASE SAVEPOINT rio_sp_2",
		"COMMIT",
	}
	got := nf.logged()
	if strings.Join(got, " | ") != strings.Join(want, " | ") {
		t.Fatalf("choreography:\n got: %v\nwant: %v", got, want)
	}
	for _, s := range nf.loggedContaining("SAVEPOINT") {
		if !s.tx {
			t.Fatalf("savepoint statement escaped the transaction: %+v", s)
		}
	}
}

// The fake refuses statements on a dead context, so savepoint cleanup must run on the decoupled context.
func TestNativeSavepointCleanupSurvivesCanceledContext(t *testing.T) {
	nf := newFakeNative()
	db := nf.open()
	nf.queueRows([]string{"id"}, []any{int64(1)})

	boom := errors.New("inner work failed after its context died")
	err := db.Tx(context.Background(), func(tx *Tx) error {
		inner, cancel := context.WithCancel(context.Background())
		defer cancel()
		spErr := tx.Tx(inner, func(sp *Tx) error {
			if err := Insert(inner, sp, &Post{UserID: 1, Title: "leak"}); err != nil {
				return err
			}
			cancel()
			return boom
		})
		if !errors.Is(spErr, boom) {
			t.Fatalf("inner savepoint must report the failure: %v", spErr)
		}
		if errors.Is(spErr, context.Canceled) {
			t.Fatalf("cleanup must not fail on the dead context: %v", spErr)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("outer Tx: %v", err)
	}
	logs := nf.logged()
	joined := strings.Join(logs, " | ")
	for _, want := range []string{"SAVEPOINT rio_sp_1", "ROLLBACK TO SAVEPOINT rio_sp_1", "RELEASE SAVEPOINT rio_sp_1"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q — the inner insert leaks into the outer commit: %s", want, joined)
		}
	}
	if logs[len(logs)-1] != "COMMIT" {
		t.Fatalf("outer transaction must commit: %s", joined)
	}
}

func TestNativeSavepointPanicRollbackSurvivesCanceledContext(t *testing.T) {
	nf := newFakeNative()
	db := nf.open()

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the panic must propagate")
			}
		}()
		_ = db.Tx(context.Background(), func(tx *Tx) error {
			inner, cancel := context.WithCancel(context.Background())
			return tx.Tx(inner, func(sp *Tx) error {
				cancel()
				panic("inner panic with a dead context")
			})
		})
	}()
	joined := strings.Join(nf.logged(), " | ")
	if !strings.Contains(joined, "ROLLBACK TO SAVEPOINT rio_sp_1") {
		t.Fatalf("panic-path ROLLBACK TO must still be sent: %s", joined)
	}
}

// finishTx must reach the channel even when the transaction's context died mid-fn.
func TestNativeRollbackRunsOnDecoupledContext(t *testing.T) {
	nf := newFakeNative()
	db := nf.open()

	boom := errors.New("fn failed after killing its ctx")
	ctx, cancel := context.WithCancel(context.Background())
	err := db.Tx(ctx, func(tx *Tx) error {
		cancel()
		return boom
	})
	if !errors.Is(err, boom) || errors.Is(err, context.Canceled) {
		t.Fatalf("want the fn error alone, got %v", err)
	}
	logs := nf.logged()
	if logs[len(logs)-1] != "ROLLBACK" {
		t.Fatalf("ROLLBACK must be sent on the decoupled context: %v", logs)
	}
}

// A transaction the driver already finished reports sql.ErrTxDone; finishTx tolerates it.
func TestNativeRollbackToleratesTxDone(t *testing.T) {
	nf := newFakeNative()
	nf.rollbackErr = fmt.Errorf("adapter translation: %w", sql.ErrTxDone)
	db := nf.open()

	boom := errors.New("boom")
	err := db.Tx(context.Background(), func(tx *Tx) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("fn error must come through: %v", err)
	}
	if strings.Contains(err.Error(), "rollback") {
		t.Fatalf("ErrTxDone rollback must not be reported: %v", err)
	}
}

func TestNativeHookEventsAndRowsAffected(t *testing.T) {
	ctx := context.Background()
	nf := newFakeNative()
	hook := &recordingHook{}
	db := nf.openWith(Postgres, WithQueryHook(hook))

	nf.queueRows([]string{"id"}, []any{int64(1)})
	_ = db.Tx(ctx, func(tx *Tx) error {
		return Insert(ctx, tx, &Post{Title: "x", UserID: 1})
	})
	joined := strings.Join(hook.events, " ")
	for _, want := range []string{"begin:", "insert:Post", "commit:"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("hook missed %q in %q", want, joined)
		}
	}

	hook.events, hook.rows = nil, nil
	nf.queueExec(3)
	if _, err := Exec(ctx, db, "UPDATE posts SET title = 'y'"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if len(hook.rows) != 1 || hook.rows[0] != 3 {
		t.Fatalf("exec hook must see the affected count, got %v", hook.rows)
	}

	hook.events, hook.rows = nil, nil
	nf.queueRows(userCols, nativeUserRow(1, "a@x"))
	if _, err := From[User]().All(ctx, db); err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(hook.rows) != 1 || hook.rows[0] != -1 {
		t.Fatalf("row-returning hook must see -1, got %v", hook.rows)
	}
}

func TestNativeHookSeesSavepointOps(t *testing.T) {
	ctx := context.Background()
	nf := newFakeNative()
	hook := &recordingHook{}
	db := nf.openWith(Postgres, WithQueryHook(hook))

	_ = db.Tx(ctx, func(tx *Tx) error {
		return tx.Tx(ctx, func(sp *Tx) error { return nil })
	})
	joined := strings.Join(hook.events, " ")
	if !strings.Contains(joined, "savepoint:") {
		t.Fatalf("savepoint statements must reach hooks: %q", joined)
	}
}

func TestNativeUnwrapAndAccessors(t *testing.T) {
	ctx := context.Background()

	t.Run("native with view and handle", func(t *testing.T) {
		nf := newFakeNative()
		view := sql.OpenDB(fakeConnector{newFakeDB()})
		handle := &struct{ name string }{"pool"}
		db := NewNative(NativeConfig{DB: nf, Handle: handle, SQLView: view}, Postgres, WithClock(fixedClock))
		if db.Unwrap() != view {
			t.Fatal("Unwrap must return the supplied view")
		}
		if db.Native() != any(handle) {
			t.Fatal("Native must return the supplied handle verbatim")
		}
		err := db.Tx(ctx, func(tx *Tx) error {
			if tx.Unwrap() != nil {
				t.Error("Tx.Unwrap must be nil on the native channel")
			}
			nt, ok := tx.NativeTx().(*fakeNativeTx)
			if !ok || nt == nil {
				t.Errorf("Tx.NativeTx must expose the channel's NativeTx, got %T", tx.NativeTx())
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Tx: %v", err)
		}
	})

	t.Run("native without view", func(t *testing.T) {
		nf := newFakeNative()
		db := nf.open()
		if db.Unwrap() != nil {
			t.Fatal("Unwrap must be nil when no view was supplied")
		}
		if db.Native() != nil {
			t.Fatal("Native must be nil when no handle was supplied")
		}
	})

	t.Run("stdlib channel", func(t *testing.T) {
		f := newFakeDB()
		db := f.open()
		if db.Native() != nil {
			t.Fatal("Native must be nil on the database/sql channel")
		}
		err := db.Tx(ctx, func(tx *Tx) error {
			if tx.NativeTx() != nil {
				t.Error("NativeTx must be nil on the database/sql channel")
			}
			if tx.Unwrap() == nil {
				t.Error("stdlib Tx.Unwrap must stay non-nil")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Tx: %v", err)
		}
	})
}

// closeOrderConnector observes the *sql.DB view's close via the connector-Closer hook.
type closeOrderConnector struct {
	driver.Connector
	onClose func()
}

func (c closeOrderConnector) Close() error {
	c.onClose()
	return nil
}

func TestNativeCloseClosesViewThenChannel(t *testing.T) {
	nf := newFakeNative()
	var order []string
	view := sql.OpenDB(closeOrderConnector{
		Connector: fakeConnector{newFakeDB()},
		onClose:   func() { order = append(order, "view") },
	})
	db := NewNative(NativeConfig{DB: &orderedCloseNative{fakeNative: nf, order: &order}, SQLView: view}, Postgres)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if strings.Join(order, ",") != "view,channel" {
		t.Fatalf("close order = %v, want view before channel", order)
	}
}

type orderedCloseNative struct {
	*fakeNative
	order *[]string
}

func (o *orderedCloseNative) Close() error {
	*o.order = append(*o.order, "channel")
	return o.fakeNative.Close()
}

func TestNewNativeConstructionPanics(t *testing.T) {
	assertPanic := func(name, wantSub string, fn func()) {
		t.Run(name, func(t *testing.T) {
			defer func() {
				p := recover()
				if p == nil {
					t.Fatal("must panic")
				}
				if s, ok := p.(string); !ok || !strings.Contains(s, wantSub) {
					t.Fatalf("panic = %v, want containing %q", p, wantSub)
				}
			}()
			fn()
		})
	}
	assertPanic("nil NativeDB", "NativeConfig.DB must not be nil", func() {
		NewNative(NativeConfig{}, Postgres)
	})
	assertPanic("nil dialect", "dialect must not be nil", func() {
		NewNative(NativeConfig{DB: newFakeNative()}, nil)
	})
	assertPanic("stmt cache", "default_query_exec_mode", func() {
		NewNative(NativeConfig{DB: newFakeNative()}, Postgres, WithStmtCache())
	})
}

// Single-row reads leave the result undrained; the deferred close-time error must still surface.
func TestNativeDeferredCloseErrorSurfaces(t *testing.T) {
	ctx := context.Background()

	t.Run("find", func(t *testing.T) {
		nf := newFakeNative()
		db := nf.open()
		closeErr := errors.New("deferred: connection reset while closing rows")
		nf.queueRowsCloseErr(closeErr, userCols, nativeUserRow(1, "a@x"))
		if _, err := Find[User](ctx, db, int64(1)); !errors.Is(err, closeErr) {
			t.Fatalf("Find must surface the deferred close error, got %v", err)
		}
	})

	t.Run("insert returning", func(t *testing.T) {
		nf := newFakeNative()
		db := nf.open()
		closeErr := errors.New("deferred: insert actually failed")
		nf.queueRowsCloseErr(closeErr, []string{"id"}, []any{int64(7)})
		u := &User{Email: "a@x", Age: 30}
		if err := Insert(ctx, db, u); !errors.Is(err, closeErr) {
			t.Fatalf("Insert must not report success over a failed statement, got %v", err)
		}
	})

	t.Run("full drain reports through Err", func(t *testing.T) {
		nf := newFakeNative()
		db := nf.open()
		deferredErr := errors.New("deferred: stream broke at the tail")
		nf.queueRowsCloseErr(deferredErr, userCols, nativeUserRow(1, "a@x"))
		if _, err := From[User]().All(ctx, db); !errors.Is(err, deferredErr) {
			t.Fatalf("All must surface the tail error, got %v", err)
		}
	})
}

// sqlStateErr mimics a driver error carrying an SQLSTATE — the fallback translator's input.
type sqlStateErr struct{ code string }

func (e sqlStateErr) Error() string    { return "driver: constraint violation " + e.code }
func (e sqlStateErr) SQLState() string { return e.code }

func TestNativeErrorTranslationApplies(t *testing.T) {
	nf := newFakeNative()
	db := nf.open()
	nf.failContaining("INSERT", sqlStateErr{"23505"})

	err := Insert(context.Background(), db, &Post{UserID: 1, Title: "dup"})
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("dialect SQLSTATE fallback must run on the native channel: %v", err)
	}
	if _, ok := errors.AsType[sqlStateErr](err); !ok {
		t.Fatalf("driver error must stay in the chain: %v", err)
	}
}

// nativeCountUser exercises WithCount's plain *int64 dest, the one non-cell dest rio passes.
type nativeCountUser struct {
	ID         int64
	Email      string
	Posts      HasMany[nativeCountPost]
	PostsCount int64 `rio:",countof:Posts"`
}

type nativeCountPost struct {
	ID                int64
	NativeCountUserID int64
}

func TestNativeWithCountScansPlainDest(t *testing.T) {
	nf := newFakeNative()
	db := nf.open()
	nf.queueRows([]string{"id", "email"}, []any{int64(1), "a@x"}, []any{int64(2), "b@x"})
	nf.queueRows([]string{"k", "n"}, []any{int64(1), int64(4)}, []any{int64(2), int64(0)})

	users, err := From[nativeCountUser]().WithCount("Posts").All(context.Background(), db)
	if err != nil {
		t.Fatalf("WithCount: %v", err)
	}
	if len(users) != 2 || users[0].PostsCount != 4 || users[1].PostsCount != 0 {
		t.Fatalf("counts misrouted: %+v", users)
	}
}

// nativeUintRow: a plain uint64, a *uint64 for the NULL rule, and a uint32 that reaches the cell only via boxed Scan.
type nativeUintRow struct {
	ID    int64
	Big   uint64
	Opt   *uint64
	Boxed uint32
}

var nativeUintCols = []string{"id", "big", "opt", "boxed"}

// The native channel delivers unsigned values verbatim — no database/sql bind coercion in between.
func TestNativeUint64SinkRoundTrip(t *testing.T) {
	nf := newFakeNative()
	db := nf.open()
	const big = uint64(math.MaxInt64) + 1 // high bit set: database/sql refuses this as a bind
	nf.queueRows(nativeUintCols,
		[]any{int64(1), big, big, uint32(7)},
		[]any{int64(2), uint64(0), nil, uint32(0)},
	)

	got, err := From[nativeUintRow]().All(context.Background(), db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("rows = %d, want 2", len(got))
	}
	if got[0].Big != big {
		t.Fatalf("Big = %d, want %d — a uint64 above MaxInt64 must survive SetUint64", got[0].Big, big)
	}
	if got[0].Opt == nil || *got[0].Opt != big {
		t.Fatalf("Opt = %v, want *%d through the pointer cell", got[0].Opt, big)
	}
	if got[0].Boxed != 7 {
		t.Fatalf("Boxed = %d, want 7 — the boxed Scan path must carry the uint32", got[0].Boxed)
	}
	if got[1].Opt != nil {
		t.Fatalf("Opt = %v, want nil — NULL into a *uint64 is SetNull", got[1].Opt)
	}
	if got[1].Big != 0 {
		t.Fatalf("Big = %d, want 0", got[1].Big)
	}
}

// --- batching fake: fakeNative implements NativeBatcher ---

type fakeNativeBatchResults struct {
	f     *fakeNative
	ctx   context.Context
	stmts []BatchStatement
	next  int
}

func (f *fakeNative) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.batches
}

func (f *fakeNative) QueryBatch(ctx context.Context, stmts []BatchStatement) (NativeBatchResults, error) {
	f.mu.Lock()
	f.batches++
	f.mu.Unlock()
	return &fakeNativeBatchResults{f: f, ctx: ctx, stmts: stmts}, nil
}

func (r *fakeNativeBatchResults) Rows() (NativeRows, bool, error) {
	if r.next >= len(r.stmts) {
		return nil, true, nil
	}
	st := r.stmts[r.next]
	r.next++
	// Route through Query itself so logs, failOn, queued results, and dead-context refusal apply.
	rows, err := r.f.Query(r.ctx, st.SQL, st.Args)
	if err != nil {
		return nil, false, err
	}
	return rows, false, nil
}

func (r *fakeNativeBatchResults) Close() error { return nil }

var _ NativeBatcher = (*fakeNative)(nil)

// A relation layer's derived statements share one batch, with per-statement events intact.
func TestNativeBatchesRelationLayer(t *testing.T) {
	ctx := context.Background()
	f := newFakeNative()
	h := &afterHook{}
	db := f.openWith(Postgres, WithQueryHook(h))

	f.queueRows([]string{"id", "name"}, []any{int64(1), "o"}, []any{int64(2), "o"})
	// Tags preloads; Posts is only counted (a full preload would be statically reused).
	f.queueRows([]string{"id", "name", "bench_owner_id"}, []any{int64(7), "tag", int64(2)})
	f.queueRows([]string{"bench_owner_id", "count"}, []any{int64(1), int64(3)})

	out, err := From[benchOwner]().With("Tags").WithCount("Posts").All(ctx, db)
	if err != nil || len(out) != 2 {
		t.Fatalf("All: len=%d err=%v", len(out), err)
	}
	if got := f.batchCount(); got != 1 {
		t.Fatalf("the whole layer must flush as one batch, got %d", got)
	}
	if len(out[1].Tags.Rows()) != 1 || out[0].PostCount != 3 || out[1].PostCount != 0 {
		t.Fatalf("assembly drifted: %+v", out)
	}
	phases := map[string]int{}
	for _, e := range h.events {
		phases[e.Phase]++
	}
	if phases["preload"] != 1 || phases["count"] != 1 {
		t.Fatalf("per-statement events must survive batching: %+v", phases)
	}

	// A failing statement surfaces at its own position and aborts the layer.
	f2 := newFakeNative()
	db2 := f2.open(Postgres)
	f2.queueRows([]string{"id", "name"}, []any{int64(1), "o"})
	f2.failContaining("bench_posts", errors.New("boom"))
	_, err = From[benchOwner]().With("Posts").With("Tags").All(ctx, db2)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("batched statement errors must surface: %v", err)
	}
}

// --- copy fake ---

func (f *fakeNative) CopyIn(_ context.Context, table []string, columns []string, next func() ([]any, error)) (int64, error) {
	f.mu.Lock()
	f.copies++
	f.mu.Unlock()
	var n int64
	for {
		vals, err := next()
		if err != nil {
			return n, err
		}
		if vals == nil {
			return n, nil
		}
		if len(vals) != len(columns) {
			return n, errors.New("fakeNative: column/value arity mismatch")
		}
		n++
	}
}

var _ NativeCopier = (*fakeNative)(nil)

// Explicit-key InsertAll streams through the copy protocol; a backfilling batch keeps chunked RETURNING.
func TestNativeInsertAllUsesCopy(t *testing.T) {
	ctx := context.Background()
	f := newFakeNative()
	h := &afterHook{}
	db := f.openWith(Postgres, WithQueryHook(h))

	rows := make([]chunkRow, 500)
	for i := range rows {
		rows[i] = chunkRow{ID: int64(i + 1), A: 1, B: 2}
	}
	if err := InsertAll(ctx, db, rows); err != nil {
		t.Fatalf("InsertAll: %v", err)
	}
	f.mu.Lock()
	copies, logged := f.copies, len(f.log)
	f.mu.Unlock()
	if copies != 1 || logged != 0 {
		t.Fatalf("explicit keys must stream one copy, got copies=%d statements=%d", copies, logged)
	}
	last := h.events[len(h.events)-1]
	if last.Op != "copy" || last.RowsAffected != 500 {
		t.Fatalf("the copy event must report the loaded rows: %+v", last)
	}
}

// plainNative strips the fake's optional capabilities so rio must take the per-statement and chunked fallbacks.
type plainNative struct{ nd NativeDB }

func (p plainNative) Query(ctx context.Context, sql string, args []any) (NativeRows, error) {
	return p.nd.Query(ctx, sql, args)
}
func (p plainNative) Exec(ctx context.Context, sql string, args []any) (int64, error) {
	return p.nd.Exec(ctx, sql, args)
}
func (p plainNative) Begin(ctx context.Context, opts *sql.TxOptions) (NativeTx, error) {
	return p.nd.Begin(ctx, opts)
}
func (p plainNative) Close() error { return p.nd.Close() }

// A channel without the optional capabilities declines before any side effect.
func TestNativeWithoutCapabilitiesFallsBack(t *testing.T) {
	ctx := context.Background()
	f := newFakeNative()
	h := &afterHook{}
	db := NewNative(NativeConfig{DB: plainNative{nd: f}}, Postgres, WithClock(fixedClock), WithQueryHook(h))

	rows := []chunkRow{{ID: 1, A: 1, B: 2}, {ID: 2, A: 3, B: 4}}
	if err := InsertAll(ctx, db, rows); err != nil {
		t.Fatalf("InsertAll: %v", err)
	}
	f.mu.Lock()
	copies, batches, logged := f.copies, f.batches, len(f.log)
	f.mu.Unlock()
	if copies != 0 || batches != 0 || logged == 0 {
		t.Fatalf("stripped channel must chunk VALUES, got copies=%d batches=%d statements=%d", copies, batches, logged)
	}
	for _, e := range h.events {
		if e.Op == "copy" {
			t.Fatalf("no copy event may fire on a declining channel: %+v", e)
		}
	}
}
