package rio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// fakeDB is a zero-dependency database/sql driver that records every statement
// and serves scripted results, so tests assert exact SQL without a database.
type fakeDB struct {
	mu          sync.Mutex
	log         []fakeStmt
	results     []fakeRows
	execs       []fakeResult
	failOn      map[string]error
	failPrepare map[string]error
	prepped     []string
	closed      []string // SQL of prepared statements whose Close ran
	rowsClosed  int      // count of driver result-set Close calls
	columnScan  bool     // serve Go 1.27 driver.RowsColumnScanner rows
	nextCalls   int      // legacy Rows.Next calls made on columnScan rows
	scanCalls   int      // direct ScanColumn calls made on columnScan rows
	// probe, when non-nil, receives the context every ExecContext/QueryContext executes under.
	probe          func(context.Context)
	prepareStarted chan struct{}
	prepareBlock   <-chan struct{}
}

type fakeStmt struct {
	sql  string
	args []driver.Value
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	// closeErr is returned by the driver rows' Close — where real drivers surface deferred protocol errors.
	closeErr error
}

type fakeResult struct {
	lastID, affected int64
	affectedErr      error // RowsAffected() failure injection
}

func newFakeDB() *fakeDB {
	return &fakeDB{failOn: map[string]error{}, failPrepare: map[string]error{}}
}

func (f *fakeDB) open(d ...Dialect) *DB {
	dialect := Dialect(Postgres)
	if len(d) > 0 {
		dialect = d[0]
	}
	return New(sql.OpenDB(fakeConnector{f}), dialect, WithClock(fixedClock))
}

func (f *fakeDB) openWith(dialect Dialect, opts ...Option) *DB {
	return New(sql.OpenDB(fakeConnector{f}), dialect, append([]Option{WithClock(fixedClock)}, opts...)...)
}

func (f *fakeDB) openColumnScanner(dialect Dialect) *DB {
	f.columnScan = true
	return f.open(dialect)
}

// queueRows scripts the next row-returning statement's result.
func (f *fakeDB) queueRows(cols []string, rows ...[]driver.Value) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results = append(f.results, fakeRows{cols: cols, rows: rows})
}

// queueRowsCloseErr scripts a result whose Close fails after the rows were served.
func (f *fakeDB) queueRowsCloseErr(closeErr error, cols []string, rows ...[]driver.Value) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results = append(f.results, fakeRows{cols: cols, rows: rows, closeErr: closeErr})
}

// queueExec scripts the next non-query result; unscripted execs report (1, 1).
func (f *fakeDB) queueExec(lastID, affected int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs = append(f.execs, fakeResult{lastID: lastID, affected: affected})
}

// queueExecAffectedErr scripts a result whose RowsAffected() fails.
func (f *fakeDB) queueExecAffectedErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs = append(f.execs, fakeResult{lastID: 1, affectedErr: err})
}

// failContaining makes any statement containing sub fail with err.
func (f *fakeDB) failContaining(sub string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failOn[sub] = err
}

// unfail removes a failContaining rule.
func (f *fakeDB) unfail(sub string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.failOn, sub)
}

// failPreparing makes Prepare of any statement containing sub fail with err.
func (f *fakeDB) failPreparing(sub string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failPrepare[sub] = err
}

// closedStmts lists the SQL of prepared statements that have been closed.
func (f *fakeDB) closedStmts() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.closed...)
}

// rowsCloseCount reports how many driver result sets have been closed — the
// signal streaming tests use to prove Rows closes on early break.
func (f *fakeDB) rowsCloseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rowsClosed
}

func (f *fakeDB) logged() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.log))
	for i, s := range f.log {
		out[i] = s.sql
	}
	return out
}

func (f *fakeDB) loggedContaining(sub string) []fakeStmt {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeStmt
	for _, s := range f.log {
		if strings.Contains(s.sql, sub) {
			out = append(out, s)
		}
	}
	return out
}

func (f *fakeDB) record(sqlText string, args []driver.Value) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, fakeStmt{sql: sqlText, args: args})
	for sub, err := range f.failOn {
		if strings.Contains(sqlText, sub) {
			return err
		}
	}
	return nil
}

func (f *fakeDB) nextRows() fakeRows {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.results) == 0 {
		return fakeRows{cols: nil}
	}
	r := f.results[0]
	f.results = f.results[1:]
	return r
}

func (f *fakeDB) nextExec() fakeResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.execs) == 0 {
		return fakeResult{lastID: 1, affected: 1}
	}
	r := f.execs[0]
	f.execs = f.execs[1:]
	return r
}

// --- driver plumbing ---

type fakeConnector struct{ f *fakeDB }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return &fakeConn{f: c.f}, nil }
func (c fakeConnector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) {
	return nil, fmt.Errorf("fakedb: use OpenDB")
}

type fakeConn struct{ f *fakeDB }

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	for sub, err := range c.f.failPrepare {
		if strings.Contains(query, sub) {
			return nil, err
		}
	}
	c.f.prepped = append(c.f.prepped, query)
	return &fakePrepared{f: c.f, sql: query}, nil
}

func (c *fakeConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.f.prepareStarted != nil {
		select {
		case c.f.prepareStarted <- struct{}{}:
		default:
		}
	}
	if c.f.prepareBlock != nil {
		select {
		case <-c.f.prepareBlock:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return c.Prepare(query)
}

func (c *fakeConn) Close() error { return nil }

// CheckNamedValue passes slices and uint64 through, as the pgx adapter and
// the ClickHouse channel do; everything else takes the default conversion.
func (c *fakeConn) CheckNamedValue(nv *driver.NamedValue) error {
	if _, ok := nv.Value.(uint64); ok {
		return nil
	}
	if _, ok := sliceValue(nv.Value); ok {
		return nil
	}
	return driver.ErrSkip
}

func (c *fakeConn) Begin() (driver.Tx, error) {
	_ = c.f.record("BEGIN", nil)
	return fakeTx{f: c.f}, nil
}

func (c *fakeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.f.probe != nil {
		c.f.probe(ctx)
	}
	if err := c.f.record(query, values(args)); err != nil {
		return nil, err
	}
	return c.f.rowsIter(c.f.nextRows()), nil
}

func (c *fakeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.f.probe != nil {
		c.f.probe(ctx)
	}
	if err := c.f.record(query, values(args)); err != nil {
		return nil, err
	}
	return fakeExecResult{c.f.nextExec()}, nil
}

type fakeExecResult struct{ r fakeResult }

func (e fakeExecResult) LastInsertId() (int64, error) { return e.r.lastID, nil }
func (e fakeExecResult) RowsAffected() (int64, error) { return e.r.affected, e.r.affectedErr }

func values(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

type fakeTx struct{ f *fakeDB }

func (t fakeTx) Commit() error   { return t.f.record("COMMIT", nil) }
func (t fakeTx) Rollback() error { return t.f.record("ROLLBACK", nil) }

type fakePrepared struct {
	f   *fakeDB
	sql string
}

func (s *fakePrepared) Close() error {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()
	s.f.closed = append(s.f.closed, s.sql)
	return nil
}

func (s *fakePrepared) NumInput() int { return -1 }

func (s *fakePrepared) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.f.record(s.sql, args); err != nil {
		return nil, err
	}
	return fakeExecResult{s.f.nextExec()}, nil
}

func (s *fakePrepared) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.f.record(s.sql, args); err != nil {
		return nil, err
	}
	return s.f.rowsIter(s.f.nextRows()), nil
}

func (f *fakeDB) rowsIter(data fakeRows) driver.Rows {
	rows := newFakeRowsIter(f, data)
	if f.columnScan {
		return &fakeRowsColumnIter{fakeRowsIter: rows}
	}
	return rows
}

type fakeRowsIter struct {
	f    *fakeDB
	data fakeRows
	pos  int
}

func newFakeRowsIter(f *fakeDB, data fakeRows) *fakeRowsIter { return &fakeRowsIter{f: f, data: data} }

func (r *fakeRowsIter) Columns() []string { return r.data.cols }

func (r *fakeRowsIter) Close() error {
	if r.f != nil { // nil for the lock-free loopDB alloc driver
		r.f.mu.Lock()
		r.f.rowsClosed++
		r.f.mu.Unlock()
	}
	return r.data.closeErr
}

func (r *fakeRowsIter) Next(dest []driver.Value) error {
	if r.pos >= len(r.data.rows) {
		return io.EOF
	}
	copy(dest, r.data.rows[r.pos])
	r.pos++
	return nil
}

// fakeRowsColumnIter exercises Go 1.27's driver.RowsColumnScanner path: typed
// setters when the dest offers them, ConvertAssign otherwise.
type fakeRowsColumnIter struct {
	*fakeRowsIter
	current []driver.Value
}

func (r *fakeRowsColumnIter) Next([]driver.Value) error {
	if r.f != nil {
		r.f.mu.Lock()
		r.f.nextCalls++
		r.f.mu.Unlock()
	}
	return fmt.Errorf("fakedb: legacy Next called on RowsColumnScanner")
}

func (r *fakeRowsColumnIter) NextRow() error {
	if r.pos >= len(r.data.rows) {
		return io.EOF
	}
	r.current = r.data.rows[r.pos]
	r.pos++
	return nil
}

func (r *fakeRowsColumnIter) ScanColumn(scanCtx driver.ScanContext, index int, dest any) error {
	if r.f != nil {
		r.f.mu.Lock()
		r.f.scanCalls++
		r.f.mu.Unlock()
	}
	v := r.current[index]
	switch v := v.(type) {
	case nil:
		if sink, ok := dest.(interface{ SetNull() error }); ok {
			return sink.SetNull()
		}
	case int64:
		if sink, ok := dest.(interface{ SetInt64(int64) error }); ok {
			return sink.SetInt64(v)
		}
	case float64:
		if sink, ok := dest.(interface{ SetFloat64(float64) error }); ok {
			return sink.SetFloat64(v)
		}
	case bool:
		if sink, ok := dest.(interface{ SetBool(bool) error }); ok {
			return sink.SetBool(v)
		}
	case string:
		if sink, ok := dest.(interface{ SetString(string) error }); ok {
			return sink.SetString(v)
		}
	case []byte:
		if sink, ok := dest.(interface{ SetBytes([]byte) error }); ok {
			return sink.SetBytes(v)
		}
	case time.Time:
		if sink, ok := dest.(interface{ SetTime(time.Time) error }); ok {
			return sink.SetTime(v)
		}
	}
	return sql.ConvertAssign(scanCtx, dest, v)
}

var _ driver.Result = fakeExecResult{}
var _ driver.RowsColumnScanner = (*fakeRowsColumnIter)(nil)

// testNow keeps timestamps deterministic across every fake-driver test.
var testNow = time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)

func fixedClock() time.Time { return testNow }
