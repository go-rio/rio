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

// fakeDB is a zero-dependency database/sql driver that records every
// statement and serves scripted results, so tests assert exact SQL
// sequences, arguments, and transaction boundaries without a database.
type fakeDB struct {
	mu          sync.Mutex
	log         []fakeStmt
	results     []fakeRows
	execs       []fakeResult
	failOn      map[string]error
	failPrepare map[string]error
	prepped     []string
	closed      []string // SQL of prepared statements whose Close ran
}

type fakeStmt struct {
	sql  string
	args []driver.Value
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
}

type fakeResult struct {
	lastID, affected int64
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

// queueRows scripts the next row-returning statement's result.
func (f *fakeDB) queueRows(cols []string, rows ...[]driver.Value) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results = append(f.results, fakeRows{cols: cols, rows: rows})
}

// queueExec scripts the next non-query statement's result. Unscripted execs
// report (1, 1).
func (f *fakeDB) queueExec(lastID, affected int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs = append(f.execs, fakeResult{lastID: lastID, affected: affected})
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

func (c *fakeConn) Close() error { return nil }

func (c *fakeConn) Begin() (driver.Tx, error) {
	_ = c.f.record("BEGIN", nil)
	return fakeTx{f: c.f}, nil
}

func (c *fakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.f.record(query, values(args)); err != nil {
		return nil, err
	}
	return newFakeRowsIter(c.f.nextRows()), nil
}

func (c *fakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.f.record(query, values(args)); err != nil {
		return nil, err
	}
	return fakeExecResult{c.f.nextExec()}, nil
}

type fakeExecResult struct{ r fakeResult }

func (e fakeExecResult) LastInsertId() (int64, error) { return e.r.lastID, nil }
func (e fakeExecResult) RowsAffected() (int64, error) { return e.r.affected, nil }

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
	return newFakeRowsIter(s.f.nextRows()), nil
}

type fakeRowsIter struct {
	data fakeRows
	pos  int
}

func newFakeRowsIter(data fakeRows) *fakeRowsIter { return &fakeRowsIter{data: data} }

func (r *fakeRowsIter) Columns() []string { return r.data.cols }
func (r *fakeRowsIter) Close() error      { return nil }

func (r *fakeRowsIter) Next(dest []driver.Value) error {
	if r.pos >= len(r.data.rows) {
		return io.EOF
	}
	copy(dest, r.data.rows[r.pos])
	r.pos++
	return nil
}

var _ driver.Result = fakeExecResult{}

// testNow keeps timestamps deterministic across every fake-driver test.
var testNow = time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)

func fixedClock() time.Time { return testNow }
