package rio

import (
	"container/list"
	"context"
	"database/sql"
	"errors"
	"sync"
)

type stmtPreparer interface {
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}

type stmtEntry struct {
	sql  string
	stmt *sql.Stmt
}

// stmtFlight is one in-progress prepare; waiters retry when it failed only
// because the preparer's context died.
type stmtFlight struct {
	done        chan struct{}
	stmt        *sql.Stmt
	err         error
	shouldRetry bool
}

// stmtCache is an LRU of prepared statements keyed by SQL text: IN (?)
// expansion makes every slice length a distinct statement. *sql.Stmt is
// reference-counted, so closing an evicted statement mid-query is safe.
type stmtCache struct {
	prepare stmtPreparer
	cap     int

	mu       sync.Mutex
	bySQL    map[string]*list.Element
	lru      *list.List // front = most recently used
	flight   map[string]*stmtFlight
	isClosed bool
}

func newStmtCache(prepare stmtPreparer, capacity int) *stmtCache {
	return &stmtCache{
		prepare: prepare,
		cap:     capacity,
		bySQL:   make(map[string]*list.Element),
		lru:     list.New(),
		flight:  make(map[string]*stmtFlight),
	}
}

func (c *stmtCache) get(ctx context.Context, sqlText string) (*sql.Stmt, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.mu.Lock()
		if c.isClosed {
			c.mu.Unlock()
			return nil, sql.ErrConnDone
		}
		if el, ok := c.bySQL[sqlText]; ok {
			c.lru.MoveToFront(el)
			st := el.Value.(*stmtEntry).stmt
			c.mu.Unlock()
			return st, nil
		}
		if f, ok := c.flight[sqlText]; ok {
			c.mu.Unlock()
			select {
			case <-f.done:
				if f.shouldRetry && ctx.Err() == nil {
					continue
				}
				return f.stmt, f.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		f := &stmtFlight{done: make(chan struct{})}
		c.flight[sqlText] = f
		c.mu.Unlock()

		st, err := c.prepare.PrepareContext(ctx, sqlText)
		shouldRetry := false
		if err != nil {
			shouldRetry = ctx.Err() != nil ||
				errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded)
		}
		var evicted, discard *sql.Stmt

		c.mu.Lock()
		if c.isClosed {
			if st != nil {
				discard = st
			}
			st, err, shouldRetry = nil, sql.ErrConnDone, false
		} else if err == nil {
			c.bySQL[sqlText] = c.lru.PushFront(&stmtEntry{sql: sqlText, stmt: st})
			if c.lru.Len() > c.cap {
				oldest := c.lru.Back()
				c.lru.Remove(oldest)
				e := oldest.Value.(*stmtEntry)
				delete(c.bySQL, e.sql)
				evicted = e.stmt
			}
		}
		f.stmt, f.err, f.shouldRetry = st, err, shouldRetry
		delete(c.flight, sqlText)
		close(f.done)
		c.mu.Unlock()
		if discard != nil {
			_ = discard.Close()
		}
		if evicted != nil {
			_ = evicted.Close()
		}
		return st, err
	}
}

func (c *stmtCache) evict(sqlText string) {
	c.mu.Lock()
	el, ok := c.bySQL[sqlText]
	if ok {
		c.lru.Remove(el)
		delete(c.bySQL, sqlText)
	}
	c.mu.Unlock()
	if ok {
		_ = el.Value.(*stmtEntry).stmt.Close()
	}
}

func (c *stmtCache) close() {
	c.mu.Lock()
	c.isClosed = true
	stmts := make([]*sql.Stmt, 0, c.lru.Len())
	for el := c.lru.Front(); el != nil; el = el.Next() {
		stmts = append(stmts, el.Value.(*stmtEntry).stmt)
	}
	c.bySQL = make(map[string]*list.Element)
	c.lru.Init()
	c.mu.Unlock()
	for _, st := range stmts {
		_ = st.Close()
	}
}
