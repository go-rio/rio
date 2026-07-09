package rio

import (
	"container/list"
	"context"
	"database/sql"
	"sync"
)

// stmtCache is an LRU of prepared statements keyed by SQL text. It exists
// because IN (?) expansion makes every slice length a distinct statement —
// unbounded growth would leak server-side prepared statements. *sql.Stmt is
// reference-counted by database/sql, so closing an evicted statement while a
// query still runs on it is safe.
type stmtCache struct {
	db  *sql.DB
	cap int

	mu    sync.Mutex
	bySQL map[string]*list.Element
	lru   *list.List // front = most recently used
}

type stmtEntry struct {
	sql  string
	stmt *sql.Stmt
}

func newStmtCache(db *sql.DB, capacity int) *stmtCache {
	return &stmtCache{db: db, cap: capacity, bySQL: make(map[string]*list.Element), lru: list.New()}
}

func (c *stmtCache) get(ctx context.Context, sqlText string) (*sql.Stmt, error) {
	c.mu.Lock()
	if el, ok := c.bySQL[sqlText]; ok {
		c.lru.MoveToFront(el)
		st := el.Value.(*stmtEntry).stmt
		c.mu.Unlock()
		return st, nil
	}
	c.mu.Unlock()

	// Prepare outside the lock: it does a network round-trip. A concurrent
	// racer may prepare the same SQL; the loser's statement is closed.
	st, err := c.db.PrepareContext(ctx, sqlText)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if el, ok := c.bySQL[sqlText]; ok {
		c.lru.MoveToFront(el)
		winner := el.Value.(*stmtEntry).stmt
		c.mu.Unlock()
		_ = st.Close()
		return winner, nil
	}
	c.bySQL[sqlText] = c.lru.PushFront(&stmtEntry{sql: sqlText, stmt: st})
	var evicted *sql.Stmt
	if c.lru.Len() > c.cap {
		oldest := c.lru.Back()
		c.lru.Remove(oldest)
		e := oldest.Value.(*stmtEntry)
		delete(c.bySQL, e.sql)
		evicted = e.stmt
	}
	c.mu.Unlock()
	if evicted != nil {
		_ = evicted.Close()
	}
	return st, nil
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
