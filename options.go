package rio

import (
	"sync"
	"time"
)

// config carries the per-DB settings frozen at New time.
type config struct {
	hooks      []QueryHook
	clock      func() time.Time
	translator func(error) error
	tableNamer func(structName string) string
	logArgs    bool
	stmtCache  bool
	stmtCap    int
}

func defaultConfig() *config {
	return &config{
		clock:   time.Now,
		logArgs: true,
		stmtCap: 512,
	}
}

// Option configures a DB handle at construction time.
type Option func(*config)

// WithQueryHook installs an observability hook. Hooks see every statement rio
// executes — entity CRUD, builder queries, compiled queries, Raw, and
// transaction control — and cannot alter them.
func WithQueryHook(h QueryHook) Option {
	return func(c *config) { c.hooks = append(c.hooks, h) }
}

// WithClock replaces the time source used for CreatedAt/UpdatedAt and soft
// deletes. Intended for tests.
func WithClock(now func() time.Time) Option {
	return func(c *config) { c.clock = now }
}

// WithErrorTranslator installs a driver-specific error translator, mapping
// driver errors to rio sentinels (ErrDuplicateKey, ErrForeignKeyViolated).
// The go-rio driver modules install one automatically; the translator runs
// before the dialect's SQLSTATE fallback. Returning nil means "not mine".
func WithErrorTranslator(f func(error) error) Option {
	return func(c *config) { c.translator = f }
}

// WithTableNamer overrides the convention-based struct-name→table-name
// derivation for this DB handle. Models implementing TableName() string still
// win. The namer is part of the DB's grammar identity: SQL caches are keyed
// by it, so two handles with different namers never share rendered SQL.
func WithTableNamer(f func(structName string) string) Option {
	return func(c *config) { c.tableNamer = f }
}

// WithoutArgs redacts bind arguments from QueryEvent before hooks see them.
func WithoutArgs() Option {
	return func(c *config) { c.logArgs = false }
}

// WithStmtCache enables the prepared-statement cache on this DB handle.
//
// Off by default: connection poolers in transaction/statement mode (PgBouncer
// et al.) break server-side prepared statements. Enable it only when rio
// talks to the database directly. Statements are cached per SQL text on the
// DB only — transactions bypass the cache — and the cache is LRU-bounded
// (cap configurable) because IN (?) expansion makes every slice length a
// distinct statement. On schema-change errors the entry is evicted and the
// error propagates; rio never retries on its own.
func WithStmtCache(capacity ...int) Option {
	return func(c *config) {
		c.stmtCache = true
		if len(capacity) > 0 && capacity[0] > 0 {
			c.stmtCap = capacity[0]
		}
	}
}

// grammar is the SQL-shaping identity of a DB handle: dialect plus every
// option that affects rendered SQL. All SQL caches (entity CRUD on plans,
// Compiled queries, the rebind cache) key by *grammar, so handles that would
// render different SQL can never poison each other.
type grammar struct {
	d          Dialect
	tableNamer func(string) string

	// crud caches rendered entity-CRUD SQL: key is crudKey.
	crud sync.Map
}

type crudKey struct {
	plan *plan
	op   string
	bits uint64 // participating-column bitmap for shape-variable statements
	rows int    // VALUES tuple count for batch statements
}

// cachedSQL renders entity-CRUD SQL once per grammar and shape. The hot path
// pays one sync.Map lookup instead of a render plus a rebind.
func (g *grammar) cachedSQL(p *plan, op string, bits uint64, rows int, build func() (string, error)) (string, error) {
	key := crudKey{plan: p, op: op, bits: bits, rows: rows}
	if v, ok := g.crud.Load(key); ok {
		return v.(string), nil
	}
	sqlText, err := build()
	if err != nil {
		return "", err
	}
	actual, _ := g.crud.LoadOrStore(key, sqlText)
	return actual.(string), nil
}

func newGrammar(d Dialect, cfg *config) *grammar {
	return &grammar{d: d, tableNamer: cfg.tableNamer}
}

// table resolves a plan's table name under this grammar: a TableName()
// override always wins, then the handle's namer, then convention.
func (g *grammar) table(p *plan) string {
	if p.tableOverride != "" {
		return p.tableOverride
	}
	if g.tableNamer != nil {
		return g.tableNamer(p.structName)
	}
	return p.defaultTable
}
