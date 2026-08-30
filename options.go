package rio

import (
	"sync"
	"time"
	"weak"
)

// config carries the per-DB settings frozen at New time.
type config struct {
	hooks        []QueryHook
	clock        func() time.Time
	translator   func(error) error
	tableNamer   func(structName string) string
	logArgs      bool
	stmtCache    bool
	stmtCap      int
	driverHandle any
}

// Option configures a DB handle at construction time.
type Option func(*config)

// WithQueryHook installs a read-only hook for executed statements and
// transaction control.
func WithQueryHook(h QueryHook) Option {
	return func(c *config) {
		if h != nil {
			c.hooks = append(c.hooks, h)
		}
	}
}

// WithClock replaces the time source used for CreatedAt/UpdatedAt and soft
// deletes. Intended for tests.
func WithClock(now func() time.Time) Option {
	return func(c *config) {
		if now != nil {
			c.clock = now
		}
	}
}

// WithErrorTranslator installs a driver-specific error translator, mapping
// driver errors to rio sentinels (ErrDuplicateKey, ErrForeignKeyViolated).
// The go-rio driver modules install one automatically; the translator runs
// before the dialect's SQLSTATE fallback. Returning nil means "not mine".
func WithErrorTranslator(f func(error) error) Option {
	return func(c *config) { c.translator = f }
}

// WithTableNamer overrides conventional table names for this handle. A model's
// TableName method takes precedence, and SQL caches remain handle-specific.
//
// The function must be a pure, stable mapping: rendered SQL is cached per
// handle, so a namer that reads mutable state (a request-scoped tenant
// variable) would leave earlier shapes pointing at the old names. Dynamic
// tenancy is a handle decision — construct one *DB per naming universe and
// pick at the call site, exactly as with read/write splitting.
func WithTableNamer(f func(structName string) string) Option {
	return func(c *config) { c.tableNamer = f }
}

// WithDriverHandle attaches a driver-owned handle (a connection pool the
// driver module built the *sql.DB over) to the DB, retrievable through
// DB.DriverHandle. It exists for driver modules: their typed accessors
// (postgres.PoolOf) read it back, and the handle's lifecycle rides on the DB
// instead of a package-level registry. rio itself never touches the value.
func WithDriverHandle(h any) Option {
	return func(c *config) { c.driverHandle = h }
}

// WithoutArgs redacts bind arguments from QueryEvent before hooks see them.
func WithoutArgs() Option {
	return func(c *config) { c.logArgs = false }
}

// WithStmtCache enables bounded prepared-statement caches. The DB and each
// transaction own separate caches; transactions never share the DB cache. It
// is off by default and unsuitable for transaction/statement-mode poolers.
// Schema-change errors evict entries and are not retried. New panics if this
// option is used with ClickHouse, which cannot prepare general queries.
func WithStmtCache(capacity ...int) Option {
	return func(c *config) {
		c.stmtCache = true
		if len(capacity) > 0 && capacity[0] > 0 {
			c.stmtCap = capacity[0]
		}
	}
}

// grammar isolates SQL caches by dialect and rendering options.
type grammar struct {
	d          Dialect
	tableNamer func(string) string

	// weakSelf keys Must-query render caches without pinning the grammar:
	// a package-level Query outlives any one handle, and a strong key would
	// keep every closed handle's grammar — and its crud cache — reachable
	// forever. Made once here so the hot path never calls weak.Make.
	weakSelf weak.Pointer[grammar]

	// crud caches rendered entity-CRUD SQL by crudKey.
	crud sync.Map
}

type crudKey struct {
	plan *plan
	op   string
	bits uint64 // participating-column bitmap for shape-variable statements
	rows int    // VALUES tuple count for batch statements
	spec upsertCacheKey
}

// cachedSQL renders entity-CRUD SQL once per grammar and shape.
func (g *grammar) cachedSQL(
	p *plan,
	op string,
	bits uint64,
	rows int,
	spec upsertCacheKey,
	build func() (string, error),
) (string, error) {
	key := crudKey{
		plan: p,
		op:   op,
		bits: bits,
		rows: rows,
		spec: spec,
	}
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

func defaultConfig() *config {
	return &config{
		clock:   time.Now,
		logArgs: true,
		stmtCap: 512,
	}
}

func newGrammar(d Dialect, cfg *config) *grammar {
	g := &grammar{d: d, tableNamer: cfg.tableNamer}
	g.weakSelf = weak.Make(g)
	return g
}
