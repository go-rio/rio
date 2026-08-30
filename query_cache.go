package rio

import (
	"fmt"
	"runtime"
	"sync"
	"time"
	"weak"
)

type queryCache struct {
	entries sync.Map
}

type queryCacheOp uint8

const (
	queryCacheAll queryCacheOp = iota
	queryCacheFirst
	queryCacheSole
	queryCacheRows
	queryCacheCount
	queryCacheExists
	queryCachePluck
)

// queryCacheKey identifies one rendered shape. The grammar is held weakly so
// a package-level Must query never pins a closed handle's grammar.
type queryCacheKey struct {
	grammar weak.Pointer[grammar]
	op      queryCacheOp
	column  string
}

// cachedQuery stores a scalar SQL shape and its deferred-argument positions.
type cachedQuery struct {
	sql             string
	args            []any
	execPos         []int
	missing         []cachedDeferred
	plan            *plan
	execCount       int
	hasIdentityArgs bool
}

type cachedDeferred struct {
	end    int
	clause string
	expr   string
}

func (c *queryCache) load(key queryCacheKey, d Dialect, execArgs []any) (*cachedQuery, []any, bool, error) {
	if c == nil || hasExpandableExecArg(execArgs) {
		return nil, nil, false, nil
	}
	v, ok := c.entries.Load(key)
	if !ok {
		return nil, nil, false, nil
	}
	entry := v.(*cachedQuery)
	args, err := entry.bind(d, execArgs)
	return entry, args, true, err
}

func (c *queryCache) store(
	key queryCacheKey,
	g *grammar,
	p *plan,
	original *queryState,
	execArgs []any,
	sqlText string,
	args []any,
) (string, []any, error) {
	if c == nil || hasExpandableExecArg(execArgs) {
		return sqlText, args, nil
	}
	d := g.d
	entry, ok := newCachedQuery(d, p, original, execArgs, sqlText, args)
	if !ok {
		return sqlText, args, nil
	}
	actual, loaded := c.entries.LoadOrStore(key, entry)
	if !loaded {
		// The first store arms a cleanup dropping the entry when the grammar
		// is collected. The cleanup holds the cache weakly too: a discarded
		// Query must not live until every handle it ever ran on dies.
		cache := weak.Make(c)
		runtime.AddCleanup(g, func(k queryCacheKey) {
			if qc := cache.Value(); qc != nil {
				qc.entries.Delete(k)
			}
		}, key)
		return sqlText, args, nil
	}
	winner := actual.(*cachedQuery)
	bound, err := winner.bind(d, execArgs)
	return winner.sql, bound, err
}

func (c *cachedQuery) bind(d Dialect, execArgs []any) ([]any, error) {
	if len(execArgs) != c.execCount {
		if len(execArgs) < c.execCount {
			for _, deferred := range c.missing {
				if len(execArgs) < deferred.end {
					return nil, fmt.Errorf(
						"rio: query needs at least %d deferred argument(s), got %d (at %s(%q))",
						deferred.end,
						len(execArgs),
						deferred.clause,
						deferred.expr,
					)
				}
			}
		}
		return nil, fmt.Errorf("rio: query takes %d deferred argument(s), got %d", c.execCount, len(execArgs))
	}
	if c.hasIdentityArgs {
		return normalizeArgs(d, execArgs)
	}
	if len(execArgs) == 0 {
		return c.args, nil
	}
	normalized, err := normalizeArgs(d, execArgs)
	if err != nil {
		return nil, err
	}
	out := append([]any(nil), c.args...)
	for i, pos := range c.execPos {
		out[pos] = normalized[i]
	}
	return out, nil
}

func hasExpandableExecArg(args []any) bool {
	for _, arg := range args {
		if _, ok := sliceValue(arg); ok {
			return true
		}
	}
	return false
}

func newCachedQuery(
	d Dialect,
	p *plan,
	s *queryState,
	execArgs []any,
	sqlText string,
	args []any,
) (*cachedQuery, bool) {
	if s.after != nil {
		// The cursor's values change every page; the shape is not stable.
		return nil, false
	}
	positions := make([]int, len(execArgs))
	var missing []cachedDeferred
	execIndex := 0
	argIndex := 0

	staticArgs := func(values []any) bool {
		for _, value := range values {
			if _, ok := sliceValue(value); ok {
				// Inline slices may change between executions.
				return false
			}
			if _, ok := value.(*time.Time); ok {
				// Normalization dereferences *time.Time, so its result is not stable.
				return false
			}
			if d.caps().bindBytesAsString {
				if _, ok := value.([]byte); ok {
					return false
				}
				if _, ok := chByteArg(value); ok {
					return false
				}
			}
			argIndex++
		}
		return true
	}
	conditions := func(clause string, conds []cond) bool {
		for _, condition := range conds {
			var holes int
			_, _, _ = rebindCount(d.lexer(), condition.expr, &holes)
			if len(condition.args) != 0 || holes == 0 {
				if !staticArgs(condition.args) {
					return false
				}
				continue
			}
			if execIndex+holes > len(execArgs) {
				return false
			}
			missing = append(missing, cachedDeferred{
				end:    execIndex + holes,
				clause: clause,
				expr:   condition.expr,
			})
			for range holes {
				positions[execIndex] = argIndex
				execIndex++
				argIndex++
			}
		}
		return true
	}

	if !conditions("Where", s.wheres) {
		return nil, false
	}
	for _, hc := range s.hasConds {
		// A RelOption may derive a different query on each call.
		if len(hc.opts) != 0 {
			return nil, false
		}
	}
	for _, spec := range s.withs {
		if len(spec.opts) != 0 {
			return nil, false
		}
	}
	if !conditions("Having", s.havings) {
		return nil, false
	}
	if execIndex != len(execArgs) || argIndex != len(args) {
		return nil, false
	}

	hasIdentityArgs := len(args) == len(execArgs)
	if hasIdentityArgs {
		for i, pos := range positions {
			if pos != i {
				hasIdentityArgs = false
				break
			}
		}
	}
	entry := &cachedQuery{
		sql:             sqlText,
		missing:         missing,
		plan:            p,
		execCount:       len(execArgs),
		hasIdentityArgs: hasIdentityArgs,
	}
	if hasIdentityArgs {
		return entry, true
	}
	entry.args = append([]any(nil), args...)
	entry.execPos = positions
	for _, pos := range positions {
		entry.args[pos] = nil
	}
	return entry, true
}

func prepareCachedSelect[T any](
	cache *queryCache,
	key queryCacheKey,
	g *grammar,
	original *queryState,
	shape selectShape,
	execArgs []any,
) (*plan, queryState, string, []any, error) {
	entry, args, ok, err := cache.load(key, g.d, execArgs)
	if err != nil {
		return nil, queryState{}, "", nil, err
	}
	if ok {
		return entry.plan, *original, entry.sql, args, nil
	}
	p, state, err := prepareQueryState[T](g.d, original, execArgs)
	if err != nil {
		return nil, queryState{}, "", nil, err
	}
	sqlText, args, err := renderSelect(g, p, &state, shape)
	if err != nil {
		return nil, queryState{}, "", nil, err
	}
	sqlText, args, err = cache.store(key, g, p, original, execArgs, sqlText, args)
	return p, state, sqlText, args, err
}
