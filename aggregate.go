package rio

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Distinct renders SELECT DISTINCT: entity rows deduplicate across joins,
// Pluck values deduplicate, Count counts distinct primary keys, and Sum and
// Avg aggregate distinct values.
func (q Query[T]) Distinct() Query[T] {
	q.cache = nil
	q.s.distinct = true
	return q
}

// Sum totals a mapped column under the query's conditions; over no rows it
// returns V's zero value (use sql.Null[V] to tell the two apart).
func (q Query[T]) Sum[V any](ctx context.Context, db Queryer, column string, args ...any) (V, error) {
	return aggregate[T, V](q, ctx, db, "sum", column, args)
}

// Min returns the smallest value of a mapped column under the query's
// conditions; over no rows it returns V's zero value.
func (q Query[T]) Min[V any](ctx context.Context, db Queryer, column string, args ...any) (V, error) {
	return aggregate[T, V](q, ctx, db, "min", column, args)
}

// Max returns the largest value of a mapped column under the query's
// conditions; over no rows it returns V's zero value.
func (q Query[T]) Max[V any](ctx context.Context, db Queryer, column string, args ...any) (V, error) {
	return aggregate[T, V](q, ctx, db, "max", column, args)
}

// Avg averages a mapped column under the query's conditions; over no rows it
// returns V's zero value.
func (q Query[T]) Avg[V any](ctx context.Context, db Queryer, column string, args ...any) (V, error) {
	return aggregate[T, V](q, ctx, db, "avg", column, args)
}

func aggregate[T, V any](q Query[T], ctx context.Context, db Queryer, fn, column string, args []any) (V, error) {
	var zero V
	if len(q.s.groups) > 0 || len(q.s.havings) > 0 {
		return zero, errors.New("rio: aggregates with GroupBy/Having are projections; use Raw")
	}
	if q.s.limitSet || q.s.offsetSet {
		return zero, errors.New("rio: aggregates cannot honor Limit/Offset (they run before LIMIT applies); drop them")
	}
	g := db.gram()
	key := queryCacheKey{grammar: g.weakSelf, op: queryCacheAggregate, column: fn + ":" + column}
	entry, outArgs, cached, err := q.cache.load(key, g.d, args)
	if err != nil {
		return zero, err
	}
	var p *plan
	var sqlText string
	if cached {
		p, sqlText = entry.plan, entry.sql
	} else {
		var state queryState
		p, state, err = prepareQueryState[T](g.d, &q.s, args)
		if err != nil {
			return zero, err
		}
		f, ok := p.byColumn[column]
		if !ok {
			return zero, fmt.Errorf("rio: %s: %s has no column %q (expressions go through Raw)", fn, p.structName, column)
		}
		sqlText, outArgs, err = renderAggregate(g, p, fn, f, &state)
		if err != nil {
			return zero, err
		}
		sqlText, outArgs, err = q.cache.store(key, g, p, &q.s, args, sqlText, outArgs)
		if err != nil {
			return zero, err
		}
	}
	rows, finish, err := runQuery(ctx, db, "select", p.structName, sqlText, outArgs)
	if err != nil {
		return zero, err
	}
	// The aggregate row is NULL over no input rows.
	v, found, err := scanScalarOne[sql.Null[V]](rows)
	finishQuery(finish, err, oneIf(found))
	if err != nil {
		return zero, err
	}
	return v.V, nil
}

// renderAggregate renders SELECT fn(column) under the query's predicates.
func renderAggregate(g *grammar, p *plan, fn string, f *field, s *queryState) (string, []any, error) {
	d := g.d
	if err := checkFinal(d, s); err != nil {
		return "", nil, err
	}
	table := g.table(p)
	var sortKeys []resolvedKey
	if len(s.orderKeys) > 0 || s.after != nil || s.before != nil {
		var err error
		if sortKeys, err = resolveSortKeys(p, s); err != nil {
			return "", nil, err
		}
	}
	b := make([]byte, 0, 128)
	b = append(b, "SELECT "...)
	b = append(b, fn...)
	b = append(b, '(')
	if s.distinct && (fn == "sum" || fn == "avg") {
		b = append(b, "DISTINCT "...)
	}
	b = d.quote(b, table)
	b = append(b, '.')
	b = d.quote(b, f.column)
	b = append(b, ") FROM "...)
	b = d.quote(b, table)
	if s.final {
		b = append(b, " FINAL"...)
	}
	for _, j := range s.joins {
		b = append(b, ' ')
		b = append(b, j...)
	}
	b, args, err := renderWhere(b, nil, g, table, p, s, sortKeys)
	if err != nil {
		return "", nil, err
	}
	return finishSQL(g, b, args)
}
