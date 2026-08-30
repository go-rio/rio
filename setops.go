package rio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
)

// Set maps database column names to UpdateAll values. Expr values are inserted
// verbatim; never construct them from untrusted input.
type Set map[string]any

// Expr is a verbatim UpdateAll value for database-side expressions. Never
// construct it from untrusted input; whitelist dynamic identifiers.
type Expr string

// UpdateAll updates matching rows and returns the affected count. It requires
// conditions or AllRows. UpdatedAt is maintained unless explicitly assigned;
// set-based writes do not use optimistic locking.
func (q Query[T]) UpdateAll(ctx context.Context, db Queryer, set Set, execArgs ...any) (int64, error) {
	return q.updateAll(ctx, db, set, execArgs, "update")
}

// DeleteAll deletes matching rows, using soft deletion when configured. It
// requires conditions or AllRows.
func (q Query[T]) DeleteAll(ctx context.Context, db Queryer, args ...any) (int64, error) {
	if len(q.s.wheres) == 0 && len(q.s.hasConds) == 0 && !q.s.allRows {
		return 0, ErrMissingWhere
	}
	if err := checkSetOpShape("DeleteAll", &q.s); err != nil {
		return 0, err
	}
	g := db.gram()
	p, state, err := prepareQueryState[T](g.d, &q.s, args)
	if err != nil {
		return 0, err
	}
	// Check before delegation so errors name DeleteAll.
	if d := g.d; !d.caps().mutations {
		return 0, checkDeleteWrite(d, "DeleteAll", g.table(p))
	}
	if p.softDel != nil {
		set := Set{p.softDel.column: g.d.bindTime(normalizeTime(db.conf().clock()))}
		return (Query[T]{s: state}).updateAll(ctx, db, set, nil, "delete")
	}
	return q.forceDeleteAll(ctx, db, p, &state)
}

// ForceDeleteAll permanently deletes matching rows. It requires conditions
// or AllRows, including on soft-delete models.
func (q Query[T]) ForceDeleteAll(ctx context.Context, db Queryer, args ...any) (int64, error) {
	if len(q.s.wheres) == 0 && len(q.s.hasConds) == 0 && !q.s.allRows {
		return 0, ErrMissingWhere
	}
	if err := checkSetOpShape("ForceDeleteAll", &q.s); err != nil {
		return 0, err
	}
	g := db.gram()
	p, state, err := prepareQueryState[T](g.d, &q.s, args)
	if err != nil {
		return 0, err
	}
	if d := g.d; !d.caps().mutations {
		return 0, checkDeleteWrite(d, "ForceDeleteAll", g.table(p))
	}
	return q.forceDeleteAll(ctx, db, p, &state)
}

// RestoreAll restores matching soft-deleted rows. It requires conditions or
// AllRows.
func (q Query[T]) RestoreAll(ctx context.Context, db Queryer, args ...any) (int64, error) {
	p, err := planOf[T]()
	if err != nil {
		return 0, err
	}
	if p.softDel == nil {
		return 0, fmt.Errorf("rio: RestoreAll: %s has no softdelete column", p.structName)
	}
	if err := checkRestoreWrite(db.gram().d, "RestoreAll"); err != nil {
		return 0, err
	}
	if err := checkSetOpShape("RestoreAll", &q.s); err != nil {
		return 0, err
	}
	if q.s.trashed == trashDefault {
		q.s.trashed = trashOnly
	}
	return q.UpdateAll(ctx, db, Set{p.softDel.column: nil}, args...)
}

func (q Query[T]) updateAll(
	ctx context.Context,
	db Queryer,
	set Set,
	execArgs []any,
	hookOp string,
) (int64, error) {
	if len(set) == 0 {
		return 0, errors.New("rio: UpdateAll with an empty Set")
	}
	if len(q.s.wheres) == 0 && len(q.s.hasConds) == 0 && !q.s.allRows {
		return 0, ErrMissingWhere
	}
	if err := checkSetOpShape("UpdateAll", &q.s); err != nil {
		return 0, err
	}
	g := db.gram()
	p, state, err := prepareQueryState[T](g.d, &q.s, execArgs)
	if err != nil {
		return 0, err
	}
	d := g.d
	if err := checkUpdateWrite(d, "UpdateAll", g.table(p)); err != nil {
		return 0, err
	}
	now := normalizeTime(db.conf().clock())
	table := g.table(p)

	keys := make([]string, 0, len(set)+1)
	for k := range set {
		keys = append(keys, k)
	}
	if p.updated != nil {
		if _, overridden := set[p.updated.column]; !overridden {
			keys = append(keys, p.updated.column)
		}
	}
	sort.Strings(keys)

	b := make([]byte, 0, 160)
	b = append(b, "UPDATE "...)
	b = d.quote(b, table)
	b = append(b, " SET "...)
	var bindArgs []any
	for i, k := range keys {
		if i > 0 {
			b = append(b, ", "...)
		}
		f, ok := p.byColumn[k]
		if !ok {
			return 0, fmt.Errorf("rio: UpdateAll: %s has no column %q", p.structName, k)
		}
		b = d.quote(b, k)
		b = append(b, " = "...)
		v, given := set[k]
		if !given { // the auto-maintained updated_at
			b = append(b, '?')
			bindArgs = append(bindArgs, d.bindTime(now))
			continue
		}
		if expr, isExpr := v.(Expr); isExpr {
			b = append(b, string(expr)...)
			continue
		}
		b = append(b, '?')
		if f.jsonCol {
			isNilPointer := v != nil &&
				reflect.TypeOf(v).Kind() == reflect.Pointer &&
				reflect.ValueOf(v).IsNil()
			if v == nil || isNilPointer {
				bindArgs = append(bindArgs, nil)
				continue
			}
			// Marshal JSON columns consistently with entity writes.
			data, err := json.Marshal(v)
			if err != nil {
				return 0, fmt.Errorf("rio: UpdateAll: column %q: encoding JSON: %w", k, err)
			}
			bindArgs = append(bindArgs, data)
			continue
		}
		if _, expands := sliceValue(v); expands {
			// Slices expand only in placeholder lists, not assignments.
			return 0, fmt.Errorf(
				"rio: UpdateAll: column %q value is a slice, which SET cannot expand; "+
					"wrap it in a driver.Valuer (e.g. pq.Array) or use rio.Expr",
				k,
			)
		}
		bindArgs = append(bindArgs, v)
	}

	b, bindArgs, err = renderWhere(b, bindArgs, g, table, p, &state)
	if err != nil {
		return 0, err
	}
	sqlText, outArgs, err := finishSQL(d, b, bindArgs)
	if err != nil {
		return 0, err
	}
	return runAffected(ctx, db, hookOp, p.structName, sqlText, outArgs)
}

func (q Query[T]) forceDeleteAll(ctx context.Context, db Queryer, p *plan, state *queryState) (int64, error) {
	if err := checkSetOpShape("DeleteAll", state); err != nil {
		return 0, err
	}
	g := db.gram()
	d := g.d
	table := g.table(p)
	b := make([]byte, 0, 96)
	b = append(b, "DELETE FROM "...)
	b = d.quote(b, table)
	var args []any
	b, args, err := renderWhere(b, args, g, table, p, state)
	if err != nil {
		return 0, err
	}
	sqlText, outArgs, err := finishSQL(d, b, args)
	if err != nil {
		return 0, err
	}
	return runAffected(ctx, db, "delete", p.structName, sqlText, outArgs)
}

// checkSetOpShape rejects query clauses a portable set-based write cannot honor.
func checkSetOpShape(op string, s *queryState) error {
	if s.limitSet || s.offsetSet {
		return fmt.Errorf(
			"rio: %s cannot honor Limit/Offset (UPDATE/DELETE with LIMIT is not portable SQL); "+
				"select the target rows in Where",
			op,
		)
	}
	if len(s.groups) > 0 || len(s.havings) > 0 {
		return fmt.Errorf(
			"rio: %s with GroupBy/Having would change which rows match; "+
				"express the condition in Where or use Raw",
			op,
		)
	}
	if len(s.joins) > 0 {
		// The write renders only its own table, so joined references are invalid.
		return fmt.Errorf(
			"rio: %s cannot honor Join (UPDATE/DELETE across joined tables is not portable SQL); "+
				"filter with WhereHas or an IN subquery in Where",
			op,
		)
	}
	if len(s.orders) > 0 {
		return fmt.Errorf("rio: %s cannot honor OrderBy (a set-based write has no row order); drop it", op)
	}
	if len(s.orderKeys) > 0 || s.after != nil {
		return fmt.Errorf("rio: %s cannot honor OrderKeys/After (a set-based write has no row order); drop them", op)
	}
	if len(s.withs) > 0 || len(s.counts) > 0 {
		return fmt.Errorf("rio: %s cannot honor With/WithCount (a set-based write returns no entities to load into); drop them", op)
	}
	if s.forUpdate {
		return fmt.Errorf("rio: %s cannot honor ForUpdate (the write takes its own row locks); drop it", op)
	}
	if s.final {
		return fmt.Errorf("rio: %s cannot honor Final (FINAL modifies reads, and ClickHouse rejects set-based writes anyway); drop it", op)
	}
	return nil
}
