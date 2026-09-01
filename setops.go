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

// Expr is a verbatim UpdateAll value for database-side expressions; never
// construct it from untrusted input.
type Expr string

// UpdateAll updates matching rows and returns the affected count. It requires
// conditions or AllRows. UpdatedAt is maintained unless explicitly assigned;
// set-based writes do not use optimistic locking.
func (q Query[T]) UpdateAll(ctx context.Context, db Queryer, set Set, args ...any) (int64, error) {
	_, n, err := q.updateAll(ctx, db, set, args, "update", false)
	return n, err
}

// UpdateAllReturning is UpdateAll returning the updated rows. Dialects
// without RETURNING (MySQL) reject it.
func (q Query[T]) UpdateAllReturning(ctx context.Context, db Queryer, set Set, args ...any) ([]T, error) {
	rows, _, err := q.updateAll(ctx, db, set, args, "update", true)
	return rows, err
}

// DeleteAll deletes matching rows, using soft deletion when configured. It
// requires conditions or AllRows.
func (q Query[T]) DeleteAll(ctx context.Context, db Queryer, args ...any) (int64, error) {
	_, n, err := q.deleteAll(ctx, db, args, false)
	return n, err
}

// DeleteAllReturning is DeleteAll returning the deleted rows, as stored after
// a soft delete. Dialects without RETURNING (MySQL) reject it.
func (q Query[T]) DeleteAllReturning(ctx context.Context, db Queryer, args ...any) ([]T, error) {
	rows, _, err := q.deleteAll(ctx, db, args, true)
	return rows, err
}

func (q Query[T]) deleteAll(ctx context.Context, db Queryer, args []any, returning bool) ([]T, int64, error) {
	if len(q.s.wheres) == 0 && len(q.s.hasConds) == 0 && !q.s.allRows {
		return nil, 0, ErrMissingWhere
	}
	if err := checkSetOpShape("DeleteAll", &q.s); err != nil {
		return nil, 0, err
	}
	g := db.gram()
	p, state, err := prepareQueryState[T](g.d, &q.s, args)
	if err != nil {
		return nil, 0, err
	}
	// Check before delegation so errors name DeleteAll.
	if d := g.d; !d.caps().mutations {
		return nil, 0, checkDeleteWrite(d, "DeleteAll", g.table(p))
	}
	if p.softDel != nil {
		set := Set{p.softDel.column: g.d.bindTime(normalizeTime(db.conf().clock()))}
		return (Query[T]{s: state}).updateAll(ctx, db, set, nil, "delete", returning)
	}
	return q.forceDeleteAll(ctx, db, p, &state, returning)
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
	_, n, err := q.forceDeleteAll(ctx, db, p, &state, false)
	return n, err
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
	args []any,
	hookOp string,
	returning bool,
) ([]T, int64, error) {
	if len(set) == 0 {
		return nil, 0, errors.New("rio: UpdateAll with an empty Set")
	}
	if len(q.s.wheres) == 0 && len(q.s.hasConds) == 0 && !q.s.allRows {
		return nil, 0, ErrMissingWhere
	}
	if err := checkSetOpShape("UpdateAll", &q.s); err != nil {
		return nil, 0, err
	}
	g := db.gram()
	p, state, err := prepareQueryState[T](g.d, &q.s, args)
	if err != nil {
		return nil, 0, err
	}
	d := g.d
	if err := checkUpdateWrite(d, "UpdateAll", g.table(p)); err != nil {
		return nil, 0, err
	}
	if err := checkReturning(d, returning, hookOp); err != nil {
		return nil, 0, err
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
			return nil, 0, fmt.Errorf("rio: UpdateAll: %s has no column %q", p.structName, k)
		}
		if f.readOnly {
			return nil, 0, fmt.Errorf("rio: UpdateAll: column %q is readonly", k)
		}
		b = d.quote(b, k)
		b = append(b, " = "...)
		v, given := set[k]
		if !given { // the auto-maintained updated_at
			b = append(b, '?')
			bindArgs = append(bindArgs, d.bindTime(now))
			continue
		}
		var err error
		if b, bindArgs, err = appendSetValue(b, bindArgs, "UpdateAll", f, v); err != nil {
			return nil, 0, err
		}
	}

	b, bindArgs, err = renderWhere(b, bindArgs, g, table, p, &state, nil)
	if err != nil {
		return nil, 0, err
	}
	if returning {
		b = appendReturning(b, d, table, p)
	}
	sqlText, outArgs, err := finishSQL(d, b, bindArgs)
	if err != nil {
		return nil, 0, err
	}
	return runSetOp[T](ctx, db, hookOp, p, sqlText, outArgs, returning)
}

// appendSetValue renders one assignment's right-hand side: an Expr verbatim,
// anything else as a bound value (JSON columns encode first).
func appendSetValue(b []byte, args []any, op string, f *field, v any) ([]byte, []any, error) {
	if expr, isExpr := v.(Expr); isExpr {
		return append(b, string(expr)...), args, nil
	}
	b = append(b, '?')
	if f.jsonCol {
		isNilPointer := v != nil &&
			reflect.TypeOf(v).Kind() == reflect.Pointer &&
			reflect.ValueOf(v).IsNil()
		if v == nil || isNilPointer {
			return b, append(args, nil), nil
		}
		data, err := json.Marshal(v)
		if err != nil {
			return nil, nil, fmt.Errorf("rio: %s: column %q: encoding JSON: %w", op, f.column, err)
		}
		return b, append(args, data), nil
	}
	if _, expands := sliceValue(v); expands {
		return nil, nil, fmt.Errorf(
			"rio: %s: column %q value is a slice, which SET cannot expand; "+
				"wrap it in a driver.Valuer (e.g. pq.Array) or use rio.Expr",
			op,
			f.column,
		)
	}
	return b, append(args, v), nil
}

// checkReturning rejects a returning set-based write on dialects without
// RETURNING.
func checkReturning(d Dialect, returning bool, op string) error {
	if !returning || d.caps().returning {
		return nil
	}
	name := "UpdateAllReturning"
	if op == "delete" {
		name = "DeleteAllReturning"
	}
	return unsupportedf("rio: %s is not supported on %s (no RETURNING clause); use the counting form", name, d.name())
}

// runSetOp executes a set-based write, scanning the RETURNING rows when asked.
func runSetOp[T any](ctx context.Context, db Queryer, op string, p *plan, sqlText string, args []any, returning bool) ([]T, int64, error) {
	if !returning {
		n, err := runAffected(ctx, db, op, p.structName, sqlText, args)
		return nil, n, err
	}
	rows, finish, err := runQuery(ctx, db, op, p.structName, sqlText, args)
	if err != nil {
		return nil, 0, err
	}
	out, err := scanAllCap[T](rows, p, false, 0, 0)
	finishQuery(finish, err, int64(len(out)))
	if err != nil {
		return nil, 0, err
	}
	return out, int64(len(out)), nil
}

func (q Query[T]) forceDeleteAll(ctx context.Context, db Queryer, p *plan, state *queryState, returning bool) ([]T, int64, error) {
	if err := checkSetOpShape("DeleteAll", state); err != nil {
		return nil, 0, err
	}
	g := db.gram()
	d := g.d
	if err := checkReturning(d, returning, "delete"); err != nil {
		return nil, 0, err
	}
	table := g.table(p)
	b := make([]byte, 0, 96)
	b = append(b, "DELETE FROM "...)
	b = d.quote(b, table)
	var args []any
	b, args, err := renderWhere(b, args, g, table, p, state, nil)
	if err != nil {
		return nil, 0, err
	}
	if returning {
		b = appendReturning(b, d, table, p)
	}
	sqlText, outArgs, err := finishSQL(d, b, args)
	if err != nil {
		return nil, 0, err
	}
	return runSetOp[T](ctx, db, "delete", p, sqlText, outArgs, returning)
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
		return fmt.Errorf(
			"rio: %s cannot honor Join (UPDATE/DELETE across joined tables is not portable SQL); "+
				"filter with WhereHas or an IN subquery in Where",
			op,
		)
	}
	if len(s.orders) > 0 {
		return fmt.Errorf("rio: %s cannot honor OrderBy (a set-based write has no row order); drop it", op)
	}
	if len(s.orderKeys) > 0 || s.after != nil || s.before != nil {
		return fmt.Errorf("rio: %s cannot honor OrderKeys/After/Before (a set-based write has no row order); drop them", op)
	}
	if len(s.withs) > 0 || len(s.counts) > 0 {
		return fmt.Errorf("rio: %s cannot honor With/WithCount (a set-based write returns no entities to load into); drop them", op)
	}
	if s.lock != lockNone {
		return fmt.Errorf("rio: %s cannot honor ForUpdate/ForShare (the write takes its own row locks); drop it", op)
	}
	if s.final {
		return fmt.Errorf("rio: %s cannot honor Final (FINAL modifies reads, and ClickHouse rejects set-based writes anyway); drop it", op)
	}
	return nil
}
