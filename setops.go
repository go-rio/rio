package rio

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// Set is the column assignment map for UpdateAll. Keys are database column
// names; assignments render in sorted key order so the SQL is deterministic
// (stable goldens, stable statement-cache keys). Values bind as parameters
// unless they are Expr.
type Set map[string]any

// Expr is a raw SQL fragment used as an UpdateAll value — the escape hatch
// for database-side arithmetic: rio.Set{"count": rio.Expr("count + 1")}.
// It is spliced into the statement verbatim; never build one from user input.
type Expr string

// checkSetOpShape refuses query state a set-based write cannot honor.
// Silently ignoring a Limit would turn "delete ten rows" into "delete every
// matching row" — the one place an ORM must fail loudly instead.
func checkSetOpShape(op string, s *queryState) error {
	if s.limitSet || s.offsetSet {
		return fmt.Errorf("rio: %s cannot honor Limit/Offset (UPDATE/DELETE with LIMIT is not portable SQL); select the target rows in Where", op)
	}
	if len(s.groups) > 0 || len(s.havings) > 0 {
		return fmt.Errorf("rio: %s with GroupBy/Having would change which rows match; express the condition in Where or use Raw", op)
	}
	if len(s.joins) > 0 {
		// A set-based write renders only its own table; a Join would leave
		// its WHERE referencing a table not in the statement. Cross-table
		// bulk writes are not portable — filter with WhereHas or a subquery.
		return fmt.Errorf("rio: %s cannot honor Join (UPDATE/DELETE across joined tables is not portable SQL); filter with WhereHas or an IN subquery in Where", op)
	}
	if len(s.orders) > 0 {
		return fmt.Errorf("rio: %s cannot honor OrderBy (a set-based write has no row order); drop it", op)
	}
	return nil
}

// UpdateAll updates every matching row in one statement, returning the
// affected count. It refuses to run without conditions unless AllRows() was
// called. UpdatedAt is maintained automatically (override by assigning it
// yourself). Set-based writes bypass the version column — like every bulk
// path, and documented as such: optimistic locking guards entity Update.
func (q Query[T]) UpdateAll(ctx context.Context, db Queryer, set Set) (int64, error) {
	if len(set) == 0 {
		return 0, fmt.Errorf("rio: UpdateAll with an empty Set")
	}
	if len(q.s.wheres) == 0 && len(q.s.hasConds) == 0 && !q.s.allRows {
		return 0, ErrMissingWhere
	}
	if err := checkSetOpShape("UpdateAll", &q.s); err != nil {
		return 0, err
	}
	p, err := planOf[T]()
	if err != nil {
		return 0, err
	}
	g := db.gram()
	d := g.d
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
	var args []any
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
			args = append(args, d.bindTime(now))
			continue
		}
		if expr, isExpr := v.(Expr); isExpr {
			b = append(b, string(expr)...)
			continue
		}
		b = append(b, '?')
		if f.jsonCol {
			// JSON columns take Go values and marshal like entity writes.
			data, err := json.Marshal(v)
			if err != nil {
				return 0, fmt.Errorf("rio: UpdateAll: column %q: encoding JSON: %w", k, err)
			}
			args = append(args, data)
			continue
		}
		if _, expands := sliceElems(v); expands {
			// This is a `SET col = ?`, not an `IN (?)`: the rebinder would
			// expand a bare slice into `SET col = ?, ?`, malformed SQL. Wrap
			// array values in a driver.Valuer (pq.Array, pgtype) or use Expr.
			return 0, fmt.Errorf("rio: UpdateAll: column %q value is a slice, which SET cannot expand; wrap it in a driver.Valuer (e.g. pq.Array) or use rio.Expr", k)
		}
		args = append(args, v)
	}

	b, args, err = renderWhere(b, args, g, table, p, &q.s)
	if err != nil {
		return 0, err
	}
	sqlText, outArgs, err := finishSQL(d, b, args)
	if err != nil {
		return 0, err
	}
	res, err := run(ctx, db, "update", p.structName, sqlText, outArgs)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// DeleteAll deletes every matching row in one statement — as an UPDATE
// setting the deletion timestamp on soft-delete models (already-trashed rows
// are excluded by the default filter), a real DELETE otherwise. It refuses
// to run without conditions unless AllRows() was called.
func (q Query[T]) DeleteAll(ctx context.Context, db Queryer) (int64, error) {
	if len(q.s.wheres) == 0 && len(q.s.hasConds) == 0 && !q.s.allRows {
		return 0, ErrMissingWhere
	}
	if err := checkSetOpShape("DeleteAll", &q.s); err != nil {
		return 0, err
	}
	p, err := planOf[T]()
	if err != nil {
		return 0, err
	}
	if p.softDel != nil {
		set := Set{p.softDel.column: db.gram().d.bindTime(normalizeTime(db.conf().clock()))}
		return q.UpdateAll(ctx, db, set)
	}
	return q.forceDeleteAll(ctx, db, p)
}

// ForceDeleteAll hard-deletes matching rows even on soft-delete models. Like
// the other set-based writes it refuses to run without conditions unless
// AllRows() was called, so emptying the recycle bin is the explicit
// OnlyTrashed().AllRows().ForceDeleteAll() (AllRows opts into the bulk write;
// OnlyTrashed scopes it to the trashed rows).
func (q Query[T]) ForceDeleteAll(ctx context.Context, db Queryer) (int64, error) {
	if len(q.s.wheres) == 0 && len(q.s.hasConds) == 0 && !q.s.allRows {
		return 0, ErrMissingWhere
	}
	if err := checkSetOpShape("ForceDeleteAll", &q.s); err != nil {
		return 0, err
	}
	p, err := planOf[T]()
	if err != nil {
		return 0, err
	}
	return q.forceDeleteAll(ctx, db, p)
}

func (q Query[T]) forceDeleteAll(ctx context.Context, db Queryer, p *plan) (int64, error) {
	if err := checkSetOpShape("DeleteAll", &q.s); err != nil {
		return 0, err
	}
	g := db.gram()
	d := g.d
	table := g.table(p)
	b := make([]byte, 0, 96)
	b = append(b, "DELETE FROM "...)
	b = d.quote(b, table)
	var args []any
	b, args, err := renderWhere(b, args, g, table, p, &q.s)
	if err != nil {
		return 0, err
	}
	sqlText, outArgs, err := finishSQL(d, b, args)
	if err != nil {
		return 0, err
	}
	res, err := run(ctx, db, "delete", p.structName, sqlText, outArgs)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// RestoreAll clears the deletion timestamp on every matching soft-deleted row
// — the set-based peer of the entity-form Restore, named like UpdateAll and
// DeleteAll. Conditions are required (or AllRows); combine with OnlyTrashed as
// needed.
func (q Query[T]) RestoreAll(ctx context.Context, db Queryer) (int64, error) {
	p, err := planOf[T]()
	if err != nil {
		return 0, err
	}
	if p.softDel == nil {
		return 0, fmt.Errorf("rio: RestoreAll: %s has no softdelete column", p.structName)
	}
	if err := checkSetOpShape("RestoreAll", &q.s); err != nil {
		return 0, err
	}
	if q.s.trashed == trashDefault {
		q.s.trashed = trashOnly
	}
	return q.UpdateAll(ctx, db, Set{p.softDel.column: nil})
}
