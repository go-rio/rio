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

// UpdateAll updates every matching row in one statement, returning the
// affected count. It refuses to run without conditions unless AllRows() was
// called. UpdatedAt is maintained automatically (override by assigning it
// yourself). Set-based writes bypass the version column — like every bulk
// path, and documented as such: optimistic locking guards entity Update.
func (q Query[T]) UpdateAll(ctx context.Context, db Queryer, set Set) (int64, error) {
	if len(set) == 0 {
		return 0, fmt.Errorf("rio: UpdateAll with an empty Set")
	}
	if len(q.s.wheres) == 0 && !q.s.allRows {
		return 0, ErrMissingWhere
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
		args = append(args, v)
	}

	b, args = renderWhere(b, args, d, table, p, &q.s)
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
	if len(q.s.wheres) == 0 && !q.s.allRows {
		return 0, ErrMissingWhere
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

// ForceDeleteAll hard-deletes matching rows even on soft-delete models.
// Combined with OnlyTrashed it empties the recycle bin.
func (q Query[T]) ForceDeleteAll(ctx context.Context, db Queryer) (int64, error) {
	if len(q.s.wheres) == 0 && !q.s.allRows {
		return 0, ErrMissingWhere
	}
	p, err := planOf[T]()
	if err != nil {
		return 0, err
	}
	return q.forceDeleteAll(ctx, db, p)
}

func (q Query[T]) forceDeleteAll(ctx context.Context, db Queryer, p *plan) (int64, error) {
	g := db.gram()
	d := g.d
	table := g.table(p)
	b := make([]byte, 0, 96)
	b = append(b, "DELETE FROM "...)
	b = d.quote(b, table)
	var args []any
	b, args = renderWhere(b, args, d, table, p, &q.s)
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

// Restore clears the deletion timestamp on every matching soft-deleted row.
// Conditions are required (or AllRows); combine with OnlyTrashed as needed.
func (q Query[T]) Restore(ctx context.Context, db Queryer) (int64, error) {
	p, err := planOf[T]()
	if err != nil {
		return 0, err
	}
	if p.softDel == nil {
		return 0, fmt.Errorf("rio: Restore: %s has no softdelete column", p.structName)
	}
	if q.s.trashed == trashDefault {
		q.s.trashed = trashOnly
	}
	return q.UpdateAll(ctx, db, Set{p.softDel.column: nil})
}
