package rio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
)

// Set maps database column names to UpdateAll values. Expression values are
// inserted verbatim; never construct them from untrusted input.
type Set map[string]any

// Expression is a verbatim SQL value for Set with the arguments its ?
// placeholders bind; Expr builds one.
type Expression struct {
	sql  string
	args []any
}

// Expr builds an Expression whose ? placeholders bind args in order; never
// build the SQL from untrusted input.
func Expr(sql string, args ...any) Expression {
	return Expression{sql: sql, args: copyArgs(args)}
}

// UpdateAll updates matching rows and returns the affected count. It requires
// conditions or AllRows. UpdatedAt is maintained unless explicitly assigned;
// set-based writes do not use optimistic locking.
func (q Query[T]) UpdateAll(ctx context.Context, db Queryer, set Set, args ...any) (int64, error) {
	_, n, err := updateAll[T](ctx, db, q, set, args, "update", false, nil)
	return n, err
}

// UpdateAllReturning is UpdateAll returning the updated rows. Dialects
// without RETURNING (MySQL) reject it.
func (q Query[T]) UpdateAllReturning(ctx context.Context, db Queryer, set Set, args ...any) ([]T, error) {
	rows, _, err := updateAll[T](ctx, db, q, set, args, "update", true, nil)
	return rows, err
}

// UpdateAllInto is UpdateAllReturning scanning into P, a struct whose fields
// map to columns of T; only those columns return.
func UpdateAllInto[P, T any](ctx context.Context, db Queryer, q Query[T], set Set, args ...any) ([]P, error) {
	into, err := intoPlan[P]("UpdateAllInto")
	if err != nil {
		return nil, err
	}
	rows, _, err := updateAll[P](ctx, db, q, set, args, "update", true, into)
	return rows, err
}

// DeleteAll deletes matching rows, using soft deletion when configured. It
// requires conditions or AllRows.
func (q Query[T]) DeleteAll(ctx context.Context, db Queryer, args ...any) (int64, error) {
	_, n, err := deleteAll[T](ctx, db, q, args, false, nil)
	return n, err
}

// DeleteAllReturning is DeleteAll returning the deleted rows, as stored after
// a soft delete. Dialects without RETURNING (MySQL) reject it.
func (q Query[T]) DeleteAllReturning(ctx context.Context, db Queryer, args ...any) ([]T, error) {
	rows, _, err := deleteAll[T](ctx, db, q, args, true, nil)
	return rows, err
}

// DeleteAllInto is DeleteAllReturning scanning into P, as UpdateAllInto does.
func DeleteAllInto[P, T any](ctx context.Context, db Queryer, q Query[T], args ...any) ([]P, error) {
	into, err := intoPlan[P]("DeleteAllInto")
	if err != nil {
		return nil, err
	}
	rows, _, err := deleteAll[P](ctx, db, q, args, true, into)
	return rows, err
}

// intoPlan resolves the struct an Into form scans RETURNING into.
func intoPlan[P any](name string) (*plan, error) {
	if t := reflect.TypeFor[P](); t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("rio: %s: %s is not a struct; declare the returned columns as fields", name, t)
	}
	return planOf[P]()
}

// ForceDeleteAll permanently deletes matching rows. It requires conditions
// or AllRows, including on soft-delete models.
func (q Query[T]) ForceDeleteAll(ctx context.Context, db Queryer, args ...any) (int64, error) {
	missingWhere := len(q.s.wheres) == 0 && len(q.s.hasConds) == 0 && !q.s.allRows
	if missingWhere {
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
		return 0, checkDeleteWrite(d, "ForceDeleteAll", state.tableOf(g, p))
	}
	op, err := renderForceDeleteAll(db, p, &state, false, nil)
	if err != nil {
		return 0, err
	}
	_, n, err := runSetOp[T](ctx, db, "delete", op)
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

// setOp is a rendered set-based write; ret is the plan RETURNING lists, nil
// when the statement only counts.
type setOp struct {
	model string
	ret   *plan
	sql   string
	args  []any
}

// updateAll renders and runs UPDATE; with returning it scans the rows into R,
// T itself or the DTO into.
func updateAll[R, T any](
	ctx context.Context,
	db Queryer,
	q Query[T],
	set Set,
	args []any,
	hookOp string,
	returning bool,
	into *plan,
) ([]R, int64, error) {
	op, err := q.renderUpdateAll(db, set, args, hookOp, returning, into)
	if err != nil {
		return nil, 0, err
	}
	return runSetOp[R](ctx, db, hookOp, op)
}

func (q Query[T]) renderUpdateAll(
	db Queryer,
	set Set,
	args []any,
	hookOp string,
	returning bool,
	into *plan,
) (setOp, error) {
	if len(set) == 0 {
		return setOp{}, errors.New("rio: UpdateAll with an empty Set")
	}
	missingWhere := len(q.s.wheres) == 0 && len(q.s.hasConds) == 0 && !q.s.allRows
	if missingWhere {
		return setOp{}, ErrMissingWhere
	}
	if err := checkSetOpShape("UpdateAll", &q.s); err != nil {
		return setOp{}, err
	}
	g := db.gram()
	p, state, err := prepareQueryState[T](g.d, &q.s, args)
	if err != nil {
		return setOp{}, err
	}
	d := g.d
	table := state.tableOf(g, p)
	if err := checkUpdateWrite(d, "UpdateAll", table); err != nil {
		return setOp{}, err
	}
	if err := checkReturning(d, returning, hookOp, into != nil); err != nil {
		return setOp{}, err
	}
	now := normalizeTime(db.conf().clock())

	keys := make([]string, 0, len(set)+1)
	for k := range set {
		keys = append(keys, k)
	}
	if p.updated != nil && !db.conf().noStamps {
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
			return setOp{}, fmt.Errorf("rio: UpdateAll: %s has no column %q", p.structName, k)
		}
		if f.readOnly {
			return setOp{}, fmt.Errorf("rio: UpdateAll: column %q is readonly", k)
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
			return setOp{}, err
		}
	}

	b, bindArgs, err = renderWhere(b, bindArgs, g, table, p, &state, nil)
	if err != nil {
		return setOp{}, err
	}
	return finishSetOp(g, b, bindArgs, table, p, hookOp, returning, into)
}

// deleteAll renders and runs the soft or hard delete, as updateAll does.
func deleteAll[R, T any](
	ctx context.Context,
	db Queryer,
	q Query[T],
	args []any,
	returning bool,
	into *plan,
) ([]R, int64, error) {
	op, err := q.renderDeleteAll(db, args, returning, into)
	if err != nil {
		return nil, 0, err
	}
	return runSetOp[R](ctx, db, "delete", op)
}

func (q Query[T]) renderDeleteAll(db Queryer, args []any, returning bool, into *plan) (setOp, error) {
	missingWhere := len(q.s.wheres) == 0 && len(q.s.hasConds) == 0 && !q.s.allRows
	if missingWhere {
		return setOp{}, ErrMissingWhere
	}
	if err := checkSetOpShape("DeleteAll", &q.s); err != nil {
		return setOp{}, err
	}
	g := db.gram()
	p, state, err := prepareQueryState[T](g.d, &q.s, args)
	if err != nil {
		return setOp{}, err
	}
	// Check before delegation so errors name DeleteAll.
	if d := g.d; !d.caps().mutations {
		return setOp{}, checkDeleteWrite(d, "DeleteAll", state.tableOf(g, p))
	}
	if p.softDel != nil {
		set := Set{p.softDel.column: g.d.bindTime(normalizeTime(db.conf().clock()))}
		return (Query[T]{s: state}).renderUpdateAll(db, set, nil, "delete", returning, into)
	}
	return renderForceDeleteAll(db, p, &state, returning, into)
}

func renderForceDeleteAll(db Queryer, p *plan, state *queryState, returning bool, into *plan) (setOp, error) {
	if err := checkSetOpShape("DeleteAll", state); err != nil {
		return setOp{}, err
	}
	g := db.gram()
	d := g.d
	if err := checkReturning(d, returning, "delete", into != nil); err != nil {
		return setOp{}, err
	}
	table := state.tableOf(g, p)
	b := make([]byte, 0, 96)
	b = append(b, "DELETE FROM "...)
	b = d.quote(b, table)
	var args []any
	b, args, err := renderWhere(b, args, g, table, p, state, nil)
	if err != nil {
		return setOp{}, err
	}
	return finishSetOp(g, b, args, table, p, "delete", returning, into)
}

// finishSetOp appends RETURNING, T's columns or the DTO's (each a column of
// T), and rebinds the statement.
func finishSetOp(
	g *grammar,
	b []byte,
	args []any,
	table string,
	p *plan,
	hookOp string,
	returning bool,
	into *plan,
) (setOp, error) {
	var ret *plan
	switch {
	case !returning:
	case into == nil:
		ret = p
	default:
		for _, f := range into.fields {
			if _, ok := p.byColumn[f.column]; !ok {
				return setOp{}, fmt.Errorf(
					"rio: %s: %s has no column %q for %s.%s",
					setOpName(hookOp, true), p.structName, f.column, into.structName, f.name,
				)
			}
		}
		ret = into
	}
	if ret != nil {
		b = appendReturning(b, g.d, table, ret)
	}
	sqlText, outArgs, err := finishSQL(g, b, args)
	if err != nil {
		return setOp{}, err
	}
	return setOp{model: p.structName, ret: ret, sql: sqlText, args: outArgs}, nil
}

// appendSetValue renders one assignment's right-hand side: an Expression
// verbatim with its arguments, anything else bound (JSON columns encode first).
func appendSetValue(b []byte, args []any, op string, f *field, v any) ([]byte, []any, error) {
	if expr, isExpr := v.(Expression); isExpr {
		return append(b, expr.sql...), append(args, expr.args...), nil
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
func checkReturning(d Dialect, returning bool, op string, into bool) error {
	if !returning || d.caps().returning {
		return nil
	}
	return unsupportedf(
		"rio: %s is not supported on %s (no RETURNING clause); use the counting form",
		setOpName(op, into), d.name(),
	)
}

// setOpName names the returning API of a set-based write for errors.
func setOpName(op string, into bool) string {
	name := "UpdateAll"
	if op == "delete" {
		name = "DeleteAll"
	}
	if into {
		return name + "Into"
	}
	return name + "Returning"
}

// runSetOp executes a set-based write, scanning the RETURNING rows into R
// when asked.
func runSetOp[R any](ctx context.Context, db Queryer, hookOp string, op setOp) ([]R, int64, error) {
	if op.ret == nil {
		n, err := runAffected(ctx, db, hookOp, op.model, op.sql, op.args)
		return nil, n, err
	}
	rows, finish, err := runQuery(ctx, db, hookOp, op.model, op.sql, op.args)
	if err != nil {
		return nil, 0, err
	}
	out, err := scanAllCap[R](rows, op.ret, false, 0, 0)
	finishQuery(finish, err, int64(len(out)))
	if err != nil {
		return nil, 0, err
	}
	return out, int64(len(out)), nil
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
	hasSortKeys := len(s.orderKeys) > 0 || s.after != nil || s.before != nil
	if hasSortKeys {
		return fmt.Errorf("rio: %s cannot honor OrderKeys/After/Before (a set-based write has no row order); drop them", op)
	}
	if len(s.withs) > 0 || len(s.counts) > 0 {
		return fmt.Errorf(
			"rio: %s cannot honor With/WithCount (a set-based write returns no entities to load into); drop them",
			op,
		)
	}
	if s.lock != lockNone {
		return fmt.Errorf("rio: %s cannot honor ForUpdate/ForShare (the write takes its own row locks); drop it", op)
	}
	if s.final {
		return fmt.Errorf(
			"rio: %s cannot honor Final (FINAL modifies reads, and ClickHouse rejects set-based writes anyway); drop it",
			op,
		)
	}
	return nil
}
