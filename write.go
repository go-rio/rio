package rio

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"
	"unsafe"
)

// Insert writes one row and backfills generated columns supported by the
// dialect. Zero omitzero fields use database defaults; zero auto-increment
// keys are omitted. It initializes timestamps and a zero version before
// execution, so a failed call may still modify those fields. Trigger changes
// not returned by the statement are not loaded.
func Insert[T any](ctx context.Context, db Queryer, row *T) error {
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	rv, err := rowValue("Insert", row)
	if err != nil {
		return err
	}
	g := db.gram()
	d := g.d
	if err := checkGeneratedID(d, "Insert", p, rv); err != nil {
		return err
	}
	now := normalizeTime(db.conf().clock())
	stampForInsert(p, rv, now)

	bn := binder{d: d, now: now}
	cols, back, args, bits, cacheable, err := insertColumns(p, rv, &bn)
	if err != nil {
		return err
	}
	if len(cols) == 0 && d.name() == "clickhouse" {
		// ClickHouse has no all-defaults row form.
		return fmt.Errorf("rio: clickhouse has no DEFAULT VALUES statement; set at least one column on %s", p.structName)
	}
	returning := d.caps().returning && len(back) > 0
	if returning && d.name() == "sqlite" && len(back) == 1 && back[0] == p.autoIncr {
		returning = false
	}
	build := func() []byte {
		b := renderInsertHead(g, p, cols)
		b = appendInsertValues(b, d, len(cols))
		if returning {
			b = append(b, " RETURNING "...)
			for i, f := range back {
				if i > 0 {
					b = append(b, ", "...)
				}
				b = d.quote(b, f.column)
			}
		}
		return b
	}
	sqlText, err := crudSQL(g, p, "insert", bits, cacheable, build)
	if err != nil {
		return err
	}
	if returning {
		rows, finish, err := runQuery(ctx, db, "insert", p.structName, sqlText, args)
		if err != nil {
			return err
		}
		err = scanBackCols(rows, back, unsafe.Pointer(row))
		finishQuery(finish, err, oneIf(err == nil))
		return err
	}
	res, err := run(ctx, db, "insert", p.structName, sqlText, args)
	if err != nil {
		return err
	}
	return fillLastInsertID(p, rv, res.LastInsertId)
}

// Update writes a row by primary key. Without cols it writes every eligible
// field, including zero values; otherwise it writes only cols and UpdatedAt.
// It enforces optimistic locking and may stamp the struct before a failed call.
func Update[T any](ctx context.Context, db Queryer, row *T, cols ...string) error {
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	if len(p.pks) == 0 {
		return fmt.Errorf("%w: %s", ErrNoPrimaryKey, p.structName)
	}
	rv, err := rowValue("Update", row)
	if err != nil {
		return err
	}
	g := db.gram()
	d := g.d
	if err := checkUpdateWrite(d, "Update", g.table(p)); err != nil {
		return err
	}
	now := normalizeTime(db.conf().clock())

	set, err := updateSet(p, cols)
	if err != nil {
		return err
	}
	if p.updated != nil {
		setTime(p.updated, rv, now)
	}

	bits, cacheable := setBits(p, set)
	sqlText, err := crudSQL(g, p, "update", bits, cacheable, func() []byte {
		b := make([]byte, 0, 160)
		b = append(b, "UPDATE "...)
		b = d.quote(b, g.table(p))
		b = append(b, " SET "...)
		for i, f := range set {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = d.quote(b, f.column)
			b = append(b, " = ?"...)
		}
		b = appendVersionBump(b, d, p)
		return appendPKWhereSQL(b, d, p)
	})
	if err != nil {
		return err
	}
	bn := binder{d: d, now: now}
	args, err := bindFields(p, rv, &bn, set)
	if err != nil {
		return err
	}
	n, err := runAffected(ctx, db, "update", p.structName, sqlText, args)
	if err != nil {
		return err
	}
	if n == 0 {
		if p.version != nil {
			return ErrStaleObject
		}
		missing, perr := zeroAffectedMeansMissing(ctx, db, p, rv)
		if perr != nil {
			return perr
		}
		if missing {
			return ErrNotFound
		}
		return nil // matched, values already identical
	}
	if p.version != nil {
		bumpVersion(p.version, rv)
	}
	return nil
}

// Delete removes a row by primary key. Models with a softdelete column get
// an UPDATE setting the deletion timestamp instead; ForceDelete really
// deletes. The version column, when present, is checked like Update.
func Delete[T any](ctx context.Context, db Queryer, row *T) error {
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	if p.softDel != nil {
		return softDelete(ctx, db, p, row)
	}
	return hardDelete(ctx, db, p, row)
}

// ForceDelete removes a row even when its model supports soft deletion.
func ForceDelete[T any](ctx context.Context, db Queryer, row *T) error {
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	return hardDelete(ctx, db, p, row)
}

// Restore clears the deletion timestamp of one soft-deleted row by primary
// key — the entity-form inverse of Delete. The version column, when present,
// is checked and bumped like any other write.
func Restore[T any](ctx context.Context, db Queryer, row *T) error {
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	if p.softDel == nil {
		return fmt.Errorf("rio: Restore: %s has no softdelete column", p.structName)
	}
	if len(p.pks) == 0 {
		return fmt.Errorf("%w: %s", ErrNoPrimaryKey, p.structName)
	}
	rv, err := rowValue("Restore", row)
	if err != nil {
		return err
	}
	g := db.gram()
	d := g.d
	if err := checkRestoreWrite(d, "Restore"); err != nil {
		return err
	}
	now := normalizeTime(db.conf().clock())

	sqlText, err := crudSQL(g, p, "restore", 0, true, func() []byte {
		b := make([]byte, 0, 128)
		b = append(b, "UPDATE "...)
		b = d.quote(b, g.table(p))
		b = append(b, " SET "...)
		b = d.quote(b, p.softDel.column)
		b = append(b, " = NULL"...)
		if p.updated != nil {
			b = append(b, ", "...)
			b = d.quote(b, p.updated.column)
			b = append(b, " = ?"...)
		}
		b = appendVersionBump(b, d, p)
		b = appendPKWhereSQL(b, d, p)
		// Only a trashed row restores: restoring a live row must not bump
		// its version or refresh its UpdatedAt.
		b = append(b, " AND "...)
		b = d.quote(b, p.softDel.column)
		return append(b, " IS NOT NULL"...)
	})
	if err != nil {
		return err
	}
	bn := binder{d: d, now: now}
	args := make([]any, 0, 1+len(p.pks)+1) // updated_at + PKs + version
	if p.updated != nil {
		args = append(args, bn.time(now))
	}
	if args, err = appendKeyArgs(args, p, rv, &bn); err != nil {
		return err
	}
	n, err := runAffected(ctx, db, "update", p.structName, sqlText, args)
	if err != nil {
		return err
	}
	if n == 0 {
		// The IS NOT NULL predicate makes "already live" a zero-affected
		// outcome on every dialect: idempotent success adopts the stored
		// state instead of bumping the version or refreshing UpdatedAt.
		return resolveSoftNoop(ctx, db, p, rv, false)
	}
	clearTime(p.softDel, rv)
	if p.updated != nil {
		setTime(p.updated, rv, now)
	}
	if p.version != nil {
		bumpVersion(p.version, rv)
	}
	return nil
}
func softDelete[T any](ctx context.Context, db Queryer, p *plan, row *T) error {
	rv, err := rowValue("Delete", row)
	if err != nil {
		return err
	}
	if len(p.pks) == 0 {
		return fmt.Errorf("%w: %s", ErrNoPrimaryKey, p.structName)
	}
	g := db.gram()
	d := g.d
	if err := checkDeleteWrite(d, "Delete", g.table(p)); err != nil {
		return err
	}
	now := normalizeTime(db.conf().clock())

	sqlText, err := crudSQL(g, p, "softdelete", 0, true, func() []byte {
		b := make([]byte, 0, 128)
		b = append(b, "UPDATE "...)
		b = d.quote(b, g.table(p))
		b = append(b, " SET "...)
		b = d.quote(b, p.softDel.column)
		b = append(b, " = ?"...)
		if p.updated != nil {
			b = append(b, ", "...)
			b = d.quote(b, p.updated.column)
			b = append(b, " = ?"...)
		}
		b = appendVersionBump(b, d, p)
		b = appendPKWhereSQL(b, d, p)
		// Only a live row deletes: an already-trashed row keeps its original
		// deletion stamp (and version) instead of being re-stamped.
		b = append(b, " AND "...)
		b = d.quote(b, p.softDel.column)
		return append(b, " IS NULL"...)
	})
	if err != nil {
		return err
	}
	bn := binder{d: d, now: now}
	args := make([]any, 0, 2+len(p.pks)+1) // deleted_at + updated_at + PKs + version
	args = append(args, bn.time(now))
	if p.updated != nil {
		args = append(args, bn.time(now))
	}
	if args, err = appendKeyArgs(args, p, rv, &bn); err != nil {
		return err
	}
	n, err := runAffected(ctx, db, "delete", p.structName, sqlText, args)
	if err != nil {
		return err
	}
	if n == 0 {
		// The IS NULL predicate makes "already trashed" a zero-affected
		// outcome on every dialect: idempotent success adopts the stored
		// stamp — re-stamping would erase the original deletion time.
		return resolveSoftNoop(ctx, db, p, rv, true)
	}
	setTime(p.softDel, rv, now)
	if p.updated != nil {
		setTime(p.updated, rv, now)
	}
	if p.version != nil {
		bumpVersion(p.version, rv)
	}
	return nil
}

func hardDelete[T any](ctx context.Context, db Queryer, p *plan, row *T) error {
	rv, err := rowValue("Delete", row)
	if err != nil {
		return err
	}
	if len(p.pks) == 0 {
		return fmt.Errorf("%w: %s", ErrNoPrimaryKey, p.structName)
	}
	g := db.gram()
	d := g.d
	if err := checkDeleteWrite(d, "Delete", g.table(p)); err != nil {
		return err
	}

	sqlText, err := crudSQL(g, p, "delete", 0, true, func() []byte {
		b := make([]byte, 0, 96)
		b = append(b, "DELETE FROM "...)
		b = d.quote(b, g.table(p))
		return appendPKWhereSQL(b, d, p)
	})
	if err != nil {
		return err
	}
	bn := binder{d: d}
	args, err := appendKeyArgs(nil, p, rv, &bn)
	if err != nil {
		return err
	}
	n, err := runAffected(ctx, db, "delete", p.structName, sqlText, args)
	if err != nil {
		return err
	}
	if n == 0 {
		if p.version != nil {
			return ErrStaleObject
		}
		return ErrNotFound
	}
	return nil
}

// These checks reject dialects without synchronous, reliable affected-row counts.
func checkUpdateWrite(d Dialect, op, table string) error {
	if d.caps().mutations {
		return nil
	}
	return unsupportedf(
		"rio: %s is not supported on %s (no synchronous UPDATE with an affected-row count); "+
			"ClickHouse updates are asynchronous mutations — "+
			"issue one explicitly with rio.Exec(ctx, db, %q) "+
			"or model updates as inserts into a ReplacingMergeTree table",
		op,
		d.name(),
		"ALTER TABLE "+table+" UPDATE ... WHERE ...",
	)
}

func checkDeleteWrite(d Dialect, op, table string) error {
	if d.caps().mutations {
		return nil
	}
	return unsupportedf(
		"rio: %s is not supported on %s; use rio.Exec with a lightweight DELETE "+
			"(%q, ClickHouse 23.3+) or ALTER TABLE ... DELETE, both asynchronous mutations",
		op,
		d.name(),
		"DELETE FROM "+table+" WHERE ...",
	)
}

func checkRestoreWrite(d Dialect, op string) error {
	if d.caps().mutations {
		return nil
	}
	return unsupportedf(
		"rio: %s is not supported on %s (soft-delete writes are UPDATEs); "+
			"use rio.Exec with ALTER TABLE ... UPDATE",
		op,
		d.name(),
	)
}

// checkGeneratedID rejects an implicit zero generated key on dialects that
// cannot generate it. Models that use zero as data must declare noautoincr.
func checkGeneratedID(
	d Dialect,
	op string,
	p *plan,
	rv reflect.Value,
) error {
	if d.caps().autoIncrPK || p.autoIncr == nil {
		return nil
	}
	if !fieldIsZero(p.autoIncr, rv.Addr().UnsafePointer(), rv) {
		return nil
	}
	return fmt.Errorf(
		"rio: %s on %s: %s.%s is zero and %s cannot generate it (no auto-increment); "+
			"assign the ID yourself (UUID/Snowflake/etc), or tag the field "+
			"`rio:\",noautoincr\"` if zero is a real value you mean to store",
		op,
		d.name(),
		p.structName,
		p.autoIncr.name,
		d.name(),
	)
}

// updateSet resolves the explicit or default Update columns in plan order.
func updateSet(p *plan, cols []string) ([]*field, error) {
	if len(cols) == 0 {
		if len(p.updatable) == 0 {
			return nil, fmt.Errorf("rio: %s has no updatable columns", p.structName)
		}
		return p.updatable, nil // precomputed at plan time; callers only read
	}
	seen := make(map[string]bool, len(cols)+1)
	out := make([]*field, 0, len(cols)+1)
	for _, c := range cols {
		f, ok := p.byColumn[c]
		if !ok {
			return nil, fmt.Errorf("rio: Update: %s has no column %q (column names, not Go field names)", p.structName, c)
		}
		if f.isPK || f.isVersion || f.isCreated {
			return nil, fmt.Errorf("rio: Update: column %q is maintained by rio and cannot be listed", c)
		}
		if f.isSoftDelete {
			return nil, fmt.Errorf("rio: Update: column %q is the softdelete column; use Delete, Restore, or ForceDelete", c)
		}
		if seen[c] {
			continue
		}
		seen[c] = true
		out = append(out, f)
	}
	if p.updated != nil && !seen[p.updated.column] {
		out = append(out, p.updated)
	}
	// Canonical order must match the order-free SQL-cache key.
	slices.SortFunc(out, func(a, b *field) int { return a.ordinal - b.ordinal })
	return out, nil
}

// stampForInsert fills zero timestamps and a zero version before binding.
func stampForInsert(p *plan, rv reflect.Value, now time.Time) {
	base := rv.Addr().UnsafePointer()
	if p.created != nil && stampFieldIsZero(p.created, base, rv) {
		setTime(p.created, rv, now)
	}
	if p.updated != nil && stampFieldIsZero(p.updated, base, rv) {
		setTime(p.updated, rv, now)
	}
	if p.version != nil {
		fv := rv.FieldByIndex(p.version.index)
		if fv.IsZero() {
			if isUintKind(fv.Kind()) {
				fv.SetUint(1)
			} else {
				fv.SetInt(1)
			}
		}
	}
}

func stampFieldIsZero(f *field, base unsafe.Pointer, rv reflect.Value) bool {
	if f.typ == timePtrType {
		v := rv.FieldByIndex(f.index)
		return v.IsNil() || v.Elem().IsZero()
	}
	return fieldIsZero(f, base, rv)
}

func setTime(f *field, rv reflect.Value, now time.Time) {
	if f.typ == timeType {
		// Mapped time fields are value-embedded, so the offset is safe here.
		*(*time.Time)(unsafe.Add(rv.Addr().UnsafePointer(), f.offset)) = now
		return
	}
	setTimePtr(f, rv, now)
}

// setTimePtr keeps pointer-field allocation off the value-field fast path.
func setTimePtr(f *field, rv reflect.Value, now time.Time) {
	rv.FieldByIndex(f.index).Set(reflect.ValueOf(&now))
}

func bumpVersion(f *field, rv reflect.Value) {
	fv := rv.FieldByIndex(f.index)
	if isUintKind(fv.Kind()) {
		fv.SetUint(fv.Uint() + 1)
		return
	}
	fv.SetInt(fv.Int() + 1)
}

func isUintKind(k reflect.Kind) bool {
	switch k {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

func rowValue[T any](op string, row *T) (reflect.Value, error) {
	if row == nil {
		return reflect.Value{}, fmt.Errorf("rio: %s: row must not be nil", op)
	}
	return reflect.ValueOf(row).Elem(), nil
}

// insertColumns binds one row and returns generated columns and its cache key.
func insertColumns(
	p *plan,
	rv reflect.Value,
	b *binder,
) (cols, back []*field, args []any, bits uint64, cacheable bool, err error) {
	cacheable = len(p.fields) <= 64
	base := rv.Addr().UnsafePointer()
	if !p.hasOmitZero {
		// plan.insCols precomputes this partition.
		cols, bits = p.fields, p.allBits
		if p.autoIncr != nil && fieldIsZero(p.autoIncr, base, rv) {
			cols, back, bits = p.insCols, p.insBack, p.insBits
		}
		args = make([]any, 0, len(cols))
		for _, f := range cols {
			a, err := fieldValue(f, base, rv, b)
			if err != nil {
				return nil, nil, nil, 0, false, err
			}
			args = append(args, a)
		}
		return cols, back, args, bits, cacheable, nil
	}
	// cols and back partition one buffer and are restored to plan order.
	buf := make([]*field, len(p.fields))
	cols = buf[:0]
	nb := len(buf)
	args = make([]any, 0, len(p.fields))
	for i, f := range p.fields {
		if (f.isAutoIncr || f.omitZero) && fieldIsZero(f, base, rv) {
			nb--
			buf[nb] = f
			continue
		}
		a, err := fieldValue(f, base, rv, b)
		if err != nil {
			return nil, nil, nil, 0, false, err
		}
		cols = append(cols, f)
		args = append(args, a)
		if cacheable {
			bits |= 1 << uint(i)
		}
	}
	back = buf[nb:]
	slices.Reverse(back)
	return cols, back, args, bits, cacheable, nil
}

// crudSQL fetches or renders a cached entity-CRUD statement.
func crudSQL(
	g *grammar,
	p *plan,
	op string,
	bits uint64,
	cacheable bool,
	build func() []byte,
) (string, error) {
	return crudSQLKeyed(
		g,
		p,
		op,
		bits,
		0,
		upsertCacheKey{},
		cacheable,
		build,
	)
}

// crudSQLRows adds the VALUES tuple count to the cache key.
func crudSQLRows(
	g *grammar,
	p *plan,
	op string,
	bits uint64,
	rows int,
	cacheable bool,
	build func() []byte,
) (string, error) {
	return crudSQLKeyed(
		g,
		p,
		op,
		bits,
		rows,
		upsertCacheKey{},
		cacheable,
		build,
	)
}

// crudSQLKeyed is the full entity-CRUD cache lookup.
func crudSQLKeyed(
	g *grammar,
	p *plan,
	op string,
	bits uint64,
	rows int,
	spec upsertCacheKey,
	cacheable bool,
	build func() []byte,
) (string, error) {
	render := func() (string, error) {
		// build returns an owned buffer that rebindTemplate may reuse.
		s, _, err := rebindTemplate(g.d.lexer(), g.d.style(), byteString(build()))
		return s, err
	}
	if !cacheable {
		return render()
	}
	return g.cachedSQL(
		p,
		op,
		bits,
		rows,
		spec,
		render,
	)
}

func renderInsertHead(g *grammar, p *plan, cols []*field) []byte {
	d := g.d
	b := make([]byte, 0, 128)
	b = append(b, "INSERT INTO "...)
	b = d.quote(b, g.table(p))
	if len(cols) == 0 {
		return b // appendInsertValues renders the dialect's empty-row form
	}
	b = append(b, " ("...)
	for i, f := range cols {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = d.quote(b, f.column)
	}
	b = append(b, ')')
	return b
}

// appendInsertValues renders a VALUES tuple or the dialect's all-defaults form.
func appendInsertValues(b []byte, d Dialect, n int) []byte {
	if n == 0 {
		if d.name() == "mysql" {
			return append(b, " () VALUES ()"...)
		}
		return append(b, " DEFAULT VALUES"...)
	}
	b = append(b, " VALUES ("...)
	for i := range n {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, '?')
	}
	return append(b, ')')
}

// appendReturning renders an explicit column list — never * — so scans stay
// pinned to the plan even when the live table has extra columns.
func appendReturning(b []byte, d Dialect, table string, p *plan) []byte {
	b = append(b, " RETURNING "...)
	for i, f := range p.fields {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = d.quote(b, table)
		b = append(b, '.')
		b = d.quote(b, f.column)
	}
	return b
}

// scanBackCols fills the database-generated columns from a single-row
// RETURNING result.
func scanBackCols(rows rows, back []*field, base unsafe.Pointer) error {
	scanned, err := scanBackColsIfRow(rows, back, base)
	if err == nil && !scanned {
		return errors.New("rio: RETURNING produced no row")
	}
	return err
}

// scanBackColsIfRow fills generated columns when RETURNING produced a row.
// Closing errors are promoted because the single-row result is not drained.
func scanBackColsIfRow(rows rows, back []*field, base unsafe.Pointer) (scanned bool, err error) {
	defer mergeClose(rows, &err)
	if !rows.Next() {
		return false, rows.Err()
	}
	rs := newRowScanner(back, nil)
	defer rs.release()
	if err := rs.scan(rows, base); err != nil {
		return true, fmt.Errorf("rio: scanning RETURNING row: %w", err)
	}
	return true, rows.Err()
}

// scanBackRow fills the whole row from a single-row RETURNING result
// (upserts: the surviving row's values are computed database-side).
func scanBackRow(rows rows, p *plan, base unsafe.Pointer) (err error) {
	defer mergeClose(rows, &err)
	fields, err := entityFields(rows, p, 0)
	if err != nil {
		return err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return errors.New("rio: RETURNING produced no row")
	}
	rs := newRowScanner(fields, nil)
	defer rs.release()
	if err := rs.scan(rows, base); err != nil {
		return err
	}
	return rows.Err()
}

// setBits maps a field set to the SQL-cache bitmap.
func setBits(p *plan, fields []*field) (uint64, bool) {
	if len(p.fields) > 64 {
		return 0, false
	}
	var bits uint64
	for _, f := range fields {
		bits |= 1 << uint(f.ordinal)
	}
	return bits, true
}

// appendPKWhereSQL renders the WHERE pk [AND version] tail, placeholders in
// unified ? form — the render half of appendPKWhere, cacheable per grammar.
func appendPKWhereSQL(b []byte, d Dialect, p *plan) []byte {
	b = appendPKOnlyWhere(b, d, p)
	if p.version != nil {
		b = append(b, " AND "...)
		b = d.quote(b, p.version.column)
		b = append(b, " = ?"...)
	}
	return b // still in unified ? form; crudSQL rebinds the whole statement
}

// appendPKOnlyWhere renders the primary-key predicate without the version
// clause appendPKWhereSQL adds: probes ask about the row itself, not the
// caller's snapshot of it.
func appendPKOnlyWhere(b []byte, d Dialect, p *plan) []byte {
	for i, pk := range p.pks {
		if i == 0 {
			b = append(b, " WHERE "...)
		} else {
			b = append(b, " AND "...)
		}
		b = d.quote(b, pk.column)
		b = append(b, " = ?"...)
	}
	return b
}

// zeroAffectedMeansMissing distinguishes a missing MySQL row from an
// idempotent versionless update.
func zeroAffectedMeansMissing(ctx context.Context, db Queryer, p *plan, rv reflect.Value) (bool, error) {
	g := db.gram()
	if g.d.name() != "mysql" {
		return true, nil
	}
	d := g.d
	sqlText, err := crudSQL(g, p, "pkprobe", 0, true, func() []byte {
		b := make([]byte, 0, 96)
		b = append(b, "SELECT 1 FROM "...)
		b = d.quote(b, g.table(p))
		b = appendPKOnlyWhere(b, d, p)
		return appendCurrentReadProbeTail(b, d)
	})
	if err != nil {
		return false, err
	}
	bn := binder{d: d}
	args, err := appendPKOnlyArgs(nil, p, rv, &bn)
	if err != nil {
		return false, err
	}
	rows, finish, err := runQueryPhase(ctx, db, "probe", "select", p.structName, sqlText, args)
	if err != nil {
		return false, err
	}
	exists := rows.Next()
	err = rows.Err()
	mergeClose(rows, &err) // the probe row leaves the result undrained
	finishQuery(finish, err, oneIf(exists))
	return !exists, err
}

// appendVersionBump renders ", version = version + 1" when the plan has an
// optimistic-lock column; Update, Restore, and softDelete all bump it.
func appendVersionBump(b []byte, d Dialect, p *plan) []byte {
	if p.version == nil {
		return b
	}
	b = append(b, ", "...)
	b = d.quote(b, p.version.column)
	b = append(b, " = "...)
	b = d.quote(b, p.version.column)
	return append(b, " + 1"...)
}

// appendCurrentReadProbeTail locks a zero-affected probe on MySQL: under
// InnoDB REPEATABLE READ a plain SELECT reads the snapshot, and the answer
// must match the UPDATE's current-read view; autocommit releases the lock
// immediately. Other dialects read committed state without it.
func appendCurrentReadProbeTail(b []byte, d Dialect) []byte {
	if d.name() != "mysql" {
		return b
	}
	return append(b, " LIMIT 1 FOR UPDATE"...)
}

// softProbeState is the row state probeSoftState scans back: the current
// softdelete stamp and, when the model has one, the current version.
type softProbeState struct {
	found   bool
	deleted reflect.Value // *T of the softdelete field's type
	version reflect.Value // *T of the version field's type; zero Value without one
}

// trashed reports whether the probed stamp marks the row deleted: a non-nil
// pointer, or (the NULL↔zero-time exception) a non-zero time.Time.
func (st softProbeState) trashed() bool {
	v := st.deleted.Elem()
	if v.Kind() == reflect.Pointer {
		return !v.IsNil()
	}
	return !v.Interface().(time.Time).IsZero()
}

// probeCell scans f into the standalone buffer dst (a *T): colScanner writes
// to base+offset, so the field copy zeroes the struct offset.
func probeCell(f *field, dst reflect.Value) *colScanner {
	cf := *f
	cf.offset = 0
	return &colScanner{f: &cf, base: dst.UnsafePointer()}
}

// probeSoftState reads the softdelete stamp (and version) by primary key —
// primary key only, never the caller's version: the probe asks about the row,
// not the caller's snapshot of it. It serves the cold zero-affected paths of
// Delete and Restore, whose trash predicates make "already in the target
// state" a zero-matched outcome on every dialect.
func probeSoftState(ctx context.Context, db Queryer, p *plan, rv reflect.Value) (softProbeState, error) {
	g := db.gram()
	d := g.d
	sqlText, err := crudSQL(g, p, "softprobe", 0, true, func() []byte {
		b := make([]byte, 0, 128)
		b = append(b, "SELECT "...)
		b = d.quote(b, p.softDel.column)
		if p.version != nil {
			b = append(b, ", "...)
			b = d.quote(b, p.version.column)
		}
		b = append(b, " FROM "...)
		b = d.quote(b, g.table(p))
		b = appendPKOnlyWhere(b, d, p)
		return appendCurrentReadProbeTail(b, d)
	})
	if err != nil {
		return softProbeState{}, err
	}
	bn := binder{d: d}
	args, err := appendPKOnlyArgs(nil, p, rv, &bn)
	if err != nil {
		return softProbeState{}, err
	}
	rows, finish, err := runQueryPhase(ctx, db, "probe", "select", p.structName, sqlText, args)
	if err != nil {
		return softProbeState{}, err
	}
	st := softProbeState{deleted: reflect.New(p.softDel.typ)}
	cells := []any{probeCell(p.softDel, st.deleted)}
	if p.version != nil {
		st.version = reflect.New(p.version.typ)
		cells = append(cells, probeCell(p.version, st.version))
	}
	if rows.Next() {
		st.found = true
		err = rows.Scan(cells...)
	}
	if err == nil {
		err = rows.Err()
	}
	mergeClose(rows, &err)
	finishQuery(finish, err, oneIf(st.found))
	if err != nil {
		return softProbeState{}, err
	}
	return st, nil
}

// resolveSoftNoop resolves a zero-affected soft write: it adopts the stored
// state when the row is already in the wanted trash state (idempotent
// success), and otherwise reports the version conflict or the missing row.
func resolveSoftNoop(ctx context.Context, db Queryer, p *plan, rv reflect.Value, wantTrashed bool) error {
	st, err := probeSoftState(ctx, db, p, rv)
	if err != nil {
		return err
	}
	if st.found && st.trashed() == wantTrashed {
		adoptSoftState(p, rv, st)
		return nil
	}
	if p.version != nil {
		return ErrStaleObject
	}
	return ErrNotFound
}

// adoptSoftState writes the probed stamp and version into the caller's
// struct, so the idempotent paths keep the invariant that the struct holds
// exactly what the database stores.
func adoptSoftState(p *plan, rv reflect.Value, st softProbeState) {
	rv.FieldByIndex(p.softDel.index).Set(st.deleted.Elem())
	if p.version != nil {
		rv.FieldByIndex(p.version.index).Set(st.version.Elem())
	}
}

// appendKeyArgs binds the PK (+version) values matching appendPKWhereSQL.
func appendKeyArgs(args []any, p *plan, rv reflect.Value, b *binder) ([]any, error) {
	args, err := appendPKOnlyArgs(args, p, rv, b)
	if err != nil {
		return nil, err
	}
	if p.version != nil {
		a, err := fieldValue(p.version, rv.Addr().UnsafePointer(), rv, b)
		if err != nil {
			return nil, err
		}
		args = append(args, a)
	}
	return args, nil
}

// appendPKOnlyArgs binds just the primary-key values, matching
// appendPKOnlyWhere.
func appendPKOnlyArgs(args []any, p *plan, rv reflect.Value, b *binder) ([]any, error) {
	base := rv.Addr().UnsafePointer()
	for _, pk := range p.pks {
		a, err := fieldValue(pk, base, rv, b)
		if err != nil {
			return nil, err
		}
		args = append(args, a)
	}
	return args, nil
}

// bindFields extracts the bind values for a rendered field list plus the
// key/version tail.
func bindFields(p *plan, rv reflect.Value, b *binder, set []*field) ([]any, error) {
	base := rv.Addr().UnsafePointer()
	args := make([]any, 0, len(set)+len(p.pks)+1)
	for _, f := range set {
		a, err := fieldValue(f, base, rv, b)
		if err != nil {
			return nil, err
		}
		args = append(args, a)
	}
	return appendKeyArgs(args, p, rv, b)
}

func fillLastInsertID(p *plan, rv reflect.Value, lastID func() (int64, error)) error {
	if p.autoIncr == nil || !rv.FieldByIndex(p.autoIncr.index).IsZero() {
		return nil
	}
	id, err := lastID()
	if err != nil || id == 0 {
		return nil // driver cannot report it; the insert itself succeeded
	}
	fv := rv.FieldByIndex(p.autoIncr.index)
	if isUintKind(fv.Kind()) {
		fv.SetUint(uint64(id))
	} else {
		fv.SetInt(id)
	}
	return nil
}

func clearTime(f *field, rv reflect.Value) {
	if f.typ == timeType {
		*(*time.Time)(unsafe.Add(rv.Addr().UnsafePointer(), f.offset)) = time.Time{}
		return
	}
	rv.FieldByIndex(f.index).SetZero()
}
