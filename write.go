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

// Insert writes one row and fills in what the database generated: the
// columns the INSERT skipped — the zero auto-increment PK and zero omitzero
// columns — via RETURNING on PostgreSQL/SQLite, the auto-increment ID on
// MySQL. Never a hidden second SELECT; columns rewritten by triggers are not
// read back. Zero values are written as-is; columns tagged omitzero are
// skipped when zero so DB defaults apply, and a zero auto-increment PK is
// skipped implicitly. CreatedAt/UpdatedAt are set to the clock when zero; a
// zero version column starts at 1. Time fields are normalized in place as
// they bind (UTC, monotonic reading stripped, microsecond precision), so
// after Insert the struct holds exactly what the database stores and an
// insert-then-reload comparison stays Equal. Stamping happens before
// execution: after a failed Insert the struct may already carry this
// attempt's timestamps and version=1 while the database is untouched —
// retrying with the same struct is safe.
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
		// Everything omitted (all omitzero columns zero on a model without a
		// generated PK): the other dialects render their all-defaults row
		// form, but ClickHouse has none.
		return fmt.Errorf("rio: clickhouse has no DEFAULT VALUES statement; set at least one column on %s", p.structName)
	}
	returning := d.caps().returning && len(back) > 0
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
		finishQuery(finish, err)
		return err
	}
	res, err := run(ctx, db, "insert", p.structName, sqlText, args)
	if err != nil {
		return err
	}
	return fillLastInsertID(p, rv, res.LastInsertId)
}

// Update writes a row by primary key. With no column list it writes every
// column — honestly, zero values included (partial scans beware: unscanned
// fields overwrite with zeros). With a list it updates exactly those columns
// plus UpdatedAt. A version column is checked and incremented atomically;
// losing the race returns ErrStaleObject. UpdatedAt is stamped before
// execution: after a failed Update the struct may already carry this
// attempt's stamp while the database is untouched — retrying with the same
// struct is safe (the stamp is reset to the clock on every call anyway).
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
		if p.version != nil {
			b = append(b, ", "...)
			b = d.quote(b, p.version.column)
			b = append(b, " = "...)
			b = d.quote(b, p.version.column)
			b = append(b, " + 1"...)
		}
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
	res, err := run(ctx, db, "update", p.structName, sqlText, args)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
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

// ForceDelete removes the row with a real DELETE even when the model soft
// deletes.
func ForceDelete[T any](ctx context.Context, db Queryer, row *T) error {
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	return hardDelete(ctx, db, p, row)
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
		if p.version != nil {
			b = append(b, ", "...)
			b = d.quote(b, p.version.column)
			b = append(b, " = "...)
			b = d.quote(b, p.version.column)
			b = append(b, " + 1"...)
		}
		return appendPKWhereSQL(b, d, p)
	})
	if err != nil {
		return err
	}
	bn := binder{d: d, now: now}
	args := []any{bn.time(now)}
	if p.updated != nil {
		args = append(args, bn.time(now))
	}
	if args, err = appendKeyArgs(args, p, rv, &bn); err != nil {
		return err
	}
	res, err := run(ctx, db, "delete", p.structName, sqlText, args)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
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
		// Matched but unchanged (a same-instant double delete): the stamps
		// below still describe the row.
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
	res, err := run(ctx, db, "delete", p.structName, sqlText, args)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
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

// --- shared helpers ---

// checkUpdateWrite, checkDeleteWrite, and checkRestoreWrite reject the
// UPDATE/DELETE statement family where the dialect has no synchronous
// mutations with an honest affected-row count — rio's ErrNotFound,
// ErrStaleObject, and idempotence probes are all built on that count, and
// clickhouse-go reports 0 unconditionally. Each message names the
// ClickHouse-native way out for its operation family.
func checkUpdateWrite(d Dialect, op, table string) error {
	if d.caps().mutations {
		return nil
	}
	return unsupportedf("rio: %s is not supported on %s (no synchronous UPDATE with an affected-row count); ClickHouse updates are asynchronous mutations — issue one explicitly with rio.Exec(ctx, db, %q) or model updates as inserts into a ReplacingMergeTree table",
		op, d.name(), "ALTER TABLE "+table+" UPDATE ... WHERE ...")
}

func checkDeleteWrite(d Dialect, op, table string) error {
	if d.caps().mutations {
		return nil
	}
	return unsupportedf("rio: %s is not supported on %s; use rio.Exec with a lightweight DELETE (%q, ClickHouse 23.3+) or ALTER TABLE ... DELETE, both asynchronous mutations",
		op, d.name(), "DELETE FROM "+table+" WHERE ...")
}

func checkRestoreWrite(d Dialect, op string) error {
	if d.caps().mutations {
		return nil
	}
	return unsupportedf("rio: %s is not supported on %s (soft-delete writes are UPDATEs); use rio.Exec with ALTER TABLE ... UPDATE", op, d.name())
}

// checkGeneratedID upgrades the zero conventional ID from "let the database
// generate it" to a loud error where the database cannot (autoIncrPK=false):
// writing the zero as a real value instead would silently accumulate id=0
// duplicates with no constraint to object. The `rio:",noautoincr"` tag is
// the existing escape hatch when zero is a real value, with identical
// semantics on every dialect (written as-is, never backfilled). Checked
// before any stamping so a rejected row is returned untouched.
func checkGeneratedID(d Dialect, op string, p *plan, rv reflect.Value) error {
	if d.caps().autoIncrPK || p.autoIncr == nil {
		return nil
	}
	if !fieldIsZero(p.autoIncr, rv.Addr().UnsafePointer(), rv) {
		return nil
	}
	return fmt.Errorf("rio: %s on %s: %s.%s is zero and %s cannot generate it (no auto-increment); assign the ID yourself (UUID/Snowflake/etc), or tag the field `rio:\",noautoincr\"` if zero is a real value you mean to store",
		op, d.name(), p.structName, p.autoIncr.name, d.name())
}

// updateSet resolves Update's column set. No list: every column except PKs,
// CreatedAt, the version column (rendered separately as version+1), and the
// softdelete column (owned by Delete/Restore/ForceDelete) — including zero
// values, honestly. With a list: exactly those columns by database name,
// plus UpdatedAt.
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
	// Canonical order, always. The SQL cache keys on an order-free column
	// bitmap: if rendering followed caller order, Update("a","b") and a later
	// Update("b","a") would share one cached statement while each binds values
	// in its own order — silently writing values into the wrong columns.
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
		// Same offset discipline as the scan fast path: mapped time fields
		// are value-embedded, so a direct store skips reflect.ValueOf's
		// interface boxing — the single largest allocation on Insert/Update.
		*(*time.Time)(unsafe.Add(rv.Addr().UnsafePointer(), f.offset)) = now
		return
	}
	setTimePtr(f, rv, now)
}

// setTimePtr sets a *time.Time field. Split out of setTime so &now's escape
// stays off the value-field fast path: escape analysis is per parameter, not
// per branch, and would otherwise heap-allocate now on every setTime call.
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

// insertColumns picks the columns for one row's INSERT and binds their
// values. back collects the columns the database generates on this insert —
// the skipped auto-increment PK and skipped omitzero columns — which is all
// a RETURNING clause needs: every other value is already in the struct.
// bits is the participating-column bitmap used as the SQL-cache key;
// cacheable is false past 64 columns (render directly, no cache).
func insertColumns(p *plan, rv reflect.Value, b *binder) (cols, back []*field, args []any, bits uint64, cacheable bool, err error) {
	cacheable = len(p.fields) <= 64
	base := rv.Addr().UnsafePointer()
	if !p.hasOmitZero {
		// The partition is one of two plan-time shapes (see plan.insCols):
		// binding is the only per-row work left.
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
	// cols and back partition p.fields, so one buffer serves both: cols
	// grows from the front while back fills the tail in reverse, restored to
	// plan order below.
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
func crudSQL(g *grammar, p *plan, op string, bits uint64, cacheable bool, build func() []byte) (string, error) {
	return crudSQLKeyed(g, p, op, bits, 0, "", cacheable, build)
}

// crudSQLRows is crudSQL with a VALUES tuple count in the cache key, for
// batch statements whose shape repeats at a fixed chunk size.
func crudSQLRows(g *grammar, p *plan, op string, bits uint64, rows int, cacheable bool, build func() []byte) (string, error) {
	return crudSQLKeyed(g, p, op, bits, rows, "", cacheable, build)
}

// crudSQLKeyed is the full-key form; spec carries the normalized upsert
// conflict shape (upsertSQL) and stays "" for every other statement.
func crudSQLKeyed(g *grammar, p *plan, op string, bits uint64, rows int, spec string, cacheable bool, build func() []byte) (string, error) {
	render := func() (string, error) {
		// build returns a fresh function-local buffer, so handing it to
		// rebindTemplate uncopied follows finishSQL's ownership rule (the
		// rebind may return it re-interpreted as the final string).
		s, _, err := rebindTemplate(g.d.lexer(), g.d.style(), byteString(build()))
		return s, err
	}
	if !cacheable {
		return render()
	}
	return g.cachedSQL(p, op, bits, rows, spec, render)
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

// appendInsertValues renders one row's VALUES tuple. When every column was
// skipped (auto-increment PK plus omitzero columns, all zero) the row is
// all-defaults: PostgreSQL/SQLite reject "() VALUES ()" and need
// DEFAULT VALUES; MySQL only accepts the former.
func appendInsertValues(b []byte, d Dialect, n int) []byte {
	if n == 0 {
		if d.name() == "mysql" {
			return append(b, " () VALUES ()"...)
		}
		return append(b, " DEFAULT VALUES"...)
	}
	b = append(b, " VALUES ("...)
	for i := 0; i < n; i++ {
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

// scanBackColsIfRow fills generated columns when RETURNING yielded a row and
// reports scanned=false when it did not (a DoNothing upsert hitting a
// conflict), leaving the struct as given. The column list is rio's own
// render, so no pre-Scan shape check: database/sql's Scan reports a count
// mismatch just as loudly, without the extra Columns() copy per insert.
// mergeClose, not a bare deferred Close: the single row leaves the result
// undrained, so Close is where the driver reports whether the write actually
// completed — dropped, a failed INSERT would return nil with a stale ID.
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
	for i, pk := range p.pks {
		if i == 0 {
			b = append(b, " WHERE "...)
		} else {
			b = append(b, " AND "...)
		}
		b = d.quote(b, pk.column)
		b = append(b, " = ?"...)
	}
	if p.version != nil {
		b = append(b, " AND "...)
		b = d.quote(b, p.version.column)
		b = append(b, " = ?"...)
	}
	return b // still in unified ? form; crudSQL rebinds the whole statement
}

// zeroAffectedMeansMissing resolves the n==0 ambiguity for versionless
// UPDATE-shaped writes. PostgreSQL and SQLite count matched rows, so zero
// means the row is gone. MySQL normally counts changed rows, so an idempotent
// UPDATE can also report 0; one extra primary-key probe, only on this
// ambiguous path, keeps the ErrNotFound contract identical on all three
// dialects. If the connection uses CLIENT_FOUND_ROWS, matched idempotent
// updates report nonzero and simply skip this probe.
//
// The probe is a locking read (FOR UPDATE): under InnoDB REPEATABLE READ a
// plain SELECT reads the transaction's snapshot while the UPDATE is a
// current read, so a row deleted and committed by another transaction would
// look present to the probe and turn the lost write into silent success. The
// current-read probe sees what the UPDATE saw. Outside a transaction the two
// statements still commit independently — a delete landing between them
// reports ErrNotFound, which is the race-honest answer (same stance as
// FirstOrCreate).
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
		b = appendPKWhereSQL(b, d, p) // version is nil on this path: PKs only
		// Single-row PK lock, MySQL-only path (see the early return above):
		// autocommit releases it immediately, and MySQL 5.7/8.x and MariaDB
		// all support the clause.
		return append(b, " LIMIT 1 FOR UPDATE"...)
	})
	if err != nil {
		return false, err
	}
	bn := binder{d: d}
	args, err := appendKeyArgs(nil, p, rv, &bn)
	if err != nil {
		return false, err
	}
	rows, finish, err := runQuery(ctx, db, "select", p.structName, sqlText, args)
	if err != nil {
		return false, err
	}
	exists := rows.Next()
	err = rows.Err()
	mergeClose(rows, &err) // the probe row leaves the result undrained
	finishQuery(finish, err)
	return !exists, err
}

// appendKeyArgs binds the PK (+version) values matching appendPKWhereSQL.
func appendKeyArgs(args []any, p *plan, rv reflect.Value, b *binder) ([]any, error) {
	base := rv.Addr().UnsafePointer()
	for _, pk := range p.pks {
		a, err := fieldValue(pk, base, rv, b)
		if err != nil {
			return nil, err
		}
		args = append(args, a)
	}
	if p.version != nil {
		a, err := fieldValue(p.version, base, rv, b)
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
		if p.version != nil {
			b = append(b, ", "...)
			b = d.quote(b, p.version.column)
			b = append(b, " = "...)
			b = d.quote(b, p.version.column)
			b = append(b, " + 1"...)
		}
		return appendPKWhereSQL(b, d, p)
	})
	if err != nil {
		return err
	}
	bn := binder{d: d, now: now}
	var args []any
	if p.updated != nil {
		args = append(args, bn.time(now))
	}
	if args, err = appendKeyArgs(args, p, rv, &bn); err != nil {
		return err
	}
	res, err := run(ctx, db, "update", p.structName, sqlText, args)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
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
		// Matched but unchanged: restoring a live row is idempotent.
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

func clearTime(f *field, rv reflect.Value) {
	if f.typ == timeType {
		*(*time.Time)(unsafe.Add(rv.Addr().UnsafePointer(), f.offset)) = time.Time{}
		return
	}
	rv.FieldByIndex(f.index).SetZero()
}
