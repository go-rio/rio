package rio

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"
	"unsafe"
)

// Insert writes one row and fills in what the database generated: the whole
// row via RETURNING on PostgreSQL/SQLite, the auto-increment ID on MySQL —
// never a hidden second SELECT. Zero values are written as-is; columns
// tagged omitzero are skipped when zero so DB defaults apply, and a zero
// auto-increment PK is skipped implicitly. CreatedAt/UpdatedAt are set to
// the clock when zero; a zero version column starts at 1.
func Insert[T any](ctx context.Context, db Queryer, row *T) error {
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	rv := reflect.ValueOf(row).Elem()
	g := db.gram()
	d := g.d
	now := normalizeTime(db.conf().clock())
	stampForInsert(p, rv, now)

	cols, back, args, bits, cacheable, err := insertColumns(p, rv, d, now)
	if err != nil {
		return err
	}
	returning := d.caps().returning && len(back) > 0
	build := func() []byte {
		b := renderInsertHead(g, p, cols)
		b = append(b, " VALUES ("...)
		for i := range cols {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = append(b, '?')
		}
		b = append(b, ')')
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
		rows, err := runQuery(ctx, db, "insert", p.structName, sqlText, args)
		if err != nil {
			return err
		}
		return scanBackCols(rows, back, unsafe.Pointer(row))
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
// losing the race returns ErrStaleObject.
func Update[T any](ctx context.Context, db Queryer, row *T, cols ...string) error {
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	if len(p.pks) == 0 {
		return fmt.Errorf("%w: %s", ErrNoPrimaryKey, p.structName)
	}
	rv := reflect.ValueOf(row).Elem()
	g := db.gram()
	d := g.d
	now := normalizeTime(db.conf().clock())

	set, err := updateSet(p, cols)
	if err != nil {
		return err
	}
	if p.updated != nil {
		setTime(p.updated, rv, now)
	}

	table := g.table(p)
	b := make([]byte, 0, 160)
	b = append(b, "UPDATE "...)
	b = d.quote(b, table)
	b = append(b, " SET "...)
	var args []any
	for i, f := range set {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = d.quote(b, f.column)
		b = append(b, " = ?"...)
		a, err := bindArg(f, rv.FieldByIndex(f.index), d, now)
		if err != nil {
			return err
		}
		args = append(args, a)
	}
	if p.version != nil {
		b = append(b, ", "...)
		b = d.quote(b, p.version.column)
		b = append(b, " = "...)
		b = d.quote(b, p.version.column)
		b = append(b, " + 1"...)
	}

	b, args, err = appendPKWhere(b, args, d, p, rv, now)
	if err != nil {
		return err
	}

	sqlText, outArgs, err := finishSQL(d, b, args)
	if err != nil {
		return err
	}
	res, err := run(ctx, db, "update", p.structName, sqlText, outArgs)
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
	rv := reflect.ValueOf(row).Elem()
	if len(p.pks) == 0 {
		return fmt.Errorf("%w: %s", ErrNoPrimaryKey, p.structName)
	}
	g := db.gram()
	d := g.d
	now := normalizeTime(db.conf().clock())

	b := make([]byte, 0, 128)
	b = append(b, "UPDATE "...)
	b = d.quote(b, g.table(p))
	b = append(b, " SET "...)
	b = d.quote(b, p.softDel.column)
	b = append(b, " = ?"...)
	args := []any{d.bindTime(now)}
	if p.updated != nil {
		b = append(b, ", "...)
		b = d.quote(b, p.updated.column)
		b = append(b, " = ?"...)
		args = append(args, d.bindTime(now))
	}
	if p.version != nil {
		b = append(b, ", "...)
		b = d.quote(b, p.version.column)
		b = append(b, " = "...)
		b = d.quote(b, p.version.column)
		b = append(b, " + 1"...)
	}
	var err error
	b, args, err = appendPKWhere(b, args, d, p, rv, now)
	if err != nil {
		return err
	}
	sqlText, outArgs, err := finishSQL(d, b, args)
	if err != nil {
		return err
	}
	res, err := run(ctx, db, "delete", p.structName, sqlText, outArgs)
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
	rv := reflect.ValueOf(row).Elem()
	if len(p.pks) == 0 {
		return fmt.Errorf("%w: %s", ErrNoPrimaryKey, p.structName)
	}
	g := db.gram()
	d := g.d
	now := normalizeTime(db.conf().clock())

	b := make([]byte, 0, 96)
	b = append(b, "DELETE FROM "...)
	b = d.quote(b, g.table(p))
	var args []any
	var err error
	b, args, err = appendPKWhere(b, args, d, p, rv, now)
	if err != nil {
		return err
	}
	sqlText, outArgs, err := finishSQL(d, b, args)
	if err != nil {
		return err
	}
	res, err := run(ctx, db, "delete", p.structName, sqlText, outArgs)
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

// updateSet resolves Update's column set. No list: every column except PKs,
// CreatedAt, and the version column (rendered separately as version+1) —
// including zero values, honestly. With a list: exactly those columns by
// database name, plus UpdatedAt.
func updateSet(p *plan, cols []string) ([]*field, error) {
	if len(cols) == 0 {
		out := make([]*field, 0, len(p.fields))
		for _, f := range p.fields {
			if f.isPK || f.isCreated || f.isVersion {
				continue
			}
			out = append(out, f)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("rio: %s has no updatable columns", p.structName)
		}
		return out, nil
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
		if seen[c] {
			continue
		}
		seen[c] = true
		out = append(out, f)
	}
	if p.updated != nil && !seen[p.updated.column] {
		out = append(out, p.updated)
	}
	return out, nil
}

// stampForInsert fills zero timestamps and a zero version before binding.
func stampForInsert(p *plan, rv reflect.Value, now time.Time) {
	if p.created != nil && rv.FieldByIndex(p.created.index).IsZero() {
		setTime(p.created, rv, now)
	}
	if p.updated != nil && rv.FieldByIndex(p.updated.index).IsZero() {
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

func setTime(f *field, rv reflect.Value, now time.Time) {
	fv := rv.FieldByIndex(f.index)
	if f.typ == timePtrType {
		fv.Set(reflect.ValueOf(&now))
		return
	}
	fv.Set(reflect.ValueOf(now))
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

// insertColumns picks the columns for one row's INSERT and binds their
// values. back collects the columns the database generates on this insert —
// the skipped auto-increment PK and skipped omitzero columns — which is all
// a RETURNING clause needs: every other value is already in the struct.
// bits is the participating-column bitmap used as the SQL-cache key;
// cacheable is false past 64 columns (render directly, no cache).
func insertColumns(p *plan, rv reflect.Value, d Dialect, now time.Time) (cols, back []*field, args []any, bits uint64, cacheable bool, err error) {
	cols = make([]*field, 0, len(p.fields))
	args = make([]any, 0, len(p.fields))
	cacheable = len(p.fields) <= 64
	base := rv.Addr().UnsafePointer()
	for i, f := range p.fields {
		if (f.isAutoIncr || f.omitZero) && fieldIsZero(f, base, rv) {
			back = append(back, f)
			continue
		}
		a, err := fieldValue(f, base, rv, d, now)
		if err != nil {
			return nil, nil, nil, 0, false, err
		}
		cols = append(cols, f)
		args = append(args, a)
		if cacheable {
			bits |= 1 << uint(i)
		}
	}
	return cols, back, args, bits, cacheable, nil
}

// crudSQL fetches or renders a cached entity-CRUD statement.
func crudSQL(g *grammar, p *plan, op string, bits uint64, cacheable bool, build func() []byte) (string, error) {
	render := func() (string, error) {
		s, _, err := rebindTemplate(g.d.lexer(), g.d.style(), string(build()))
		return s, err
	}
	if !cacheable {
		return render()
	}
	return g.cachedSQL(p, op, bits, render)
}

func renderInsertHead(g *grammar, p *plan, cols []*field) []byte {
	d := g.d
	b := make([]byte, 0, 128)
	b = append(b, "INSERT INTO "...)
	b = d.quote(b, g.table(p))
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
func scanBackCols(rows *sql.Rows, back []*field, base unsafe.Pointer) error {
	scanned, err := scanBackColsIfRow(rows, back, base)
	if err == nil && !scanned {
		return errors.New("rio: RETURNING produced no row")
	}
	return err
}

// scanBackColsIfRow fills generated columns when RETURNING yielded a row and
// reports scanned=false when it did not (a DoNothing upsert hitting a
// conflict), leaving the struct as given.
func scanBackColsIfRow(rows *sql.Rows, back []*field, base unsafe.Pointer) (scanned bool, err error) {
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return false, err
	}
	if len(cols) != len(back) {
		return false, fmt.Errorf("rio: RETURNING yielded %d columns, expected %d", len(cols), len(back))
	}
	if !rows.Next() {
		return false, rows.Err()
	}
	rs := newRowScanner(back, nil)
	if err := rs.scan(rows, base); err != nil {
		return true, err
	}
	return true, rows.Err()
}

// scanBackRow fills the whole row from a single-row RETURNING result
// (upserts: the surviving row's values are computed database-side).
func scanBackRow(rows *sql.Rows, p *plan, base unsafe.Pointer) error {
	defer rows.Close()
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
	if err := rs.scan(rows, base); err != nil {
		return err
	}
	return rows.Err()
}

// appendPKWhere renders WHERE pk1 = ? AND ... [AND version = ?].
func appendPKWhere(b []byte, args []any, d Dialect, p *plan, rv reflect.Value, now time.Time) ([]byte, []any, error) {
	base := rv.Addr().UnsafePointer()
	for i, pk := range p.pks {
		if i == 0 {
			b = append(b, " WHERE "...)
		} else {
			b = append(b, " AND "...)
		}
		b = d.quote(b, pk.column)
		b = append(b, " = ?"...)
		a, err := fieldValue(pk, base, rv, d, now)
		if err != nil {
			return nil, nil, err
		}
		args = append(args, a)
	}
	if p.version != nil {
		b = append(b, " AND "...)
		b = d.quote(b, p.version.column)
		b = append(b, " = ?"...)
		a, err := fieldValue(p.version, base, rv, d, now)
		if err != nil {
			return nil, nil, err
		}
		args = append(args, a)
	}
	return b, args, nil
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
	rv := reflect.ValueOf(row).Elem()
	g := db.gram()
	d := g.d
	now := normalizeTime(db.conf().clock())

	b := make([]byte, 0, 128)
	b = append(b, "UPDATE "...)
	b = d.quote(b, g.table(p))
	b = append(b, " SET "...)
	b = d.quote(b, p.softDel.column)
	b = append(b, " = NULL"...)
	var args []any
	if p.updated != nil {
		b = append(b, ", "...)
		b = d.quote(b, p.updated.column)
		b = append(b, " = ?"...)
		args = append(args, d.bindTime(now))
	}
	if p.version != nil {
		b = append(b, ", "...)
		b = d.quote(b, p.version.column)
		b = append(b, " = "...)
		b = d.quote(b, p.version.column)
		b = append(b, " + 1"...)
	}
	b, args, err = appendPKWhere(b, args, d, p, rv, now)
	if err != nil {
		return err
	}
	sqlText, outArgs, err := finishSQL(d, b, args)
	if err != nil {
		return err
	}
	res, err := run(ctx, db, "update", p.structName, sqlText, outArgs)
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
	rv.FieldByIndex(f.index).SetZero()
}
