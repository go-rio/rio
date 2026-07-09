package rio

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"unsafe"
)

// UpsertOption shapes the conflict clause.
type UpsertOption func(*upsertSpec)

type upsertSpec struct {
	conflict    []string
	update      []string
	doNothing   bool
	keepTrashed bool
}

// OnConflict names the conflict target columns (the unique index). Required
// with DoUpdate on PostgreSQL/SQLite; MySQL has no conflict target — its
// ON DUPLICATE KEY reacts to any unique index, which is a documented
// semantic difference, not something rio papers over.
func OnConflict(cols ...string) UpsertOption {
	return func(s *upsertSpec) { s.conflict = append(s.conflict, cols...) }
}

// DoUpdate lists the columns to overwrite on conflict. With no columns,
// every non-PK, non-CreatedAt, non-conflict-target column updates.
func DoUpdate(cols ...string) UpsertOption {
	return func(s *upsertSpec) { s.update = append(s.update, cols...) }
}

// DoNothing turns conflicts into no-ops. On MySQL this renders as a no-op
// assignment rather than INSERT IGNORE, which would swallow unrelated
// errors. A conflicting soft-deleted row stays deleted.
func DoNothing() UpsertOption {
	return func(s *upsertSpec) { s.doNothing = true }
}

// KeepTrashed opts out of the restore-on-upsert invariant: with it, an
// upsert hitting a soft-deleted row updates the tombstone without clearing
// deleted_at — and the row stays invisible to default queries.
func KeepTrashed() UpsertOption {
	return func(s *upsertSpec) { s.keepTrashed = true }
}

// Upsert inserts the row or updates it on unique-key conflict, in one
// statement. All four elements ship together: conflict target, update
// whitelist, RETURNING backfill (PG/SQLite; MySQL fills the auto-increment
// ID only on the insert path), and timestamp maintenance.
//
// Soft-delete invariant: a successful DoUpdate upsert leaves the row visible
// — deleted_at is cleared unless KeepTrashed is given. The explicit
// softdelete tag opted the model into deletion semantics; resurrect-on-upsert
// is its consistent extension (the alternative is Eloquent's famous trap:
// "upsert succeeded but the row is invisible").
func Upsert[T any](ctx context.Context, db Queryer, row *T, opts ...UpsertOption) error {
	var spec upsertSpec
	for _, opt := range opts {
		opt(&spec)
	}
	if spec.doNothing && len(spec.update) > 0 {
		return errors.New("rio: Upsert cannot combine DoNothing with DoUpdate")
	}
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	g := db.gram()
	d := g.d
	// MySQL has no conflict target — ON DUPLICATE KEY reacts to any unique
	// index — so OnConflict is only required where the SQL needs it.
	if !spec.doNothing && len(spec.conflict) == 0 && d.caps().conflictTarget {
		return errors.New("rio: Upsert with DoUpdate needs OnConflict(columns...) naming the unique index")
	}

	rv := reflect.ValueOf(row).Elem()
	now := normalizeTime(db.conf().clock())
	stampForInsert(p, rv, now)
	cols, back, args, _, _, err := insertColumns(p, rv, d, now)
	if err != nil {
		return err
	}

	update, err := upsertUpdateSet(p, &spec)
	if err != nil {
		return err
	}

	b := renderInsertHead(g, p, cols)
	b = append(b, " VALUES ("...)
	for i := range cols {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, '?')
	}
	b = append(b, ')')

	table := g.table(p)
	if d.caps().conflictTarget {
		b = appendConflictClause(b, d, &spec)
		if spec.doNothing {
			b = append(b, "DO NOTHING"...)
		} else {
			b = append(b, "DO UPDATE SET "...)
			b = appendConflictSets(b, d, table, p, update, &spec, "excluded")
		}
		if d.caps().returning && !spec.doNothing {
			b = appendReturning(b, d, table, p)
			sqlText, outArgs, err := finishSQL(d, b, args)
			if err != nil {
				return err
			}
			rows, finish, err := runQuery(ctx, db, "upsert", p.structName, sqlText, outArgs)
			if err != nil {
				return err
			}
			err = scanBackRow(rows, p, unsafe.Pointer(row))
			finishQuery(finish, err)
			return err
		}
		if d.caps().returning && spec.doNothing && len(back) > 0 {
			// A fresh insert still reports its generated columns; on
			// conflict RETURNING yields no row and the struct stays as
			// given — matching MySQL's insert-path-only backfill.
			b = append(b, " RETURNING "...)
			for i, f := range back {
				if i > 0 {
					b = append(b, ", "...)
				}
				b = d.quote(b, f.column)
			}
			sqlText, outArgs, err := finishSQL(d, b, args)
			if err != nil {
				return err
			}
			rows, finish, err := runQuery(ctx, db, "upsert", p.structName, sqlText, outArgs)
			if err != nil {
				return err
			}
			_, err = scanBackColsIfRow(rows, back, unsafe.Pointer(row))
			finishQuery(finish, err)
			return err
		}
		sqlText, outArgs, err := finishSQL(d, b, args)
		if err != nil {
			return err
		}
		_, err = run(ctx, db, "upsert", p.structName, sqlText, outArgs)
		return err
	}

	// MySQL: ON DUPLICATE KEY UPDATE. VALUES(col) refers to the would-be
	// inserted value; a bare column name refers to the existing row.
	b = append(b, " ON DUPLICATE KEY UPDATE "...)
	if spec.doNothing {
		// A no-op assignment needs some column; models without a PK still
		// have at least one mapped field.
		col := p.fields[0].column
		if len(p.pks) > 0 {
			col = p.pks[0].column
		}
		b = d.quote(b, col)
		b = append(b, " = "...)
		b = d.quote(b, col)
	} else {
		b = appendConflictSets(b, d, table, p, update, &spec, "")
	}
	sqlText, outArgs, err := finishSQL(d, b, args)
	if err != nil {
		return err
	}
	res, err := run(ctx, db, "upsert", p.structName, sqlText, outArgs)
	if err != nil {
		return err
	}
	// MySQL reports 1 row affected for a fresh insert, 2 for an update.
	if n, aerr := res.RowsAffected(); aerr == nil && n == 1 {
		return fillLastInsertID(p, rv, res.LastInsertId)
	}
	return nil
}

// upsertUpdateSet resolves the DoUpdate whitelist (or derives the default:
// everything except PKs, CreatedAt, the version column, the softdelete
// column, and the conflict target itself). An empty resolved set with no
// maintained columns to render either would emit "DO UPDATE SET" with no
// assignments — invalid SQL on every dialect — so it errors with the fix.
func upsertUpdateSet(p *plan, spec *upsertSpec) ([]*field, error) {
	if spec.doNothing {
		return nil, nil
	}
	if len(spec.update) > 0 {
		out := make([]*field, 0, len(spec.update))
		for _, c := range spec.update {
			f, ok := p.byColumn[c]
			if !ok {
				return nil, fmt.Errorf("rio: DoUpdate: %s has no column %q", p.structName, c)
			}
			if f.isPK || f.isVersion || f.isSoftDelete || f.isCreated || f.isUpdated {
				return nil, fmt.Errorf("rio: DoUpdate: column %q is maintained by rio and cannot be listed", c)
			}
			out = append(out, f)
		}
		return out, nil
	}
	inTarget := make(map[string]bool, len(spec.conflict))
	for _, c := range spec.conflict {
		inTarget[c] = true
	}
	var out []*field
	for _, f := range p.fields {
		if f.isPK || f.isVersion || f.isSoftDelete || f.isCreated || f.isUpdated || f.isAutoIncr || inTarget[f.column] {
			continue
		}
		out = append(out, f)
	}
	if len(out) == 0 && p.updated == nil && p.version == nil && (p.softDel == nil || spec.keepTrashed) {
		// Nothing would render after DO UPDATE SET / ON DUPLICATE KEY UPDATE
		// — invalid SQL on every dialect (lookup tables, join tables).
		return nil, fmt.Errorf("rio: upsert on %s has nothing to update on conflict (every column is a key or rio-maintained); use DoNothing()", p.structName)
	}
	return out, nil
}

// appendConflictClause renders "ON CONFLICT (cols) " — or the bare
// "ON CONFLICT " when DoNothing has no target, since "ON CONFLICT ()" is a
// syntax error on PostgreSQL and SQLite.
func appendConflictClause(b []byte, d Dialect, spec *upsertSpec) []byte {
	b = append(b, " ON CONFLICT"...)
	if len(spec.conflict) == 0 {
		return append(b, ' ')
	}
	b = append(b, " ("...)
	for i, c := range spec.conflict {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = d.quote(b, c)
	}
	return append(b, ") "...)
}

// appendConflictSets renders the DO UPDATE SET list. newRow is "excluded"
// for PG/SQLite; empty selects MySQL's VALUES(col) form.
func appendConflictSets(b []byte, d Dialect, table string, p *plan, update []*field, spec *upsertSpec, newRow string) []byte {
	first := true
	sep := func() {
		if !first {
			b = append(b, ", "...)
		}
		first = false
	}
	newVal := func(col string) {
		if newRow != "" {
			b = append(b, newRow...)
			b = append(b, '.')
			b = d.quote(b, col)
		} else {
			b = append(b, "VALUES("...)
			b = d.quote(b, col)
			b = append(b, ')')
		}
	}
	for _, f := range update {
		sep()
		b = d.quote(b, f.column)
		b = append(b, " = "...)
		newVal(f.column)
	}
	if p.updated != nil {
		sep()
		b = d.quote(b, p.updated.column)
		b = append(b, " = "...)
		newVal(p.updated.column)
	}
	if p.version != nil {
		// The surviving row's version keeps counting: old version + 1.
		sep()
		b = d.quote(b, p.version.column)
		b = append(b, " = "...)
		if newRow != "" {
			b = d.quote(b, table)
			b = append(b, '.')
		}
		b = d.quote(b, p.version.column)
		b = append(b, " + 1"...)
	}
	if p.softDel != nil && !spec.keepTrashed {
		// Restore-on-upsert: a successful upsert leaves the row visible.
		sep()
		b = d.quote(b, p.softDel.column)
		b = append(b, " = NULL"...)
	}
	return b
}

// FirstOrCreate returns the first row matching the query, inserting row when
// none exists. SELECT-then-INSERT races: a concurrent creator makes the
// INSERT hit ErrDuplicateKey, and FirstOrCreate then re-reads. When even the
// re-read misses, a soft-deleted row is probably squatting on the unique key
// (WithTrashed reveals it) and the duplicate-key error is returned as-is.
func (q Query[T]) FirstOrCreate(ctx context.Context, db Queryer, row *T) error {
	found, err := q.First(ctx, db)
	if err == nil {
		*row = *found
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	insErr := Insert(ctx, db, row)
	if insErr == nil {
		return nil
	}
	if !errors.Is(insErr, ErrDuplicateKey) {
		return insErr
	}
	found, err = q.First(ctx, db)
	if err == nil {
		*row = *found
		return nil
	}
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w (a soft-deleted row may hold the unique key; query WithTrashed to see it)", insErr)
	}
	return err
}

// CreateOrFirst inserts row, and on unique-key conflict returns the existing
// row instead — the race-honest inverse of FirstOrCreate (INSERT first, so
// the unique constraint arbitrates instead of a racy SELECT).
func (q Query[T]) CreateOrFirst(ctx context.Context, db Queryer, row *T) error {
	insErr := Insert(ctx, db, row)
	if insErr == nil {
		return nil
	}
	if !errors.Is(insErr, ErrDuplicateKey) {
		return insErr
	}
	found, err := q.First(ctx, db)
	if err == nil {
		*row = *found
		return nil
	}
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w (a soft-deleted row may hold the unique key; query WithTrashed to see it)", insErr)
	}
	return err
}
