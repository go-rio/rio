package rio

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"
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

// normalize dedupes the conflict target in first-seen order, once the options
// have all been applied. ON CONFLICT (email, email) matches no unique index —
// PostgreSQL answers with the famously opaque "no unique or exclusion
// constraint matching the ON CONFLICT specification" — so a repeated column
// (one OnConflict call or several) collapses to the index the caller meant.
// The DoUpdate whitelist dedupes separately in upsertUpdateSet.
func (s *upsertSpec) normalize() {
	if len(s.conflict) < 2 {
		return
	}
	seen := make(map[string]bool, len(s.conflict))
	out := s.conflict[:0]
	for _, c := range s.conflict {
		if seen[c] {
			continue
		}
		seen[c] = true
		out = append(out, c)
	}
	s.conflict = out
}

// OnConflict names the conflict target columns (the unique index). Required
// with DoUpdate on PostgreSQL/SQLite; MySQL has no conflict target — its
// ON DUPLICATE KEY reacts to any unique index, which is a documented
// semantic difference, not something rio papers over.
func OnConflict(cols ...string) UpsertOption {
	return func(s *upsertSpec) { s.conflict = append(s.conflict, cols...) }
}

// DoUpdate lists the columns to overwrite on conflict. With no columns,
// every non-PK, non-CreatedAt, non-conflict-target column updates. Listed
// columns render deduplicated in canonical field order regardless of call
// order — the same rule as Update's whitelist, for the same SQL-cache reason.
func DoUpdate(cols ...string) UpsertOption {
	return func(s *upsertSpec) { s.update = append(s.update, cols...) }
}

// DoNothing turns conflicts into no-ops. On MySQL this renders as a no-op
// assignment rather than INSERT IGNORE, which would swallow unrelated
// errors — and without the DoUpdate row alias, so it stays valid on MariaDB
// and MySQL before 8.0.19. A conflicting soft-deleted row stays deleted.
func DoNothing() UpsertOption {
	return func(s *upsertSpec) { s.doNothing = true }
}

// KeepTrashed opts out of the restore-on-upsert invariant: with it, an
// upsert hitting a soft-deleted row updates the tombstone without clearing
// deleted_at — and the row stays invisible to default queries. On an insert
// path, KeepTrashed also preserves an explicitly set deleted_at value.
func KeepTrashed() UpsertOption {
	return func(s *upsertSpec) { s.keepTrashed = true }
}

// Upsert inserts the row or updates it on unique-key conflict, in one
// statement. All four elements ship together: conflict target, update
// whitelist, RETURNING backfill (PG/SQLite; MySQL fills the auto-increment
// ID only on the insert path), and timestamp maintenance.
//
// Version backfill on the update path is RETURNING-only. Timestamps and every
// bound column already match the database on all three dialects — rio wrote
// the in-memory values into the statement — but a version column is
// incremented server-side (version = version + 1) from the row's *current*
// database value, which rio cannot know without RETURNING. On PostgreSQL and
// SQLite it is read back; on MySQL (no RETURNING) the in-memory version stays
// at what you set while the database moved on, and rio will not issue a hidden
// second SELECT to reconcile it. A later optimistic-locked Update would then
// see ErrStaleObject: reload the row after a MySQL upsert when you keep
// updating through the same struct, or run version-tracking upserts on
// PostgreSQL/SQLite.
//
// Soft-delete invariant: a successful DoUpdate upsert leaves the row visible
// — deleted_at is cleared on the inserted values and conflict update unless
// KeepTrashed is given. The explicit softdelete tag opted the model into
// deletion semantics; resurrect-on-upsert is its consistent extension (the
// alternative is Eloquent's famous trap: "upsert succeeded but the row is
// invisible").
//
// UpdatedAt is reset to the clock on every non-DoNothing upsert, even when
// nonzero: the conflict branch applies the would-be inserted row's stamp, so
// that stamp must be this call's now — the same unconditional rule as entity
// Update. DoNothing keeps Insert's fill-only-when-zero rule. All of this
// stamping happens before execution: after a failed Upsert the struct may
// already carry this attempt's timestamps and version=1 while the database
// is untouched — retrying with the same struct is safe.
//
// omitzero: a zero omitzero column is skipped from the INSERT column list,
// and the default conflict update set skips it too — on conflict the
// existing value survives (an uninserted column's excluded value is the DB
// DEFAULT, not something rio should write over data). Naming such a column
// in DoUpdate errors. UpsertAll differs: the batch path binds every column,
// so batch zeros are written on both branches.
//
// MySQL version floor: the DoUpdate branch references the would-be inserted
// row through a row alias — 8.0.19+ syntax (the VALUES() function it
// replaces is deprecated). MySQL before 8.0.19 and MariaDB reject it;
// DoNothing renders alias-free and runs everywhere.
func Upsert[T any](ctx context.Context, db Queryer, row *T, opts ...UpsertOption) error {
	var spec upsertSpec
	for _, opt := range opts {
		opt(&spec)
	}
	spec.normalize()
	if spec.doNothing && len(spec.update) > 0 {
		return errors.New("rio: Upsert cannot combine DoNothing with DoUpdate")
	}
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	g := db.gram()
	d := g.d
	if err := checkUpsertWrite(d, "Upsert"); err != nil {
		return err
	}
	// MySQL has no conflict target — ON DUPLICATE KEY reacts to any unique
	// index — so OnConflict is only required where the SQL needs it.
	if !spec.doNothing && len(spec.conflict) == 0 && d.caps().conflictTarget {
		return errors.New("rio: Upsert with DoUpdate needs OnConflict(columns...) naming the unique index")
	}

	rv, err := rowValue("Upsert", row)
	if err != nil {
		return err
	}
	now := normalizeTime(db.conf().clock())
	prepareUpsertRow(p, rv, &spec, now)
	bn := binder{d: d, now: now}
	cols, back, args, bits, cacheable, err := insertColumns(p, rv, &bn)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		// SQLite cannot attach a conflict clause to DEFAULT VALUES, so the
		// all-defaults upsert is not portable; the same row inserts fine.
		return fmt.Errorf("rio: Upsert on %s with every column defaulted (all omitzero columns zero); set a column or use Insert", p.structName)
	}

	update, err := upsertUpdateSet(p, &spec, back)
	if err != nil {
		return err
	}

	table := g.table(p)
	returning := d.caps().returning
	// The whole statement — conflict clause and RETURNING included — is a
	// pure function of the cache key (see upsertSQL), so it renders once per
	// grammar and shape. Arguments come from insertColumns: the conflict
	// assignments bind nothing.
	sqlText, err := upsertSQL(g, p, "upsert", bits, 0, &spec, update, cacheable, func() []byte {
		b := renderInsertHead(g, p, cols)
		b = appendInsertValues(b, d, len(cols))
		if d.caps().conflictTarget {
			b = appendConflictClause(b, d, &spec)
			if spec.doNothing {
				b = append(b, "DO NOTHING"...)
			} else {
				b = append(b, "DO UPDATE SET "...)
				b = appendConflictSets(b, d, table, p, update, &spec, "excluded")
			}
			if returning && !spec.doNothing {
				b = appendReturning(b, d, table, p)
			}
			if returning && spec.doNothing && len(back) > 0 {
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
			}
			return b
		}
		// The row alias is DoUpdate-only — 8.0.19+ syntax that MariaDB and
		// older MySQL reject; DoNothing's no-op assignment renders alias-free.
		if !spec.doNothing {
			b = appendMySQLUpsertAlias(b)
		}
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
			b = appendConflictSets(b, d, table, p, update, &spec, mysqlUpsertAlias)
		}
		return b
	})
	if err != nil {
		return err
	}

	if d.caps().conflictTarget {
		if returning && !spec.doNothing {
			rows, finish, err := runQuery(ctx, db, "upsert", p.structName, sqlText, args)
			if err != nil {
				return err
			}
			err = scanBackRow(rows, p, unsafe.Pointer(row))
			finishQuery(finish, err)
			return err
		}
		if returning && spec.doNothing && len(back) > 0 {
			rows, finish, err := runQuery(ctx, db, "upsert", p.structName, sqlText, args)
			if err != nil {
				return err
			}
			_, err = scanBackColsIfRow(rows, back, unsafe.Pointer(row))
			finishQuery(finish, err)
			return err
		}
		_, err = run(ctx, db, "upsert", p.structName, sqlText, args)
		return err
	}

	res, err := run(ctx, db, "upsert", p.structName, sqlText, args)
	if err != nil {
		return err
	}
	// MySQL reports 1 row affected for a fresh insert, 2 for a changed
	// conflict update, and 0 for an unchanged conflict update.
	n, err := res.RowsAffected()
	if err != nil {
		// go-sql-driver never fails here today, but silently skipping the
		// backfill would turn a broken driver result into "upsert succeeded,
		// ID missing" — the wrong one of the two honest options.
		return err
	}
	if n == 1 {
		return fillLastInsertID(p, rv, res.LastInsertId)
	}
	return nil
}

// checkUpsertWrite rejects the upsert family where no unique constraints
// exist: without one there is no conflict to react to — every upsert would
// silently insert another row.
func checkUpsertWrite(d Dialect, op string) error {
	if d.caps().uniqueKeys {
		return nil
	}
	return unsupportedf("rio: %s is not supported on %s (no unique constraints, no conflict clause); insert a new row version into a ReplacingMergeTree table and read with Final() — background merges keep the latest version per sorting key", op, d.name())
}

// checkRacedCreate rejects FirstOrCreate/CreateOrFirst where no unique
// constraint can arbitrate their race: both contracts are race-honest only
// because the constraint decides the winner.
func checkRacedCreate(d Dialect, op string) error {
	if d.caps().uniqueKeys {
		return nil
	}
	return unsupportedf("rio: %s is not supported on %s (no unique constraint to arbitrate the race — concurrent callers would both insert); use ReplacingMergeTree semantics or coordinate in the application", op, d.name())
}

// upsertSQL is crudSQLKeyed for the upsert family: the key additionally
// carries the normalized conflict shape. Caching the text is safe because
// the conflict assignments bind no parameters — they reference the
// excluded/alias row, "version + 1", and NULL only — so two calls with the
// same shape differ exclusively in the VALUES arguments.
func upsertSQL(g *grammar, p *plan, op string, bits uint64, rows int, spec *upsertSpec, update []*field, cacheable bool, build func() []byte) (string, error) {
	key := ""
	if cacheable {
		key = upsertSpecKey(spec, update)
	}
	return crudSQLKeyed(g, p, op, bits, rows, key, cacheable, build)
}

// upsertSpecKey normalizes an upsertSpec into the cache key's spec string:
// one flags byte, the resolved conflict-update set as an ordinal bitmap
// (upsertUpdateSet renders canonical order, so the set determines the text;
// callers only cache ≤64-column plans), and the conflict target columns
// verbatim — they render in caller order, and NUL separators keep distinct
// lists ("a","bc" vs "ab","c") from colliding.
func upsertSpecKey(spec *upsertSpec, update []*field) string {
	n := 9 + len(spec.conflict)
	for _, c := range spec.conflict {
		n += len(c)
	}
	b := make([]byte, 1, n)
	if spec.doNothing {
		b[0] |= 1
	}
	if spec.keepTrashed {
		b[0] |= 2
	}
	var set uint64
	for _, f := range update {
		set |= 1 << uint(f.ordinal)
	}
	b = binary.BigEndian.AppendUint64(b, set)
	for _, c := range spec.conflict {
		b = append(b, c...)
		b = append(b, 0)
	}
	return byteString(b)
}

const mysqlUpsertAlias = "_rio_new"

func appendMySQLUpsertAlias(b []byte) []byte {
	return append(b, " AS "+mysqlUpsertAlias...)
}

func prepareUpsertRow(p *plan, rv reflect.Value, spec *upsertSpec, now time.Time) {
	stampForInsert(p, rv, now)
	if p.updated != nil && !spec.doNothing {
		// The conflict branch renders updated_at = excluded.updated_at (row
		// alias on MySQL): the would-be inserted row must carry this call's
		// clock even when the struct holds a stale stamp from an earlier load
		// — matching entity Update's unconditional now. DoNothing never
		// references the new row on conflict and keeps Insert's
		// fill-only-when-zero rule.
		setTime(p.updated, rv, now)
	}
	if p.softDel != nil && !spec.doNothing && !spec.keepTrashed {
		clearTime(p.softDel, rv)
	}
}

// upsertUpdateSet resolves the DoUpdate whitelist (or derives the default:
// everything except PKs, CreatedAt, the version column, the softdelete
// column, the conflict target itself, and the columns this statement skipped).
// skipped holds the columns absent from the INSERT list (zero omitzero
// columns and the zero auto-increment PK): excluded.col for an uninserted
// column is its DB DEFAULT, so referencing one would silently reset the
// existing row's data on conflict — the default set leaves those columns
// untouched and the whitelist refuses them. The derivation is a pure
// function of (insert column set, spec) — skipped is the insert bitmap's
// complement — and the resolved set joins the cached-SQL key as an ordinal
// bitmap (upsertSpecKey). An empty resolved set with no maintained columns
// to render either would emit "DO UPDATE SET" with no assignments — invalid
// SQL on every dialect — so it errors with the fix.
func upsertUpdateSet(p *plan, spec *upsertSpec, skipped []*field) ([]*field, error) {
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
			if fieldIn(skipped, f) {
				return nil, fmt.Errorf("rio: DoUpdate: column %q is tagged omitzero and %s.%s is zero, so this statement inserts no value the conflict update could reference; set the field or drop the column from DoUpdate", c, p.structName, f.name)
			}
			if fieldIn(out, f) {
				continue // dedup by identity; whitelists are a handful of columns
			}
			out = append(out, f)
		}
		// Canonical order, always — updateSet's rule for the same reason: the
		// SQL cache keys on an order-free column set, so rendering must not
		// depend on caller order. Conflict assignments are order-independent
		// (each references only the excluded/alias row or constants), so the
		// reordering cannot change what the statement writes.
		slices.SortFunc(out, func(a, b *field) int { return a.ordinal - b.ordinal })
		return out, nil
	}
	inTarget := make(map[string]bool, len(spec.conflict))
	for _, c := range spec.conflict {
		inTarget[c] = true
	}
	var out []*field
	omitted := false
	for _, f := range p.fields {
		if f.isPK || f.isVersion || f.isSoftDelete || f.isCreated || f.isUpdated || f.isAutoIncr || inTarget[f.column] {
			continue
		}
		if fieldIn(skipped, f) {
			omitted = true
			continue
		}
		out = append(out, f)
	}
	if len(out) == 0 && p.updated == nil && p.version == nil && (p.softDel == nil || spec.keepTrashed) {
		// Nothing would render after DO UPDATE SET / ON DUPLICATE KEY UPDATE
		// — invalid SQL on every dialect (lookup tables, join tables).
		if omitted {
			return nil, fmt.Errorf("rio: upsert on %s has nothing to update on conflict (every column is a key, rio-maintained, or a zero omitzero column skipped this call); use DoNothing()", p.structName)
		}
		return nil, fmt.Errorf("rio: upsert on %s has nothing to update on conflict (every column is a key or rio-maintained); use DoNothing()", p.structName)
	}
	return out, nil
}

// fieldIn reports membership by identity; plan fields are canonical pointers.
func fieldIn(fs []*field, f *field) bool {
	for _, s := range fs {
		if s == f {
			return true
		}
	}
	return false
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

// appendConflictSets renders the DO UPDATE SET list. newRow is "excluded" for
// PG/SQLite and mysqlUpsertAlias for MySQL.
func appendConflictSets(b []byte, d Dialect, table string, p *plan, update []*field, spec *upsertSpec, newRow string) []byte {
	first := true
	sep := func() {
		if !first {
			b = append(b, ", "...)
		}
		first = false
	}
	newVal := func(col string) {
		b = append(b, newRow...)
		b = append(b, '.')
		b = d.quote(b, col)
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
	if err := checkRacedCreate(db.gram().d, "FirstOrCreate"); err != nil {
		return err
	}
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
	if err := checkRacedCreate(db.gram().d, "CreateOrFirst"); err != nil {
		return err
	}
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
