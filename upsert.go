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

// UpsertOption shapes the conflict clause.
type UpsertOption func(*upsertSpec)

type upsertSpec struct {
	conflict    []string
	update      []string
	doNothing   bool
	keepTrashed bool
	conflictBuf [1]string
	updateBuf   [1]string
}

type upsertCacheKey struct {
	update   uint64
	flags    uint8
	count    int
	conflict [4]string
	overflow string
}

const mysqlUpsertAlias = "_rio_new"

// OnConflict names the unique-index columns. DoUpdate requires it on
// PostgreSQL and SQLite; MySQL reacts to any unique index.
func OnConflict(cols ...string) UpsertOption {
	return func(s *upsertSpec) { s.conflict = append(s.conflict, cols...) }
}

// DoUpdate selects columns to overwrite on conflict. Without columns it uses
// every eligible column; explicit columns are deduplicated in model order.
func DoUpdate(cols ...string) UpsertOption {
	return func(s *upsertSpec) { s.update = append(s.update, cols...) }
}

// DoNothing turns conflicts into no-ops without suppressing unrelated errors.
// It remains compatible with MariaDB and MySQL before 8.0.19.
func DoNothing() UpsertOption {
	return func(s *upsertSpec) { s.doNothing = true }
}

// KeepTrashed preserves deleted_at on both insert and conflict-update paths.
func KeepTrashed() UpsertOption {
	return func(s *upsertSpec) { s.keepTrashed = true }
}

// Upsert inserts a row or updates it on unique-key conflict in one statement.
// Unless KeepTrashed is set, a successful update restores a soft-deleted row.
// Zero omitzero fields are excluded from both insert and the default update
// set; naming one explicitly in DoUpdate is an error.
//
// PostgreSQL and SQLite backfill the conflict result. MySQL backfills an
// auto-increment key only on insert and cannot refresh a server-incremented
// version; reload before updating the same versioned struct. MySQL DoUpdate
// requires MySQL 8.0.19 or later and is not supported by MariaDB.
//
// Timestamp and version initialization occurs before execution, so a failed
// call may still modify the struct.
func Upsert[T any](ctx context.Context, db Queryer, row *T, opts ...UpsertOption) error {
	var spec upsertSpec
	spec.init()
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
	// MySQL has no conflict target.
	if !spec.doNothing && len(spec.conflict) == 0 && d.caps().conflictTarget {
		return errors.New("rio: Upsert with DoUpdate needs OnConflict(columns...) naming the unique index")
	}

	rv, err := rowValue("Upsert", row)
	if err != nil {
		return err
	}
	now := normalizeTime(db.conf().clock())
	prepareUpsertRow(
		p,
		rv,
		&spec,
		now,
	)
	bn := binder{d: d, now: now}
	cols, back, args, bits, cacheable, err := insertColumns(p, rv, &bn)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		// SQLite cannot attach a conflict clause to DEFAULT VALUES.
		return fmt.Errorf(
			"rio: Upsert on %s with every column defaulted (all omitzero columns zero); "+
				"set a column or use Insert",
			p.structName,
		)
	}

	update, err := upsertUpdateSet(p, &spec, back)
	if err != nil {
		return err
	}

	table := g.table(p)
	returning := d.caps().returning
	// Conflict assignments bind no arguments, so the shape is cacheable.
	sqlText, err := upsertSQL(
		g,
		p,
		"upsert",
		bits,
		0,
		&spec,
		update,
		cacheable,
		func() []byte {
			b := renderInsertHead(g, p, cols)
			b = appendInsertValues(b, d, len(cols))
			b = appendConflictBranch(b, d, table, p, update, &spec)
			// returning is false on MySQL, whose branch above ends the statement.
			if returning && !spec.doNothing {
				b = appendReturning(b, d, table, p)
			}
			if returning && spec.doNothing && len(back) > 0 {
				// DoNothing returns generated columns only for a fresh insert.
				b = append(b, " RETURNING "...)
				for i, f := range back {
					if i > 0 {
						b = append(b, ", "...)
					}
					b = d.quote(b, f.column)
				}
			}
			return b
		},
	)
	if err != nil {
		return err
	}

	if d.caps().conflictTarget {
		if returning && !spec.doNothing {
			rows, finish, err := runQuery(
				ctx,
				db,
				"upsert",
				p.structName,
				sqlText,
				args,
			)
			if err != nil {
				return err
			}
			err = scanBackRow(rows, p, unsafe.Pointer(row))
			finishQuery(finish, err, oneIf(err == nil))
			return err
		}
		if returning && spec.doNothing && len(back) > 0 {
			rows, finish, err := runQuery(
				ctx,
				db,
				"upsert",
				p.structName,
				sqlText,
				args,
			)
			if err != nil {
				return err
			}
			scanned, err := scanBackColsIfRow(rows, back, unsafe.Pointer(row))
			finishQuery(finish, err, oneIf(scanned))
			return err
		}
		_, err = run(
			ctx,
			db,
			"upsert",
			p.structName,
			sqlText,
			args,
		)
		return err
	}

	res, err := run(
		ctx,
		db,
		"upsert",
		p.structName,
		sqlText,
		args,
	)
	if err != nil {
		return err
	}
	// MySQL reports 1 for insert, 2 for changed update, and 0 for no change.
	n, err := res.RowsAffected()
	if err != nil {
		// A RowsAffected failure makes generated-key backfill unreliable.
		return err
	}
	if n == 1 {
		return fillLastInsertID(p, rv, res.LastInsertId)
	}
	return nil
}

// FirstOrCreate returns the first match or inserts row. If a concurrent insert
// wins, it re-reads after ErrDuplicateKey; if that still misses, it returns the
// duplicate-key error, which may identify a hidden soft-deleted row.
func (q Query[T]) FirstOrCreate(ctx context.Context, db Queryer, row *T, args ...any) error {
	if err := checkRacedCreate(db.gram().d, "FirstOrCreate"); err != nil {
		return err
	}
	_, state, err := prepareQueryState[T](db.gram().d, &q.s, args)
	if err != nil {
		return err
	}
	bound := Query[T]{s: state}
	found, err := bound.First(ctx, db)
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
	found, err = bound.First(ctx, db)
	if err == nil {
		*row = *found
		return nil
	}
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w (a soft-deleted row may hold the unique key; query WithTrashed to see it)", insErr)
	}
	return err
}

// CreateOrFirst inserts row or returns the existing match after a unique-key
// conflict.
func (q Query[T]) CreateOrFirst(ctx context.Context, db Queryer, row *T, args ...any) error {
	if err := checkRacedCreate(db.gram().d, "CreateOrFirst"); err != nil {
		return err
	}
	_, state, err := prepareQueryState[T](db.gram().d, &q.s, args)
	if err != nil {
		return err
	}
	bound := Query[T]{s: state}
	insErr := Insert(ctx, db, row)
	if insErr == nil {
		return nil
	}
	if !errors.Is(insErr, ErrDuplicateKey) {
		return insErr
	}
	found, err := bound.First(ctx, db)
	if err == nil {
		*row = *found
		return nil
	}
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w (a soft-deleted row may hold the unique key; query WithTrashed to see it)", insErr)
	}
	return err
}
func (s *upsertSpec) init() {
	s.conflict = s.conflictBuf[:0]
	s.update = s.updateBuf[:0]
}

// normalize deduplicates the conflict target while preserving caller order.
func (s *upsertSpec) normalize() {
	if len(s.conflict) < 2 {
		return
	}
	out := s.conflict[:0]
	for _, c := range s.conflict {
		if slices.Contains(out, c) {
			continue
		}
		out = append(out, c)
	}
	s.conflict = out
}

// checkUpsertWrite rejects dialects without unique constraints.
func checkUpsertWrite(d Dialect, op string) error {
	if d.caps().uniqueKeys {
		return nil
	}
	return unsupportedf(
		"rio: %s is not supported on %s (no unique constraints, no conflict clause); "+
			"insert a new row version into a ReplacingMergeTree table and read with Final() — "+
			"background merges keep the latest version per sorting key",
		op,
		d.name(),
	)
}

// checkRacedCreate requires a unique constraint to arbitrate concurrent creates.
func checkRacedCreate(d Dialect, op string) error {
	if d.caps().uniqueKeys {
		return nil
	}
	return unsupportedf(
		"rio: %s is not supported on %s "+
			"(no unique constraint to arbitrate the race — concurrent callers would both insert); "+
			"use ReplacingMergeTree semantics or coordinate in the application",
		op,
		d.name(),
	)
}

// upsertSQL caches statements by their normalized conflict shape.
func upsertSQL(
	g *grammar,
	p *plan,
	op string,
	bits uint64,
	rows int,
	spec *upsertSpec,
	update []*field,
	cacheable bool,
	build func() []byte,
) (string, error) {
	key := upsertCacheKey{}
	if cacheable {
		key = upsertSpecKey(spec, update)
	}
	return crudSQLKeyed(
		g,
		p,
		op,
		bits,
		rows,
		key,
		cacheable,
		build,
	)
}

// upsertSpecKey encodes flags, update fields, and ordered conflict columns.
func upsertSpecKey(spec *upsertSpec, update []*field) upsertCacheKey {
	key := upsertCacheKey{count: len(spec.conflict)}
	if spec.doNothing {
		key.flags |= 1
	}
	if spec.keepTrashed {
		key.flags |= 2
	}
	for _, f := range update {
		key.update |= 1 << uint(f.ordinal)
	}
	for i, c := range spec.conflict[:min(len(spec.conflict), len(key.conflict))] {
		key.conflict[i] = c
	}
	if len(spec.conflict) <= len(key.conflict) {
		return key
	}
	n := len(spec.conflict) - len(key.conflict)
	for _, c := range spec.conflict[len(key.conflict):] {
		n += len(c)
	}
	b := make([]byte, 0, n)
	for _, c := range spec.conflict[len(key.conflict):] {
		b = append(b, c...)
		b = append(b, 0)
	}
	key.overflow = byteString(b)
	return key
}

// appendConflictBranch renders the dialect's conflict clause and update set:
// ON CONFLICT … DO NOTHING/DO UPDATE on conflict-target dialects, otherwise
// MySQL's ON DUPLICATE KEY UPDATE — where DoNothing still needs one no-op
// assignment, keyed by the PK when the model has one. Upsert and UpsertAll
// share this tail verbatim.
func appendConflictBranch(b []byte, d Dialect, table string, p *plan, update []*field, spec *upsertSpec) []byte {
	if d.caps().conflictTarget {
		b = appendConflictClause(b, d, spec)
		if spec.doNothing {
			return append(b, "DO NOTHING"...)
		}
		b = append(b, "DO UPDATE SET "...)
		return appendConflictSets(b, d, table, p, update, spec, "excluded")
	}
	// The DoUpdate row alias requires MySQL 8.0.19 or later.
	if !spec.doNothing {
		b = appendMySQLUpsertAlias(b)
	}
	b = append(b, " ON DUPLICATE KEY UPDATE "...)
	if spec.doNothing {
		// A no-op assignment still needs one mapped column.
		col := p.fields[0].column
		if len(p.pks) > 0 {
			col = p.pks[0].column
		}
		b = d.quote(b, col)
		b = append(b, " = "...)
		return d.quote(b, col)
	}
	return appendConflictSets(b, d, table, p, update, spec, mysqlUpsertAlias)
}

func appendMySQLUpsertAlias(b []byte) []byte {
	return append(b, " AS "+mysqlUpsertAlias...)
}

func prepareUpsertRow(p *plan, rv reflect.Value, spec *upsertSpec, now time.Time) {
	stampForInsert(p, rv, now)
	if p.updated != nil && !spec.doNothing {
		// The conflict branch reads UpdatedAt from the would-be inserted row.
		setTime(p.updated, rv, now)
	}
	if p.softDel != nil && !spec.doNothing && !spec.keepTrashed {
		clearTime(p.softDel, rv)
	}
}

// upsertUpdateSet resolves explicit or default conflict-update columns.
// Columns omitted from INSERT cannot be referenced through the excluded row.
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
				return nil, fmt.Errorf(
					"rio: DoUpdate: column %q is tagged omitzero and %s.%s is zero, "+
						"so this statement inserts no value the conflict update could reference; "+
						"set the field or drop the column from DoUpdate",
					c,
					p.structName,
					f.name,
				)
			}
			if fieldIn(out, f) {
				continue // dedup by identity; whitelists are a handful of columns
			}
			out = append(out, f)
		}
		// Canonical order must match the order-free SQL-cache key.
		slices.SortFunc(out, func(a, b *field) int { return a.ordinal - b.ordinal })
		return out, nil
	}
	var out []*field
	hasOmitted := false
	for _, f := range p.fields {
		isMaintained := f.isPK || f.isVersion || f.isSoftDelete ||
			f.isCreated || f.isUpdated || f.isAutoIncr
		if isMaintained || slices.Contains(spec.conflict, f.column) {
			continue
		}
		if fieldIn(skipped, f) {
			hasOmitted = true
			continue
		}
		out = append(out, f)
	}
	hasNoAssignments := len(out) == 0 && p.updated == nil && p.version == nil &&
		(p.softDel == nil || spec.keepTrashed)
	if hasNoAssignments {
		// Every dialect requires at least one update assignment.
		if hasOmitted {
			return nil, fmt.Errorf(
				"rio: upsert on %s has nothing to update on conflict "+
					"(every column is a key, rio-maintained, or a zero omitzero column skipped this call); "+
					"use DoNothing()",
				p.structName,
			)
		}
		return nil, fmt.Errorf(
			"rio: upsert on %s has nothing to update on conflict "+
				"(every column is a key or rio-maintained); use DoNothing()",
			p.structName,
		)
	}
	return out, nil
}

// fieldIn reports membership by identity; plan fields are canonical pointers.
func fieldIn(fs []*field, f *field) bool {
	return slices.Contains(fs, f)
}

// appendConflictClause omits parentheses for targetless DoNothing.
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
func appendConflictSets(
	b []byte,
	d Dialect,
	table string,
	p *plan,
	update []*field,
	spec *upsertSpec,
	newRow string,
) []byte {
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
		// Increment the surviving row's version.
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
		// Restore the row unless KeepTrashed was requested.
		sep()
		b = d.quote(b, p.softDel.column)
		b = append(b, " = NULL"...)
	}
	return b
}
