package rio

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
)

// InsertAll inserts rows in chunks within the dialect's bind limit. Chunks
// commit independently unless the caller supplies a transaction. It backfills
// generated keys only where ordering is reliable; omitzero does not apply.
func InsertAll[T any](ctx context.Context, db Queryer, rows []T) error {
	if len(rows) == 0 {
		return nil
	}
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	g := db.gram()
	d := g.d
	now := normalizeTime(db.conf().clock())

	if !d.caps().autoIncrPK && p.autoIncr != nil {
		// This dialect can neither generate nor backfill a zero conventional ID.
		for i := range rows {
			if err := checkGeneratedID(
				d,
				"InsertAll",
				p,
				reflect.ValueOf(&rows[i]).Elem(),
			); err != nil {
				return err
			}
		}
	}
	for i := range rows {
		stampForInsert(p, reflect.ValueOf(&rows[i]).Elem(), now)
	}

	cols, backfill, err := batchColumns(p, rows)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fmt.Errorf("rio: InsertAll: %s has no insertable columns", p.structName)
	}

	chunk := max(d.caps().maxBindParams/len(cols), 1)
	bn := binder{d: d, now: now}
	// Size by the actual batch: a full chunk on PG/MySQL is 65535 binds, and
	// a three-row insert should not pay a ~1 MiB buffer for it.
	args := make([]any, 0, min(chunk, len(rows))*len(cols))
	for start := 0; start < len(rows); start += chunk {
		end := min(start+chunk, len(rows))
		if err := insertChunk(
			ctx,
			db,
			p,
			cols,
			rows[start:end],
			backfill,
			&bn,
			chunk,
			args,
		); err != nil {
			return err
		}
	}
	return nil
}

// UpsertAll applies Upsert conflict behavior in chunked multi-VALUES
// statements. It does not backfill generated values, and omitzero does not
// apply. MySQL DoUpdate requires MySQL 8.0.19 or later and excludes MariaDB.
func UpsertAll[T any](ctx context.Context, db Queryer, rows []T, opts ...UpsertOption) error {
	if len(rows) == 0 {
		return nil
	}
	var spec upsertSpec
	spec.init()
	for _, opt := range opts {
		opt(&spec)
	}
	spec.normalize()
	if spec.doNothing && len(spec.update) > 0 {
		return errors.New("rio: UpsertAll cannot combine DoNothing with DoUpdate")
	}
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	g := db.gram()
	d := g.d
	if err := checkUpsertWrite(d, "UpsertAll"); err != nil {
		return err
	}
	if !spec.doNothing && len(spec.conflict) == 0 && d.caps().conflictTarget {
		return errors.New("rio: UpsertAll with DoUpdate needs OnConflict(columns...) naming the unique index")
	}
	now := normalizeTime(db.conf().clock())
	for i := range rows {
		prepareUpsertRow(
			p,
			reflect.ValueOf(&rows[i]).Elem(),
			&spec,
			now,
		)
	}
	cols, _, err := batchColumns(p, rows)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fmt.Errorf("rio: UpsertAll: %s has no insertable columns", p.structName)
	}
	// Batch columns do not apply omitzero, so none are skipped.
	update, err := upsertUpdateSet(p, &spec, nil)
	if err != nil {
		return err
	}

	chunk := max(d.caps().maxBindParams/len(cols), 1)
	table := g.table(p)
	bits, cacheable := setBits(p, cols)
	bn := binder{d: d, now: now}
	// Size by the actual batch, as in InsertAll: never a full-chunk buffer
	// for a handful of rows.
	args := make([]any, 0, min(chunk, len(rows))*len(cols))
	for start := 0; start < len(rows); start += chunk {
		end := min(start+chunk, len(rows))
		part := rows[start:end]

		// Cache full chunks by tuple and conflict shape.
		sqlText, err := upsertSQL(
			g,
			p,
			"upsertall",
			bits,
			len(part),
			&spec,
			update,
			cacheable && len(part) == chunk,
			func() []byte {
				b := renderInsertHead(g, p, cols)
				b = append(b, " VALUES "...)
				b = appendValuesTuples(b, len(part), len(cols))
				return appendConflictBranch(b, d, table, p, update, &spec)
			},
		)
		if err != nil {
			return err
		}

		args, err = bindRowArgs(args[:0], part, cols, &bn)
		if err != nil {
			return err
		}
		if _, err := run(
			ctx,
			db,
			"upsert",
			p.structName,
			sqlText,
			args,
		); err != nil {
			return err
		}
	}
	return nil
}

// batchColumns requires generated keys to be either all zero or all explicit.
func batchColumns[T any](p *plan, rows []T) (cols []*field, backfill bool, err error) {
	backfill = p.autoIncr != nil
	if p.autoIncr != nil {
		var zero, nonzero int
		for i := range rows {
			if reflect.ValueOf(&rows[i]).Elem().FieldByIndex(p.autoIncr.index).IsZero() {
				zero++
			} else {
				nonzero++
			}
		}
		switch {
		case zero == len(rows):
			backfill = true
		case nonzero == len(rows):
			backfill = false
		default:
			return nil, false, fmt.Errorf("rio: batch write: %s rows mix zero and explicit %s values; split the batch",
				p.structName, p.autoIncr.column)
		}
	}
	if backfill {
		return p.insCols, true, nil
	}
	return p.fields, false, nil
}

func insertChunk[T any](
	ctx context.Context,
	db Queryer,
	p *plan,
	cols []*field,
	rows []T,
	backfill bool,
	bn *binder,
	fullChunk int,
	args []any,
) error {
	g := db.gram()
	d := g.d
	bits, cacheable := setBits(p, cols)
	// Cache only reusable full-chunk shapes, not arbitrary tail lengths.
	cacheable = cacheable && len(rows) == fullChunk
	returning := backfill && d.caps().returning
	op := "insertall"
	if returning {
		op = "insertall+ret"
	}
	sqlText, err := crudSQLRows(
		g,
		p,
		op,
		bits,
		len(rows),
		cacheable,
		func() []byte {
			b := renderInsertHead(g, p, cols)
			b = append(b, " VALUES "...)
			b = appendValuesTuples(b, len(rows), len(cols))
			if returning {
				b = append(b, " RETURNING "...)
				b = d.quote(b, p.autoIncr.column)
			}
			return b
		},
	)
	if err != nil {
		return err
	}
	args, err = bindRowArgs(args[:0], rows, cols, bn)
	if err != nil {
		return err
	}

	if returning {
		sqlRows, finish, err := runQuery(
			ctx,
			db,
			"insert",
			p.structName,
			sqlText,
			args,
		)
		if err != nil {
			return err
		}
		ids, err := scanScalarsCap[int64](sqlRows, 0, len(rows))
		finishQuery(finish, err, int64(len(ids)))
		if err != nil {
			return err
		}
		if len(ids) != len(rows) {
			return fmt.Errorf("rio: InsertAll: RETURNING yielded %d ids for %d rows", len(ids), len(rows))
		}
		if d.name() == "sqlite" {
			// SQLite RETURNING order is undefined; generated rowids are monotonic.
			slices.Sort(ids)
		}
		for i := range rows {
			fv := reflect.ValueOf(&rows[i]).Elem().FieldByIndex(p.autoIncr.index)
			if isUintKind(fv.Kind()) {
				fv.SetUint(uint64(ids[i]))
			} else {
				fv.SetInt(ids[i])
			}
		}
		return nil
	}

	_, err = run(
		ctx,
		db,
		"insert",
		p.structName,
		sqlText,
		args,
	)
	return err
}

// appendValuesTuples renders nRows placeholder tuples: (?, ?), (?, ?), …
func appendValuesTuples(b []byte, nRows, nCols int) []byte {
	for r := range nRows {
		if r > 0 {
			b = append(b, ", "...)
		}
		b = append(b, '(')
		for i := range nCols {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = append(b, '?')
		}
		b = append(b, ')')
	}
	return b
}

// bindRowArgs appends every row's column values in plan order, matching the
// tuples appendValuesTuples rendered.
func bindRowArgs[T any](args []any, rows []T, cols []*field, bn *binder) ([]any, error) {
	for r := range rows {
		rv := reflect.ValueOf(&rows[r]).Elem()
		base := rv.Addr().UnsafePointer()
		for _, f := range cols {
			a, err := fieldValue(f, base, rv, bn)
			if err != nil {
				return nil, err
			}
			args = append(args, a)
		}
	}
	return args, nil
}
