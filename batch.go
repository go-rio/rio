package rio

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"
)

// InsertAll writes rows in multi-VALUES statements, auto-chunked to the
// dialect's bind-parameter ceiling. Outside a transaction each chunk commits
// independently — a failure in chunk N leaves earlier chunks written; wrap
// the call in db.Tx for atomicity (rio adds no hidden transaction).
//
// Backfill promises only what the dialects can keep: auto-increment PKs on
// PostgreSQL (RETURNING, positional) and SQLite (RETURNING sorted by PK —
// its output order is documented as undefined), nothing on MySQL (its
// default interleaved autoinc mode makes first-ID+i arithmetic wrong).
// Timestamps, version, and every explicit value are already in your slice.
// omitzero does not apply on the batch path: one statement, one column list.
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

	chunk := d.caps().maxBindParams / len(cols)
	if chunk < 1 {
		chunk = 1
	}
	for start := 0; start < len(rows); start += chunk {
		end := min(start+chunk, len(rows))
		if err := insertChunk(ctx, db, p, cols, rows[start:end], backfill, now); err != nil {
			return err
		}
	}
	return nil
}

// batchColumns picks one column list for the whole batch. Auto-increment
// PKs must be all-zero (skip and backfill) or all-set (write, no backfill):
// mixing would silently reassign explicit IDs.
func batchColumns[T any](p *plan, rows []T) (cols []*field, backfill bool, err error) {
	backfill = p.autoIncr != nil
	if p.autoIncr != nil {
		zero, nonzero := 0, 0
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
	for _, f := range p.fields {
		if f.isAutoIncr && backfill {
			continue
		}
		cols = append(cols, f)
	}
	return cols, backfill, nil
}

func insertChunk[T any](ctx context.Context, db Queryer, p *plan, cols []*field, rows []T, backfill bool, now time.Time) error {
	g := db.gram()
	d := g.d
	bits, cacheable := setBits(p, cols)
	returning := backfill && d.caps().returning
	op := "insertall"
	if returning {
		op = "insertall+ret"
	}
	sqlText, err := crudSQLRows(g, p, op, bits, len(rows), cacheable, func() []byte {
		b := renderInsertHead(g, p, cols)
		b = append(b, " VALUES "...)
		for r := range rows {
			if r > 0 {
				b = append(b, ", "...)
			}
			b = append(b, '(')
			for i := range cols {
				if i > 0 {
					b = append(b, ", "...)
				}
				b = append(b, '?')
			}
			b = append(b, ')')
		}
		if returning {
			b = append(b, " RETURNING "...)
			b = d.quote(b, p.autoIncr.column)
		}
		return b
	})
	if err != nil {
		return err
	}
	args := make([]any, 0, len(rows)*len(cols))
	for r := range rows {
		rv := reflect.ValueOf(&rows[r]).Elem()
		base := rv.Addr().UnsafePointer()
		for _, f := range cols {
			a, err := fieldValue(f, base, rv, d)
			if err != nil {
				return err
			}
			args = append(args, a)
		}
	}

	if returning {
		sqlRows, finish, err := runQuery(ctx, db, "insert", p.structName, sqlText, args)
		if err != nil {
			return err
		}
		ids, err := scanScalars[int64](sqlRows)
		finishQuery(finish, err)
		if err != nil {
			return err
		}
		if len(ids) != len(rows) {
			return fmt.Errorf("rio: InsertAll: RETURNING yielded %d ids for %d rows", len(ids), len(rows))
		}
		if d.name() == "sqlite" {
			// SQLite documents RETURNING output order as undefined; rowids
			// are assigned monotonically within one statement, so sorted
			// ids correspond to input order.
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

	_, err = run(ctx, db, "insert", p.structName, sqlText, args)
	return err
}

// UpsertAll upserts rows in multi-VALUES statements with the same conflict
// clause as Upsert. It never backfills: DoNothing shrinks the returned row
// set, so positional matching would silently misalign (the batch-backfill
// killer). Reload rows you need generated values for.
//
// omitzero does not apply on the batch path (one statement, one column
// list): unlike Upsert, zero omitzero columns are inserted and stay in the
// default conflict update set, so batch zeros overwrite on conflict.
// UpdatedAt is reset to the clock on every non-DoNothing row, like Upsert.
// The MySQL DoUpdate version floor (8.0.19+, no MariaDB) also matches Upsert.
func UpsertAll[T any](ctx context.Context, db Queryer, rows []T, opts ...UpsertOption) error {
	if len(rows) == 0 {
		return nil
	}
	var spec upsertSpec
	for _, opt := range opts {
		opt(&spec)
	}
	if spec.doNothing && len(spec.update) > 0 {
		return errors.New("rio: UpsertAll cannot combine DoNothing with DoUpdate")
	}
	p, err := planOf[T]()
	if err != nil {
		return err
	}
	g := db.gram()
	d := g.d
	if !spec.doNothing && len(spec.conflict) == 0 && d.caps().conflictTarget {
		return errors.New("rio: UpsertAll with DoUpdate needs OnConflict(columns...) naming the unique index")
	}
	now := normalizeTime(db.conf().clock())
	for i := range rows {
		prepareUpsertRow(p, reflect.ValueOf(&rows[i]).Elem(), &spec, now)
	}
	cols, _, err := batchColumns(p, rows)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fmt.Errorf("rio: UpsertAll: %s has no insertable columns", p.structName)
	}
	// The batch column list applies no omitzero (batchColumns), so nothing is
	// skipped — every column the conflict update references was inserted.
	update, err := upsertUpdateSet(p, &spec, nil)
	if err != nil {
		return err
	}

	chunk := d.caps().maxBindParams / len(cols)
	if chunk < 1 {
		chunk = 1
	}
	table := g.table(p)
	for start := 0; start < len(rows); start += chunk {
		end := min(start+chunk, len(rows))
		part := rows[start:end]

		b := renderInsertHead(g, p, cols)
		b = append(b, " VALUES "...)
		args := make([]any, 0, len(part)*len(cols))
		for r := range part {
			if r > 0 {
				b = append(b, ", "...)
			}
			b = append(b, '(')
			rv := reflect.ValueOf(&part[r]).Elem()
			base := rv.Addr().UnsafePointer()
			for i, f := range cols {
				if i > 0 {
					b = append(b, ", "...)
				}
				b = append(b, '?')
				a, err := fieldValue(f, base, rv, d)
				if err != nil {
					return err
				}
				args = append(args, a)
			}
			b = append(b, ')')
		}

		if d.caps().conflictTarget {
			b = appendConflictClause(b, d, &spec)
			if spec.doNothing {
				b = append(b, "DO NOTHING"...)
			} else {
				b = append(b, "DO UPDATE SET "...)
				b = appendConflictSets(b, d, table, p, update, &spec, "excluded")
			}
		} else {
			// The row alias is DoUpdate-only, as in Upsert: 8.0.19+ syntax
			// that DoNothing's no-op assignment never needs.
			if !spec.doNothing {
				b = appendMySQLUpsertAlias(b)
			}
			b = append(b, " ON DUPLICATE KEY UPDATE "...)
			if spec.doNothing {
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
		}

		sqlText, outArgs, err := finishSQL(d, b, args)
		if err != nil {
			return err
		}
		if _, err := run(ctx, db, "upsert", p.structName, sqlText, outArgs); err != nil {
			return err
		}
	}
	return nil
}
