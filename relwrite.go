package rio

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

// Attach links rows to a ManyToMany relation by inserting join-table rows —
// idempotently: existing links are left alone (bare ON CONFLICT DO NOTHING;
// a no-op assignment on MySQL), assuming the join table's standard composite
// unique key. Nothing here is implicit: this is the explicit inverse of
// hand-writing the join-table INSERT. Typed id slices spread directly:
// Attach(ctx, db, &u, "Tags", tagIDs...). Attaching zero ids is a no-op
// (spell the id type as Attach[User, int64] when nothing infers it).
//
// Large id sets are chunked to the dialect's bind-parameter ceiling, like
// InsertAll. Outside a transaction each chunk commits independently — a
// failure leaves earlier chunks linked; wrap the call in db.Tx for
// atomicity, or simply retry: idempotency makes a rerun converge.
func Attach[T any, K any](ctx context.Context, db Queryer, row *T, relation string, ids ...K) error {
	if len(ids) == 0 {
		return nil
	}
	p, _, res, ownerKey, err := joinWriteTarget(db, row, relation)
	if err != nil {
		return err
	}
	if d := db.gram().d; !d.caps().uniqueKeys {
		// The rendered INSERT leans on ON CONFLICT DO NOTHING (a no-op
		// assignment on MySQL) over the join table's composite unique key for
		// its idempotency promise; without unique keys, reruns duplicate.
		return fmt.Errorf("rio: Attach is not supported on %s (idempotency needs a unique key over the join table); insert join rows with rio.Exec or InsertAll on a ReplacingMergeTree join table", d.name())
	}
	return insertJoinRows(ctx, db, p, res, ownerKey, anySlice(ids))
}

// insertJoinRows renders the idempotent join-table INSERT, chunked so each
// statement stays under the dialect's bind limit (two parameters per row).
func insertJoinRows(ctx context.Context, db Queryer, p *plan, res *resolvedRel, ownerKey any, ids []any) error {
	g := db.gram()
	d := g.d
	chunk := d.caps().maxBindParams / 2
	if chunk < 1 {
		chunk = 1
	}
	for start := 0; start < len(ids); start += chunk {
		end := min(start+chunk, len(ids))
		part := ids[start:end]

		b := make([]byte, 0, 128)
		b = append(b, "INSERT INTO "...)
		b = d.quote(b, res.joinTable)
		b = append(b, " ("...)
		b = d.quote(b, res.joinFK)
		b = append(b, ", "...)
		b = d.quote(b, res.joinRef)
		b = append(b, ") VALUES "...)
		args := make([]any, 0, len(part)*2)
		for i, id := range part {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = append(b, "(?, ?)"...)
			args = append(args, ownerKey, id)
		}
		if d.caps().conflictTarget {
			b = append(b, " ON CONFLICT DO NOTHING"...)
		} else {
			b = append(b, " ON DUPLICATE KEY UPDATE "...)
			b = d.quote(b, res.joinFK)
			b = append(b, " = "...)
			b = d.quote(b, res.joinFK)
		}
		sqlText, outArgs, err := finishSQL(d, b, args)
		if err != nil {
			return err
		}
		if _, err := run(ctx, db, "insert", p.structName, sqlText, outArgs); err != nil {
			return err
		}
	}
	return nil
}

// Detach unlinks rows from a ManyToMany relation by deleting join-table
// rows. The ids are required — detaching "everything" must be written as an
// explicit set-based delete on the join table, never implied by an empty
// call. Typed id slices spread directly: Detach(ctx, db, &u, "Tags", ids...).
//
// Large id sets are chunked to the dialect's bind-parameter ceiling. Outside
// a transaction each chunk commits independently; deleting is naturally
// idempotent, so a retry after a partial failure converges.
func Detach[T any, K any](ctx context.Context, db Queryer, row *T, relation string, ids ...K) error {
	if len(ids) == 0 {
		return fmt.Errorf("rio: Detach needs the ids to unlink; clearing a whole relation is an explicit join-table delete")
	}
	p, _, res, ownerKey, err := joinWriteTarget(db, row, relation)
	if err != nil {
		return err
	}
	if d := db.gram().d; !d.caps().mutations {
		return fmt.Errorf("rio: Detach is not supported on %s (join-table DELETE is an asynchronous mutation); use rio.Exec", d.name())
	}
	// Pass the ids as []any, not []K: a byte-kind id type would make []K a
	// []byte that IN (?) expansion treats as one BLOB (rebind.sliceElems),
	// silently matching nothing.
	return deleteJoinRows(ctx, db, p, res, ownerKey, anySlice(ids))
}

// deleteJoinRows renders the join-table DELETE, its IN list chunked so each
// statement stays under the dialect's bind limit (one parameter is the owner
// key).
func deleteJoinRows(ctx context.Context, db Queryer, p *plan, res *resolvedRel, ownerKey any, ids []any) error {
	d := db.gram().d
	chunk := d.caps().maxBindParams - 1
	if chunk < 1 {
		chunk = 1
	}
	for start := 0; start < len(ids); start += chunk {
		end := min(start+chunk, len(ids))

		b := make([]byte, 0, 96)
		b = append(b, "DELETE FROM "...)
		b = d.quote(b, res.joinTable)
		b = append(b, " WHERE "...)
		b = d.quote(b, res.joinFK)
		b = append(b, " = ? AND "...)
		b = d.quote(b, res.joinRef)
		b = append(b, " IN (?)"...)
		args := []any{ownerKey, ids[start:end]}

		sqlText, outArgs, err := finishSQL(d, b, args)
		if err != nil {
			return err
		}
		if _, err := run(ctx, db, "delete", p.structName, sqlText, outArgs); err != nil {
			return err
		}
	}
	return nil
}

// anySlice widens a typed id slice to []any so IN (?) expansion never depends
// on the element kind (a []byte-kind slice would otherwise bind as one BLOB).
func anySlice[K any](ids []K) []any {
	out := make([]any, len(ids))
	for i, id := range ids {
		out[i] = id
	}
	return out
}

// SyncRelation makes the ManyToMany relation match ids exactly, inside one
// transaction (or savepoint, when db already is one): links not in ids are
// removed, missing ones are added idempotently. An empty ids slice
// explicitly empties the relation — unlike Detach, where "no ids" is a
// refused footgun, Sync's whole meaning is "converge on this set".
//
// Sync reads the existing join rows and computes the difference in memory:
// removals and additions are then chunked to the dialect's bind limit — a
// NOT IN over the full id set would break at the limit, and chunking a NOT
// IN would delete every other chunk's ids. Rows already in sync are not
// touched.
//
// Concurrent SyncRelation calls on the same owner serialize on a row lock
// (SELECT ... FOR UPDATE on the owner; SQLite's single-writer model
// serializes anyway) — without it, two syncs interleaving their DELETE and
// INSERT under READ COMMITTED would converge on the union of both sets,
// which is exactly not what "sync" promises. The join-row read locks too,
// so it sees committed state rather than an older snapshot under REPEATABLE
// READ.
func SyncRelation[T any, K any](ctx context.Context, db Queryer, row *T, relation string, ids []K) error {
	p, _, res, ownerKey, err := joinWriteTarget(db, row, relation)
	if err != nil {
		return err
	}
	if d := db.gram().d; !d.caps().transactions {
		// The transactions door is the first of three this convergence needs
		// (transaction, owner row lock, join-table DELETE); one honest error
		// beats reporting them piecemeal.
		return fmt.Errorf("rio: SyncRelation is not supported on %s (needs a transaction and row locks)", d.name())
	}
	return db.Tx(ctx, func(tx *Tx) error {
		d := tx.gram().d
		if d.caps().forUpdate == forUpdateRender {
			g := tx.gram()
			lb := make([]byte, 0, 96)
			lb = append(lb, "SELECT "...)
			lb = d.quote(lb, res.ref.column)
			lb = append(lb, " FROM "...)
			lb = d.quote(lb, g.table(p))
			lb = append(lb, " WHERE "...)
			lb = d.quote(lb, res.ref.column)
			lb = append(lb, " = ? FOR UPDATE"...)
			lockSQL, lockArgs, err := finishSQL(d, lb, []any{ownerKey})
			if err != nil {
				return err
			}
			rows, finish, err := runQuery(ctx, tx, "select", p.structName, lockSQL, lockArgs)
			if err != nil {
				return err
			}
			err = rows.Close()
			finishQuery(finish, err)
			if err != nil {
				return err
			}
		}
		if len(ids) == 0 {
			// Emptying needs no diff: one unconditional delete.
			b := make([]byte, 0, 96)
			b = append(b, "DELETE FROM "...)
			b = d.quote(b, res.joinTable)
			b = append(b, " WHERE "...)
			b = d.quote(b, res.joinFK)
			b = append(b, " = ?"...)
			sqlText, outArgs, err := finishSQL(d, b, []any{ownerKey})
			if err != nil {
				return err
			}
			_, err = run(ctx, tx, "delete", p.structName, sqlText, outArgs)
			return err
		}

		existingKeys, existingVals, existingSet, err := selectJoinRefs(ctx, tx, p, res, ownerKey)
		if err != nil {
			return err
		}
		desired := make(map[any]struct{}, len(ids))
		var toInsert []any
		for _, id := range ids {
			k := canonKey(reflect.ValueOf(id))
			if _, dup := desired[k]; dup {
				continue
			}
			desired[k] = struct{}{}
			if _, have := existingSet[k]; !have {
				toInsert = append(toInsert, id)
			}
		}
		var toDelete []any
		for i, k := range existingKeys {
			if _, keep := desired[k]; !keep {
				toDelete = append(toDelete, existingVals[i])
			}
		}
		if err := deleteJoinRows(ctx, tx, p, res, ownerKey, toDelete); err != nil {
			return err
		}
		return insertJoinRows(ctx, tx, p, res, ownerKey, toInsert)
	})
}

// selectJoinRefs reads the owner's existing target-side join keys, in scan
// order and deduplicated: canonical keys for the set difference, original
// values for binding the DELETE (a canonicalized []byte would not match a
// BLOB column). The read locks (FOR UPDATE) where the dialect supports it,
// keeping the diff on committed state under REPEATABLE READ.
func selectJoinRefs(ctx context.Context, tx *Tx, p *plan, res *resolvedRel, ownerKey any) (keys []any, vals []any, set map[any]struct{}, err error) {
	d := tx.gram().d
	b := make([]byte, 0, 96)
	b = append(b, "SELECT "...)
	b = d.quote(b, res.joinRef)
	b = append(b, " FROM "...)
	b = d.quote(b, res.joinTable)
	b = append(b, " WHERE "...)
	b = d.quote(b, res.joinFK)
	b = append(b, " = ?"...)
	if d.caps().forUpdate == forUpdateRender {
		b = append(b, " FOR UPDATE"...)
	}
	sqlText, outArgs, err := finishSQL(d, b, []any{ownerKey})
	if err != nil {
		return nil, nil, nil, err
	}
	rows, finish, err := runQuery(ctx, tx, "select", p.structName, sqlText, outArgs)
	if err != nil {
		return nil, nil, nil, err
	}
	keys, vals, set, err = scanJoinRefs(rows, res)
	finishQuery(finish, err)
	if err != nil {
		return nil, nil, nil, err
	}
	return keys, vals, set, nil
}

func scanJoinRefs(rows *sql.Rows, res *resolvedRel) (keys []any, vals []any, set map[any]struct{}, err error) {
	defer rows.Close()
	// res.fk is the target's PK — the type the join column's values are.
	kf := &field{name: "join key", column: res.joinRef, typ: res.fk.typ}
	codec, err := codecFor(kf)
	if err != nil {
		return nil, nil, nil, err
	}
	kf.code = codec
	var cell colScanner
	cell.f = kf
	set = make(map[any]struct{})
	for rows.Next() {
		keyBuf := reflect.New(res.fk.typ) // fresh cell: values outlive the scan
		cell.base = keyBuf.UnsafePointer()
		if err := rows.Scan(&cell); err != nil {
			return nil, nil, nil, err
		}
		k := canonKey(keyBuf.Elem())
		if _, dup := set[k]; dup {
			continue
		}
		set[k] = struct{}{}
		keys = append(keys, k)
		vals = append(vals, keyBuf.Elem().Interface())
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, err
	}
	return keys, vals, set, nil
}

// joinWriteTarget resolves the ManyToMany wiring and the owner's key value.
func joinWriteTarget[T any](db Queryer, row *T, relation string) (*plan, *relField, *resolvedRel, any, error) {
	p, err := planOf[T]()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rel, ok := p.rels[relation]
	if !ok {
		return nil, nil, nil, nil, fmt.Errorf("rio: %s has no relation %q", p.structName, relation)
	}
	if rel.kind != relManyToMany {
		return nil, nil, nil, nil, fmt.Errorf("rio: Attach/Detach handle ManyToMany relations; a %s child moves by updating its foreign key", rel.kind)
	}
	res, err := rel.resolve(p)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rv, err := rowValue("Attach/Detach", row)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	kv := rv.FieldByIndex(res.ref.index)
	if kv.IsZero() {
		return nil, nil, nil, nil, fmt.Errorf("rio: Attach/Detach need %s's primary key to be set", p.structName)
	}
	return p, rel, res, kv.Interface(), nil
}
