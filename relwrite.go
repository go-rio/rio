package rio

import (
	"context"
	"fmt"
	"reflect"
)

// Attach links rows to a ManyToMany relation by inserting join-table rows —
// idempotently: existing links are left alone (bare ON CONFLICT DO NOTHING;
// a no-op assignment on MySQL), assuming the join table's standard composite
// unique key. Nothing here is implicit: this is the explicit inverse of
// hand-writing the join-table INSERT. Attaching zero ids is a no-op.
func Attach[T any](ctx context.Context, db Queryer, row *T, relation string, ids ...any) error {
	if len(ids) == 0 {
		return nil
	}
	p, rel, res, ownerKey, err := joinWriteTarget(db, row, relation)
	if err != nil {
		return err
	}
	_ = rel
	g := db.gram()
	d := g.d

	b := make([]byte, 0, 128)
	b = append(b, "INSERT INTO "...)
	b = d.quote(b, res.joinTable)
	b = append(b, " ("...)
	b = d.quote(b, res.joinFK)
	b = append(b, ", "...)
	b = d.quote(b, res.joinRef)
	b = append(b, ") VALUES "...)
	args := make([]any, 0, len(ids)*2)
	for i, id := range ids {
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
	_, err = run(ctx, db, "insert", p.structName, sqlText, outArgs)
	return err
}

// Detach unlinks rows from a ManyToMany relation by deleting join-table
// rows. The ids are required — detaching "everything" must be written as an
// explicit set-based delete on the join table, never implied by an empty
// call.
func Detach[T any](ctx context.Context, db Queryer, row *T, relation string, ids ...any) error {
	if len(ids) == 0 {
		return fmt.Errorf("rio: Detach needs the ids to unlink; clearing a whole relation is an explicit join-table delete")
	}
	p, _, res, ownerKey, err := joinWriteTarget(db, row, relation)
	if err != nil {
		return err
	}
	d := db.gram().d

	b := make([]byte, 0, 96)
	b = append(b, "DELETE FROM "...)
	b = d.quote(b, res.joinTable)
	b = append(b, " WHERE "...)
	b = d.quote(b, res.joinFK)
	b = append(b, " = ? AND "...)
	b = d.quote(b, res.joinRef)
	b = append(b, " IN (?)"...)
	args := []any{ownerKey, ids}

	sqlText, outArgs, err := finishSQL(d, b, args)
	if err != nil {
		return err
	}
	_, err = run(ctx, db, "delete", p.structName, sqlText, outArgs)
	return err
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
	rv := reflect.ValueOf(row).Elem()
	kv := rv.FieldByIndex(res.ref.index)
	if kv.IsZero() {
		return nil, nil, nil, nil, fmt.Errorf("rio: Attach/Detach need %s's primary key to be set", p.structName)
	}
	return p, rel, res, kv.Interface(), nil
}
