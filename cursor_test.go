package rio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"
)

// calendarDay binds and scans a DATE column through driver.Valuer and
// sql.Scanner, the way date wrappers do; the zero day binds NULL.
type calendarDay struct{ t time.Time }

func (d calendarDay) Value() (driver.Value, error) {
	if d.t.IsZero() {
		return nil, nil
	}
	return d.t, nil
}

func (d *calendarDay) Scan(src any) error {
	t, ok := src.(time.Time)
	if !ok {
		return errors.New("calendarDay: not a time")
	}
	d.t = t
	return nil
}

type dayItem struct {
	ID  int64
	Day calendarDay
}

var _ sql.Scanner = (*calendarDay)(nil)

func TestOrderKeysAcceptValuerColumns(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	day := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	q := From[dayItem]().OrderKeys(SortKey{Column: "day", Desc: true})

	f.queueRows([]string{"id", "day"}, []driver.Value{int64(3), day})
	page, err := q.Limit(1).All(ctx, db)
	if err != nil || len(page) != 1 || !page[0].Day.t.Equal(day) {
		t.Fatalf("All: %v %+v", err, page)
	}
	cur, err := q.CursorAt(&page[0])
	if err != nil {
		t.Fatalf("CursorAt: %v", err)
	}
	parsed, err := ParseCursor(cur.String())
	if err != nil || len(parsed.values) != 2 || !parsed.values[0].(time.Time).Equal(day) {
		t.Fatalf("token carries the bound value: %v %+v", err, parsed.values)
	}
	f.queueRows([]string{"id", "day"})
	if _, err := q.After(cur).Limit(1).All(ctx, db); err != nil {
		t.Fatalf("After: %v", err)
	}
	got := f.loggedContaining("SELECT")[1]
	if !strings.Contains(got.sql, `("day_items"."day" < $1) OR ("day_items"."day" = $2 AND "day_items"."id" < $3)`) {
		t.Fatalf("keyset sql: %s", got.sql)
	}
	if bound, ok := got.args[0].(time.Time); !ok || !bound.Equal(day) || got.args[2] != int64(3) {
		t.Fatalf("keyset args: %v", got.args)
	}

	if _, err := q.CursorAt(&dayItem{ID: 1}); err == nil || !strings.Contains(err.Error(), "binds NULL") {
		t.Fatalf("a NULL-binding Valuer has no keyset order: %v", err)
	}
}
