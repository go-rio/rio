package rio

import (
	"encoding/json"
	"reflect"
	"testing"
)

// Contract tests for the relation containers' JSON and Loaded surface.
// Pinned behavior: an unloaded container marshals as null; a loaded slice
// marshals as its array ([] when empty); a loaded pointer marshals as its
// object, or null when the row is nil. UnmarshalJSON treats null as "reset to
// unloaded" and any other payload as "loaded"; malformed input errors without
// mutating the container. Loaded() reports false before, true after.

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return string(b)
}

func TestHasManyJSON(t *testing.T) {
	// Unloaded: null, and Loaded() is false.
	var r HasMany[Tag]
	if r.Loaded() {
		t.Fatal("zero value must be unloaded")
	}
	if got := mustJSON(t, r); got != "null" {
		t.Fatalf("unloaded marshal: got %s want null", got)
	}

	// Loaded-empty: [] (Set(nil) normalizes to a non-nil empty slice).
	var empty HasMany[Tag]
	empty.Set(nil)
	if !empty.Loaded() {
		t.Fatal("Set must mark loaded")
	}
	if got := mustJSON(t, empty); got != "[]" {
		t.Fatalf("loaded-empty marshal: got %s want []", got)
	}

	// Loaded-with-data: the array payload.
	var full HasMany[Tag]
	full.Set([]Tag{{ID: 1, Name: "go"}, {ID: 2, Name: "db"}})
	if got := mustJSON(t, full); got != `[{"ID":1,"Name":"go"},{"ID":2,"Name":"db"}]` {
		t.Fatalf("loaded marshal: %s", got)
	}

	// null input resets to unloaded, even over an already-loaded container.
	u := full
	if err := json.Unmarshal([]byte("null"), &u); err != nil {
		t.Fatalf("Unmarshal null: %v", err)
	}
	if u.Loaded() {
		t.Fatal("null input must leave the relation unloaded")
	}

	// Array input marks loaded and round-trips the rows.
	var got HasMany[Tag]
	if err := json.Unmarshal([]byte(`[{"ID":1,"Name":"go"}]`), &got); err != nil {
		t.Fatalf("Unmarshal array: %v", err)
	}
	if !got.Loaded() {
		t.Fatal("array input must mark the relation loaded")
	}
	if rows := got.Rows(); !reflect.DeepEqual(rows, []Tag{{ID: 1, Name: "go"}}) {
		t.Fatalf("rows: %+v", rows)
	}

	// Empty array is loaded-empty (non-nil) and re-marshals to [].
	var e HasMany[Tag]
	if err := json.Unmarshal([]byte("[]"), &e); err != nil {
		t.Fatalf("Unmarshal []: %v", err)
	}
	if !e.Loaded() || e.Rows() == nil || len(e.Rows()) != 0 {
		t.Fatalf("empty array must be loaded-empty non-nil, got %#v", e.Rows())
	}
	if out := mustJSON(t, e); out != "[]" {
		t.Fatalf("loaded-empty re-marshal: %s", out)
	}

	// Malformed input errors without loading the container.
	var bad HasMany[Tag]
	if err := json.Unmarshal([]byte(`"oops"`), &bad); err == nil {
		t.Fatal("string payload must fail to unmarshal into a slice relation")
	}
	if bad.Loaded() {
		t.Fatal("failed unmarshal must not mark loaded")
	}
}

func TestManyToManyJSON(t *testing.T) {
	// Unloaded: null, and Loaded() is false.
	var r ManyToMany[Tag]
	if r.Loaded() {
		t.Fatal("zero value must be unloaded")
	}
	if got := mustJSON(t, r); got != "null" {
		t.Fatalf("unloaded marshal: got %s want null", got)
	}

	// Loaded-empty: [].
	var empty ManyToMany[Tag]
	empty.Set(nil)
	if !empty.Loaded() {
		t.Fatal("Set must mark loaded")
	}
	if got := mustJSON(t, empty); got != "[]" {
		t.Fatalf("loaded-empty marshal: got %s want []", got)
	}

	// Loaded-with-data: the array payload.
	var full ManyToMany[Tag]
	full.Set([]Tag{{ID: 3, Name: "sql"}})
	if got := mustJSON(t, full); got != `[{"ID":3,"Name":"sql"}]` {
		t.Fatalf("loaded marshal: %s", got)
	}

	// null input resets to unloaded.
	u := full
	if err := json.Unmarshal([]byte("null"), &u); err != nil {
		t.Fatalf("Unmarshal null: %v", err)
	}
	if u.Loaded() {
		t.Fatal("null input must leave the relation unloaded")
	}

	// Array input marks loaded and round-trips the rows.
	var got ManyToMany[Tag]
	if err := json.Unmarshal([]byte(`[{"ID":3,"Name":"sql"}]`), &got); err != nil {
		t.Fatalf("Unmarshal array: %v", err)
	}
	if !got.Loaded() {
		t.Fatal("array input must mark the relation loaded")
	}
	if rows := got.Rows(); !reflect.DeepEqual(rows, []Tag{{ID: 3, Name: "sql"}}) {
		t.Fatalf("rows: %+v", rows)
	}

	// Malformed input errors without loading the container.
	var bad ManyToMany[Tag]
	if err := json.Unmarshal([]byte(`"oops"`), &bad); err == nil {
		t.Fatal("string payload must fail to unmarshal into a slice relation")
	}
	if bad.Loaded() {
		t.Fatal("failed unmarshal must not mark loaded")
	}
}

func TestHasOneJSON(t *testing.T) {
	// Unloaded: null, and Loaded() is false.
	var r HasOne[Profile]
	if r.Loaded() {
		t.Fatal("zero value must be unloaded")
	}
	if got := mustJSON(t, r); got != "null" {
		t.Fatalf("unloaded marshal: got %s want null", got)
	}

	// Loaded-nil: also null (indistinguishable from unloaded in JSON).
	var none HasOne[Profile]
	none.Set(nil)
	if !none.Loaded() {
		t.Fatal("Set(nil) must mark loaded")
	}
	if got := mustJSON(t, none); got != "null" {
		t.Fatalf("loaded-nil marshal: got %s want null", got)
	}

	// Loaded-with-data: the object payload.
	var full HasOne[Profile]
	full.Set(&Profile{ID: 9, UserID: 1, Nick: "gopher"})
	if got := mustJSON(t, full); got != `{"ID":9,"UserID":1,"Nick":"gopher"}` {
		t.Fatalf("loaded marshal: %s", got)
	}

	// null input resets to unloaded — not loaded-nil.
	u := full
	if err := json.Unmarshal([]byte("null"), &u); err != nil {
		t.Fatalf("Unmarshal null: %v", err)
	}
	if u.Loaded() {
		t.Fatal("null input must leave the relation unloaded, not loaded-nil")
	}

	// Object input marks loaded and round-trips the row.
	var got HasOne[Profile]
	if err := json.Unmarshal([]byte(`{"ID":9,"UserID":1,"Nick":"gopher"}`), &got); err != nil {
		t.Fatalf("Unmarshal object: %v", err)
	}
	if !got.Loaded() {
		t.Fatal("object input must mark the relation loaded")
	}
	if row := got.Row(); row == nil || *row != (Profile{ID: 9, UserID: 1, Nick: "gopher"}) {
		t.Fatalf("row: %+v", row)
	}

	// Loaded-nil does not survive a JSON round-trip: it marshals to null,
	// which unmarshals back to the unloaded state.
	var rt HasOne[Profile]
	if err := json.Unmarshal([]byte(mustJSON(t, none)), &rt); err != nil {
		t.Fatalf("round-trip loaded-nil: %v", err)
	}
	if rt.Loaded() {
		t.Fatal("loaded-nil must round-trip through JSON as unloaded")
	}

	// Malformed input errors without loading the container.
	var bad HasOne[Profile]
	if err := json.Unmarshal([]byte(`"oops"`), &bad); err == nil {
		t.Fatal("string payload must fail to unmarshal into a single-value relation")
	}
	if bad.Loaded() {
		t.Fatal("failed unmarshal must not mark loaded")
	}
}

func TestBelongsToJSON(t *testing.T) {
	// Unloaded: null, and Loaded() is false.
	var r BelongsTo[Org]
	if r.Loaded() {
		t.Fatal("zero value must be unloaded")
	}
	if got := mustJSON(t, r); got != "null" {
		t.Fatalf("unloaded marshal: got %s want null", got)
	}

	// Loaded-nil (NULL foreign key): also null.
	var none BelongsTo[Org]
	none.Set(nil)
	if !none.Loaded() {
		t.Fatal("Set(nil) must mark loaded")
	}
	if got := mustJSON(t, none); got != "null" {
		t.Fatalf("loaded-nil marshal: got %s want null", got)
	}

	// Loaded-with-data: the object payload.
	var full BelongsTo[Org]
	full.Set(&Org{ID: 7, Name: "acme"})
	if got := mustJSON(t, full); got != `{"ID":7,"Name":"acme"}` {
		t.Fatalf("loaded marshal: %s", got)
	}

	// null input resets to unloaded — not loaded-nil.
	u := full
	if err := json.Unmarshal([]byte("null"), &u); err != nil {
		t.Fatalf("Unmarshal null: %v", err)
	}
	if u.Loaded() {
		t.Fatal("null input must leave the relation unloaded, not loaded-nil")
	}

	// Object input marks loaded and round-trips the row.
	var got BelongsTo[Org]
	if err := json.Unmarshal([]byte(`{"ID":7,"Name":"acme"}`), &got); err != nil {
		t.Fatalf("Unmarshal object: %v", err)
	}
	if !got.Loaded() {
		t.Fatal("object input must mark the relation loaded")
	}
	if row := got.Row(); row == nil || *row != (Org{ID: 7, Name: "acme"}) {
		t.Fatalf("row: %+v", row)
	}

	// Loaded-nil does not survive a JSON round-trip.
	var rt BelongsTo[Org]
	if err := json.Unmarshal([]byte(mustJSON(t, none)), &rt); err != nil {
		t.Fatalf("round-trip loaded-nil: %v", err)
	}
	if rt.Loaded() {
		t.Fatal("loaded-nil must round-trip through JSON as unloaded")
	}

	// Malformed input errors without loading the container.
	var bad BelongsTo[Org]
	if err := json.Unmarshal([]byte(`"oops"`), &bad); err == nil {
		t.Fatal("string payload must fail to unmarshal into a single-value relation")
	}
	if bad.Loaded() {
		t.Fatal("failed unmarshal must not mark loaded")
	}
}
