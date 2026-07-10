package rio

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
)

// chunkRow has three always-bound columns (noautoincr keeps explicit IDs off
// the backfill path), so SQLite's 999-parameter ceiling gives 333-row chunks.
type chunkRow struct {
	ID int64 `rio:",noautoincr"`
	A  int64
	B  int64
}

func mkChunkRows(n int) []chunkRow {
	rows := make([]chunkRow, n)
	for i := range rows {
		rows[i] = chunkRow{ID: int64(i + 1), A: 1, B: 2}
	}
	return rows
}

// batchCrudEntries counts grammar.crud entries keyed with a VALUES tuple
// count — the axis the cache key must bound.
func batchCrudEntries(g *grammar) int {
	n := 0
	g.crud.Range(func(k, _ any) bool {
		if k.(crudKey).rows > 0 {
			n++
		}
		return true
	})
	return n
}

func TestBatchSQLCacheOnlyFullChunks(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	ctx := context.Background()

	// A lone tail chunk renders directly and caches nothing.
	if err := InsertAll(ctx, db, mkChunkRows(100)); err != nil {
		t.Fatal(err)
	}
	if got := batchCrudEntries(db.g); got != 0 {
		t.Fatalf("tail-only InsertAll cached %d batch entries, want 0", got)
	}

	// Sweeping total row counts produces many distinct tail sizes but only
	// one full-chunk shape (333 rows); the cache must hold exactly that one.
	totals := []int{333, 400, 500, 665, 666, 800, 999, 47, 1000, 333}
	for _, n := range totals {
		if err := InsertAll(ctx, db, mkChunkRows(n)); err != nil {
			t.Fatal(err)
		}
	}
	if got := batchCrudEntries(db.g); got != 1 {
		t.Fatalf("batch-keyed cache entries = %d, want 1 (the full 333-row chunk only)", got)
	}

	// Replaying the same mix adds nothing: entries are keyed by shape, and
	// full-chunk-only keying removes the workload-diversity dimension.
	for _, n := range totals {
		if err := InsertAll(ctx, db, mkChunkRows(n)); err != nil {
			t.Fatal(err)
		}
	}
	if got := batchCrudEntries(db.g); got != 1 {
		t.Fatalf("batch-keyed cache entries after replay = %d, want 1", got)
	}
}

func TestBatchSQLCacheHitSkipsRender(t *testing.T) {
	f := newFakeDB()
	db := f.open(SQLite)
	ctx := context.Background()

	if err := InsertAll(ctx, db, mkChunkRows(333)); err != nil {
		t.Fatal(err)
	}
	// Swap the cached text for a sentinel: a repeated full chunk must execute
	// it verbatim — proof the statement came from the cache, not a re-render.
	replaced := false
	db.g.crud.Range(func(k, _ any) bool {
		if k.(crudKey).rows == 333 {
			db.g.crud.Store(k, "SENTINEL FULL CHUNK")
			replaced = true
			return false
		}
		return true
	})
	if !replaced {
		t.Fatal("no cached full-chunk entry found")
	}
	if err := InsertAll(ctx, db, mkChunkRows(333)); err != nil {
		t.Fatal(err)
	}
	logged := f.logged()
	if got := logged[len(logged)-1]; got != "SENTINEL FULL CHUNK" {
		t.Fatalf("full chunk re-rendered instead of hitting the cache: %s", got)
	}
}

// upsertShapeRow exercises every key dimension: an omitzero column varies
// the insert bitmap, and plain columns feed conflict/update variants.
type upsertShapeRow struct {
	ID    int64
	Email string
	Age   int64
	Note  string `rio:",omitzero"`
}

var upsertShapeCols = []string{"id", "email", "age", "note"}

func upsertShapeResult() ([]string, []driver.Value) {
	return upsertShapeCols, []driver.Value{int64(1), "a@x", int64(30), "n"}
}

// runUpsertLogged performs one Upsert on db and returns the SQL it executed.
func runUpsertLogged(t *testing.T, f *fakeDB, db *DB, row upsertShapeRow, opts ...UpsertOption) string {
	t.Helper()
	needsRows := true
	for _, opt := range opts {
		probe := upsertSpec{}
		opt(&probe)
		if probe.doNothing {
			needsRows = false
		}
	}
	if db.g.d.caps().returning && needsRows {
		cols, vals := upsertShapeResult()
		f.queueRows(cols, vals)
	}
	before := len(f.logged())
	if err := Upsert(context.Background(), db, &row, opts...); err != nil {
		t.Fatal(err)
	}
	logged := f.logged()
	if len(logged) != before+1 {
		t.Fatalf("expected 1 statement, got %d", len(logged)-before)
	}
	return logged[before]
}

// TestUpsertCachedSQLMatchesFreshRender pins the cache-transparency
// contract: the second call with an identical shape (a cache hit) must
// execute byte-identical SQL to the first (a fresh render), across dialects
// and spec shapes.
func TestUpsertCachedSQLMatchesFreshRender(t *testing.T) {
	row := upsertShapeRow{ID: 1, Email: "a@x", Age: 30, Note: "n"}
	shapes := []struct {
		name string
		opts []UpsertOption
	}{
		{"do-update", []UpsertOption{OnConflict("email"), DoUpdate("age")}},
		{"do-update-default", []UpsertOption{OnConflict("email")}},
		{"multi-conflict", []UpsertOption{OnConflict("email", "age"), DoUpdate("note")}},
		{"do-nothing", []UpsertOption{DoNothing()}},
		{"do-nothing-target", []UpsertOption{OnConflict("email"), DoNothing()}},
	}
	for _, d := range []Dialect{Postgres, SQLite, MySQL} {
		for _, tc := range shapes {
			f := newFakeDB()
			db := f.open(d)
			first := runUpsertLogged(t, f, db, row, tc.opts...)
			second := runUpsertLogged(t, f, db, row, tc.opts...)
			if first != second {
				t.Errorf("%s/%s: cached SQL differs from fresh render:\n first: %s\nsecond: %s",
					d.name(), tc.name, first, second)
			}
		}
	}
}

// TestUpsertCacheKeySeparation interleaves distinct shapes on one handle and
// asserts no shape ever receives another shape's cached text.
func TestUpsertCacheKeySeparation(t *testing.T) {
	f := newFakeDB()
	db := f.open(Postgres)
	row := upsertShapeRow{ID: 1, Email: "a@x", Age: 30, Note: "n"}
	zeroNote := upsertShapeRow{ID: 1, Email: "a@x", Age: 30} // omitzero column skipped: different bits

	shapes := []struct {
		name string
		row  upsertShapeRow
		opts []UpsertOption
	}{
		{"conflict-email", row, []UpsertOption{OnConflict("email"), DoUpdate("age")}},
		{"conflict-age", row, []UpsertOption{OnConflict("age"), DoUpdate("note")}},
		{"conflict-order-ab", row, []UpsertOption{OnConflict("email", "age"), DoUpdate("note")}},
		{"conflict-order-ba", row, []UpsertOption{OnConflict("age", "email"), DoUpdate("note")}},
		{"default-set", row, []UpsertOption{OnConflict("email")}},
		{"do-nothing", row, []UpsertOption{DoNothing()}},
		{"omitzero-skipped", zeroNote, []UpsertOption{OnConflict("email"), DoUpdate("age")}},
	}
	firstSQL := make([]string, len(shapes))
	for i, tc := range shapes {
		firstSQL[i] = runUpsertLogged(t, f, db, tc.row, tc.opts...)
	}
	for i, tc := range shapes {
		got := runUpsertLogged(t, f, db, tc.row, tc.opts...)
		if got != firstSQL[i] {
			t.Errorf("%s: second pass SQL changed:\n first: %s\nsecond: %s", tc.name, firstSQL[i], got)
		}
		for j := range shapes {
			if j != i && got == firstSQL[j] {
				t.Errorf("%s rendered %s's SQL: %s", tc.name, shapes[j].name, got)
			}
		}
	}
}

// TestUpsertDoUpdateCanonicalOrder pins the whitelist normalization: listing
// order and duplicates cannot change the rendered statement (and therefore
// cannot multiply cache entries).
func TestUpsertDoUpdateCanonicalOrder(t *testing.T) {
	row := upsertShapeRow{ID: 1, Email: "a@x", Age: 30, Note: "n"}
	variants := [][]UpsertOption{
		{OnConflict("email"), DoUpdate("age", "note")},
		{OnConflict("email"), DoUpdate("note", "age")},
		{OnConflict("email"), DoUpdate("note", "age", "note")},
	}
	var want string
	for i, opts := range variants {
		f := newFakeDB()
		db := f.open(Postgres)
		got := runUpsertLogged(t, f, db, row, opts...)
		if i == 0 {
			want = got
			continue
		}
		if got != want {
			t.Errorf("variant %d rendered differently:\nwant %s\n got %s", i, want, got)
		}
	}
}

// TestUpsertSpecKeyDistinctness unit-tests the key encoding against
// collisions between different conflict shapes.
func TestUpsertSpecKeyDistinctness(t *testing.T) {
	p, err := planOf[upsertShapeRow]()
	if err != nil {
		t.Fatal(err)
	}
	email, age := p.byColumn["email"], p.byColumn["age"]
	key := func(conflict []string, update []*field, doNothing, keepTrashed bool) string {
		return upsertSpecKey(&upsertSpec{conflict: conflict, doNothing: doNothing, keepTrashed: keepTrashed}, update)
	}
	keys := map[string]string{
		"base":            key([]string{"a", "b"}, []*field{email}, false, false),
		"join-boundary-1": key([]string{"a", "bc"}, []*field{email}, false, false),
		"join-boundary-2": key([]string{"ab", "c"}, []*field{email}, false, false),
		"conflict-order":  key([]string{"b", "a"}, []*field{email}, false, false),
		"update-set":      key([]string{"a", "b"}, []*field{age}, false, false),
		"update-both":     key([]string{"a", "b"}, []*field{email, age}, false, false),
		"do-nothing":      key([]string{"a", "b"}, nil, true, false),
		"keep-trashed":    key([]string{"a", "b"}, []*field{email}, false, true),
	}
	seen := map[string]string{}
	for name, k := range keys {
		if prev, dup := seen[k]; dup {
			t.Errorf("specs %s and %s collide on key %q", prev, name, k)
		}
		seen[k] = name
	}
	if a, b := key([]string{"a", "b"}, []*field{email}, false, false), keys["base"]; a != b {
		t.Error("identical specs produced different keys")
	}
}

// TestUpsertKeepTrashedRendersDistinct pins the keepTrashed key dimension on
// a soft-deleting model: with it, the conflict clause must not clear
// deleted_at, and the two shapes must not share cached text.
func TestUpsertKeepTrashedRendersDistinct(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(Postgres)
	run := func(opts ...UpsertOption) string {
		u := &User{ID: 1, Email: "a@x", Age: 30}
		f.queueRows(userCols, userRow(1, "a@x"))
		before := len(f.logged())
		if err := Upsert(ctx, db, u, opts...); err != nil {
			t.Fatal(err)
		}
		return f.logged()[before]
	}
	plain := run(OnConflict("email"), DoUpdate("age"))
	kept := run(OnConflict("email"), DoUpdate("age"), KeepTrashed())
	plain2 := run(OnConflict("email"), DoUpdate("age"))
	if plain == kept {
		t.Errorf("KeepTrashed shares SQL with the restoring form: %s", plain)
	}
	if plain != plain2 {
		t.Errorf("restoring form changed after KeepTrashed call:\n first: %s\n third: %s", plain, plain2)
	}
	if !strings.Contains(plain, `"deleted_at" = NULL`) {
		t.Errorf("restoring form must clear deleted_at: %s", plain)
	}
	if strings.Contains(kept, `"deleted_at" = NULL`) {
		t.Errorf("KeepTrashed form must not clear deleted_at: %s", kept)
	}
}

// TestUpsertAllFullChunksCached extends the InsertAll bound to UpsertAll:
// varied totals leave exactly one batch-keyed entry per conflict shape, a
// repeated full chunk hits the cache, and cached SQL equals a fresh render.
func TestUpsertAllFullChunksCached(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(SQLite)
	opts := []UpsertOption{OnConflict("id"), DoUpdate("a")}

	for _, n := range []int{100, 333, 400, 500, 999, 47, 333} {
		if err := UpsertAll(ctx, db, mkChunkRows(n), opts...); err != nil {
			t.Fatal(err)
		}
	}
	if got := batchCrudEntries(db.g); got != 1 {
		t.Fatalf("batch-keyed cache entries = %d, want 1", got)
	}

	// Full-chunk statements must be byte-stable across calls (fresh render
	// on another handle vs cache hit here).
	full := f.loggedContaining("VALUES")
	var chunkSQL string
	for _, s := range full {
		if len(s.args) == 999 {
			if chunkSQL == "" {
				chunkSQL = s.sql
			} else if s.sql != chunkSQL {
				t.Fatalf("full-chunk SQL not stable:\n%s\n%s", chunkSQL, s.sql)
			}
		}
	}
	if chunkSQL == "" {
		t.Fatal("no full-chunk statement logged")
	}
	f2 := newFakeDB()
	db2 := f2.open(SQLite)
	if err := UpsertAll(ctx, db2, mkChunkRows(333), opts...); err != nil {
		t.Fatal(err)
	}
	if fresh := f2.logged()[0]; fresh != chunkSQL {
		t.Fatalf("fresh render differs from cached text:\ncached: %s\n fresh: %s", chunkSQL, fresh)
	}

	// And the sentinel proof that a repeated full chunk skips the render.
	db.g.crud.Range(func(k, _ any) bool {
		if k.(crudKey).rows == 333 {
			db.g.crud.Store(k, "SENTINEL UPSERT CHUNK")
			return false
		}
		return true
	})
	if err := UpsertAll(ctx, db, mkChunkRows(333), opts...); err != nil {
		t.Fatal(err)
	}
	logged := f.logged()
	if got := logged[len(logged)-1]; got != "SENTINEL UPSERT CHUNK" {
		t.Fatalf("full upsert chunk re-rendered instead of hitting the cache: %s", got)
	}
}
