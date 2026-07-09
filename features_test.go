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

type Profile struct {
	ID     int64
	UserID int64
	Nick   string
}

type Owner struct {
	ID      int64
	Profile HasOne[Profile] `rio:",fk:user_id"`
}

func TestPreloadHasOne(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id"}, []driver.Value{int64(1)}, []driver.Value{int64(2)})
	f.queueRows([]string{"id", "user_id", "nick"}, []driver.Value{int64(9), int64(1), "gopher"})

	owners, err := From[Owner]().With("Profile").All(context.Background(), db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if p := owners[0].Profile.Row(); p == nil || p.Nick != "gopher" {
		t.Fatalf("owner1 profile: %+v", p)
	}
	if p := owners[1].Profile.Row(); p != nil {
		t.Fatalf("owner2 has none, loaded-nil expected: %+v", p)
	}
}

func TestFirstOrCreate(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	// Miss → insert.
	f.queueRows(userCols)
	f.queueRows([]string{"id"}, []driver.Value{int64(5)})
	u := &User{Email: "new@x"}
	if err := From[User]().Where("email = ?", "new@x").FirstOrCreate(ctx, db, u); err != nil {
		t.Fatalf("FirstOrCreate insert path: %v", err)
	}
	if u.ID != 5 {
		t.Fatalf("insert path backfill: %d", u.ID)
	}

	// Hit → no insert.
	f.queueRows(userCols, userRow(7, "hit@x"))
	v := &User{Email: "hit@x"}
	if err := From[User]().Where("email = ?", "hit@x").FirstOrCreate(ctx, db, v); err != nil {
		t.Fatalf("FirstOrCreate hit path: %v", err)
	}
	if v.ID != 7 {
		t.Fatalf("hit path must adopt the found row: %+v", v)
	}
	if n := len(f.loggedContaining("INSERT")); n != 1 {
		t.Fatalf("hit path must not insert, saw %d inserts", n)
	}
}

func TestCreateOrFirstRaceAndTombstone(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	dup := errors.New("unique dup")
	dbT := f.openWith(Postgres, WithErrorTranslator(func(err error) error {
		if errors.Is(err, dup) {
			return ErrDuplicateKey
		}
		return nil
	}))

	// Insert conflicts → find wins the race.
	f.failContaining("INSERT", dup)
	f.queueRows(userCols, userRow(3, "race@x"))
	u := &User{Email: "race@x"}
	if err := From[User]().Where("email = ?", "race@x").CreateOrFirst(ctx, dbT, u); err != nil {
		t.Fatalf("CreateOrFirst: %v", err)
	}
	if u.ID != 3 {
		t.Fatalf("must adopt existing row: %+v", u)
	}

	// Insert conflicts and find misses: a soft-deleted tombstone holds the
	// key — the error says so.
	f.queueRows(userCols)
	err := From[User]().Where("email = ?", "ghost@x").CreateOrFirst(ctx, dbT, &User{Email: "ghost@x"})
	if !errors.Is(err, ErrDuplicateKey) || !strings.Contains(err.Error(), "soft-deleted") {
		t.Fatalf("tombstone hint: %v", err)
	}
	_ = db
}

func TestStmtCache(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.openWith(SQLite, WithStmtCache(2))

	f.queueRows(userCols)
	f.queueRows(userCols)
	if _, err := From[User]().Where("age > ?", 1).All(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := From[User]().Where("age > ?", 2).All(ctx, db); err != nil {
		t.Fatal(err)
	}
	if len(f.prepped) != 1 {
		t.Fatalf("same shape must prepare once, prepared %d times: %v", len(f.prepped), f.prepped)
	}

	// Transactions bypass the cache.
	before := len(f.prepped)
	_ = db.Tx(ctx, func(tx *Tx) error {
		f.queueRows(userCols)
		_, err := From[User]().Where("age > ?", 3).All(ctx, tx)
		return err
	})
	if len(f.prepped) != before {
		t.Fatal("transactions must not prepare through the cache")
	}
}

type recordingHook struct {
	events []string
	rows   []int64
}

func (h *recordingHook) BeforeQuery(ctx context.Context, e *QueryEvent) context.Context {
	return context.WithValue(ctx, hookKey{}, e.Op)
}

func (h *recordingHook) AfterQuery(ctx context.Context, e *QueryEvent) {
	if ctx.Value(hookKey{}) != e.Op {
		panic("hook context must flow from Before to After")
	}
	h.events = append(h.events, e.Op+":"+e.Model)
	h.rows = append(h.rows, e.RowsAffected)
}

type hookKey struct{}

func TestQueryHookSeesEverything(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	hook := &recordingHook{}
	db := f.openWith(SQLite, WithQueryHook(hook))

	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	_ = db.Tx(ctx, func(tx *Tx) error {
		return Insert(ctx, tx, &Post{Title: "x", UserID: 1})
	})
	joined := strings.Join(hook.events, " ")
	for _, want := range []string{"begin:", "insert:Post", "commit:"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("hook missed %q in %q", want, joined)
		}
	}
}

func TestQueryHookArgRedaction(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	var sawArgs []any
	hook := hookFunc(func(_ context.Context, e *QueryEvent) { sawArgs = e.Args })
	db := f.openWith(SQLite, WithQueryHook(hook), WithoutArgs())

	f.queueRows(userCols)
	_, _ = From[User]().Where("email = ?", "secret@x").All(ctx, db)
	if sawArgs != nil {
		t.Fatalf("WithoutArgs must redact: %v", sawArgs)
	}
}

type hookFunc func(context.Context, *QueryEvent)

func (hookFunc) BeforeQuery(ctx context.Context, _ *QueryEvent) context.Context { return ctx }
func (f hookFunc) AfterQuery(ctx context.Context, e *QueryEvent)                { f(ctx, e) }

func TestForUpdateCapability(t *testing.T) {
	ctx := context.Background()
	fpg := newFakeDB()
	pg := fpg.open(Postgres)
	fpg.queueRows(userCols)
	_, _ = From[User]().Where("id = ?", 1).ForUpdate().All(ctx, pg)
	if !strings.HasSuffix(fpg.logged()[0], " FOR UPDATE") {
		t.Fatalf("pg: %s", fpg.logged()[0])
	}

	flite := newFakeDB()
	lite := flite.open(SQLite)
	flite.queueRows(userCols)
	_, _ = From[User]().Where("id = ?", 1).ForUpdate().All(ctx, lite)
	if strings.Contains(flite.logged()[0], "FOR UPDATE") {
		t.Fatalf("sqlite locks the whole db, FOR UPDATE must be a no-op: %s", flite.logged()[0])
	}
}

type Grant struct {
	UserID int64  `rio:",pk"`
	Scope  string `rio:",pk"`
	Level  int
}

func TestCompositePrimaryKey(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(SQLite)

	f.queueRows([]string{"user_id", "scope", "level"}, []driver.Value{int64(1), "repo", int64(2)})
	g, err := Find[Grant](ctx, db, 1, "repo")
	if err != nil || g.Level != 2 {
		t.Fatalf("Find: %v %+v", err, g)
	}
	if !strings.Contains(f.logged()[0], `"user_id" = ? AND "grants"."scope" = ?`) {
		t.Fatalf("composite where: %s", f.logged()[0])
	}

	f.queueExec(0, 1)
	g.Level = 3
	if err := Update(ctx, db, g); err != nil {
		t.Fatalf("Update: %v", err)
	}
	upd := f.logged()[1]
	if !strings.Contains(upd, `WHERE "user_id" = ? AND "scope" = ?`) {
		t.Fatalf("composite update where: %s", upd)
	}

	f.queueExec(0, 1)
	if err := Delete(ctx, db, g); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !strings.Contains(f.logged()[2], `DELETE FROM "grants" WHERE "user_id" = ? AND "scope" = ?`) {
		t.Fatalf("composite delete: %s", f.logged()[2])
	}
}

func TestTableNamerAndTableName(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.openWith(Postgres, WithTableNamer(func(s string) string { return "app_" + snakeCase(s) }))

	f.queueRows([]string{"id", "email", "age", "bio", "version", "deleted_at", "created_at", "updated_at"})
	_, _ = From[User]().All(ctx, db)
	if !strings.Contains(f.logged()[0], `FROM "app_user"`) {
		t.Fatalf("namer: %s", f.logged()[0])
	}
}

type Legacy struct {
	ID int64
}

func (Legacy) TableName() string { return "legacy_things" }

func TestTableNameOverrideWins(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.openWith(Postgres, WithTableNamer(func(s string) string { return "ignored" }))

	f.queueRows([]string{"id"})
	_, _ = From[Legacy]().All(ctx, db)
	if !strings.Contains(f.logged()[0], `FROM "legacy_things"`) {
		t.Fatalf("TableName must beat the namer: %s", f.logged()[0])
	}
}

func TestRelWithTrashed(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	// Sub 是软删模型:默认过滤,RelWithTrashed 放开。
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	f.queueRows([]string{"id", "holder_id", "deleted_at"})
	_, err := From[Holder]().With("Subs").All(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.logged()[1], `"deleted_at" IS NULL`) {
		t.Fatalf("preload filters trashed by default: %s", f.logged()[1])
	}

	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	f.queueRows([]string{"id", "holder_id", "deleted_at"})
	_, err = From[Holder]().With("Subs", RelWithTrashed()).All(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	logs := f.logged()
	if strings.Contains(logs[3], "IS NULL") {
		t.Fatalf("RelWithTrashed must not filter: %s", logs[3])
	}
}

type Holder struct {
	ID   int64
	Subs HasMany[Sub]
}

type Sub struct {
	ID        int64
	HolderID  int64
	DeletedAt *time.Time `rio:",softdelete"`
}

// --- audit regression: opus audit before v0.1.0 ---

type Doc struct {
	ID     int64
	Config *Prefs `rio:",json"`
	Note   *sql.NullString
}

type Prefs struct {
	Theme string `json:"theme"`
}

// Audit: a nil *T json field stores SQL NULL but the scan side had no NULL
// arm for scanJSON — rio wrote rows it could not read back.
func TestJSONPointerNullRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(SQLite)

	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	if err := Insert(ctx, db, &Doc{}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	ins := f.loggedContaining("INSERT")[0]
	if ins.args[0] != nil {
		t.Fatalf("nil json pointer must bind SQL NULL, got %v", ins.args[0])
	}

	// Audit: *sql.NullString fields panicked in slowScanner (nil receiver);
	// both fixes verify in one scan.
	f.queueRows([]string{"id", "config", "note"}, []driver.Value{int64(1), nil, nil})
	got, err := Find[Doc](ctx, db, 1)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.Config != nil || got.Note != nil {
		t.Fatalf("NULLs must come back as nil: %+v", got)
	}

	f.queueRows([]string{"id", "config", "note"}, []driver.Value{int64(1), []byte(`{"theme":"dark"}`), "hi"})
	got, err = Find[Doc](ctx, db, 1)
	if err != nil {
		t.Fatalf("Find non-null: %v", err)
	}
	if got.Config == nil || got.Config.Theme != "dark" {
		t.Fatalf("json pointer: %+v", got.Config)
	}
	if got.Note == nil || !got.Note.Valid || got.Note.String != "hi" {
		t.Fatalf("pointer Scanner must allocate and scan: %+v", got.Note)
	}
}

type Counter struct {
	ID      int64
	N       int
	Version uint32 `rio:",version"`
}

// Audit: a zero unsigned version column hit SetInt and panicked.
func TestUnsignedVersionColumn(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(MySQL)
	f.queueExec(1, 1)

	c := &Counter{N: 1}
	if err := Insert(ctx, db, c); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if c.Version != 1 {
		t.Fatalf("unsigned version must initialize to 1, got %d", c.Version)
	}
}

type Lookup struct {
	ID   int64
	Slug string
}

// Audit: a model whose every column is a key or maintained rendered
// "DO UPDATE SET" with no assignments — invalid SQL on all three dialects.
func TestUpsertEmptyUpdateSetRefused(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	err := Upsert(ctx, db, &Lookup{Slug: "x"}, OnConflict("slug", "id"))
	if err == nil || !strings.Contains(err.Error(), "DoNothing") {
		t.Fatalf("empty conflict update set must error with guidance: %v", err)
	}
	if n := len(f.logged()); n != 0 {
		t.Fatalf("nothing may execute, logged %d statements", n)
	}
}

// Audit: OFFSET without LIMIT is invalid SQL on MySQL and SQLite.
func TestOffsetWithoutLimit(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		d    Dialect
		want string
	}{
		{Postgres, " OFFSET 5"},
		{MySQL, " LIMIT 18446744073709551615 OFFSET 5"},
		{SQLite, " LIMIT -1 OFFSET 5"},
	} {
		f := newFakeDB()
		db := f.open(tc.d)
		f.queueRows([]string{"id"})
		_, _ = From[Org]().Offset(5).All(ctx, db)
		if got := f.logged()[0]; !strings.HasSuffix(got, tc.want) {
			t.Fatalf("%s: %s (want suffix %q)", tc.d.name(), got, tc.want)
		}
	}
}

// Audit: Exists on a query that already had LIMIT/OFFSET doubled the LIMIT.
func TestExistsIgnoresUserLimit(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"1"})
	_, _ = From[Org]().Limit(10).Offset(5).Exists(ctx, db)
	got := f.logged()[0]
	if strings.Count(got, "LIMIT") != 1 || strings.Contains(got, "OFFSET") {
		t.Fatalf("Exists renders exactly one probe LIMIT: %s", got)
	}
}

// Audit: DoNothing on PG/SQLite never backfilled a fresh insert's PK while
// MySQL did; now RETURNING reports generated columns and conflicts no-op.
func TestUpsertDoNothingBackfill(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	f.queueRows([]string{"id"}, []driver.Value{int64(41)}) // fresh insert
	u := &User{Email: "a@x"}
	if err := Upsert(ctx, db, u, OnConflict("email"), DoNothing()); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if u.ID != 41 {
		t.Fatalf("fresh insert must backfill: %d", u.ID)
	}
	if !strings.Contains(f.logged()[0], `DO NOTHING RETURNING "id"`) {
		t.Fatalf("sql: %s", f.logged()[0])
	}

	f.queueRows([]string{"id"}) // conflict: zero rows
	v := &User{Email: "a@x"}
	if err := Upsert(ctx, db, v, OnConflict("email"), DoNothing()); err != nil {
		t.Fatalf("Upsert conflict: %v", err)
	}
	if v.ID != 0 {
		t.Fatal("conflict path must leave the struct as given")
	}
}

func TestRestoreEntity(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueExec(0, 1)

	del := testNow
	u := &User{ID: 5, Email: "a@x", Version: 2, DeletedAt: &del}
	if err := Restore(ctx, db, u); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got := f.logged()[0]
	want := `UPDATE "users" SET "deleted_at" = NULL, "updated_at" = ?, "version" = "version" + 1 WHERE "id" = ? AND "version" = ?`
	if got != want {
		t.Fatalf("sql:\n got: %s\nwant: %s", got, want)
	}
	if u.DeletedAt != nil || u.Version != 3 {
		t.Fatalf("write-back: %+v", u)
	}
}

// Codex review: Upsert(DoNothing) without OnConflict rendered the invalid
// "ON CONFLICT ()" on PG/SQLite; the idempotent-insert shape must work bare.
func TestUpsertDoNothingWithoutTarget(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id"}, []driver.Value{int64(9)})

	u := &User{Email: "a@x"}
	if err := Upsert(ctx, db, u, DoNothing()); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got := f.logged()[0]
	if !strings.Contains(got, ") ON CONFLICT DO NOTHING") || strings.Contains(got, "ON CONFLICT ()") {
		t.Fatalf("bare DoNothing must omit the empty target: %s", got)
	}
}

// Codex review: partial entity scans through Raw risked zeroed-out writes;
// missing mapped columns now fail with DTO guidance.
func TestRawPartialEntityRefused(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id", "email"}, []driver.Value{int64(1), "a@x"})

	_, err := Raw[User]("SELECT id, email FROM users").All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "DTO") {
		t.Fatalf("partial entity scan must error with guidance: %v", err)
	}
}

// Codex review: uint64 keys above MaxInt64 must bind on query paths too,
// not just through entity writes.
func TestHugeUint64QueryArgs(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(MySQL)
	f.queueRows([]string{"id", "org_id"})

	huge := uint64(1) << 63
	if _, err := From[Account]().Where("id = ?", huge).All(ctx, db); err != nil {
		t.Fatalf("huge uint64 arg: %v", err)
	}
	stmt := f.loggedContaining("SELECT")[0]
	if s, ok := stmt.args[0].(string); !ok || s != "9223372036854775808" {
		t.Fatalf("huge uint64 must bind as decimal string, got %T %v", stmt.args[0], stmt.args[0])
	}
}
