package rio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
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

type mutatingHook struct{}

func (mutatingHook) BeforeQuery(ctx context.Context, e *QueryEvent) context.Context {
	e.Args[0] = "mutated"
	e.Args[1].([]byte)[0] = 'z'
	return ctx
}

func (mutatingHook) AfterQuery(context.Context, *QueryEvent) {}

func TestQueryHookCannotMutateExecutedArgs(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.openWith(SQLite, WithQueryHook(mutatingHook{}))

	payload := []byte("abc")
	if _, err := Exec(ctx, db, "INSERT INTO audit VALUES (?, ?)", "original", payload); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	args := f.logged()[0]
	stmt := f.loggedContaining("INSERT")[0]
	if stmt.args[0] != "original" {
		t.Fatalf("hook mutation reached executed scalar arg: %#v", stmt.args[0])
	}
	if got := string(stmt.args[1].([]byte)); got != "abc" {
		t.Fatalf("hook mutation reached executed []byte arg: %q", got)
	}
	if args == "" {
		t.Fatal("statement was not logged")
	}
}

func TestNilHookAndClockOptionsAreIgnored(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.openWith(SQLite, WithQueryHook(nil), WithClock(nil))

	f.queueRows(userCols)
	if _, err := From[User]().All(ctx, db); err != nil {
		t.Fatalf("nil options must not poison later queries: %v", err)
	}
}

func TestNewRejectsNilInputs(t *testing.T) {
	mustPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic", name)
			}
		}()
		fn()
	}
	mustPanic("nil db", func() { New(nil, SQLite) })
	mustPanic("nil dialect", func() { New(sql.OpenDB(fakeConnector{newFakeDB()}), nil) })
}

func TestNilRowsReturnErrors(t *testing.T) {
	ctx := context.Background()
	db := newFakeDB().open()
	var user *User
	var account *Account

	for name, err := range map[string]error{
		"Insert":       Insert(ctx, db, user),
		"Update":       Update(ctx, db, user),
		"Delete":       Delete(ctx, db, user),
		"ForceDelete":  ForceDelete(ctx, db, user),
		"Restore":      Restore(ctx, db, user),
		"Upsert":       Upsert(ctx, db, user, OnConflict("email")),
		"Attach":       Attach[Account, int64](ctx, db, account, "Tags", 1),
		"Detach":       Detach[Account, int64](ctx, db, account, "Tags", 1),
		"SyncRelation": SyncRelation[Account, int64](ctx, db, account, "Tags", []int64{1}),
	} {
		if err == nil || !strings.Contains(err.Error(), "nil") {
			t.Fatalf("%s must return a nil-row error, got %v", name, err)
		}
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

func TestNegativeLimitOffsetRejected(t *testing.T) {
	ctx := context.Background()
	db := newFakeDB().open(SQLite)

	if _, err := From[User]().Limit(-1).All(ctx, db); err == nil || !strings.Contains(err.Error(), "Limit") {
		t.Fatalf("negative Limit must be refused: %v", err)
	}
	if _, err := From[User]().Offset(-1).All(ctx, db); err == nil || !strings.Contains(err.Error(), "Offset") {
		t.Fatalf("negative Offset must be refused: %v", err)
	}
	if _, err := Pluck[string](ctx, db, From[User]().Limit(-2), "email"); err == nil || !strings.Contains(err.Error(), "Limit") {
		t.Fatalf("negative Limit in Pluck must be refused: %v", err)
	}

	f := newFakeDB()
	db = f.open(SQLite)
	f.queueRows(userCols, userRow(1, "a@x"))
	if _, err := From[User]().With("Posts", RelLimit(-1)).All(ctx, db); err == nil || !strings.Contains(err.Error(), "RelLimit") {
		t.Fatalf("negative RelLimit must be refused: %v", err)
	}
}

func TestPreloadRelWhereBindLimit(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueRows(userCols, userRow(1, "a@x"))
	titles := make([]string, 999)
	for i := range titles {
		titles[i] = "x"
	}

	_, err := From[User]().With("Posts", RelWhere("title IN (?)", titles)).All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "leaving none for parent keys") {
		t.Fatalf("RelWhere exhausting bind limit must fail clearly: %v", err)
	}
	if len(f.logged()) != 1 {
		t.Fatalf("relation query must not execute after bind-limit error: %v", f.logged())
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

type RawDuplicateColumn struct {
	Email string
}

func TestRawDuplicateColumnsRefused(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"email", "email"}, []driver.Value{"first", "second"})

	_, err := Raw[RawDuplicateColumn]("SELECT a AS email, b AS email FROM users").All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "appears more than once") {
		t.Fatalf("duplicate Raw result column must be refused: %v", err)
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

// --- v0.2: WhereHas / WhereHasNot ---

func TestWhereHas(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols)

	_, err := From[User]().WhereHas("Posts", RelWhere("title <> ?", "")).All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	got := f.logged()[0]
	want := `EXISTS (SELECT 1 FROM "posts" AS "rio_h1" WHERE "rio_h1"."user_id" = "users"."id" AND (title <> $1))`
	if !strings.Contains(got, want) {
		t.Fatalf("sql:\n got: %s\nwant fragment: %s", got, want)
	}
}

func TestWhereHasNotAndNested(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols)

	_, err := From[User]().WhereHasNot("Posts.Author").All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	got := f.logged()[0]
	for _, frag := range []string{
		`NOT EXISTS (SELECT 1 FROM "posts" AS "rio_h1"`,
		`AND EXISTS (SELECT 1 FROM "users" AS "rio_h2" WHERE "rio_h2"."id" = "rio_h1"."user_id"`,
		`AND "rio_h2"."deleted_at" IS NULL`, // nested soft-delete filtering
	} {
		if !strings.Contains(got, frag) {
			t.Fatalf("missing %q in:\n%s", frag, got)
		}
	}
}

func TestWhereHasManyToMany(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id", "org_id"})

	_, err := From[Account]().WhereHas("Tags", RelWhere("name = ?", "go")).All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	got := f.logged()[0]
	want := `EXISTS (SELECT 1 FROM "account_tags" AS "rio_j1" INNER JOIN "tags" AS "rio_h1" ON "rio_j1"."tag_id" = "rio_h1"."id" WHERE "rio_j1"."account_id" = "accounts"."id" AND (name = $1))`
	if !strings.Contains(got, want) {
		t.Fatalf("m2m exists:\n got: %s\nwant fragment: %s", got, want)
	}
}

func TestWhereHasWorksOnSetOps(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueExec(0, 2)

	n, err := From[User]().WhereHas("Posts").UpdateAll(ctx, db, Set{"age": 1})
	if err != nil || n != 2 {
		t.Fatalf("UpdateAll: %v n=%d", err, n)
	}
	if !strings.Contains(f.logged()[0], "EXISTS (SELECT 1 FROM") {
		t.Fatalf("sql: %s", f.logged()[0])
	}
}

func TestCompiledRejectsParamedWhereHas(t *testing.T) {
	_, err := Compile[User](From[User]().Where("age > ?").WhereHas("Posts", RelWhere("title = ?", "x")))
	if err == nil || !strings.Contains(err.Error(), "WhereHas") {
		t.Fatalf("exec-mode compile with paramed WhereHas must be refused: %v", err)
	}
	// Fully inline compiles fine.
	if _, err := Compile[User](From[User]().Where("age > ?", 1).WhereHas("Posts", RelWhere("title = ?", "x"))); err != nil {
		t.Fatalf("inline compile: %v", err)
	}
}

// --- v0.2: WithCount + RelLimit ---

type Board struct {
	ID         int64
	PostsCount int64 `rio:",countof:Posts"`
	Posts      HasMany[BoardPost]
}

type BoardPost struct {
	ID      int64
	BoardID int64
}

func TestWithCount(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id"}, []driver.Value{int64(1)}, []driver.Value{int64(2)})
	f.queueRows([]string{"board_id", "count"}, []driver.Value{int64(1), int64(3)})

	boards, err := From[Board]().WithCount("Posts").All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	rel := f.logged()[1]
	want := `SELECT "board_posts"."board_id", count(*) FROM "board_posts" WHERE "board_posts"."board_id" IN ($1, $2) GROUP BY "board_posts"."board_id"`
	if rel != want {
		t.Fatalf("count sql:\n got: %s\nwant: %s", rel, want)
	}
	if boards[0].PostsCount != 3 || boards[1].PostsCount != 0 {
		t.Fatalf("counts: %+v", boards)
	}
	// The count target itself is not a column.
	if !strings.Contains(f.logged()[0], `SELECT "boards"."id" FROM "boards"`) {
		t.Fatalf("countof field must not map to a column: %s", f.logged()[0])
	}
}

func TestWithCountRejectsBelongsTo(t *testing.T) {
	f := newFakeDB()
	db := f.open()
	f.queueRows(postCols, []driver.Value{int64(1), int64(1), "x"})
	_, err := From[Post]().WithCount("Author").All(context.Background(), db)
	if err == nil || !strings.Contains(err.Error(), "count target") {
		t.Fatalf("BelongsTo count needs a countof target first: %v", err)
	}
}

func TestRelLimitWindowQuery(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"))
	f.queueRows(postCols,
		[]driver.Value{int64(10), int64(1), "first"},
		[]driver.Value{int64(11), int64(1), "second"},
	)

	users, err := From[User]().With("Posts", RelOrder("id DESC"), RelLimit(2)).All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	rel := f.logged()[1]
	for _, frag := range []string{
		`SELECT "id", "user_id", "title" FROM (SELECT "posts"."id", "posts"."user_id", "posts"."title", ROW_NUMBER() OVER (PARTITION BY "posts"."user_id" ORDER BY id DESC) AS "__rio_rn" FROM "posts" WHERE "posts"."user_id" IN ($1)`,
		`) AS "rio_w" WHERE "rio_w"."__rio_rn" <= 2`,
	} {
		if !strings.Contains(rel, frag) {
			t.Fatalf("missing %q in:\n%s", frag, rel)
		}
	}
	if len(users[0].Posts.Rows()) != 2 {
		t.Fatalf("rows: %+v", users[0].Posts.Rows())
	}
}

func TestRelLimitManyToMany(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id", "org_id"}, []driver.Value{int64(1), nil})
	f.queueRows([]string{"id", "name", "__rio_key"}, []driver.Value{int64(100), "go", int64(1)})

	accounts, err := From[Account]().With("Tags", RelLimit(1)).All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	rel := f.logged()[1]
	for _, frag := range []string{
		`"account_tags"."account_id" AS "__rio_key"`,
		`ROW_NUMBER() OVER (PARTITION BY "account_tags"."account_id" ORDER BY "tags"."id")`,
		`WHERE "rio_w"."__rio_rn" <= 1`,
	} {
		if !strings.Contains(rel, frag) {
			t.Fatalf("missing %q in:\n%s", frag, rel)
		}
	}
	if got := accounts[0].Tags.Rows(); len(got) != 1 || got[0].Name != "go" {
		t.Fatalf("tags: %+v", got)
	}
}

// --- v0.2: Attach/Detach, Rows, Pluck ---

func TestAttachDetach(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	acc := &Account{ID: 7}
	f.queueExec(0, 2)
	if err := Attach(ctx, db, acc, "Tags", 100, 101); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	got := f.logged()[0]
	want := `INSERT INTO "account_tags" ("account_id", "tag_id") VALUES ($1, $2), ($3, $4) ON CONFLICT DO NOTHING`
	if got != want {
		t.Fatalf("attach sql:\n got: %s\nwant: %s", got, want)
	}

	f.queueExec(0, 1)
	if err := Detach(ctx, db, acc, "Tags", 100); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	got = f.logged()[1]
	want = `DELETE FROM "account_tags" WHERE "account_id" = $1 AND "tag_id" IN ($2)`
	if got != want {
		t.Fatalf("detach sql:\n got: %s\nwant: %s", got, want)
	}

	if err := Detach[Account, int64](ctx, db, acc, "Tags"); err == nil {
		t.Fatal("Detach without ids must refuse")
	}
	if err := Attach[Account, int64](ctx, db, acc, "Tags"); err != nil {
		t.Fatalf("Attach with zero ids is a no-op: %v", err)
	}
	if err := Attach(ctx, db, &User{ID: 1}, "Posts", 1); err == nil || !strings.Contains(err.Error(), "ManyToMany") {
		t.Fatalf("HasMany attach must refuse: %v", err)
	}
}

func TestAttachMySQLForm(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(MySQL)
	f.queueExec(0, 1)
	if err := Attach(ctx, db, &Account{ID: 7}, "Tags", 100); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if !strings.Contains(f.logged()[0], "ON DUPLICATE KEY UPDATE `account_id` = `account_id`") {
		t.Fatalf("mysql attach: %s", f.logged()[0])
	}
}

func TestRowsStreams(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"), userRow(2, "b@x"), userRow(3, "c@x"))

	var seen []int64
	for u, err := range From[User]().Where("age > ?", 0).Rows(ctx, db) {
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		seen = append(seen, u.ID)
		if len(seen) == 2 {
			break // early break must close cleanly
		}
	}
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 2 {
		t.Fatalf("streamed: %v", seen)
	}

	for _, err := range From[User]().With("Posts").Rows(ctx, db) {
		if err == nil || !strings.Contains(err.Error(), "cannot stream") {
			t.Fatalf("With must refuse streaming: %v", err)
		}
		break
	}
}

func TestPluck(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"email"}, []driver.Value{"a@x"}, []driver.Value{"b@x"})

	emails, err := Pluck[string](ctx, db, From[User]().Where("age > ?", 18).OrderBy("id"), "email")
	if err != nil {
		t.Fatalf("Pluck: %v", err)
	}
	if len(emails) != 2 || emails[0] != "a@x" {
		t.Fatalf("emails: %v", emails)
	}
	got := f.logged()[0]
	want := `SELECT "users"."email" FROM "users" WHERE (age > $1) AND "users"."deleted_at" IS NULL ORDER BY id`
	if got != want {
		t.Fatalf("pluck sql:\n got: %s\nwant: %s", got, want)
	}

	if _, err := Pluck[string](ctx, db, From[User](), "no_such"); err == nil || !strings.Contains(err.Error(), "Raw") {
		t.Fatalf("unknown column must point at Raw: %v", err)
	}
}

// --- v0.3: WriteColumns, SyncRelation, Scope, Compiled.Rows ---

func TestWriteColumns(t *testing.T) {
	var buf strings.Builder
	if err := WriteColumns(&buf, "models", User{}, &Post{}); err != nil {
		t.Fatalf("WriteColumns: %v", err)
	}
	got := buf.String()
	for _, frag := range []string{
		"// Code generated by rio.WriteColumns; DO NOT EDIT.",
		"package models",
		`const PostTable = "posts"`,
		`const UserTable = "users"`,
		"var UserCols = struct {",
		"\tEmail string",
		"\tEmail: \"email\",",
		"\tDeletedAt: \"deleted_at\",",
	} {
		if !strings.Contains(got, frag) {
			t.Fatalf("missing %q in generated:\n%s", frag, got)
		}
	}
	// Models sort deterministically (Post before User).
	if strings.Index(got, "PostTable") > strings.Index(got, "UserTable") {
		t.Fatal("output must sort by model name")
	}
	// Relation containers and countof targets are not columns.
	if strings.Contains(got, "Posts ") || strings.Contains(got, "Author ") {
		t.Fatalf("relations must not appear:\n%s", got)
	}
}

func TestSyncRelation(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	acc := &Account{ID: 7}
	f.queueExec(0, 1) // delete not-in
	f.queueExec(0, 2) // attach
	if err := SyncRelation(ctx, db, acc, "Tags", []int64{100, 101}); err != nil {
		t.Fatalf("SyncRelation: %v", err)
	}
	logs := f.logged()
	joined := strings.Join(logs, " | ")
	for _, frag := range []string{
		"BEGIN",
		`DELETE FROM "account_tags" WHERE "account_id" = $1 AND "tag_id" NOT IN ($2, $3)`,
		`INSERT INTO "account_tags" ("account_id", "tag_id") VALUES ($1, $2), ($3, $4) ON CONFLICT DO NOTHING`,
		"COMMIT",
	} {
		if !strings.Contains(joined, frag) {
			t.Fatalf("missing %q in %s", frag, joined)
		}
	}

	// Empty set explicitly empties the relation.
	f2 := newFakeDB()
	db2 := f2.open()
	f2.queueExec(0, 3)
	if err := SyncRelation(ctx, db2, acc, "Tags", []int64{}); err != nil {
		t.Fatalf("SyncRelation empty: %v", err)
	}
	if !strings.Contains(strings.Join(f2.logged(), " | "), `DELETE FROM "account_tags" WHERE "account_id" = $1 |`) {
		t.Fatalf("empty sync must delete all: %v", f2.logged())
	}
}

func TestScope(t *testing.T) {
	adults := func(q Query[User]) Query[User] { return q.Where("age >= ?", 18) }
	recent := func(q Query[User]) Query[User] { return q.OrderBy("created_at DESC") }

	q := From[User]().Scope(adults, recent).Limit(5)
	if len(q.s.wheres) != 1 || len(q.s.orders) != 1 || !q.s.limitSet {
		t.Fatalf("scope composition: %+v", q.s)
	}
}

func TestCompiledRows(t *testing.T) {
	ctx := context.Background()
	q := MustCompile[User](From[User]().Where("age > ?"))

	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"), userRow(2, "b@x"))

	var ids []int64
	for u, err := range q.Rows(ctx, db, 18) {
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		ids = append(ids, u.ID)
	}
	if len(ids) != 2 || ids[1] != 2 {
		t.Fatalf("streamed: %v", ids)
	}
}

// Codex v0.3 review: concurrent SyncRelation must serialize on the owner row.
func TestSyncRelationLocksOwner(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open() // postgres: forUpdate capable
	f.queueRows([]string{"id"}, []driver.Value{int64(7)})
	f.queueExec(0, 0)
	f.queueExec(0, 1)

	if err := SyncRelation(ctx, db, &Account{ID: 7}, "Tags", []int64{100}); err != nil {
		t.Fatalf("SyncRelation: %v", err)
	}
	joined := strings.Join(f.logged(), " | ")
	lock := `SELECT "id" FROM "accounts" WHERE "id" = $1 FOR UPDATE`
	del := `DELETE FROM "account_tags"`
	if !strings.Contains(joined, lock) {
		t.Fatalf("missing owner lock in %s", joined)
	}
	if strings.Index(joined, lock) > strings.Index(joined, del) {
		t.Fatal("the lock must precede the delete")
	}

	// SQLite: single-writer, no FOR UPDATE — and none rendered.
	f2 := newFakeDB()
	db2 := f2.open(SQLite)
	f2.queueExec(0, 0)
	f2.queueExec(0, 1)
	if err := SyncRelation(ctx, db2, &Account{ID: 7}, "Tags", []int64{100}); err != nil {
		t.Fatalf("sqlite sync: %v", err)
	}
	if strings.Contains(strings.Join(f2.logged(), " "), "FOR UPDATE") {
		t.Fatal("sqlite must not render FOR UPDATE")
	}
}

// Codex v0.3 review: flattened embedded structs with same-named fields would
// generate uncompilable column structs; refuse with guidance.
func TestWriteColumnsRefusesDuplicateFieldNames(t *testing.T) {
	type Meta struct {
		ID int64 `rio:"meta_id,pk,noautoincr"`
	}
	type Base struct {
		ID int64 `rio:"base_id,noautoincr"`
	}
	type Doubled struct {
		Meta
		Base
	}
	var buf strings.Builder
	err := WriteColumns(&buf, "models", Doubled{})
	if err == nil || !strings.Contains(err.Error(), "two fields named ID") {
		t.Fatalf("duplicate field names must refuse: %v", err)
	}
}

// --- post-v0.3.0 self-review hardening ---

// Compiled.All ran preloads but silently dropped WithCount.
func TestCompiledAllFillsWithCount(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	f.queueRows([]string{"board_id", "count"}, []driver.Value{int64(1), int64(4)})

	counted := MustCompile(From[Board]().WithCount("Posts"))
	boards, err := counted.All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(boards) != 1 || boards[0].PostsCount != 4 {
		t.Fatalf("compiled WithCount must fill counts: %+v", boards)
	}
}

// A silently ignored Limit would turn "delete ten rows" into "delete every
// matching row"; set-based writes refuse shapes they cannot honor.
func TestSetOpsRefuseLimitOffsetGroupBy(t *testing.T) {
	ctx := context.Background()
	db := newFakeDB().open()

	if _, err := From[User]().Where("age > ?", 1).Limit(10).DeleteAll(ctx, db); err == nil || !strings.Contains(err.Error(), "DeleteAll cannot honor Limit/Offset") {
		t.Fatalf("DeleteAll with Limit: %v", err)
	}
	if _, err := From[User]().Where("age > ?", 1).Offset(5).UpdateAll(ctx, db, Set{"age": 2}); err == nil || !strings.Contains(err.Error(), "UpdateAll cannot honor Limit/Offset") {
		t.Fatalf("UpdateAll with Offset: %v", err)
	}
	if _, err := From[User]().Where("age > ?", 1).Limit(10).ForceDeleteAll(ctx, db); err == nil || !strings.Contains(err.Error(), "ForceDeleteAll cannot honor Limit/Offset") {
		t.Fatalf("ForceDeleteAll with Limit: %v", err)
	}
	if _, err := From[User]().Where("age > ?", 1).GroupBy("age").UpdateAll(ctx, db, Set{"age": 2}); err == nil || !strings.Contains(err.Error(), "GroupBy") {
		t.Fatalf("UpdateAll with GroupBy: %v", err)
	}
	if _, err := From[User]().Where("age > ?", 1).Limit(3).RestoreAll(ctx, db); err == nil || !strings.Contains(err.Error(), "RestoreAll cannot honor Limit/Offset") {
		t.Fatalf("Restore with Limit: %v", err)
	}
}

// Pluck refuses row-set-changing clauses it does not render, and honors
// ForUpdate instead of silently skipping the lock.
func TestPluckShapeGuards(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	if _, err := Pluck[string](ctx, db, From[User]().GroupBy("age"), "email"); err == nil || !strings.Contains(err.Error(), "Raw") {
		t.Fatalf("Pluck with GroupBy: %v", err)
	}
	f.queueRows([]string{"email"}, []driver.Value{"a@x"})
	if _, err := Pluck[string](ctx, db, From[User]().Where("id = ?", 1).ForUpdate(), "email"); err != nil {
		t.Fatalf("Pluck: %v", err)
	}
	if got := f.logged()[0]; !strings.HasSuffix(got, " FOR UPDATE") {
		t.Fatalf("Pluck must render FOR UPDATE: %s", got)
	}
}

// WhereHas conditions bind inside the EXISTS subquery; a bare ? there can
// never be an exec-time parameter and must refuse at compile time.
func TestCompileRejectsBareWhereHasPlaceholder(t *testing.T) {
	_, err := Compile[User](From[User]().Where("age > ?").WhereHas("Posts", RelWhere("title = ?")))
	if err == nil || !strings.Contains(err.Error(), "bind inline") {
		t.Fatalf("bare ? in WhereHas must refuse at compile: %v", err)
	}
	_, err = Compile[User](From[User]().WhereHas("Posts", RelWhere("title = ?")))
	if err == nil || !strings.Contains(err.Error(), "bind inline") {
		t.Fatalf("bare ? in WhereHas (no other conds) must refuse: %v", err)
	}
}

func TestCompileValidatesWhereHasAndWithCountPaths(t *testing.T) {
	if _, err := Compile[User](From[User]().WhereHas("Nope")); err == nil || !strings.Contains(err.Error(), "Nope") {
		t.Fatalf("unknown WhereHas path must fail at compile: %v", err)
	}
	if _, err := Compile[Board](From[Board]().WithCount("Nope")); err == nil || !strings.Contains(err.Error(), "Nope") {
		t.Fatalf("unknown WithCount relation must fail at compile: %v", err)
	}
	type PostWithAuthorCount struct {
		ID          int64
		UserID      int64
		Author      BelongsTo[User] `rio:",fk:user_id"`
		AuthorCount int64           `rio:",countof:Author"`
	}
	if _, err := Compile[PostWithAuthorCount](From[PostWithAuthorCount]().WithCount("Author")); err == nil || !strings.Contains(err.Error(), "meaningless") {
		t.Fatalf("non-aggregate WithCount relation must fail at compile: %v", err)
	}
}

// Two models sharing a struct name would emit colliding declarations.
func TestWriteColumnsRefusesDuplicateModelNames(t *testing.T) {
	var buf strings.Builder
	err := WriteColumns(&buf, "models", User{}, User{})
	if err == nil || !strings.Contains(err.Error(), "separate files") {
		t.Fatalf("duplicate model names must refuse: %v", err)
	}
}

// A [16]byte UUID is one value; expanding it into sixteen placeholders would
// splice a list into "= ?".
func TestByteArrayArgStaysScalar(t *testing.T) {
	sqlText, args, err := rebind(pgLex, bindDollar, "id = ?", []any{[16]byte{1}})
	if err != nil {
		t.Fatalf("rebind: %v", err)
	}
	if sqlText != "id = $1" || len(args) != 1 {
		t.Fatalf("byte array must not expand: %s %v", sqlText, args)
	}
}

// sql.NullTime as a query argument must bind rio's own time encoding — on
// SQLite the driver's format would miss every stored value.
func TestNormalizeArgsNullTime(t *testing.T) {
	at := time.Date(2026, 7, 9, 3, 4, 5, 0, time.UTC)
	out := normalizeArgs(SQLite, []any{sql.NullTime{Time: at, Valid: true}, sql.NullTime{}})
	if s, ok := out[0].(string); !ok || !strings.HasPrefix(s, "2026-07-09 03:04:05") {
		t.Fatalf("valid NullTime must bind rio's text form: %#v", out[0])
	}
	if out[1] != nil {
		t.Fatalf("invalid NullTime must bind NULL: %#v", out[1])
	}
	out = normalizeArgs(SQLite, []any{sql.Null[time.Time]{V: at, Valid: true}})
	if s, ok := out[0].(string); !ok || !strings.HasPrefix(s, "2026-07-09 03:04:05") {
		t.Fatalf("sql.Null[time.Time] must bind rio's text form: %#v", out[0])
	}
}

// --- opus multi-lens audit (post-v0.3.0) regressions ---

// The SQL cache keys on an order-free column bitmap; whitelist rendering and
// binding must therefore use one canonical order, or the second of two
// same-columns-different-order Updates would bind values into the wrong
// columns through the first call's cached statement.
func TestUpdateWhitelistOrderInsensitive(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	u1 := User{ID: 1, Email: "a@x", Age: 30}
	if err := Update(ctx, db, &u1, "email", "age"); err != nil {
		t.Fatalf("first update: %v", err)
	}
	u2 := User{ID: 2, Email: "b@x", Age: 40}
	if err := Update(ctx, db, &u2, "age", "email"); err != nil {
		t.Fatalf("second update: %v", err)
	}
	stmts := f.loggedContaining("UPDATE")
	if len(stmts) != 2 || stmts[0].sql != stmts[1].sql {
		t.Fatalf("both orders must share one canonical statement:\n%s\n%s", stmts[0].sql, stmts[1].sql)
	}
	// Canonical order is field order: email before age. The second call's
	// args must match that layout, not its caller order.
	if stmts[1].args[0] != "b@x" || stmts[1].args[1] != int64(40) {
		t.Fatalf("second call bound values in caller order, not canonical: %v", stmts[1].args)
	}
}

type AllDefaults struct {
	ID   int64
	Slot int `rio:",omitzero"`
}

// A row whose every column is skipped (auto-increment PK + zero omitzero
// columns) must render the dialect's empty-row form, not "() VALUES ()"
// which PostgreSQL and SQLite reject.
func TestInsertAllDefaultsRendersDefaultValues(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueRows([]string{"id", "slot"}, []driver.Value{int64(7), int64(0)})
	row := AllDefaults{}
	if err := Insert(ctx, db, &row); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	want := `INSERT INTO "all_defaultses" DEFAULT VALUES RETURNING "id", "slot"`
	if got := f.logged()[0]; got != want {
		t.Fatalf("sqlite all-defaults insert:\n got: %s\nwant: %s", got, want)
	}
	if row.ID != 7 {
		t.Fatalf("backfill: %+v", row)
	}

	fm := newFakeDB()
	dbm := fm.open(MySQL)
	fm.queueExec(9, 1)
	rowm := AllDefaults{}
	if err := Insert(ctx, dbm, &rowm); err != nil {
		t.Fatalf("mysql Insert: %v", err)
	}
	if got := fm.logged()[0]; got != "INSERT INTO `all_defaultses` () VALUES ()" {
		t.Fatalf("mysql all-defaults insert: %s", got)
	}

	// Upsert cannot express DEFAULT VALUES + conflict clause on SQLite;
	// refuse uniformly instead of working on two dialects out of three.
	if err := Upsert(ctx, db, &AllDefaults{}, OnConflict("id")); err == nil || !strings.Contains(err.Error(), "use Insert") {
		t.Fatalf("all-defaults upsert must refuse: %v", err)
	}
}

// json.RawMessage is one JSONB value, not a list of bytes.
func TestNamedByteSliceStaysScalar(t *testing.T) {
	raw := json.RawMessage(`{"k":1}`)
	sqlText, args, err := rebind(pgLex, bindDollar, "data @> ?", []any{raw})
	if err != nil {
		t.Fatalf("rebind: %v", err)
	}
	if sqlText != "data @> $1" || len(args) != 1 {
		t.Fatalf("named byte slice must not expand: %s %v", sqlText, args)
	}
}

type Student struct {
	ID      int64
	Courses ManyToMany[CourseX] `rio:",join:enrollments,fk:learner_id,ref:course_ref"`
}

type CourseX struct {
	ID int64
}

type Node struct {
	ID      int64
	Related ManyToMany[Node] `rio:",join:node_links"`
}

type NodeOK struct {
	ID      int64
	Related ManyToMany[NodeOK] `rio:",join:node_links,fk:src_id,ref:dst_id"`
}

// fk:/ref: on ManyToMany name the join table's columns; the convention would
// otherwise hardcode struct names — and collide on self-referential m2m.
func TestManyToManyJoinColumnOverrides(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	f.queueRows([]string{"id", "learner_id"})

	_, err := From[Student]().With("Courses").All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	rel := f.logged()[1]
	for _, frag := range []string{`"enrollments"."course_ref" = "course_xes"."id"`, `"enrollments"."learner_id" IN ($1)`} {
		if !strings.Contains(rel, frag) {
			t.Fatalf("fk:/ref: overrides missing, got:\n%s", rel)
		}
	}

	// Self-referential m2m without explicit columns: both join columns would
	// be "node_id" — refuse with the fix.
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	_, err = From[Node]().With("Related").All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "fk: and ref:") {
		t.Fatalf("self-referential m2m must demand explicit columns: %v", err)
	}

	// With explicit columns it renders both sides distinctly.
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	f.queueRows([]string{"id", "src_id"})
	_, err = From[NodeOK]().With("Related").All(ctx, db)
	if err != nil {
		t.Fatalf("self-ref with tags: %v", err)
	}
	rel = f.logged()[len(f.logged())-1]
	for _, frag := range []string{`"node_links"."dst_id" = "node_oks"."id"`, `"node_links"."src_id" IN ($1)`} {
		if !strings.Contains(rel, frag) {
			t.Fatalf("self-ref join columns wrong, got:\n%s", rel)
		}
	}
}

// MySQL counts changed rows, not matched rows: an idempotent Update must not
// report ErrNotFound. One PK probe resolves the ambiguity; a truly missing
// row still errors.
func TestMySQLIdempotentUpdateProbes(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(MySQL)

	f.queueExec(0, 0)                                    // UPDATE: matched but unchanged
	f.queueRows([]string{"1"}, []driver.Value{int64(1)}) // probe finds the row
	if err := Update(ctx, db, &Post{ID: 1, UserID: 5}); err != nil {
		t.Fatalf("idempotent update must succeed: %v", err)
	}
	probe := f.logged()[1]
	if !strings.Contains(probe, "SELECT 1 FROM `posts` WHERE `id` = ? LIMIT 1") {
		t.Fatalf("probe sql: %s", probe)
	}

	f.queueExec(0, 0)          // UPDATE: no such row
	f.queueRows([]string{"1"}) // probe finds nothing
	err := Update(ctx, db, &Post{ID: 99, UserID: 5})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing row must stay ErrNotFound: %v", err)
	}

	// PostgreSQL counts matched rows; no probe happens.
	fp := newFakeDB()
	dbp := fp.open()
	fp.queueExec(0, 0)
	if err := Update(ctx, dbp, &Post{ID: 1, UserID: 5}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pg zero-affected means missing: %v", err)
	}
	if len(fp.logged()) != 1 {
		t.Fatalf("pg must not probe: %v", fp.logged())
	}
}

// Attach/Detach accept typed id slices spread directly, like SyncRelation.
func TestAttachDetachTypedIDs(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	ids := []int64{100, 101}

	f.queueExec(0, 2)
	if err := Attach(ctx, db, &Account{ID: 7}, "Tags", ids...); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	f.queueExec(0, 1)
	if err := Detach(ctx, db, &Account{ID: 7}, "Tags", ids...); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	if got := f.logged()[1]; !strings.Contains(got, `"tag_id" IN ($2, $3)`) {
		t.Fatalf("detach expansion: %s", got)
	}
}

// Row locks never reach the aggregate count shape (PostgreSQL rejects them);
// Exists keeps the lock — its probe row is well-defined.
func TestCountForUpdateOmitsLock(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()

	f.queueRows([]string{"count"}, []driver.Value{int64(3)})
	if _, err := From[Post]().Where("user_id = ?", 5).ForUpdate().Count(ctx, db); err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got := f.logged()[0]; strings.Contains(got, "FOR UPDATE") {
		t.Fatalf("count must not lock: %s", got)
	}
	f.queueRows([]string{"1"}, []driver.Value{int64(1)})
	if _, err := From[Post]().Where("user_id = ?", 5).ForUpdate().Exists(ctx, db); err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if got := f.logged()[1]; !strings.HasSuffix(got, "LIMIT 1 FOR UPDATE") {
		t.Fatalf("exists keeps the lock: %s", got)
	}
}

type Reminder struct {
	ID     int64
	Remind sql.NullTime
}

// sql.NullTime fields bind rio's canonical encoding on the entity write path
// — the same value must store identically under Insert and Upsert, and on
// SQLite in rio's own text form.
func TestNullTimeFieldBindsCanonical(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(SQLite)

	at := time.Date(2026, 7, 9, 3, 4, 5, 123456789, time.UTC)
	f.queueExec(1, 1)
	if err := Insert(ctx, db, &Reminder{ID: 1, Remind: sql.NullTime{Time: at, Valid: true}}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	args := f.loggedContaining("INSERT")[0].args
	s, ok := args[1].(string)
	if !ok || s != "2026-07-09 03:04:05.123456+00:00" {
		t.Fatalf("NullTime must bind rio's text form (microseconds, UTC): %#v", args[1])
	}

	f.queueExec(1, 1)
	if err := Insert(ctx, db, &Reminder{ID: 2, Remind: sql.NullTime{}}); err != nil {
		t.Fatalf("Insert invalid: %v", err)
	}
	if got := f.loggedContaining("INSERT")[1].args[1]; got != nil {
		t.Fatalf("invalid NullTime must bind NULL: %#v", got)
	}
}

// Set-based writes render only their own table with no row order, so Join and
// OrderBy cannot be honored — refuse loudly rather than drop them silently
// (a dropped Join leaves the WHERE referencing a table not in the statement).
func TestSetOpsRefuseJoinAndOrderBy(t *testing.T) {
	ctx := context.Background()
	db := newFakeDB().open()
	if _, err := From[User]().Join("INNER JOIN orgs ON orgs.id = users.org_id").
		Where("orgs.active = ?", true).UpdateAll(ctx, db, Set{"age": 5}); err == nil || !strings.Contains(err.Error(), "Join") {
		t.Fatalf("UpdateAll+Join must refuse: %v", err)
	}
	if _, err := From[User]().Where("age > ?", 1).OrderBy("id DESC").DeleteAll(ctx, db); err == nil || !strings.Contains(err.Error(), "OrderBy") {
		t.Fatalf("DeleteAll+OrderBy must refuse: %v", err)
	}
	if _, err := From[User]().Where("age > ?", 1).Join("JOIN x ON 1=1").ForceDeleteAll(ctx, db); err == nil || !strings.Contains(err.Error(), "Join") {
		t.Fatalf("ForceDeleteAll+Join must refuse: %v", err)
	}
}

// An embedded struct promotes its exported fields even when the embedded
// type's own name is unexported — matching encoding/json. Silently dropping
// them (the old behavior) is the kind of data-omission surprise rio refuses.
type embeddedMeta struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	Note      string
	private   int //nolint:unused // fixture: proves unexported fields stay unmapped
}

type EmbedModel struct {
	ID   int64
	Name string
	embeddedMeta
}

func TestEmbeddedUnexportedTypeFlattens(t *testing.T) {
	p, err := planOf[EmbedModel]()
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	cols := map[string]bool{}
	for _, f := range p.fields {
		cols[f.column] = true
	}
	for _, want := range []string{"id", "name", "created_at", "updated_at", "note"} {
		if !cols[want] {
			t.Errorf("embedded exported field %q was dropped", want)
		}
	}
	if cols["private"] {
		t.Error("unexported inner field must not map")
	}
	if p.created == nil || p.updated == nil {
		t.Fatal("embedded CreatedAt/UpdatedAt not detected")
	}
	// End-to-end stamp through the unexported embedding (offset write).
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(MySQL)
	f.queueExec(1, 1)
	m := EmbedModel{Name: "x"}
	if err := Insert(ctx, db, &m); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if m.CreatedAt.IsZero() {
		t.Fatal("embedded timestamp not stamped")
	}
}

// The whitelist-order fix must hold under concurrency: the map-iteration
// order that originally triggered the miswrite is nondeterministic, so hammer
// one handle with randomized column orders and assert every statement binds
// its values into the right columns.
func TestConcurrentUpdateWhitelistOrder(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	orders := [][]string{{"email", "age"}, {"age", "email"}, {"email", "age"}, {"age", "email"}}
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(cols []string) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				f.queueExec(0, 1)
				u := User{ID: 1, Email: "e@x", Age: 42}
				if err := Update(ctx, db, &u, cols...); err != nil {
					t.Errorf("update: %v", err)
					return
				}
			}
		}(orders[g])
	}
	wg.Wait()
	for _, st := range f.loggedContaining("UPDATE") {
		if len(st.args) >= 2 && (st.args[0] != "e@x" || st.args[1] != int64(42)) {
			t.Fatalf("mis-bound columns: args=%v sql=%s", st.args, st.sql)
		}
	}
}

// --- round-2 opus audit regressions ---

// Count must refuse Having (with or without GroupBy): a bare HAVING filters
// the single implicit aggregate group and silently returns 0.
func TestCountRefusesHaving(t *testing.T) {
	ctx := context.Background()
	db := newFakeDB().open()
	if _, err := From[User]().Having("count(*) > ?", 5).Count(ctx, db); err == nil || !strings.Contains(err.Error(), "Raw") {
		t.Fatalf("Count with Having must refuse: %v", err)
	}
}

// m2m WithCount must INNER JOIN the target, exactly like the With load, so the
// count matches the rows With would return even without a softdelete column.
func TestManyToManyWithCountJoinsTarget(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	f.queueRows([]string{"account_id", "count"}, []driver.Value{int64(1), int64(2)})
	_, err := From[CountAcct]().WithCount("Tags").All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	sql := f.logged()[1]
	if !strings.Contains(sql, "INNER JOIN") {
		t.Fatalf("m2m WithCount must INNER JOIN target: %s", sql)
	}
}

// Duplicate WithCount for one relation counts once, matching With's dedup.
func TestWithCountDeduplicates(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	f.queueRows([]string{"board_id", "count"}, []driver.Value{int64(1), int64(3)})
	_, err := From[Board]().WithCount("Posts").WithCount("Posts").All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	counts := 0
	for _, s := range f.logged() {
		if strings.Contains(s, "count(*)") {
			counts++
		}
	}
	if counts != 1 {
		t.Fatalf("duplicate WithCount must issue one count query, got %d", counts)
	}
}

// A *time.Time CreatedAt/UpdatedAt is auto-stamped, like the value form and
// like softdelete's *time.Time acceptance.
type PtrStamped struct {
	ID        int64
	Name      string
	CreatedAt *time.Time
	UpdatedAt *time.Time
}

func TestPointerTimestampsStamped(t *testing.T) {
	p, err := planOf[PtrStamped]()
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if p.created == nil || p.updated == nil {
		t.Fatalf("*time.Time CreatedAt/UpdatedAt not detected: created=%v updated=%v", p.created != nil, p.updated != nil)
	}
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(MySQL)
	f.queueExec(1, 1)
	row := PtrStamped{Name: "x"}
	if err := Insert(ctx, db, &row); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if row.CreatedAt == nil || row.CreatedAt.IsZero() {
		t.Fatal("*time.Time CreatedAt not stamped")
	}

	zero := time.Time{}
	row = PtrStamped{Name: "zero", CreatedAt: &zero, UpdatedAt: &zero}
	f.queueExec(1, 1)
	if err := Insert(ctx, db, &row); err != nil {
		t.Fatalf("insert zero pointer: %v", err)
	}
	if row.CreatedAt == nil || row.CreatedAt.IsZero() || row.UpdatedAt == nil || row.UpdatedAt.IsZero() {
		t.Fatal("*time.Time fields pointing at zero time must be stamped")
	}
}

// Two fields both claiming the CreatedAt role (reachable via embedding + a
// column rename) must fail loud, like version/softdelete duplicates.
type DupCreatedInner struct {
	CreatedAt time.Time `rio:"made_at"`
}
type DupCreated struct {
	ID        int64
	CreatedAt time.Time
	DupCreatedInner
}

func TestDuplicateCreatedRoleRejected(t *testing.T) {
	_, err := planOf[DupCreated]()
	if err == nil || !strings.Contains(err.Error(), "CreatedAt") {
		t.Fatalf("two CreatedAt roles must be rejected: %v", err)
	}
}

// Detach with a byte-kind id type must expand IN (?) to one placeholder per
// id, not bind the whole slice as one BLOB.
type ByteTag struct {
	ID uint8
}
type ByteAcct struct {
	ID   int64
	Tags ManyToMany[ByteTag] `rio:",join:byte_acct_tags,fk:byte_acct_id,ref:byte_tag_id"`
}

func TestDetachByteKindIDsExpand(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueExec(0, 2)
	if err := Detach(ctx, db, &ByteAcct{ID: 1}, "Tags", uint8(1), uint8(2)); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	sql := f.logged()[0]
	if !strings.Contains(sql, "IN ($2, $3)") {
		t.Fatalf("byte-kind ids must expand, got: %s", sql)
	}
}

type CountAcct struct {
	ID        int64
	TagsCount int64                `rio:",countof:Tags"`
	Tags      ManyToMany[CountTag] `rio:",join:count_acct_tags"`
}
type CountTag struct {
	ID int64
}

func (CountAcct) TableName() string { return "count_accts" }

// An explicit role tag wins over the name-based timestamp convention: a field
// named UpdatedAt but tagged softdelete is the soft-delete column, not also
// the updated_at stamp (which would fight over the same field).
type SoftDelNamedTimestamp struct {
	ID        int64
	Name      string
	UpdatedAt time.Time `rio:",softdelete"`
}

func TestExplicitTagBeatsTimestampName(t *testing.T) {
	p, err := planOf[SoftDelNamedTimestamp]()
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	f := p.byColumn["updated_at"]
	if f.isUpdated {
		t.Error("softdelete-tagged field must not also be the updated_at stamp")
	}
	if !f.isSoftDelete || p.softDel == nil {
		t.Error("softdelete tag must still take effect")
	}
	if p.updated != nil {
		t.Error("p.updated must be nil when the only UpdatedAt is tagged softdelete")
	}
}

// --- round-3 opus audit regressions ---

// A slice Set value in UpdateAll must be refused, not IN-expanded into a
// malformed "SET col = ?, ?".
func TestUpdateAllRefusesSliceSetValue(t *testing.T) {
	ctx := context.Background()
	db := newFakeDB().open()
	_, err := From[User]().Where("id = ?", 1).UpdateAll(ctx, db, Set{"email": []string{"a", "b"}})
	if err == nil || !strings.Contains(err.Error(), "slice") {
		t.Fatalf("slice Set value must be refused: %v", err)
	}
}

func TestUpdateAllJSONNilBindsSQLNull(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(SQLite)
	f.queueExec(0, 1)

	if _, err := From[Doc]().Where("id = ?", 1).UpdateAll(ctx, db, Set{"config": nil}); err != nil {
		t.Fatalf("UpdateAll json nil: %v", err)
	}
	stmt := f.loggedContaining("UPDATE")[0]
	if stmt.args[0] != nil {
		t.Fatalf("nil JSON Set value must bind SQL NULL, got %#v", stmt.args[0])
	}

	f.queueExec(0, 1)
	var cfg *Prefs
	if _, err := From[Doc]().Where("id = ?", 1).UpdateAll(ctx, db, Set{"config": cfg}); err != nil {
		t.Fatalf("UpdateAll typed nil json pointer: %v", err)
	}
	stmt = f.loggedContaining("UPDATE")[1]
	if stmt.args[0] != nil {
		t.Fatalf("typed nil JSON pointer must bind SQL NULL, got %#v", stmt.args[0])
	}
}

// WithCount("") must error (unknown relation), not be silently swallowed by
// the dedup sentinel.
func TestWithCountEmptyNameErrors(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows([]string{"id"}, []driver.Value{int64(1)})
	_, err := From[Board]().WithCount("").All(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "no relation") {
		t.Fatalf("WithCount(\"\") must error: %v", err)
	}
}

// RelLimit's windowed preload must carry an outer ORDER BY so the per-parent
// child order the user asked for via RelOrder survives.
func TestRelLimitOuterOrderBy(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"))
	f.queueRows(postCols, []driver.Value{int64(10), int64(1), "t"})
	_, err := From[User]().With("Posts", RelOrder("id DESC"), RelLimit(2)).All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	sql := f.logged()[1]
	// The outer query (after the rio_w subquery) must ORDER BY the partition
	// and the row number.
	if !strings.Contains(sql, `AS "rio_w" WHERE "rio_w"."__rio_rn" <= 2 ORDER BY "rio_w"."user_id", "rio_w"."__rio_rn"`) {
		t.Fatalf("RelLimit missing outer ORDER BY: %s", sql)
	}
}

// The set-based bulk restore is RestoreAll, matching UpdateAll/DeleteAll.
func TestRestoreAllNaming(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueExec(0, 2)
	n, err := From[User]().Where("id = ?", 1).RestoreAll(ctx, db)
	if err != nil {
		t.Fatalf("RestoreAll: %v", err)
	}
	if n != 2 {
		t.Fatalf("RestoreAll affected: %d", n)
	}
}

// On MySQL a restore-on-upsert clears deleted_at server-side; rio reconciles
// the in-memory softdelete field so the row reads as visible without a reload.
func TestMySQLUpsertReconcilesDeletedAt(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(MySQL)
	f.queueExec(0, 2) // conflict → update path (affected 2)
	deleted := testNow.Add(-time.Hour)
	row := User{ID: 1, Email: "a@x", DeletedAt: &deleted}
	if err := Upsert(ctx, db, &row, OnConflict("id"), DoUpdate("email")); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if row.DeletedAt != nil && !row.DeletedAt.IsZero() {
		t.Fatalf("MySQL restore-on-upsert must clear in-memory deleted_at, got %v", row.DeletedAt)
	}
}

// --- round-4 opus audit regressions ---

// A basic-kind field that implements driver.Valuer must bind through Value(),
// not the unsafe fast read that hands the driver the raw underlying value.
type lowerName string

func (s lowerName) Value() (driver.Value, error) { return strings.ToLower(string(s)), nil }

type ValuerRow struct {
	ID   int64
	Name lowerName
}

func TestValuerBasicFieldBindsThroughValue(t *testing.T) {
	p, err := planOf[ValuerRow]()
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if !p.byColumn["name"].code.bindValuer {
		t.Fatal("Valuer basic field must be flagged bindValuer")
	}
	ctx := context.Background()
	f := newFakeDB()
	db := f.open(MySQL)
	f.queueExec(1, 1)
	if err := Insert(ctx, db, &ValuerRow{ID: 1, Name: "MixedCase"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// database/sql invokes Value() before the driver sees the arg.
	args := f.loggedContaining("INSERT")[0].args
	if args[1] != "mixedcase" {
		t.Fatalf("Valuer must lowercase on write, got %#v", args[1])
	}
}

type pointerSecret string

func (s *pointerSecret) Value() (driver.Value, error) {
	if s == nil {
		return nil, nil
	}
	return "encoded:" + strings.ToLower(string(*s)), nil
}

type PointerValuerRow struct {
	ID     int64
	Secret pointerSecret
	Maybe  *pointerSecret
}

func TestPointerReceiverValuerBindsThroughValue(t *testing.T) {
	p, err := planOf[PointerValuerRow]()
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if !p.byColumn["secret"].code.bindPtrValuer {
		t.Fatal("value field with pointer-receiver Valuer must be flagged bindPtrValuer")
	}
	if !p.byColumn["maybe"].code.bindValuer {
		t.Fatal("pointer field implementing Valuer must be flagged bindValuer")
	}

	ctx := context.Background()
	f := newFakeDB()
	db := f.open(MySQL)
	f.queueExec(1, 1)
	maybe := pointerSecret("Maybe")
	row := PointerValuerRow{ID: 1, Secret: "Secret", Maybe: &maybe}
	if err := Insert(ctx, db, &row); err != nil {
		t.Fatalf("insert: %v", err)
	}
	args := f.loggedContaining("INSERT")[0].args
	if args[1] != "encoded:secret" || args[2] != "encoded:maybe" {
		t.Fatalf("pointer receiver Valuer must encode both fields, got %#v", args)
	}
}

// RelLimit(0) loads no children (like Query.Limit(0)), not all of them.
func TestRelLimitZeroLoadsNone(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	db := f.open()
	f.queueRows(userCols, userRow(1, "a@x"))
	f.queueRows([]string{"id", "user_id", "title"}) // window query, zero rows
	_, err := From[User]().With("Posts", RelLimit(0)).All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	// The windowed subquery renders even for limit 0 (rn <= 0 → no rows).
	if !strings.Contains(f.logged()[1], "ROW_NUMBER()") || !strings.Contains(f.logged()[1], "<= 0") {
		t.Fatalf("RelLimit(0) must render the bounded window: %s", f.logged()[1])
	}
}

// A uint64 above MaxInt64 binds as a decimal string on MySQL/Postgres
// (BIGINT UNSIGNED / numeric), but fails loud on SQLite, which would coerce
// the oversized INTEGER literal to REAL and silently lose precision.
type BigUint struct {
	ID int64
	N  uint64
}

func TestBigUintDialectBinding(t *testing.T) {
	ctx := context.Background()
	// MySQL: string bind, no error.
	fm := newFakeDB()
	dm := fm.open(MySQL)
	fm.queueExec(1, 1)
	if err := Insert(ctx, dm, &BigUint{ID: 1, N: math.MaxUint64}); err != nil {
		t.Fatalf("mysql big uint: %v", err)
	}
	if got := fm.loggedContaining("INSERT")[0].args[1]; got != "18446744073709551615" {
		t.Fatalf("mysql must bind decimal string, got %#v", got)
	}
	// SQLite: fail loud.
	fs := newFakeDB()
	ds := fs.open(SQLite)
	if err := Insert(ctx, ds, &BigUint{ID: 1, N: math.MaxUint64}); err == nil || !strings.Contains(err.Error(), "SQLite") {
		t.Fatalf("sqlite must fail loud on uint64 overflow: %v", err)
	}
}
