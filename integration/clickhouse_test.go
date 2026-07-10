package integration

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-rio/rio"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// The ClickHouse leg runs when RIO_CLICKHOUSE_DSN is set, e.g.
// RIO_CLICKHOUSE_DSN="clickhouse://rio:rio@localhost:19000/rio_test".
//
// It has three layers:
//   - driver probes pinning the clickhouse-go behaviors rio's dialect design
//     depends on (if upstream changes one, the matching probe fails first);
//   - server-semantics regressions replaying the design's experiment matrix;
//   - the rio end-to-end pass over the supported surface, plus real-database
//     proof that the rejected surface sends nothing.

func clickhouseDB(t *testing.T, opts ...rio.Option) *rio.DB {
	t.Helper()
	dsn := os.Getenv("RIO_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("RIO_CLICKHOUSE_DSN not set")
	}
	// The DSN passes through untouched — rio inlines time arguments as
	// explicit parseDateTime64BestEffort calls, so no input-format setting
	// is needed on any server version.
	raw, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatal(err)
	}
	// No error translator: ClickHouse has no unique or FK constraints, so no
	// server error maps to a rio sentinel (the go-rio/clickhouse module
	// installs none either).
	db := rio.New(raw, rio.ClickHouse, opts...)
	if err := db.Unwrap().Ping(); err != nil {
		t.Fatalf("ping clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func chExec(t *testing.T, ctx context.Context, db *rio.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := rio.Exec(ctx, db, s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// chServerVersion returns the server's major/minor version.
func chServerVersion(t *testing.T, ctx context.Context, db *rio.DB) (major, minor int) {
	t.Helper()
	v, err := rio.Raw[string]("SELECT version()").First(ctx, db)
	if err != nil {
		t.Fatalf("version(): %v", err)
	}
	parts := strings.Split(*v, ".")
	if len(parts) < 2 {
		t.Fatalf("unparseable version %q", *v)
	}
	major, _ = strconv.Atoi(parts[0])
	minor, _ = strconv.Atoi(parts[1])
	return major, minor
}

// --- layer 1: driver probes (upstream contracts, §-pinned) ---

// TestClickHouseDriverProbes locks the clickhouse-go behaviors the dialect is
// built around. Each subtest failing means upstream changed the contract —
// revisit the matching design decision before "fixing" the test.
func TestClickHouseDriverProbes(t *testing.T) {
	db := clickhouseDB(t)
	raw := db.Unwrap()
	ctx := context.Background()

	chExec(t, ctx, db,
		"DROP TABLE IF EXISTS probe_times", "DROP TABLE IF EXISTS probe_bytes",
		"CREATE TABLE probe_times (id Int64, at DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY id",
		"CREATE TABLE probe_bytes (id Int64, s String) ENGINE = MergeTree ORDER BY id",
	)

	// Contract (a), the go.mod floor: the binder is quote-aware — a ? inside
	// a string literal or comment survives; only the bare ? binds.
	t.Run("quote-aware binding", func(t *testing.T) {
		var lit, val string
		row := raw.QueryRowContext(ctx, "SELECT '?' AS lit, ? AS val -- trailing ? stays\n", "bound")
		if err := row.Scan(&lit, &val); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if lit != "?" || val != "bound" {
			t.Fatalf("binder rewrote protected regions: lit=%q val=%q (driver below v2.47.0?)", lit, val)
		}
	})

	// Contract (b): \? is un-escaped to a literal ? — the target of rio's ??
	// rendering (ClickHouse's ternary operator reaches the server this way).
	t.Run("backslash-question restores literal", func(t *testing.T) {
		var y string
		row := raw.QueryRowContext(ctx, `SELECT 2 > ? \? 'y' : 'n'`, 1)
		if err := row.Scan(&y); err != nil {
			t.Fatalf("ternary through \\?: %v", err)
		}
		if y != "y" {
			t.Fatalf("ternary result: %q", y)
		}
	})

	// Contract (c): a time.Time positional argument is silently truncated to
	// whole seconds by the driver — the reason rio binds chTimeFormat text
	// instead. If this starts preserving sub-seconds, upstream fixed
	// formatTime and rio could simplify.
	t.Run("time.Time direct pass truncates to seconds", func(t *testing.T) {
		at := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)
		if _, err := raw.ExecContext(ctx, "INSERT INTO probe_times (id, at) VALUES (?, ?)", 1, at); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var got time.Time
		if err := raw.QueryRowContext(ctx, "SELECT at FROM probe_times WHERE id = 1").Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !got.Equal(at.Truncate(time.Second)) {
			t.Fatalf("driver time handling changed: wrote %v, read %v (expected second truncation)", at, got)
		}
	})

	// []byte has no dedicated formatValue branch: it renders as an
	// Array(UInt8) literal. Against a String column the server does not even
	// error — it stores the literal's text form ("hi" lands as "[104,105]"),
	// silent corruption — the reason rio's ClickHouse funnels bind byte
	// payloads as strings. If this round-trips the original bytes one day,
	// upstream added a []byte branch and the funnel can go.
	t.Run("bytes direct pass corrupts", func(t *testing.T) {
		if _, err := raw.ExecContext(ctx, "INSERT INTO probe_bytes (id, s) VALUES (?, ?)", 1, []byte("hi")); err != nil {
			return // rejected loudly: also proof the funnel is required
		}
		var stored string
		if err := raw.QueryRowContext(ctx, "SELECT s FROM probe_bytes WHERE id = 1").Scan(&stored); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if stored == "hi" {
			t.Fatal("driver []byte handling changed: the bytes arrived intact — rio's string funnel may be droppable")
		}
	})

	// RowsAffected is unconditionally 0 and LastInsertId always errors —
	// why entity Update/Delete (built on honest counts) are rejected.
	t.Run("RowsAffected is always zero", func(t *testing.T) {
		res, err := raw.ExecContext(ctx, "INSERT INTO probe_bytes (id, s) VALUES (?, ?)", 2, "real row")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		if n, err := res.RowsAffected(); err != nil || n != 0 {
			t.Fatalf("driver RowsAffected changed: n=%d err=%v", n, err)
		}
		if _, err := res.LastInsertId(); err == nil {
			t.Fatal("driver LastInsertId changed: must error")
		}
		var count uint64
		if err := raw.QueryRowContext(ctx, "SELECT count(*) FROM probe_bytes WHERE id = 2").Scan(&count); err != nil || count != 1 {
			t.Fatalf("the insert itself must land: count=%d err=%v", count, err)
		}
	})

	// Begin is a no-op shim: a "rolled back transaction"'s insert stays
	// visible — why db.Tx is rejected outright.
	t.Run("Begin is a fake transaction", func(t *testing.T) {
		tx, err := raw.Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO probe_bytes (id, s) VALUES (?, ?)", 3, "phantom"); err != nil {
			t.Fatalf("insert in fake tx: %v", err)
		}
		_ = tx.Rollback()
		var count uint64
		if err := raw.QueryRowContext(ctx, "SELECT count(*) FROM probe_bytes WHERE id = 3").Scan(&count); err != nil {
			t.Fatalf("count: %v", err)
		}
		if count != 1 {
			t.Fatalf("driver transaction handling changed: rolled-back insert vanished (count=%d)", count)
		}
	})

	// Prepare implements INSERT batching only; a prepared SELECT is unusable
	// — why WithStmtCache panics at construction.
	t.Run("Prepare(SELECT) is broken", func(t *testing.T) {
		stmt, err := raw.PrepareContext(ctx, "SELECT 1")
		if err != nil {
			return // rejected at prepare: contract holds
		}
		defer stmt.Close()
		rows, err := stmt.QueryContext(ctx)
		if err == nil {
			rows.Close()
			t.Fatal("driver Prepare changed: a prepared SELECT executed successfully")
		}
	})
}

// --- layer 2: server semantics (the design's experiment matrix, T1–T8) ---

func TestClickHouseServerSemantics(t *testing.T) {
	db := clickhouseDB(t)
	ctx := context.Background()

	chExec(t, ctx, db,
		"DROP TABLE IF EXISTS sem_tt", "DROP TABLE IF EXISTS sem_plain", "DROP TABLE IF EXISTS sem_repl",
		"CREATE TABLE sem_tt (id UInt64, d DateTime64(6), dz DateTime64(6, 'Asia/Shanghai'), ds DateTime) ENGINE = MergeTree ORDER BY id",
		"CREATE TABLE sem_plain (id UInt64, v String) ENGINE = MergeTree ORDER BY id",
		"CREATE TABLE sem_repl (id UInt64, v String, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY id",
	)
	scalarU64 := func(q string, args ...any) uint64 {
		t.Helper()
		n, err := rio.Raw[uint64](q, args...).First(ctx, db)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return *n
	}

	// Base instant: 2024-01-02 03:04:05.123456 UTC = epoch 1704164645.123456.
	const epoch = "1704164645.123456"

	// T1: offset-free wall-clock text parses in the *column's* timezone — the
	// Asia/Shanghai column lands 8h off. (Why rio's format carries an offset.)
	t.Run("T1 bare text obeys column timezone", func(t *testing.T) {
		chExec(t, ctx, db, "INSERT INTO sem_tt VALUES (1, '2024-01-02 03:04:05.123456', '2024-01-02 03:04:05.123456', '2024-01-02 03:04:05')")
		if n := scalarU64("SELECT count(*) FROM sem_tt WHERE id = 1 AND toUnixTimestamp64Micro(d) = 1704164645123456"); n != 1 {
			t.Fatal("UTC column must parse bare text at face value")
		}
		if n := scalarU64("SELECT count(*) FROM sem_tt WHERE id = 1 AND toUnixTimestamp64Micro(dz) = 1704164645123456"); n != 0 {
			t.Fatal("zoned column must interpret bare text in its own zone (8h off)")
		}
	})

	// T3: the time channels, by server version. The explicit
	// parseDateTime64BestEffort call — the channel rio inlines — works on
	// every supported version, instant-exact in both columns for INSERT and
	// comparison alike. Bare offset text only works from 26.x: below that,
	// the implicit String cast in comparisons rejects it (TYPE_MISMATCH,
	// immune to session settings) and basic-format INSERT parsing rejects it
	// too. Both sides pinned: if the old servers' rejection ever passes, the
	// inlining may be simplifiable; if the function channel drifts, rio's
	// time promise breaks loudly here first.
	t.Run("T3 time channels by version", func(t *testing.T) {
		const fn = "parseDateTime64BestEffort('2024-01-02 03:04:05.123456+00:00', 6, 'UTC')"
		// ds takes offset-free text: the seconds column is text-writable on
		// every version, function-writable on none (pinned below).
		chExec(t, ctx, db, "INSERT INTO sem_tt VALUES (3, "+fn+", "+fn+", '2024-01-02 03:04:05')")
		if n := scalarU64("SELECT count(*) FROM sem_tt WHERE id = 3 AND toUnixTimestamp64Micro(d) = 1704164645123456 AND toUnixTimestamp64Micro(dz) = 1704164645123456"); n != 1 {
			t.Fatal("the function channel must land the same instant in both columns")
		}
		if n := scalarU64("SELECT count(*) FROM sem_tt WHERE id = 3 AND d = " + fn + " AND dz = " + fn); n != 1 {
			t.Fatal("function-channel equality must match on both columns")
		}
		major, _ := chServerVersion(t, ctx, db)
		if major >= 26 {
			if n := scalarU64("SELECT count(*) FROM sem_tt WHERE id = 3 AND d = '2024-01-02 03:04:05.123456+00:00'"); n != 1 {
				t.Fatal("offset-text comparison must match on 26.x")
			}
		} else {
			if _, err := rio.Raw[uint64]("SELECT count(*) FROM sem_tt WHERE d = '2024-01-02 03:04:05.123456+00:00'").First(ctx, db); err == nil {
				t.Fatal("offset-text comparison passed before 26.x — the function inlining may be simplifiable")
			}
			if _, err := rio.Exec(ctx, db, "INSERT INTO sem_tt VALUES (30, '2024-01-02 03:04:05.123456+00:00', '2024-01-02 03:04:05.123456+00:00', '2024-01-02 03:04:05')"); err == nil {
				t.Fatal("offset-text INSERT parsed before 26.x — the function inlining may be simplifiable")
			}
		}
		// Negative epoch (pre-1970) through the function channel: exact from
		// 26.x. Before that, the server parser flips the fractional part's
		// sign — second − fraction instead of second + fraction — so .123456
		// lands as the previous second's .876544. Pinned so drift inside 25.x
		// is caught; the clickhouse module README documents the defect, and
		// rio does not compensate (a fixed server would then be wrong the
		// other way).
		chExec(t, ctx, db, "INSERT INTO sem_tt VALUES (31, parseDateTime64BestEffort('1950-01-02 03:04:05.123456+00:00', 6, 'UTC'), "+fn+", '1970-01-02 03:04:05')")
		wantNeg := "1950-01-02 03:04:05.123456"
		if major < 26 {
			wantNeg = "1950-01-02 03:04:04.876544" // 05 − 0.123456: the flipped fraction
		}
		if n := scalarU64("SELECT count(*) FROM sem_tt WHERE id = 31 AND toString(d) = '" + wantNeg + "'"); n != 1 {
			got, _ := rio.Raw[string]("SELECT toString(d) FROM sem_tt WHERE id = 31").First(ctx, db)
			t.Fatalf("negative-epoch parsing changed: got %v, want %s", got, wantNeg)
		}
		// DateTime (seconds) columns and the function channel: the VALUES
		// section refuses to narrow the DateTime64 expression (TYPE_MISMATCH,
		// every version), while comparisons coerce numerically — rio time
		// arguments therefore read DateTime columns but cannot write them
		// (README: use DateTime64 for columns rio writes).
		if _, err := rio.Exec(ctx, db, "INSERT INTO sem_tt VALUES (32, "+fn+", "+fn+", "+fn+")"); err == nil {
			t.Fatal("function-channel INSERT into DateTime(seconds) should TYPE_MISMATCH — if it narrows now, the README's DateTime row is stale")
		}
		if n := scalarU64("SELECT count(*) FROM sem_tt WHERE id = 3 AND ds = parseDateTime64BestEffort('2024-01-02 03:04:05.000000+00:00', 6, 'UTC')"); n != 1 {
			t.Fatal("function-channel comparison against DateTime(seconds) must coerce")
		}
	})

	// T2: fractional epoch strings are the channel that lost — their
	// negative range is version-dependent (26.x rejects it outright, 25.x
	// parses it), and the server's numeric-literal path runs through Float64,
	// losing microseconds past 2^53 (late-2299 instants). Both halves pinned.
	t.Run("T2 epoch strings are partial", func(t *testing.T) {
		if n := scalarU64("SELECT count(*) FROM sem_tt WHERE id = 3 AND d = '" + epoch + "'"); n != 1 {
			t.Fatal("epoch decimal string must compare on DateTime64")
		}
		neg, err := rio.Raw[uint64]("SELECT count(*) FROM sem_tt WHERE d = '-631152000.5'").First(ctx, db)
		if major, _ := chServerVersion(t, ctx, db); major >= 26 {
			if err == nil {
				t.Fatal("negative epoch string parsed on 26.x — the channel trade-offs moved")
			}
		} else if err != nil || *neg != 0 {
			t.Fatalf("negative epoch string must parse (to no match) before 26.x: %v", err)
		}
	})

	// T4: out-of-range times clamp silently — INSERT included. The reason
	// rio range-checks client-side and refuses.
	t.Run("T4 server clamps out-of-range silently", func(t *testing.T) {
		lo, err := rio.Raw[string]("SELECT toString(toDateTime64('0001-01-01 00:00:00', 6))").First(ctx, db)
		if err != nil {
			t.Fatalf("clamp probe: %v", err)
		}
		if !strings.HasPrefix(*lo, "1900-01-01") {
			t.Fatalf("low clamp moved: %s", *lo)
		}
		hi, err := rio.Raw[string]("SELECT toString(toDateTime64('2999-01-01 00:00:00', 6))").First(ctx, db)
		if err != nil {
			t.Fatalf("clamp probe: %v", err)
		}
		if !strings.HasPrefix(*hi, "2299-12-31") {
			t.Fatalf("high clamp moved: %s", *hi)
		}
		// Offset-free text: parses under the basic input format on every
		// version (T3 owns the offset-text story).
		chExec(t, ctx, db, "INSERT INTO sem_tt VALUES (4, '2999-01-01 00:00:00.000000', '2024-01-02 03:04:05.123456', '2024-01-02 03:04:05')")
		if n := scalarU64("SELECT count(*) FROM sem_tt WHERE id = 4 AND toString(d) LIKE '2299-12-31%'"); n != 1 {
			t.Fatal("INSERT must clamp too (still silent server-side)")
		}
	})

	// T5: a max-uint64 decimal string compares correctly — rio's overflow
	// binding strategy holds on ClickHouse.
	t.Run("T5 uint64 max as string", func(t *testing.T) {
		chExec(t, ctx, db, "INSERT INTO sem_plain VALUES (18446744073709551615, 'max')")
		if n := scalarU64("SELECT count(*) FROM sem_plain WHERE id = '18446744073709551615'"); n != 1 {
			t.Fatal("string comparison against UInt64 max must match")
		}
	})

	// T6: FINAL dedupes on ReplacingMergeTree and errors loudly elsewhere.
	t.Run("T6 FINAL", func(t *testing.T) {
		chExec(t, ctx, db,
			"INSERT INTO sem_repl VALUES (1, 'old', 1)",
			"INSERT INTO sem_repl VALUES (1, 'new', 2)",
		)
		if n := scalarU64("SELECT count(*) FROM sem_repl FINAL"); n != 1 {
			t.Fatalf("FINAL must merge versions, got %d", n)
		}
		v, err := rio.Raw[string]("SELECT v FROM sem_repl FINAL WHERE id = 1").First(ctx, db)
		if err != nil || *v != "new" {
			t.Fatalf("FINAL must keep the latest version: %v %v", v, err)
		}
		if _, err := rio.Raw[uint64]("SELECT count(*) FROM sem_plain FINAL").First(ctx, db); err == nil {
			t.Fatal("FINAL on plain MergeTree must error (ILLEGAL_FINAL)")
		}
	})

	// T7: bare OFFSET without LIMIT is native.
	t.Run("T7 bare OFFSET", func(t *testing.T) {
		if _, err := rio.Raw[uint64]("SELECT id FROM sem_plain ORDER BY id OFFSET 1").All(ctx, db); err != nil {
			t.Fatalf("bare OFFSET: %v", err)
		}
	})

	// T8: correlated EXISTS (WhereHas's shape), GA since 25.8.
	t.Run("T8 correlated EXISTS", func(t *testing.T) {
		if major, minor := chServerVersion(t, ctx, db); major < 25 || (major == 25 && minor < 8) {
			t.Skipf("correlated subqueries need ClickHouse ≥ 25.8, server is %d.%d", major, minor)
		}
		if _, err := rio.Raw[uint64](
			"SELECT count(*) FROM sem_plain AS o WHERE EXISTS (SELECT 1 FROM sem_repl AS r WHERE r.id = o.id)").First(ctx, db); err != nil {
			t.Fatalf("correlated EXISTS: %v", err)
		}
	})
}

// --- layer 3: rio end to end ---

// ClickHouse-suite models; explicit IDs everywhere (nothing generates them).
type Writer struct {
	ID         int64
	Email      string
	PostsCount int64 `rio:",countof:Posts"`

	Posts  rio.HasMany[Story]
	Bio    rio.HasOne[BioCard]
	Topics rio.ManyToMany[Subject] `rio:",join:writer_subjects"`
}

type Story struct {
	ID       int64
	WriterID int64
	Title    string
	Score    int64

	Author rio.BelongsTo[Writer] `rio:",fk:writer_id"`
}

type BioCard struct {
	ID       int64
	WriterID int64
	Text     string
}

type Subject struct {
	ID   int64
	Name string
}

// Reading exercises the time promises: value, nullable, and stamps.
type Reading struct {
	ID        int64
	At        time.Time
	Maybe     *time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ProfileRow is the ReplacingMergeTree recipe: app-owned version column,
// Insert new versions, read through Final().
type ProfileRow struct {
	ID      int64
	Name    string
	Version int64 `rio:",version"`
}

func (ProfileRow) TableName() string { return "ch_profiles" }

// Note exercises soft-delete read filtering (the writes are rejected).
type Note struct {
	ID        int64
	Body      string
	DeletedAt *time.Time `rio:",softdelete"`
}

var chSuiteDDL = []string{
	"DROP TABLE IF EXISTS writer_subjects", "DROP TABLE IF EXISTS subjects", "DROP TABLE IF EXISTS bio_cards",
	"DROP TABLE IF EXISTS stories", "DROP TABLE IF EXISTS writers", "DROP TABLE IF EXISTS readings",
	"DROP TABLE IF EXISTS ch_profiles", "DROP TABLE IF EXISTS notes",
	"CREATE TABLE writers (id Int64, email String) ENGINE = MergeTree ORDER BY id",
	"CREATE TABLE stories (id Int64, writer_id Int64, title String, score Int64) ENGINE = MergeTree ORDER BY id",
	"CREATE TABLE bio_cards (id Int64, writer_id Int64, text String) ENGINE = MergeTree ORDER BY id",
	"CREATE TABLE subjects (id Int64, name String) ENGINE = MergeTree ORDER BY id",
	"CREATE TABLE writer_subjects (writer_id Int64, subject_id Int64) ENGINE = MergeTree ORDER BY (writer_id, subject_id)",
	"CREATE TABLE readings (id Int64, at DateTime64(6, 'UTC'), maybe Nullable(DateTime64(6)), created_at DateTime64(6), updated_at DateTime64(6)) ENGINE = MergeTree ORDER BY id",
	"CREATE TABLE ch_profiles (id Int64, name String, version UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY id",
	"CREATE TABLE notes (id Int64, body String, deleted_at Nullable(DateTime64(6))) ENGINE = MergeTree ORDER BY id",
}

func TestClickHouseSuite(t *testing.T) {
	db := clickhouseDB(t)
	ctx := context.Background()
	chExec(t, ctx, db, chSuiteDDL...)

	// Insert + reload: microsecond times survive Equal, nothing backfills.
	at := time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.UTC)
	maybe := time.Date(1950, 6, 1, 12, 0, 0, 500000000, time.UTC) // pre-1970 on purpose
	r := Reading{ID: 1, At: at, Maybe: &maybe}
	if err := rio.Insert(ctx, db, &r); err != nil {
		t.Fatalf("insert reading: %v", err)
	}
	if r.CreatedAt.IsZero() || r.UpdatedAt.IsZero() {
		t.Fatal("stamps must apply client-side")
	}
	got, err := rio.Find[Reading](ctx, db, int64(1))
	if err != nil {
		t.Fatalf("find reading: %v", err)
	}
	if !got.At.Equal(at.Truncate(time.Microsecond)) {
		t.Fatalf("time round-trip drifted: wrote %v, read %v", at, got.At)
	}
	wantMaybe := maybe.Truncate(time.Microsecond)
	if major, _ := chServerVersion(t, ctx, db); major < 26 {
		// Known server defect before 26.x, every input channel: the
		// fractional part of pre-1970 times parses with its sign flipped —
		// second − fraction instead of second + fraction (a .5 fraction lands
		// exactly one second low; see the clickhouse module README). Pin the
		// defect so drift inside 25.x is caught; rio does not compensate —
		// a fixed server would then be wrong the other way.
		sec := wantMaybe.Truncate(time.Second)
		wantMaybe = sec.Add(-wantMaybe.Sub(sec))
	}
	if got.Maybe == nil || !got.Maybe.Equal(wantMaybe) {
		t.Fatalf("nullable pre-1970 time round-trip: got %v, want %v", got.Maybe, wantMaybe)
	}
	if !got.CreatedAt.Equal(r.CreatedAt) {
		t.Fatalf("created_at round-trip: wrote %v, read %v", r.CreatedAt, got.CreatedAt)
	}
	// Time arguments compare against stored values (same encoding both ways).
	n, err := rio.From[Reading]().Where("at = ?", at).Count(ctx, db)
	if err != nil || n != 1 {
		t.Fatalf("time equality through args: n=%d err=%v", n, err)
	}
	n, err = rio.From[Reading]().Where("maybe < ?", time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)).Count(ctx, db)
	if err != nil || n != 1 {
		t.Fatalf("pre-1970 range through args: n=%d err=%v", n, err)
	}

	// InsertAll + reload-Equal.
	batch := []Reading{
		{ID: 2, At: at.Add(time.Hour)},
		{ID: 3, At: at.Add(2 * time.Hour)},
	}
	if err := rio.InsertAll(ctx, db, batch); err != nil {
		t.Fatalf("insert all: %v", err)
	}
	back, err := rio.From[Reading]().Where("id IN (?)", []int64{2, 3}).OrderBy("id").All(ctx, db)
	if err != nil || len(back) != 2 {
		t.Fatalf("reload batch: %v %d", err, len(back))
	}
	if !back[0].At.Equal(batch[0].At) || !back[1].At.Equal(batch[1].At) {
		t.Fatalf("batch time round-trip: %+v vs %+v", back, batch)
	}

	// Relations: all four kinds preload, WithCount aggregates, RelLimit
	// windows, WhereHas filters (version-gated).
	alice := Writer{ID: 1, Email: "alice@example.com"}
	bob := Writer{ID: 2, Email: "bob@example.com"}
	if err := rio.InsertAll(ctx, db, []Writer{alice, bob}); err != nil {
		t.Fatalf("insert writers: %v", err)
	}
	stories := []Story{
		{ID: 1, WriterID: 1, Title: "one", Score: 0},
		{ID: 2, WriterID: 1, Title: "two", Score: 1},
		{ID: 3, WriterID: 1, Title: "three", Score: 2},
	}
	if err := rio.InsertAll(ctx, db, stories); err != nil {
		t.Fatalf("insert stories: %v", err)
	}
	if err := rio.Insert(ctx, db, &BioCard{ID: 1, WriterID: 1, Text: "hi"}); err != nil {
		t.Fatalf("insert bio: %v", err)
	}
	if err := rio.InsertAll(ctx, db, []Subject{{ID: 1, Name: "go"}, {ID: 2, Name: "olap"}}); err != nil {
		t.Fatalf("insert subjects: %v", err)
	}
	chExec(t, ctx, db, "INSERT INTO writer_subjects VALUES (1, 1), (1, 2)")

	writers, err := rio.From[Writer]().OrderBy("id").
		With("Posts", rio.RelOrder("score DESC"), rio.RelLimit(2)).
		With("Bio").
		With("Topics", rio.RelOrder("name")).
		WithCount("Posts").
		All(ctx, db)
	if err != nil {
		t.Fatalf("relations query: %v", err)
	}
	a := writers[0]
	if got := a.Posts.Rows(); len(got) != 2 || got[0].Title != "three" || got[1].Title != "two" {
		t.Fatalf("HasMany + RelLimit + RelOrder: %+v", got)
	}
	if bio := a.Bio.Row(); bio == nil || bio.Text != "hi" {
		t.Fatalf("HasOne: %+v", bio)
	}
	if topics := a.Topics.Rows(); len(topics) != 2 || topics[0].Name != "go" {
		t.Fatalf("ManyToMany: %+v", topics)
	}
	if a.PostsCount != 3 || writers[1].PostsCount != 0 {
		t.Fatalf("WithCount: %d, %d", a.PostsCount, writers[1].PostsCount)
	}
	posts, err := rio.From[Story]().OrderBy("id").With("Author").All(ctx, db)
	if err != nil || posts[0].Author.Row() == nil || posts[0].Author.Row().Email != "alice@example.com" {
		t.Fatalf("BelongsTo: %v %+v", err, posts)
	}

	if major, minor := chServerVersion(t, ctx, db); major > 25 || (major == 25 && minor >= 8) {
		with, err := rio.From[Writer]().WhereHas("Posts", rio.RelWhere("score >= ?", 2)).All(ctx, db)
		if err != nil || len(with) != 1 || with[0].ID != 1 {
			t.Fatalf("WhereHas: %v %+v", err, with)
		}
		without, err := rio.From[Writer]().WhereHasNot("Posts").All(ctx, db)
		if err != nil || len(without) != 1 || without[0].ID != 2 {
			t.Fatalf("WhereHasNot: %v %+v", err, without)
		}
	} else {
		t.Logf("skipping WhereHas: server < 25.8")
	}

	// Scalar plumbing: Pluck, Count, Exists, Rows streaming, compiled.
	titles, err := rio.Pluck[string](ctx, db, rio.From[Story]().Where("writer_id = ?", 1).OrderBy("score DESC"), "title")
	if err != nil || len(titles) != 3 || titles[0] != "three" {
		t.Fatalf("Pluck: %v %v", err, titles)
	}
	streamed := 0
	for _, err := range rio.From[Story]().OrderBy("id").Rows(ctx, db) {
		if err != nil {
			t.Fatalf("Rows: %v", err)
		}
		streamed++
		if streamed == 2 {
			break
		}
	}
	if streamed != 2 {
		t.Fatalf("streamed %d", streamed)
	}
	compiled := rio.MustCompile[Story](rio.From[Story]().Where("score >= ?").OrderBy("id"))
	cs, err := compiled.All(ctx, db, 1)
	if err != nil || len(cs) != 2 {
		t.Fatalf("compiled: %v %d", err, len(cs))
	}

	// The ReplacingMergeTree recipe: Insert new versions, Final() merges.
	if err := rio.Insert(ctx, db, &ProfileRow{ID: 1, Name: "v1"}); err != nil { // version stamps to 1
		t.Fatalf("profile v1: %v", err)
	}
	if err := rio.Insert(ctx, db, &ProfileRow{ID: 1, Name: "v2", Version: 2}); err != nil {
		t.Fatalf("profile v2: %v", err)
	}
	merged, err := rio.From[ProfileRow]().Final().All(ctx, db)
	if err != nil || len(merged) != 1 || merged[0].Name != "v2" {
		t.Fatalf("Final merge: %v %+v", err, merged)
	}
	nFinal, err := rio.From[ProfileRow]().Final().Count(ctx, db)
	if err != nil || nFinal != 1 {
		t.Fatalf("Final count: %v %d", err, nFinal)
	}
	nRaw, err := rio.From[ProfileRow]().Count(ctx, db)
	if err != nil || nRaw < 1 {
		t.Fatalf("unmerged count: %v %d", err, nRaw)
	}

	// Soft-delete reads filter; the writes stay rejected (below).
	trash := time.Date(2024, 5, 5, 5, 5, 5, 0, time.UTC)
	if err := rio.InsertAll(ctx, db, []Note{
		{ID: 1, Body: "live"},
		{ID: 2, Body: "gone", DeletedAt: &trash},
	}); err != nil {
		t.Fatalf("insert notes: %v", err)
	}
	live, err := rio.From[Note]().All(ctx, db)
	if err != nil || len(live) != 1 || live[0].ID != 1 {
		t.Fatalf("soft-delete default filter: %v %+v", err, live)
	}
	all, err := rio.From[Note]().WithTrashed().Count(ctx, db)
	if err != nil || all != 2 {
		t.Fatalf("WithTrashed: %v %d", err, all)
	}
	onlyGone, err := rio.From[Note]().OnlyTrashed().First(ctx, db)
	if err != nil || onlyGone.ID != 2 {
		t.Fatalf("OnlyTrashed: %v %+v", err, onlyGone)
	}

	// The mutation escape hatch works and reports the documented zero count.
	res, err := rio.Exec(ctx, db, "ALTER TABLE notes UPDATE body = ? WHERE id = ? SETTINGS mutations_sync = 1", "edited", 1)
	if err != nil {
		t.Fatalf("mutation escape hatch: %v", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 0 {
		t.Fatalf("driver-documented zero count: n=%d err=%v", n, err)
	}
	edited, err := rio.Find[Note](ctx, db, int64(1))
	if err != nil || edited.Body != "edited" {
		t.Fatalf("mutation effect: %v %+v", err, edited)
	}
}

// TestClickHouseRejectionsSendNothing proves on a real server that the
// rejected surface fails before any statement leaves rio: a counting hook
// observes zero queries during the rejected calls.
func TestClickHouseRejectionsSendNothing(t *testing.T) {
	var statements atomic.Int64
	db := clickhouseDB(t, rio.WithQueryHook(countingHook{&statements}))
	ctx := context.Background()
	chExec(t, ctx, db,
		"DROP TABLE IF EXISTS rej_notes",
		"CREATE TABLE rej_notes (id Int64, body String, deleted_at Nullable(DateTime64(6))) ENGINE = MergeTree ORDER BY id",
	)

	type RejNote struct {
		ID        int64
		Body      string
		DeletedAt *time.Time `rio:",softdelete"`
	}
	row := &RejNote{ID: 1, Body: "x"}
	if err := rio.Insert(ctx, db, row); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	before := statements.Load()
	rejected := []struct {
		name string
		call func() error
	}{
		{"Update", func() error { return rio.Update(ctx, db, row) }},
		{"Delete", func() error { return rio.Delete(ctx, db, row) }},
		{"ForceDelete", func() error { return rio.ForceDelete(ctx, db, row) }},
		{"Restore", func() error { return rio.Restore(ctx, db, row) }},
		{"UpdateAll", func() error {
			_, err := rio.From[RejNote]().Where("id = ?", 1).UpdateAll(ctx, db, rio.Set{"body": "y"})
			return err
		}},
		{"DeleteAll", func() error {
			_, err := rio.From[RejNote]().Where("id = ?", 1).DeleteAll(ctx, db)
			return err
		}},
		{"Upsert", func() error { return rio.Upsert(ctx, db, row) }},
		{"UpsertAll", func() error { return rio.UpsertAll(ctx, db, []RejNote{*row}) }},
		{"FirstOrCreate", func() error { return rio.From[RejNote]().Where("id = ?", 1).FirstOrCreate(ctx, db, row) }},
		{"Tx", func() error { return db.Tx(ctx, func(tx *rio.Tx) error { return nil }) }},
		{"ForUpdate", func() error {
			_, err := rio.From[RejNote]().ForUpdate().All(ctx, db)
			return err
		}},
		{"InsertZeroID", func() error { return rio.Insert(ctx, db, &RejNote{Body: "zero"}) }},
	}
	for _, tc := range rejected {
		if err := tc.call(); err == nil || !strings.Contains(err.Error(), "clickhouse") {
			t.Fatalf("%s must reject with a clickhouse message, got: %v", tc.name, err)
		}
	}
	if after := statements.Load(); after != before {
		t.Fatalf("rejected calls sent %d statement(s) to the server", after-before)
	}

	// The row is untouched — nothing executed.
	n, err := rio.From[RejNote]().Count(ctx, db)
	if err != nil || n != 1 {
		t.Fatalf("table drifted after rejections: n=%d err=%v", n, err)
	}
}

type countingHook struct{ n *atomic.Int64 }

func (countingHook) BeforeQuery(ctx context.Context, _ *rio.QueryEvent) context.Context { return ctx }
func (h countingHook) AfterQuery(_ context.Context, _ *rio.QueryEvent)                  { h.n.Add(1) }

// A rio sentinel that can never fire on ClickHouse must really never fire:
// double-inserting the same sorting key succeeds (no unique constraints).
func TestClickHouseNoConstraintSentinels(t *testing.T) {
	db := clickhouseDB(t)
	ctx := context.Background()
	chExec(t, ctx, db,
		"DROP TABLE IF EXISTS dup_rows",
		"CREATE TABLE dup_rows (id Int64, v String) ENGINE = MergeTree ORDER BY id",
	)
	type DupRow struct {
		ID int64
		V  string
	}
	if err := rio.Insert(ctx, db, &DupRow{ID: 1, V: "a"}); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	err := rio.Insert(ctx, db, &DupRow{ID: 1, V: "b"})
	if err != nil {
		t.Fatalf("same-key insert must succeed (no unique constraints): %v", err)
	}
	if errors.Is(err, rio.ErrDuplicateKey) {
		t.Fatal("ErrDuplicateKey cannot exist on clickhouse")
	}
	n, err := rio.From[DupRow]().Count(ctx, db)
	if err != nil || n != 2 {
		t.Fatalf("both rows must land: n=%d err=%v", n, err)
	}
}
