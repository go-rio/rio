package bench

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-rio/rio"

	_ "modernc.org/sqlite"

	gormsqlite "github.com/libtnb/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// rio vs hand-written database/sql vs GORM on the same in-memory SQLite
// database. See README.md for methodology.

type BenchUser struct {
	ID        int64
	Email     string
	Age       int
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (BenchUser) TableName() string { return "bench_users" }

const benchDDL = `CREATE TABLE bench_users (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	email VARCHAR(191) NOT NULL,
	age INTEGER NOT NULL,
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL
)`

const (
	benchBatchSize       = 100
	benchMaxInsertedRows = 1000
	benchResetSQL        = "DELETE FROM bench_users"
)

func TestBatchInsertSQLShapes(t *testing.T) {
	sqlite := questionBatchInsertSQL(true)
	if got := strings.Count(sqlite, "?"); got != benchBatchSize*4 {
		t.Fatalf("SQLite placeholders = %d, want %d", got, benchBatchSize*4)
	}
	if !strings.HasSuffix(sqlite, " RETURNING id") {
		t.Fatalf("SQLite batch must return generated ids: %s", sqlite)
	}

	mysql := questionBatchInsertSQL(false)
	if strings.Contains(mysql, "RETURNING") {
		t.Fatalf("MySQL batch must not render RETURNING: %s", mysql)
	}

	postgres := postgresBatchInsertSQL()
	if got := strings.Count(postgres, "$"); got != benchBatchSize*4 {
		t.Fatalf("PostgreSQL placeholders = %d, want %d", got, benchBatchSize*4)
	}
	if !strings.Contains(postgres, "($397, $398, $399, $400) RETURNING id") {
		t.Fatalf("PostgreSQL batch tail is malformed: %s", postgres)
	}

	if got := len(batchInsertArgs(newBenchBatch(), time.Now())); got != benchBatchSize*4 {
		t.Fatalf("batch args = %d, want %d", got, benchBatchSize*4)
	}
}

// benchRawDB opens a shared-memory SQLite DB (one pinned conn keeps it
// alive) and applies ddl — benchDDL when none given.
func benchRawDB(b *testing.B, name string, ddl ...string) *sql.DB {
	b.Helper()
	raw, err := sql.Open("sqlite", "file:"+name+"?mode=memory&cache=shared")
	if err != nil {
		b.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	if len(ddl) == 0 {
		ddl = []string{benchDDL}
	}
	for _, stmt := range ddl {
		if _, err := raw.Exec(stmt); err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() { _ = raw.Close() })
	return raw
}

func seed(b *testing.B, raw *sql.DB, n int) {
	b.Helper()
	now := time.Now().UTC().Format("2006-01-02 15:04:05.999999+00:00")
	for i := range n {
		if _, err := raw.Exec("INSERT INTO bench_users (email, age, created_at, updated_at) VALUES (?, ?, ?, ?)",
			fmt.Sprintf("u%d@example.com", i), 20+i%50, now, now); err != nil {
			b.Fatal(err)
		}
	}
}

func benchGorm(b *testing.B, name string) *gorm.DB {
	b.Helper()
	gdb, err := gorm.Open(gormsqlite.Open("file:"+name+"?mode=memory&cache=shared"), &gorm.Config{
		Logger:      logger.Discard,
		QueryFields: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	sqlDB := gormSQLDB(b, gdb)
	sqlDB.SetMaxOpenConns(1)
	if _, err := sqlDB.Exec(benchDDL); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = sqlDB.Close() })
	return gdb
}

func BenchmarkReadOne_Rio(b *testing.B) {
	raw := benchRawDB(b, "rio_read1")
	seed(b, raw, 100)
	db := rio.New(raw, rio.SQLite)
	ctx := context.Background()
	q := rio.From[BenchUser]().Where("id = ?").Limit(1).Must()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.First(ctx, db, int64(i%100+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadOne_RioBuilder(b *testing.B) {
	raw := benchRawDB(b, "rio_read1b")
	seed(b, raw, 100)
	db := rio.New(raw, rio.SQLite)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rio.Find[BenchUser](ctx, db, int64(i%100+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadOne_Stdlib(b *testing.B) {
	raw := benchRawDB(b, "std_read1")
	seed(b, raw, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var u BenchUser
		err := raw.QueryRow(
			`SELECT "id", "email", "age", "created_at", "updated_at" FROM bench_users WHERE id = ? LIMIT 1`,
			int64(i%100+1),
		).Scan(&u.ID, &u.Email, &u.Age, &u.CreatedAt, &u.UpdatedAt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadOne_Gorm(b *testing.B) {
	gdb := benchGorm(b, "gorm_read1")
	sqlDB := gormSQLDB(b, gdb)
	seed(b, sqlDB, 100)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var u BenchUser
		if err := gdb.WithContext(ctx).Where("id = ?", int64(i%100+1)).Take(&u).Error; err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadHundred_Rio(b *testing.B) {
	raw := benchRawDB(b, "rio_read100")
	seed(b, raw, 100)
	db := rio.New(raw, rio.SQLite)
	ctx := context.Background()
	q := rio.From[BenchUser]().Where("age >= ?").Must()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := q.All(ctx, db, 0)
		if err != nil || len(rows) != 100 {
			b.Fatalf("%v %d", err, len(rows))
		}
	}
}

func BenchmarkReadHundred_Stdlib(b *testing.B) {
	raw := benchRawDB(b, "std_read100")
	seed(b, raw, 100)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := readHundredStdlib(
			ctx,
			raw,
			`SELECT "id", "email", "age", "created_at", "updated_at" `+
				`FROM bench_users WHERE age >= ?`,
			0,
		)
		if err != nil || len(out) != 100 {
			b.Fatalf("%v %d", err, len(out))
		}
	}
}

// readHundredStdlib uses idiomatic Rows.Scan on purpose: its variadic boxing
// is part of the baseline.
func readHundredStdlib(ctx context.Context, db *sql.DB, query string, args ...any) (out []BenchUser, err error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	for rows.Next() {
		var u BenchUser
		if err := rows.Scan(&u.ID, &u.Email, &u.Age, &u.CreatedAt, &u.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func BenchmarkReadHundred_Gorm(b *testing.B) {
	gdb := benchGorm(b, "gorm_read100")
	sqlDB := gormSQLDB(b, gdb)
	seed(b, sqlDB, 100)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out []BenchUser
		if err := gdb.WithContext(ctx).Where("age >= ?", 0).Find(&out).Error; err != nil || len(out) != 100 {
			b.Fatalf("%v %d", err, len(out))
		}
	}
}

func BenchmarkInsert_Rio(b *testing.B) {
	raw := benchRawDB(b, "rio_ins")
	db := rio.New(raw, rio.SQLite)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetInsertTable(b, raw, i, 1, benchResetSQL)
		u := BenchUser{Email: "x@example.com", Age: 30}
		if err := rio.Insert(ctx, db, &u); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsert_Stdlib(b *testing.B) {
	raw := benchRawDB(b, "std_ins")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetInsertTable(b, raw, i, 1, benchResetSQL)
		now := time.Now().UTC().Truncate(time.Microsecond).Format("2006-01-02 15:04:05.999999+00:00")
		res, err := raw.Exec("INSERT INTO bench_users (email, age, created_at, updated_at) VALUES (?, ?, ?, ?)",
			"x@example.com", 30, now, now)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := res.LastInsertId(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsert_Gorm(b *testing.B) {
	gdb := benchGorm(b, "gorm_ins")
	sqlDB := gormSQLDB(b, gdb)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetInsertTable(b, sqlDB, i, 1, benchResetSQL)
		u := BenchUser{Email: "x@example.com", Age: 30}
		if err := gdb.WithContext(ctx).Create(&u).Error; err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate_Rio(b *testing.B) {
	raw := benchRawDB(b, "rio_upd")
	seed(b, raw, 100)
	db := rio.New(raw, rio.SQLite)
	ctx := context.Background()
	u, err := rio.Find[BenchUser](ctx, db, 1)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u.Age = 20 + i%50
		if err := rio.Update(ctx, db, u); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate_Stdlib(b *testing.B) {
	raw := benchRawDB(b, "std_upd")
	seed(b, raw, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		now := time.Now().UTC().Truncate(time.Microsecond).Format("2006-01-02 15:04:05.999999+00:00")
		if _, err := raw.Exec(`UPDATE bench_users SET email = ?, age = ?, updated_at = ? WHERE id = ?`,
			"u1@example.com", 20+i%50, now, int64(1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate_Gorm(b *testing.B) {
	gdb := benchGorm(b, "gorm_upd")
	sqlDB := gormSQLDB(b, gdb)
	seed(b, sqlDB, 100)
	ctx := context.Background()
	var u BenchUser
	if err := gdb.WithContext(ctx).Take(&u, 1).Error; err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u.Age = 20 + i%50
		if err := gdb.WithContext(ctx).Save(&u).Error; err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertBatch100_Rio(b *testing.B) {
	raw := benchRawDB(b, "rio_batch")
	db := rio.New(raw, rio.SQLite)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetInsertTable(b, raw, i, benchBatchSize, benchResetSQL)
		rows := newBenchBatch()
		if err := rio.InsertAll(ctx, db, rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertBatch100_Stdlib(b *testing.B) {
	raw := benchRawDB(b, "std_batch")
	ctx := context.Background()
	query := questionBatchInsertSQL(true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetInsertTable(b, raw, i, benchBatchSize, benchResetSQL)
		rows := newBenchBatch()
		now := time.Now().UTC().Truncate(time.Microsecond).Format("2006-01-02 15:04:05.999999+00:00")
		if err := insertBatchReturningStdlib(ctx, raw, query, batchInsertArgs(rows, now), rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertBatch100_Gorm(b *testing.B) {
	gdb := benchGorm(b, "gorm_batch")
	sqlDB := gormSQLDB(b, gdb)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetInsertTable(b, sqlDB, i, benchBatchSize, benchResetSQL)
		rows := newBenchBatch()
		if err := gdb.WithContext(ctx).Create(&rows).Error; err != nil {
			b.Fatal(err)
		}
	}
}

func resetInsertTable(b *testing.B, db *sql.DB, iteration, rowsPerIteration int, resetSQL string) {
	b.Helper()
	every := max(benchMaxInsertedRows/rowsPerIteration, 1)
	if iteration == 0 || iteration%every != 0 {
		return
	}
	b.StopTimer()
	if _, err := db.Exec(resetSQL); err != nil {
		b.Fatal(err)
	}
	b.StartTimer()
}

func newBenchBatch() []BenchUser {
	rows := make([]BenchUser, benchBatchSize)
	for i := range rows {
		rows[i] = BenchUser{Email: "x@example.com", Age: i}
	}
	return rows
}

func batchInsertArgs(rows []BenchUser, now any) []any {
	args := make([]any, 0, len(rows)*4)
	for i := range rows {
		args = append(args, rows[i].Email, rows[i].Age, now, now)
	}
	return args
}

func questionBatchInsertSQL(returning bool) string {
	var query strings.Builder
	query.WriteString("INSERT INTO bench_users (email, age, created_at, updated_at) VALUES ")
	for i := range benchBatchSize {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString("(?, ?, ?, ?)")
	}
	if returning {
		query.WriteString(" RETURNING id")
	}
	return query.String()
}

func postgresBatchInsertSQL() string {
	var query strings.Builder
	query.WriteString("INSERT INTO bench_users (email, age, created_at, updated_at) VALUES ")
	for i := range benchBatchSize {
		if i > 0 {
			query.WriteString(", ")
		}
		base := i * 4
		fmt.Fprintf(&query, "($%d, $%d, $%d, $%d)", base+1, base+2, base+3, base+4)
	}
	query.WriteString(" RETURNING id")
	return query.String()
}

func insertBatchReturningStdlib(
	ctx context.Context,
	db *sql.DB,
	query string,
	args []any,
	out []BenchUser,
) (err error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()

	n := 0
	for rows.Next() {
		if n == len(out) {
			return fmt.Errorf("batch insert returned more than %d ids", len(out))
		}
		if err := rows.Scan(&out[n].ID); err != nil {
			return err
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if n != len(out) {
		return fmt.Errorf("batch insert returned %d ids, want %d", n, len(out))
	}
	return nil
}

func gormSQLDB(b *testing.B, db *gorm.DB) *sql.DB {
	b.Helper()
	raw, err := db.DB()
	if err != nil {
		b.Fatal(err)
	}
	return raw
}
