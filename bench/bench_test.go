package bench

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-rio/rio"

	_ "github.com/glebarez/go-sqlite"

	glebarez "github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// The benchmarks compare rio, hand-written database/sql, and GORM on the
// same in-memory SQLite database (pure Go on both sides), measuring the ORM
// overhead itself. Run: go test -bench . -benchmem -run=NONE ./...

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

func benchRawDB(b *testing.B, name string) *sql.DB {
	b.Helper()
	raw, err := sql.Open("sqlite", "file:"+name+"?mode=memory&cache=shared")
	if err != nil {
		b.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec(benchDDL); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = raw.Close() })
	return raw
}

func seed(b *testing.B, raw *sql.DB, n int) {
	b.Helper()
	now := time.Now().UTC().Format("2006-01-02 15:04:05.999999+00:00")
	for i := 0; i < n; i++ {
		if _, err := raw.Exec("INSERT INTO bench_users (email, age, created_at, updated_at) VALUES (?, ?, ?, ?)",
			fmt.Sprintf("u%d@example.com", i), 20+i%50, now, now); err != nil {
			b.Fatal(err)
		}
	}
}

func benchGorm(b *testing.B, name string) *gorm.DB {
	b.Helper()
	gdb, err := gorm.Open(glebarez.Open("file:"+name+"?mode=memory&cache=shared"), &gorm.Config{
		Logger:      logger.Discard,
		QueryFields: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	sqlDB, _ := gdb.DB()
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
	q := rio.MustCompile[BenchUser](rio.From[BenchUser]().Where("id = ?").Limit(1))
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
	sqlDB, _ := gdb.DB()
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
	q := rio.MustCompile[BenchUser](rio.From[BenchUser]().Where("age >= ?"))
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
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := raw.Query(`SELECT "id", "email", "age", "created_at", "updated_at" FROM bench_users WHERE age >= ?`, 0)
		if err != nil {
			b.Fatal(err)
		}
		var out []BenchUser
		for rows.Next() {
			var u BenchUser
			if err := rows.Scan(&u.ID, &u.Email, &u.Age, &u.CreatedAt, &u.UpdatedAt); err != nil {
				b.Fatal(err)
			}
			out = append(out, u)
		}
		if rows.Close(); len(out) != 100 {
			b.Fatal(len(out))
		}
	}
}

func BenchmarkReadHundred_Gorm(b *testing.B) {
	gdb := benchGorm(b, "gorm_read100")
	sqlDB, _ := gdb.DB()
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
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	sqlDB, _ := gdb.DB()
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
		rows := make([]BenchUser, 100)
		for j := range rows {
			rows[j] = BenchUser{Email: "x@example.com", Age: j}
		}
		if err := rio.InsertAll(ctx, db, rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertBatch100_Gorm(b *testing.B) {
	gdb := benchGorm(b, "gorm_batch")
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := make([]BenchUser, 100)
		for j := range rows {
			rows[j] = BenchUser{Email: "x@example.com", Age: j}
		}
		if err := gdb.WithContext(ctx).Create(&rows).Error; err != nil {
			b.Fatal(err)
		}
	}
}
