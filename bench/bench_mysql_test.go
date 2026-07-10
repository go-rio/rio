package bench

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-rio/rio"

	riomysql "github.com/go-rio/mysql"

	_ "github.com/go-sql-driver/mysql"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Real-network MySQL benchmarks, same three legs and five shapes as the
// SQLite file. Gated on RIO_BENCH_MYSQL_DSN so `go test ./...` stays hermetic:
//
//	docker run -d --name rio-bench-mysql -e MYSQL_ROOT_PASSWORD=bench \
//	  -e MYSQL_DATABASE=bench -p 13306:3306 mysql:8.4 \
//	  --skip-log-bin --innodb-flush-log-at-trx-commit=0
//	RIO_BENCH_MYSQL_DSN='root:bench@tcp(127.0.0.1:13306)/bench?parseTime=true' \
//	  go test -bench MySQL -benchmem -run NONE ./...
//
// All three legs share go-sql-driver/mysql (default DSN options beyond
// parseTime=true, which rio requires and GORM recommends), so the delta is
// ORM overhead. The GORM leg runs GORM's default configuration — PrepareStmt
// off — plus QueryFields to select explicit columns like rio does.
const mysqlDDL = `CREATE TABLE bench_users (
	id BIGINT AUTO_INCREMENT PRIMARY KEY,
	email VARCHAR(191) NOT NULL,
	age INT NOT NULL,
	created_at DATETIME(6) NOT NULL,
	updated_at DATETIME(6) NOT NULL
)`

func mysqlDSN(b *testing.B) string {
	b.Helper()
	dsn := os.Getenv("RIO_BENCH_MYSQL_DSN")
	if dsn == "" {
		b.Skip("RIO_BENCH_MYSQL_DSN not set")
	}
	return dsn
}

func benchMySQLRaw(b *testing.B) *sql.DB {
	b.Helper()
	raw, err := sql.Open("mysql", mysqlDSN(b))
	if err != nil {
		b.Fatal(err)
	}
	if _, err := raw.Exec(`DROP TABLE IF EXISTS bench_users`); err != nil {
		b.Fatal(err)
	}
	if _, err := raw.Exec(mysqlDDL); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = raw.Close() })
	return raw
}

func seedMySQL(b *testing.B, raw *sql.DB, n int) {
	b.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < n; i++ {
		if _, err := raw.Exec("INSERT INTO bench_users (email, age, created_at, updated_at) VALUES (?, ?, ?, ?)",
			fmt.Sprintf("u%d@example.com", i), 20+i%50, now, now); err != nil {
			b.Fatal(err)
		}
	}
}

func benchMySQLGorm(b *testing.B) *gorm.DB {
	b.Helper()
	raw := benchMySQLRaw(b)
	gdb, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: raw}), &gorm.Config{
		Logger:      logger.Discard,
		QueryFields: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	return gdb
}

func BenchmarkMySQLReadOne_Rio(b *testing.B) {
	raw := benchMySQLRaw(b)
	seedMySQL(b, raw, 100)
	db := riomysql.New(raw)
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

// BenchmarkMySQLReadOne_RioStmtCache measures rio.WithStmtCache on MySQL.
// go-sql-driver runs every parameterized query as prepare+execute (two
// blocking round-trips); a cached prepared statement drops that to one.
func BenchmarkMySQLReadOne_RioStmtCache(b *testing.B) {
	raw := benchMySQLRaw(b)
	seedMySQL(b, raw, 100)
	db := riomysql.New(raw, rio.WithStmtCache())
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

func BenchmarkMySQLInsert_RioStmtCache(b *testing.B) {
	raw := benchMySQLRaw(b)
	db := riomysql.New(raw, rio.WithStmtCache())
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

func BenchmarkMySQLReadOne_RioBuilder(b *testing.B) {
	raw := benchMySQLRaw(b)
	seedMySQL(b, raw, 100)
	db := riomysql.New(raw)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rio.Find[BenchUser](ctx, db, int64(i%100+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMySQLReadOne_Stdlib(b *testing.B) {
	raw := benchMySQLRaw(b)
	seedMySQL(b, raw, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var u BenchUser
		err := raw.QueryRow(
			"SELECT `id`, `email`, `age`, `created_at`, `updated_at` FROM bench_users WHERE id = ? LIMIT 1",
			int64(i%100+1),
		).Scan(&u.ID, &u.Email, &u.Age, &u.CreatedAt, &u.UpdatedAt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMySQLReadOne_Gorm(b *testing.B) {
	gdb := benchMySQLGorm(b)
	sqlDB, _ := gdb.DB()
	seedMySQL(b, sqlDB, 100)
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

func BenchmarkMySQLReadHundred_Rio(b *testing.B) {
	raw := benchMySQLRaw(b)
	seedMySQL(b, raw, 100)
	db := riomysql.New(raw)
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

func BenchmarkMySQLReadHundred_Stdlib(b *testing.B) {
	raw := benchMySQLRaw(b)
	seedMySQL(b, raw, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := raw.Query("SELECT `id`, `email`, `age`, `created_at`, `updated_at` FROM bench_users WHERE age >= ?", 0)
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

func BenchmarkMySQLReadHundred_Gorm(b *testing.B) {
	gdb := benchMySQLGorm(b)
	sqlDB, _ := gdb.DB()
	seedMySQL(b, sqlDB, 100)
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

func BenchmarkMySQLInsert_Rio(b *testing.B) {
	raw := benchMySQLRaw(b)
	db := riomysql.New(raw)
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

func BenchmarkMySQLInsert_Stdlib(b *testing.B) {
	raw := benchMySQLRaw(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		now := time.Now().UTC().Truncate(time.Microsecond)
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

func BenchmarkMySQLInsert_Gorm(b *testing.B) {
	gdb := benchMySQLGorm(b)
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

func BenchmarkMySQLUpdate_Rio(b *testing.B) {
	raw := benchMySQLRaw(b)
	seedMySQL(b, raw, 100)
	db := riomysql.New(raw)
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

func BenchmarkMySQLUpdate_Stdlib(b *testing.B) {
	raw := benchMySQLRaw(b)
	seedMySQL(b, raw, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		now := time.Now().UTC().Truncate(time.Microsecond)
		if _, err := raw.Exec("UPDATE bench_users SET email = ?, age = ?, updated_at = ? WHERE id = ?",
			"u1@example.com", 20+i%50, now, int64(1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMySQLUpdate_Gorm(b *testing.B) {
	gdb := benchMySQLGorm(b)
	sqlDB, _ := gdb.DB()
	seedMySQL(b, sqlDB, 100)
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

func BenchmarkMySQLInsertBatch100_Rio(b *testing.B) {
	raw := benchMySQLRaw(b)
	db := riomysql.New(raw)
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

func BenchmarkMySQLInsertBatch100_Gorm(b *testing.B) {
	gdb := benchMySQLGorm(b)
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
