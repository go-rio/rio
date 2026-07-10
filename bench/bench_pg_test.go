package bench

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-rio/rio"

	riopostgres "github.com/go-rio/postgres"

	_ "github.com/jackc/pgx/v5/stdlib"

	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Real-network PostgreSQL benchmarks, same three legs and five shapes as the
// SQLite file. Gated on RIO_BENCH_PG_DSN so `go test ./...` stays hermetic:
//
//	docker run -d --name rio-bench-pg -e POSTGRES_PASSWORD=bench -p 15432:5432 \
//	  postgres:17 -c fsync=off -c synchronous_commit=off -c full_page_writes=off
//	RIO_BENCH_PG_DSN='postgres://postgres:bench@127.0.0.1:15432/postgres?sslmode=disable' \
//	  go test -bench 'PG' -benchmem -run NONE ./...
//
// All three legs share pgx's database/sql adapter (and its default per-conn
// statement cache), so the delta is ORM overhead, not driver choice. The GORM
// leg runs GORM's default configuration — PrepareStmt off — plus QueryFields
// to select explicit columns like rio does.
const pgDDL = `CREATE TABLE bench_users (
	id BIGSERIAL PRIMARY KEY,
	email VARCHAR(191) NOT NULL,
	age INTEGER NOT NULL,
	created_at TIMESTAMPTZ NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL
)`

func pgDSN(b *testing.B) string {
	b.Helper()
	dsn := os.Getenv("RIO_BENCH_PG_DSN")
	if dsn == "" {
		b.Skip("RIO_BENCH_PG_DSN not set")
	}
	return dsn
}

func benchPGRaw(b *testing.B) *sql.DB {
	b.Helper()
	raw, err := sql.Open("pgx", pgDSN(b))
	if err != nil {
		b.Fatal(err)
	}
	if _, err := raw.Exec(`DROP TABLE IF EXISTS bench_users`); err != nil {
		b.Fatal(err)
	}
	if _, err := raw.Exec(pgDDL); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = raw.Close() })
	return raw
}

func seedPG(b *testing.B, raw *sql.DB, n int) {
	b.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < n; i++ {
		if _, err := raw.Exec("INSERT INTO bench_users (email, age, created_at, updated_at) VALUES ($1, $2, $3, $4)",
			fmt.Sprintf("u%d@example.com", i), 20+i%50, now, now); err != nil {
			b.Fatal(err)
		}
	}
}

func benchPGGorm(b *testing.B) *gorm.DB {
	b.Helper()
	raw := benchPGRaw(b)
	gdb, err := gorm.Open(gormpostgres.New(gormpostgres.Config{Conn: raw}), &gorm.Config{
		Logger:      logger.Discard,
		QueryFields: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	return gdb
}

func BenchmarkPGReadOne_Rio(b *testing.B) {
	raw := benchPGRaw(b)
	seedPG(b, raw, 100)
	db := riopostgres.New(raw)
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

// BenchmarkPGReadOne_RioStmtCache measures rio.WithStmtCache against the
// default pgx-stdlib path (which already caches statements per connection),
// isolating what an explicit database/sql prepared statement saves on top.
func BenchmarkPGReadOne_RioStmtCache(b *testing.B) {
	raw := benchPGRaw(b)
	seedPG(b, raw, 100)
	db := riopostgres.New(raw, rio.WithStmtCache())
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

func BenchmarkPGInsert_RioStmtCache(b *testing.B) {
	raw := benchPGRaw(b)
	db := riopostgres.New(raw, rio.WithStmtCache())
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

func BenchmarkPGReadOne_RioBuilder(b *testing.B) {
	raw := benchPGRaw(b)
	seedPG(b, raw, 100)
	db := riopostgres.New(raw)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rio.Find[BenchUser](ctx, db, int64(i%100+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPGReadOne_Stdlib(b *testing.B) {
	raw := benchPGRaw(b)
	seedPG(b, raw, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var u BenchUser
		err := raw.QueryRow(
			`SELECT "id", "email", "age", "created_at", "updated_at" FROM bench_users WHERE id = $1 LIMIT 1`,
			int64(i%100+1),
		).Scan(&u.ID, &u.Email, &u.Age, &u.CreatedAt, &u.UpdatedAt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPGReadOne_Gorm(b *testing.B) {
	gdb := benchPGGorm(b)
	sqlDB, _ := gdb.DB()
	seedPG(b, sqlDB, 100)
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

func BenchmarkPGReadHundred_Rio(b *testing.B) {
	raw := benchPGRaw(b)
	seedPG(b, raw, 100)
	db := riopostgres.New(raw)
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

func BenchmarkPGReadHundred_Stdlib(b *testing.B) {
	raw := benchPGRaw(b)
	seedPG(b, raw, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := raw.Query(`SELECT "id", "email", "age", "created_at", "updated_at" FROM bench_users WHERE age >= $1`, 0)
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

func BenchmarkPGReadHundred_Gorm(b *testing.B) {
	gdb := benchPGGorm(b)
	sqlDB, _ := gdb.DB()
	seedPG(b, sqlDB, 100)
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

func BenchmarkPGInsert_Rio(b *testing.B) {
	raw := benchPGRaw(b)
	db := riopostgres.New(raw)
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

func BenchmarkPGInsert_Stdlib(b *testing.B) {
	raw := benchPGRaw(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		now := time.Now().UTC().Truncate(time.Microsecond)
		var id int64
		err := raw.QueryRow(`INSERT INTO bench_users (email, age, created_at, updated_at) VALUES ($1, $2, $3, $4) RETURNING id`,
			"x@example.com", 30, now, now).Scan(&id)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPGInsert_Gorm(b *testing.B) {
	gdb := benchPGGorm(b)
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

func BenchmarkPGUpdate_Rio(b *testing.B) {
	raw := benchPGRaw(b)
	seedPG(b, raw, 100)
	db := riopostgres.New(raw)
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

func BenchmarkPGUpdate_Stdlib(b *testing.B) {
	raw := benchPGRaw(b)
	seedPG(b, raw, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		now := time.Now().UTC().Truncate(time.Microsecond)
		if _, err := raw.Exec(`UPDATE bench_users SET email = $1, age = $2, updated_at = $3 WHERE id = $4`,
			"u1@example.com", 20+i%50, now, int64(1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPGUpdate_Gorm(b *testing.B) {
	gdb := benchPGGorm(b)
	sqlDB, _ := gdb.DB()
	seedPG(b, sqlDB, 100)
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

func BenchmarkPGInsertBatch100_Rio(b *testing.B) {
	raw := benchPGRaw(b)
	db := riopostgres.New(raw)
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

func BenchmarkPGInsertBatch100_Gorm(b *testing.B) {
	gdb := benchPGGorm(b)
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
