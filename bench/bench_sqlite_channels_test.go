package bench

import (
	"context"
	"testing"

	"github.com/go-rio/rio"
	"github.com/go-rio/sqlite"
)

// The same in-memory shape as bench_test.go on two channels: database/sql
// over the modernc driver with rio's statement cache (the sqlite module's
// former default), and the sqlite module's native channel.

func benchCachedDB(b *testing.B, name string, seedRows int) *rio.DB {
	b.Helper()
	raw := benchRawDB(b, name)
	if seedRows > 0 {
		seed(b, raw, seedRows)
	}
	return rio.New(raw, rio.SQLite, rio.WithStmtCache())
}

// benchNativeDB opens the native channel on a shared-cache memory database;
// its Unwrap view seeds and resets the tables through database/sql.
func benchNativeDB(b *testing.B, name string, seedRows int) *rio.DB {
	b.Helper()
	db, err := sqlite.Open("file:" + name + "?mode=memory&cache=shared")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if _, err := rio.Exec(context.Background(), db, benchDDL); err != nil {
		b.Fatal(err)
	}
	if seedRows > 0 {
		seed(b, db.Unwrap(), seedRows)
	}
	return db
}

func BenchmarkReadOne_RioCached(b *testing.B) { benchReadOne(b, benchCachedDB(b, "rioc_read1", 100)) }
func BenchmarkReadOne_RioNative(b *testing.B) { benchReadOne(b, benchNativeDB(b, "rion_read1", 100)) }
func BenchmarkReadHundred_RioCached(b *testing.B) {
	benchReadHundred(b, benchCachedDB(b, "rioc_read100", 100))
}
func BenchmarkReadHundred_RioNative(b *testing.B) {
	benchReadHundred(b, benchNativeDB(b, "rion_read100", 100))
}
func BenchmarkInsert_RioCached(b *testing.B) { benchInsert(b, benchCachedDB(b, "rioc_ins", 0)) }
func BenchmarkInsert_RioNative(b *testing.B) { benchInsert(b, benchNativeDB(b, "rion_ins", 0)) }
func BenchmarkUpdate_RioCached(b *testing.B) { benchUpdate(b, benchCachedDB(b, "rioc_upd", 100)) }
func BenchmarkUpdate_RioNative(b *testing.B) { benchUpdate(b, benchNativeDB(b, "rion_upd", 100)) }
func BenchmarkInsertBatch100_RioCached(b *testing.B) {
	benchInsertBatch(b, benchCachedDB(b, "rioc_batch", 0))
}
func BenchmarkInsertBatch100_RioNative(b *testing.B) {
	benchInsertBatch(b, benchNativeDB(b, "rion_batch", 0))
}

func benchReadOne(b *testing.B, db *rio.DB) {
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

func benchReadHundred(b *testing.B, db *rio.DB) {
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

func benchInsert(b *testing.B, db *rio.DB) {
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetInsertTable(b, db.Unwrap(), i, 1, benchResetSQL)
		u := BenchUser{Email: "x@example.com", Age: 30}
		if err := rio.Insert(ctx, db, &u); err != nil {
			b.Fatal(err)
		}
	}
}

func benchUpdate(b *testing.B, db *rio.DB) {
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

func benchInsertBatch(b *testing.B, db *rio.DB) {
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetInsertTable(b, db.Unwrap(), i, benchBatchSize, benchResetSQL)
		if err := rio.InsertAll(ctx, db, newBenchBatch()); err != nil {
			b.Fatal(err)
		}
	}
}
