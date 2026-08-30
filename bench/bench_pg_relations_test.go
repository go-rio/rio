package bench

import (
	"context"
	"os"
	"testing"

	"github.com/go-rio/postgres"
	"github.com/go-rio/rio"
)

// PostgreSQL relation-path benchmarks: the same 100×5 shape as the SQLite
// pair, but over a real network round trip per statement — the dimension
// batching addresses. Both channels run so the native seam stays honest.

func benchPGRelationDB(b *testing.B, native bool) *rio.DB {
	b.Helper()
	dsn := os.Getenv("RIO_BENCH_PG_DSN")
	if dsn == "" {
		b.Skip("RIO_BENCH_PG_DSN not set")
	}
	ctx := context.Background()
	var db *rio.DB
	var err error
	if native {
		db, err = postgres.OpenNative(ctx, dsn)
	} else {
		db, err = postgres.Open(dsn)
	}
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	ddl := []string{
		`DROP TABLE IF EXISTS bench_articles`,
		`DROP TABLE IF EXISTS bench_reviews`,
		`DROP TABLE IF EXISTS bench_authors`,
		`CREATE TABLE bench_authors (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL)`,
		`CREATE TABLE bench_articles (
			id BIGSERIAL PRIMARY KEY,
			bench_author_id BIGINT NOT NULL,
			title TEXT NOT NULL
		)`,
		`CREATE INDEX idx_bench_articles_author ON bench_articles (bench_author_id)`,
		`CREATE TABLE bench_reviews (
			id BIGSERIAL PRIMARY KEY,
			bench_author_id BIGINT NOT NULL,
			stars BIGINT NOT NULL
		)`,
		`CREATE INDEX idx_bench_reviews_author ON bench_reviews (bench_author_id)`,
	}
	for _, stmt := range ddl {
		if _, err := rio.Exec(ctx, db, stmt); err != nil {
			b.Fatal(err)
		}
	}
	for a := 1; a <= 100; a++ {
		if _, err := rio.Exec(ctx, db, "INSERT INTO bench_authors (name) VALUES ('author')"); err != nil {
			b.Fatal(err)
		}
		for range 5 {
			if _, err := rio.Exec(ctx, db,
				"INSERT INTO bench_articles (bench_author_id, title) VALUES (?, 'title')", a); err != nil {
				b.Fatal(err)
			}
		}
		for range 3 {
			if _, err := rio.Exec(ctx, db,
				"INSERT INTO bench_reviews (bench_author_id, stars) VALUES (?, 5)", a); err != nil {
				b.Fatal(err)
			}
		}
	}
	return db
}

func benchPGWithPosts(b *testing.B, db *rio.DB) {
	// Two preloads and a reused count: three statements, and on a batching
	// channel the relation layer's two collapse into one round trip.
	q := rio.From[BenchAuthor]().With("Posts").With("Reviews").WithCount("Posts").Must()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := q.All(ctx, db)
		if err != nil || len(out) != 100 || len(out[0].Posts.Rows()) != 5 || len(out[0].Reviews.Rows()) != 3 || out[0].PostCount != 5 {
			b.Fatalf("len=%d err=%v", len(out), err)
		}
	}
}

func BenchmarkPGReadHundredWithPosts_Rio(b *testing.B) {
	benchPGWithPosts(b, benchPGRelationDB(b, false))
}

func BenchmarkPGReadHundredWithPosts_RioNative(b *testing.B) {
	benchPGWithPosts(b, benchPGRelationDB(b, true))
}
