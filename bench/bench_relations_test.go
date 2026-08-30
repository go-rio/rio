package bench

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/go-rio/rio"
)

// Relation-path benchmarks: 100 parents × 5 children on real SQLite, rio's
// preload against the hand-written two-query equivalent it generates.

type BenchAuthor struct {
	ID        int64
	Name      string
	Posts     rio.HasMany[BenchArticle]
	PostCount int64 `rio:",countof:Posts"`
}

func (BenchAuthor) TableName() string { return "bench_authors" }

type BenchArticle struct {
	ID            int64
	BenchAuthorID int64
	Title         string
}

func (BenchArticle) TableName() string { return "bench_articles" }

func benchRelationDB(b *testing.B, name string) *sql.DB {
	b.Helper()
	raw := benchRawDB(b, name,
		`CREATE TABLE bench_authors (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)`,
		`CREATE TABLE bench_articles (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bench_author_id INTEGER NOT NULL,
			title TEXT NOT NULL
		)`,
		`CREATE INDEX idx_bench_articles_author ON bench_articles (bench_author_id)`,
	)
	for a := 1; a <= 100; a++ {
		if _, err := raw.Exec("INSERT INTO bench_authors (name) VALUES ('author')"); err != nil {
			b.Fatal(err)
		}
		for range 5 {
			if _, err := raw.Exec(
				"INSERT INTO bench_articles (bench_author_id, title) VALUES (?, 'title')",
				a,
			); err != nil {
				b.Fatal(err)
			}
		}
	}
	return raw
}

func BenchmarkReadHundredWithPosts_Rio(b *testing.B) {
	raw := benchRelationDB(b, "rio_rel100")
	db := rio.New(raw, rio.SQLite)
	q := rio.From[BenchAuthor]().With("Posts").Must()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := q.All(ctx, db)
		if err != nil || len(out) != 100 || len(out[0].Posts.Rows()) != 5 {
			b.Fatalf("len=%d err=%v", len(out), err)
		}
	}
}

func BenchmarkReadHundredWithPosts_Stdlib(b *testing.B) {
	raw := benchRelationDB(b, "std_rel100")
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		authors, err := readAuthorsWithPostsStdlib(ctx, raw)
		if err != nil || len(authors) != 100 || len(authors[0].posts) != 5 {
			b.Fatalf("len=%d err=%v", len(authors), err)
		}
	}
}

type stdAuthor struct {
	id    int64
	name  string
	posts []BenchArticle
}

// readAuthorsWithPostsStdlib is the two-query plan rio generates, written by
// hand: select the parents, then one IN query grouped back per parent.
func readAuthorsWithPostsStdlib(ctx context.Context, db *sql.DB) ([]stdAuthor, error) {
	rows, err := db.QueryContext(ctx, "SELECT id, name FROM bench_authors")
	if err != nil {
		return nil, err
	}
	authors := make([]stdAuthor, 0, 100)
	index := make(map[int64]int, 100)
	ids := make([]any, 0, 100)
	placeholders := make([]byte, 0, 300)
	for rows.Next() {
		var a stdAuthor
		if err := rows.Scan(&a.id, &a.name); err != nil {
			_ = rows.Close()
			return nil, err
		}
		index[a.id] = len(authors)
		if len(ids) > 0 {
			placeholders = append(placeholders, ", "...)
		}
		placeholders = append(placeholders, '?')
		ids = append(ids, a.id)
		authors = append(authors, a)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	posts, err := db.QueryContext(
		ctx,
		fmt.Sprintf(
			"SELECT id, bench_author_id, title FROM bench_articles WHERE bench_author_id IN (%s)",
			placeholders,
		),
		ids...,
	)
	if err != nil {
		return nil, err
	}
	for posts.Next() {
		var p BenchArticle
		if err := posts.Scan(&p.ID, &p.BenchAuthorID, &p.Title); err != nil {
			_ = posts.Close()
			return nil, err
		}
		i := index[p.BenchAuthorID]
		authors[i].posts = append(authors[i].posts, p)
	}
	return authors, posts.Close()
}

func BenchmarkReadHundredWithCount_Rio(b *testing.B) {
	raw := benchRelationDB(b, "rio_relcount100")
	db := rio.New(raw, rio.SQLite)
	q := rio.From[BenchAuthor]().WithCount("Posts").Must()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := q.All(ctx, db)
		if err != nil || len(out) != 100 || out[0].PostCount != 5 {
			b.Fatalf("len=%d err=%v", len(out), err)
		}
	}
}
