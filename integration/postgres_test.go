package integration

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/go-rio/rio"
)

// PostgreSQL-only rich-type coverage: jsonb via rio's json tag (both
// channels) and text[] via a Scanner/Valuer wrapper (stdlib-only).

// PgMeta is the Go shape stored in a jsonb column via rio:",json".
type PgMeta struct {
	Theme string `json:"theme"`
	Sizes []int  `json:"sizes"`
}

// PgDoc maps a jsonb payload through rio's json tag.
type PgDoc struct {
	ID      int64
	Payload PgMeta `rio:",json"`
}

func (PgDoc) TableName() string { return "pg_docs" }

// runPostgresJSONB round-trips a struct through jsonb and reaches the jsonb
// ? key-exists operator through rio's ?? escape.
func runPostgresJSONB(t *testing.T, db *rio.DB) {
	ctx := context.Background()
	for _, ddl := range []string{
		"DROP TABLE IF EXISTS pg_docs",
		"CREATE TABLE pg_docs (id BIGSERIAL PRIMARY KEY, payload JSONB NOT NULL)",
	} {
		if _, err := rio.Exec(ctx, db, ddl); err != nil {
			t.Fatalf("jsonb ddl %q: %v", ddl, err)
		}
	}

	doc := PgDoc{Payload: PgMeta{Theme: "dark", Sizes: []int{1, 2, 3}}}
	if err := rio.Insert(ctx, db, &doc); err != nil {
		t.Fatalf("insert jsonb doc: %v", err)
	}
	got, err := rio.Find[PgDoc](ctx, db, doc.ID)
	if err != nil {
		t.Fatalf("find jsonb doc: %v", err)
	}
	if got.Payload.Theme != "dark" || !reflect.DeepEqual(got.Payload.Sizes, []int{1, 2, 3}) {
		t.Fatalf("jsonb struct round-trip: %+v", got.Payload)
	}

	// payload ?? ? renders to payload ? $1 — the jsonb key-exists operator.
	hit, err := rio.From[PgDoc]().Where("payload ?? ?", "theme").Count(ctx, db)
	if err != nil || hit != 1 {
		t.Fatalf("jsonb ? on an existing key: hit=%d err=%v", hit, err)
	}
	miss, err := rio.From[PgDoc]().Where("payload ?? ?", "missing").Count(ctx, db)
	if err != nil || miss != 0 {
		t.Fatalf("jsonb ? on an absent key: miss=%d err=%v", miss, err)
	}
}

// pgTextArray is a minimal text[] Valuer/Scanner (unquoted tokens only);
// implementing driver.Valuer keeps rio from expanding it as an IN-list.
type pgTextArray []string

func (a pgTextArray) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, s := range a {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s)
	}
	b.WriteByte('}')
	return b.String(), nil
}

func (a *pgTextArray) Scan(src any) error {
	var text string
	switch v := src.(type) {
	case nil:
		*a = nil
		return nil
	case string:
		text = v
	case []byte:
		text = string(v)
	default:
		return fmt.Errorf("pgTextArray: cannot scan %T", src)
	}
	text = strings.TrimSpace(text)
	if len(text) < 2 || text[0] != '{' || text[len(text)-1] != '}' {
		return fmt.Errorf("pgTextArray: malformed array literal %q", text)
	}
	if inner := text[1 : len(text)-1]; inner != "" {
		*a = pgTextArray(strings.Split(inner, ","))
	} else {
		*a = pgTextArray{}
	}
	return nil
}

// PgTagged carries a text[] column through the Scanner/Valuer wrapper.
type PgTagged struct {
	ID   int64
	Tags pgTextArray
}

func (PgTagged) TableName() string { return "pg_tagged" }

// runPostgresTextArray round-trips text[] through the wrapper. Stdlib-only:
// the pgx-native channel hands Scanners the binary array format.
func runPostgresTextArray(t *testing.T, db *rio.DB) {
	ctx := context.Background()
	for _, ddl := range []string{
		"DROP TABLE IF EXISTS pg_tagged",
		"CREATE TABLE pg_tagged (id BIGSERIAL PRIMARY KEY, tags TEXT[] NOT NULL)",
	} {
		if _, err := rio.Exec(ctx, db, ddl); err != nil {
			t.Fatalf("text[] ddl %q: %v", ddl, err)
		}
	}
	row := PgTagged{Tags: pgTextArray{"go", "sql", "olap"}}
	if err := rio.Insert(ctx, db, &row); err != nil {
		t.Fatalf("insert text[]: %v", err)
	}
	got, err := rio.Find[PgTagged](ctx, db, row.ID)
	if err != nil {
		t.Fatalf("find text[]: %v", err)
	}
	if !reflect.DeepEqual([]string(got.Tags), []string{"go", "sql", "olap"}) {
		t.Fatalf("text[] round-trip through Scanner/Valuer: %v", got.Tags)
	}
}
