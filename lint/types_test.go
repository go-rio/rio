package lint

import (
	"reflect"
	"testing"
	"time"

	"github.com/go-rio/rio"
)

func col(t reflect.Type, json bool) rio.ColumnSchema {
	return rio.ColumnSchema{GoType: t, JSON: json}
}

// The verdict table splits three ways: a matching class confirms, a known
// type in the wrong class refutes, and anything else stays silent.
func TestVerdictFor(t *testing.T) {
	i64 := col(reflect.TypeFor[int64](), false)
	str := col(reflect.TypeFor[string](), false)
	tm := col(reflect.TypeFor[time.Time](), false)
	js := col(reflect.TypeFor[map[string]int](), true)

	cases := []struct {
		dialect, dataType string
		c                 rio.ColumnSchema
		want              verdict
	}{
		{"postgres", "bigint", i64, verdictOK},
		{"postgres", "text", i64, verdictMismatch},
		{"postgres", "tsvector", i64, verdictUnknown}, // outside every class
		{"postgres", "jsonb", js, verdictOK},
		{"postgres", "timestamp with time zone", tm, verdictOK},
		{"mysql", "bigint", i64, verdictOK},
		{"mysql", "varchar", str, verdictOK},
		{"mysql", "datetime", str, verdictMismatch},
		{"mysql", "geometry", str, verdictUnknown},

		// SQLite affinity confirms but never refutes.
		{"sqlite", "integer", i64, verdictOK},
		{"sqlite", "text", i64, verdictUnknown},
		{"sqlite", "datetime", tm, verdictOK},
	}
	for _, tt := range cases {
		if got := verdictFor(tt.dialect, tt.dataType, tt.c); got != tt.want {
			t.Errorf("verdictFor(%s, %q, %s) = %d, want %d", tt.dialect, tt.dataType, tt.c.GoType, got, tt.want)
		}
	}

	// A pointer field classes by its element; a Scanner stays undecidable.
	if got := verdictFor("postgres", "bigint", col(reflect.TypeFor[*int64](), false)); got != verdictOK {
		t.Errorf("pointer element class: %d", got)
	}
	if got := classOf(col(reflect.TypeFor[struct{ X int }](), false)); got != classOther {
		t.Errorf("undecidable type must stay classOther, got %d", got)
	}
}
