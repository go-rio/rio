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
		dialect  rio.Dialect
		dataType string
		c        rio.ColumnSchema
		want     verdict
	}{
		{rio.Postgres, "bigint", i64, verdictOK},
		{rio.Postgres, "text", i64, verdictMismatch},
		{rio.Postgres, "tsvector", i64, verdictUnknown}, // outside every class
		{rio.Postgres, "jsonb", js, verdictOK},
		{rio.Postgres, "timestamp with time zone", tm, verdictOK},
		{rio.MySQL, "bigint", i64, verdictOK},
		{rio.MySQL, "varchar", str, verdictOK},
		{rio.MySQL, "datetime", str, verdictMismatch},
		{rio.MySQL, "geometry", str, verdictUnknown},

		// SQLite affinity confirms but never refutes.
		{rio.SQLite, "integer", i64, verdictOK},
		{rio.SQLite, "text", i64, verdictUnknown},
		{rio.SQLite, "datetime", tm, verdictOK},
	}
	for _, tt := range cases {
		if got := verdictFor(tt.dialect, tt.dataType, tt.c); got != tt.want {
			t.Errorf("verdictFor(%v, %q, %s) = %d, want %d", tt.dialect, tt.dataType, tt.c.GoType, got, tt.want)
		}
	}

	// A pointer field classes by its element; a Scanner stays undecidable.
	if got := verdictFor(rio.Postgres, "bigint", col(reflect.TypeFor[*int64](), false)); got != verdictOK {
		t.Errorf("pointer element class: %d", got)
	}
	if got := classOf(col(reflect.TypeFor[struct{ X int }](), false)); got != classOther {
		t.Errorf("undecidable type must stay classOther, got %d", got)
	}
}
