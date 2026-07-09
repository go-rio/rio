package rio

import (
	"strings"
	"testing"
)

type offsetReproUser struct {
	ID   int64
	Name string
}

func TestOffsetReproRender(t *testing.T) {
	p, err := planOf[offsetReproUser]()
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []Dialect{Postgres, MySQL, SQLite} {
		g := newGrammar(d, defaultConfig())

		// All() path -> selectRows with offset set, limit unset.
		var s queryState
		s.offset, s.offsetSet = 20, true
		rowsSQL, _, err := renderSelect(g, p, &s, selectRows)
		if err != nil {
			t.Fatalf("%s: renderSelect rows: %v", d.name(), err)
		}
		t.Logf("[%s] All():    %s", d.name(), rowsSQL)
		t.Logf("            LIMIT=%v OFFSET=%v", strings.Contains(rowsSQL, "LIMIT"), strings.Contains(rowsSQL, "OFFSET"))

		// Exists() path -> selectExists with offset set, limit unset.
		var s2 queryState
		s2.offset, s2.offsetSet = 20, true
		exSQL, _, err := renderSelect(g, p, &s2, selectExists)
		if err != nil {
			t.Fatalf("%s: renderSelect exists: %v", d.name(), err)
		}
		t.Logf("[%s] Exists(): %s", d.name(), exSQL)
	}
}
