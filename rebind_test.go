package rio

import (
	"reflect"
	"strings"
	"testing"
)

// The rebinder is the single component that rewrites user SQL, so its
// behavior is pinned by a golden table: every entry is a promise about how a
// (profile, style, query, args) tuple lexes and rebinds. Entries are only
// ever appended, never edited.

func TestRebind(t *testing.T) {
	golden := []struct {
		name     string
		p        lexProfile
		style    bindStyle
		query    string
		args     []any
		want     string
		wantArgs []any
		wantErr  string // substring of the error; empty means success
	}{
		// Placeholder rewriting.
		{"pg numbers placeholders", pgLex, bindDollar,
			"SELECT * FROM t WHERE a = ? AND b = ?", []any{1, "x"},
			"SELECT * FROM t WHERE a = $1 AND b = $2", []any{1, "x"}, ""},
		{"mysql keeps question marks", mysqlLex, bindQuestion,
			"SELECT * FROM t WHERE a = ? AND b = ?", []any{1, "x"},
			"SELECT * FROM t WHERE a = ? AND b = ?", []any{1, "x"}, ""},
		{"sqlite keeps question marks", sqliteLex, bindQuestion,
			"SELECT * FROM t WHERE a = ? AND b = ?", []any{1, "x"},
			"SELECT * FROM t WHERE a = ? AND b = ?", []any{1, "x"}, ""},
		{"lone placeholder", pgLex, bindDollar, "?", []any{42}, "$1", []any{42}, ""},
		{"no placeholders no args", pgLex, bindDollar, "SELECT 1 FROM t", nil, "SELECT 1 FROM t", nil, ""},
		{"empty query", pgLex, bindDollar, "", nil, "", nil, ""},

		// ?? collapses to a literal ? on every dialect and consumes no argument.
		{"?? pg", pgLex, bindDollar, "SELECT data ?? 'k' FROM t", nil, "SELECT data ? 'k' FROM t", nil, ""},
		{"?? mysql", mysqlLex, bindQuestion, "SELECT data ?? 'k' FROM t", nil, "SELECT data ? 'k' FROM t", nil, ""},
		{"?? sqlite", sqliteLex, bindQuestion, "SELECT data ?? 'k' FROM t", nil, "SELECT data ? 'k' FROM t", nil, ""},
		{"?? at end of query", pgLex, bindDollar, "SELECT ??", nil, "SELECT ?", nil, ""},
		{"??? is literal plus placeholder", pgLex, bindDollar, "SELECT ???", []any{1}, "SELECT ?$1", []any{1}, ""},
		{"??? mysql", mysqlLex, bindQuestion, "SELECT ???", []any{1}, "SELECT ??", []any{1}, ""},
		{"???? is two literals", pgLex, bindDollar, "SELECT ????", nil, "SELECT ??", nil, ""},

		// Single-quoted strings.
		{"? inside string", pgLex, bindDollar, "SELECT '?'", nil, "SELECT '?'", nil, ""},
		{"? inside string mysql", mysqlLex, bindQuestion, "SELECT '?'", nil, "SELECT '?'", nil, ""},
		{"doubled quote escape", pgLex, bindDollar, "SELECT 'it''s ?' , ?", []any{1}, "SELECT 'it''s ?' , $1", []any{1}, ""},
		{"empty string literal", pgLex, bindDollar, "SELECT '' , ?", []any{1}, "SELECT '' , $1", []any{1}, ""},
		{"unterminated string", pgLex, bindDollar, "SELECT '?", nil, "SELECT '?", nil, ""},

		// Backslashes end strings on MySQL only: the same bytes lex as one
		// string on MySQL (its ? is dead) but as string-then-placeholder on
		// PG/SQLite (its ? is live).
		{"backslash quote mysql", mysqlLex, bindQuestion, `SELECT '\'? '`, nil, `SELECT '\'? '`, nil, ""},
		{"backslash quote mysql rejects arg", mysqlLex, bindQuestion, `SELECT '\'? '`, []any{1}, "", nil, "0 placeholder(s) but 1 argument(s)"},
		{"backslash quote pg", pgLex, bindDollar, `SELECT '\'? '`, []any{1}, `SELECT '\'$1 '`, []any{1}, ""},
		{"backslash quote sqlite", sqliteLex, bindQuestion, `SELECT '\'? '`, []any{1}, `SELECT '\'? '`, []any{1}, ""},

		// PostgreSQL E-strings do honor backslashes.
		{"E-string", pgLex, bindDollar, `SELECT E'\'?' , ?`, []any{1}, `SELECT E'\'?' , $1`, []any{1}, ""},
		{"e-string lowercase", pgLex, bindDollar, `SELECT e'\'?' , ?`, []any{1}, `SELECT e'\'?' , $1`, []any{1}, ""},
		{"identifier byte defuses E-string", pgLex, bindDollar, `SELECT table_e'\' ?`, []any{1}, `SELECT table_e'\' $1`, []any{1}, ""},

		// Dollar quoting (PostgreSQL).
		{"anonymous dollar quote", pgLex, bindDollar, "SELECT $$a?b$$ , ?", []any{1}, "SELECT $$a?b$$ , $1", []any{1}, ""},
		{"tagged dollar quote", pgLex, bindDollar, "SELECT $tag$ ? $tag$ , ?", []any{1}, "SELECT $tag$ ? $tag$ , $1", []any{1}, ""},
		{"identifier byte defuses dollar quote", pgLex, bindDollar, "SELECT col$x$y$ ?", []any{1}, "SELECT col$x$y$ $1", []any{1}, ""},
		{"fresh dollar tag does quote", pgLex, bindDollar, "SELECT $y$ ? $y$", nil, "SELECT $y$ ? $y$", nil, ""},
		{"existing $N passes through", pgLex, bindDollar, "SELECT $1 + $2", nil, "SELECT $1 + $2", nil, ""},
		{"unterminated dollar quote", pgLex, bindDollar, "SELECT $$ ?", nil, "SELECT $$ ?", nil, ""},

		// Double-quoted identifiers (strings on MySQL): skipped whole either way.
		{"? inside double quotes", pgLex, bindDollar, `SELECT "a?b" , ?`, []any{1}, `SELECT "a?b" , $1`, []any{1}, ""},
		{"doubled double quote", pgLex, bindDollar, `SELECT "a""b?"`, nil, `SELECT "a""b?"`, nil, ""},
		{"double-quoted string mysql", mysqlLex, bindQuestion, `SELECT "a?b"`, nil, `SELECT "a?b"`, nil, ""},

		// Backtick and bracket identifiers.
		{"backtick mysql", mysqlLex, bindQuestion, "SELECT `a?b`", nil, "SELECT `a?b`", nil, ""},
		{"backtick sqlite", sqliteLex, bindQuestion, "SELECT `a?b`", nil, "SELECT `a?b`", nil, ""},
		{"backtick not special on pg", pgLex, bindDollar, "SELECT `a?b`", []any{1}, "SELECT `a$1b`", []any{1}, ""},
		{"bracket sqlite", sqliteLex, bindQuestion, "SELECT [a?b]", nil, "SELECT [a?b]", nil, ""},
		{"bracket not special on mysql", mysqlLex, bindQuestion, "SELECT [a?b]", []any{1}, "SELECT [a?b]", []any{1}, ""},

		// Line comments. MySQL's -- needs trailing whitespace; PG/SQLite's
		// does not; # comments on MySQL only.
		{"tight dash comment pg", pgLex, bindDollar, "SELECT 1--?x", nil, "SELECT 1--?x", nil, ""},
		{"tight dashes are not a comment on mysql", mysqlLex, bindQuestion, "SELECT 1--?x", []any{7}, "SELECT 1--?x", []any{7}, ""},
		{"spaced dash comment pg", pgLex, bindDollar, "SELECT 1 -- ?", nil, "SELECT 1 -- ?", nil, ""},
		{"spaced dash comment mysql", mysqlLex, bindQuestion, "SELECT 1 -- ?", nil, "SELECT 1 -- ?", nil, ""},
		{"spaced dash comment sqlite", sqliteLex, bindQuestion, "SELECT 1 -- ?", nil, "SELECT 1 -- ?", nil, ""},
		{"dash comment at end of statement mysql", mysqlLex, bindQuestion, "SELECT 1--", nil, "SELECT 1--", nil, ""},
		{"tab after dashes comments on mysql", mysqlLex, bindQuestion, "SELECT 1--\t?", nil, "SELECT 1--\t?", nil, ""},
		{"newline ends dash comment", pgLex, bindDollar, "SELECT 1 -- ?\n , ?", []any{1}, "SELECT 1 -- ?\n , $1", []any{1}, ""},
		{"hash comment mysql", mysqlLex, bindQuestion, "SELECT 1 # ?", nil, "SELECT 1 # ?", nil, ""},
		{"hash not a comment on pg", pgLex, bindDollar, "SELECT 1 # ?", []any{1}, "SELECT 1 # $1", []any{1}, ""},
		{"hash not a comment on sqlite", sqliteLex, bindQuestion, "SELECT 1 # ?", []any{1}, "SELECT 1 # ?", []any{1}, ""},

		// Block comments nest on PostgreSQL only: the inner */ pops
		// MySQL/SQLite out early, so their second ? is live.
		{"block comment pg", pgLex, bindDollar, "SELECT /* ? */ 1", nil, "SELECT /* ? */ 1", nil, ""},
		{"block comment mysql", mysqlLex, bindQuestion, "SELECT /* ? */ 1", nil, "SELECT /* ? */ 1", nil, ""},
		{"block comment sqlite", sqliteLex, bindQuestion, "SELECT /* ? */ 1", nil, "SELECT /* ? */ 1", nil, ""},
		{"nested block comment pg", pgLex, bindDollar, "SELECT /* /* ? */ ? */ 1", nil, "SELECT /* /* ? */ ? */ 1", nil, ""},
		{"block comments do not nest on mysql", mysqlLex, bindQuestion, "SELECT /* /* ? */ ? */ 1", []any{1}, "SELECT /* /* ? */ ? */ 1", []any{1}, ""},
		{"block comments do not nest on sqlite", sqliteLex, bindQuestion, "SELECT /* /* ? */ ? */ 1", []any{1}, "SELECT /* /* ? */ ? */ 1", []any{1}, ""},
		{"unterminated block comment", pgLex, bindDollar, "SELECT /* ?", nil, "SELECT /* ?", nil, ""},

		// IN expansion is flat, the sqlx convention: "IN (?)" keeps the
		// caller's own parentheses and the single ? multiplies in place.
		{"int slice pg", pgLex, bindDollar, "SELECT * FROM t WHERE id IN (?)", []any{[]int{1, 2, 3}},
			"SELECT * FROM t WHERE id IN ($1, $2, $3)", []any{1, 2, 3}, ""},
		{"int slice mysql", mysqlLex, bindQuestion, "SELECT * FROM t WHERE id IN (?)", []any{[]int{1, 2, 3}},
			"SELECT * FROM t WHERE id IN (?, ?, ?)", []any{1, 2, 3}, ""},
		{"string slice", pgLex, bindDollar, "SELECT * FROM t WHERE name IN (?)", []any{[]string{"a", "b"}},
			"SELECT * FROM t WHERE name IN ($1, $2)", []any{"a", "b"}, ""},
		{"expansion adds no parentheses itself", pgLex, bindDollar, "SELECT * FROM t WHERE id IN ?", []any{[]int{1, 2}},
			"SELECT * FROM t WHERE id IN $1, $2", []any{1, 2}, ""},
		{"empty slice errors", pgLex, bindDollar, "SELECT * FROM t WHERE id IN (?)", []any{[]int{}}, "", nil, "empty slice"},
		{"[]byte stays scalar", pgLex, bindDollar, "SELECT * FROM t WHERE blob = ?", []any{[]byte{1, 2}},
			"SELECT * FROM t WHERE blob = $1", []any{[]byte{1, 2}}, ""},
		{"slice between scalars keeps numbering", pgLex, bindDollar,
			"SELECT * FROM t WHERE a = ? AND id IN (?) AND b = ?", []any{1, []int{2, 3}, 4},
			"SELECT * FROM t WHERE a = $1 AND id IN ($2, $3) AND b = $4", []any{1, 2, 3, 4}, ""},
		{"slice between scalars mysql", mysqlLex, bindQuestion,
			"SELECT * FROM t WHERE a = ? AND id IN (?) AND b = ?", []any{1, []int{2, 3}, 4},
			"SELECT * FROM t WHERE a = ? AND id IN (?, ?) AND b = ?", []any{1, 2, 3, 4}, ""},
		{"array expands", pgLex, bindDollar, "SELECT * FROM t WHERE id IN (?)", []any{[2]int{7, 8}},
			"SELECT * FROM t WHERE id IN ($1, $2)", []any{7, 8}, ""},

		// A digit straight after ? would glue onto an emitted $N ($1 + "0"
		// reads back as $10), and ?N numbered placeholders are not part of
		// the unified syntax: rejected on every dialect rather than corrupted.
		{"digit after placeholder", pgLex, bindDollar, "SELECT ?0", []any{1}, "", nil, "followed by a digit"},
		{"digit after placeholder mysql", mysqlLex, bindQuestion, "SELECT ?1", []any{1}, "", nil, "followed by a digit"},
		{"digit after placeholder sqlite", sqliteLex, bindQuestion, "SELECT ?1", []any{1}, "", nil, "followed by a digit"},
		{"?? before digit stays literal", pgLex, bindDollar, "SELECT ??0", nil, "SELECT ?0", nil, ""},

		// Arity mismatches report both counts.
		{"more placeholders than args", pgLex, bindDollar, "SELECT ? + ?", []any{1}, "", nil, "2 placeholder(s), 1 argument(s)"},
		{"more args than placeholders", pgLex, bindDollar, "SELECT ?", []any{1, 2}, "", nil, "1 placeholder(s) but 2 argument(s)"},
		{"args without placeholders", mysqlLex, bindQuestion, "SELECT 1", []any{1}, "", nil, "0 placeholder(s) but 1 argument(s)"},
	}
	for _, g := range golden {
		got, gotArgs, err := rebind(g.p, g.style, g.query, g.args)
		if g.wantErr != "" {
			if err == nil {
				t.Errorf("%s: rebind(%q) = %q, want error containing %q", g.name, g.query, got, g.wantErr)
			} else if !strings.Contains(err.Error(), g.wantErr) {
				t.Errorf("%s: rebind(%q) error = %q, want it to contain %q", g.name, g.query, err, g.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: rebind(%q) unexpected error: %v", g.name, g.query, err)
			continue
		}
		if got != g.want {
			t.Errorf("%s: rebind(%q) = %q, want %q", g.name, g.query, got, g.want)
		}
		if !equalArgs(gotArgs, g.wantArgs) {
			t.Errorf("%s: rebind(%q) args = %#v, want %#v", g.name, g.query, gotArgs, g.wantArgs)
		}
	}
}

// TestRebindDialectDivergence feeds byte-identical SQL to two profiles and
// pins the spots where their lexers must disagree; if a refactor ever
// unifies these paths, one side of each pair fails.
func TestRebindDialectDivergence(t *testing.T) {
	// MySQL's backslash keeps the string open, so its ? is dead; PostgreSQL
	// ends the string at the backslash-preceded quote, so its ? is live.
	q := `SELECT '\'? '`
	myOut, _, err := rebind(mysqlLex, bindQuestion, q, nil)
	if err != nil {
		t.Fatalf("rebind(mysqlLex, %q) unexpected error: %v", q, err)
	}
	pgOut, _, err := rebind(pgLex, bindDollar, q, []any{1})
	if err != nil {
		t.Fatalf("rebind(pgLex, %q) unexpected error: %v", q, err)
	}
	if myOut == pgOut {
		t.Errorf("backslash handling: mysql and pg agreed on %q: both %q", q, myOut)
	}

	// -- without trailing whitespace comments on PG but not on MySQL, where
	// the ? stays live and is rewritten.
	q = "SELECT 1--?x"
	pgOut, _, err = rebind(pgLex, bindDollar, q, nil)
	if err != nil {
		t.Fatalf("rebind(pgLex, %q) unexpected error: %v", q, err)
	}
	myOut, _, err = rebind(mysqlLex, bindDollar, q, []any{1})
	if err != nil {
		t.Fatalf("rebind(mysqlLex, %q) unexpected error: %v", q, err)
	}
	if want := "SELECT 1--$1x"; myOut != want {
		t.Errorf("rebind(mysqlLex, %q) = %q, want %q", q, myOut, want)
	}
	if pgOut == myOut {
		t.Errorf("dash comment handling: mysql and pg agreed on %q: both %q", q, pgOut)
	}

	// Block comments nest on PG only: the inner */ pops MySQL out early, so
	// the second ? is live there.
	q = "SELECT /* /* ? */ ? */ 1"
	pgOut, _, err = rebind(pgLex, bindDollar, q, nil)
	if err != nil {
		t.Fatalf("rebind(pgLex, %q) unexpected error: %v", q, err)
	}
	myOut, _, err = rebind(mysqlLex, bindDollar, q, []any{1})
	if err != nil {
		t.Fatalf("rebind(mysqlLex, %q) unexpected error: %v", q, err)
	}
	if want := "SELECT /* /* ? */ $1 */ 1"; myOut != want {
		t.Errorf("rebind(mysqlLex, %q) = %q, want %q", q, myOut, want)
	}
	if pgOut == myOut {
		t.Errorf("block comment nesting: mysql and pg agreed on %q: both %q", q, pgOut)
	}
}

// equalArgs compares argument lists by value, treating nil and empty as the
// same so table entries can spell "no arguments" as nil.
func equalArgs(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}
