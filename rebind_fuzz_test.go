package rio

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// This file pins rebind by differential fuzzing against naiveRebind, an
// independent reference implementation. naiveRebind trades everything for
// obviousness: one explicit state per lexical region, one byte per step, no
// shared code with rebind.go beyond the lexProfile flags it interprets.

const (
	nvSQL      = iota // plain statement text
	nvSingle          // inside '...'
	nvDouble          // inside "..."
	nvBacktick        // inside `...`
	nvBracket         // inside [...]
	nvEString         // inside E'...' (backslashes always escape)
	nvDollar          // inside $tag$...$tag$
	nvLine            // inside -- or # comment, to end of line
	nvBlock           // inside /* ... */
)

func naiveRebind(p lexProfile, style bindStyle, query string, args []any) (string, []any, error) {
	var (
		out     []byte
		flat    []any // every consumed value, slices flattened
		expand  bool
		argIdx  int
		emitted int
		tag     string // closing delimiter of the open dollar quote
		depth   int    // block comment nesting depth
	)

	state := nvSQL
	i := 0
	for i < len(query) {
		c := query[i]
		switch state {
		case nvSQL:
			switch {
			case c == '\'':
				out = append(out, c)
				i++
				state = nvSingle
			case c == '"':
				out = append(out, c)
				i++
				state = nvDouble
			case c == '`' && p.backtickIdent:
				out = append(out, c)
				i++
				state = nvBacktick
			case c == '[' && p.bracketIdent:
				out = append(out, c)
				i++
				state = nvBracket
			case (c == 'E' || c == 'e') && p.eStrings && i+1 < len(query) && query[i+1] == '\'' && !naiveIdentByte(query, i):
				out = append(out, c, '\'')
				i += 2
				state = nvEString
			case c == '$' && p.dollarQuote && !naiveIdentByte(query, i):
				j := i + 1
				for j < len(query) {
					cj := query[j]
					if cj == '_' || (cj >= 'a' && cj <= 'z') || (cj >= 'A' && cj <= 'Z') ||
						(j > i+1 && cj >= '0' && cj <= '9') {
						j++
						continue
					}
					break
				}
				if j < len(query) && query[j] == '$' {
					tag = query[i : j+1]
					out = append(out, tag...)
					i = j + 1
					state = nvDollar
				} else {
					out = append(out, c)
					i++
				}
			case c == '-' && i+1 < len(query) && query[i+1] == '-' && naiveDashComment(p, query, i):
				out = append(out, '-', '-')
				i += 2
				state = nvLine
			case c == '#' && p.hashComment:
				out = append(out, c)
				i++
				state = nvLine
			case c == '/' && i+1 < len(query) && query[i+1] == '*':
				out = append(out, '/', '*')
				i += 2
				depth = 1
				state = nvBlock
			case c == '?':
				if i+1 < len(query) && query[i+1] == '?' {
					out = append(out, '?')
					i += 2
					continue
				}
				if i+1 < len(query) && '0' <= query[i+1] && query[i+1] <= '9' {
					return "", nil, fmt.Errorf("naive: digit directly after placeholder at byte %d", i)
				}
				if argIdx >= len(args) {
					return "", nil, fmt.Errorf("naive: placeholder at byte %d has no argument", i)
				}
				arg := args[argIdx]
				argIdx++
				if elems, ok := naiveSliceElems(arg); ok {
					if len(elems) == 0 {
						return "", nil, fmt.Errorf("naive: empty slice at byte %d", i)
					}
					// Flat expansion: the caller's "IN (?)" keeps its own
					// parentheses, the sqlx convention.
					expand = true
					for k, e := range elems {
						if k > 0 {
							out = append(out, ',', ' ')
						}
						emitted++
						out = naiveEmit(out, style, emitted)
						flat = append(flat, e)
					}
				} else {
					emitted++
					out = naiveEmit(out, style, emitted)
					flat = append(flat, arg)
				}
				i++
			default:
				out = append(out, c)
				i++
			}
		case nvSingle:
			switch {
			case p.backslashEscapes && c == '\\':
				out = append(out, c)
				i++
				if i < len(query) {
					out = append(out, query[i])
					i++
				}
			case c == '\'' && i+1 < len(query) && query[i+1] == '\'':
				out = append(out, '\'', '\'')
				i += 2
			case c == '\'':
				out = append(out, c)
				i++
				state = nvSQL
			default:
				out = append(out, c)
				i++
			}
		case nvDouble:
			backslash := p.backslashEscapes && p.doubleQuoteIsString
			switch {
			case backslash && c == '\\':
				out = append(out, c)
				i++
				if i < len(query) {
					out = append(out, query[i])
					i++
				}
			case c == '"' && i+1 < len(query) && query[i+1] == '"':
				out = append(out, '"', '"')
				i += 2
			case c == '"':
				out = append(out, c)
				i++
				state = nvSQL
			default:
				out = append(out, c)
				i++
			}
		case nvBacktick:
			switch {
			case c == '`' && i+1 < len(query) && query[i+1] == '`':
				out = append(out, '`', '`')
				i += 2
			case c == '`':
				out = append(out, c)
				i++
				state = nvSQL
			default:
				out = append(out, c)
				i++
			}
		case nvBracket:
			out = append(out, c)
			i++
			if c == ']' {
				state = nvSQL
			}
		case nvEString:
			switch {
			case c == '\\':
				out = append(out, c)
				i++
				if i < len(query) {
					out = append(out, query[i])
					i++
				}
			case c == '\'' && i+1 < len(query) && query[i+1] == '\'':
				out = append(out, '\'', '\'')
				i += 2
			case c == '\'':
				out = append(out, c)
				i++
				state = nvSQL
			default:
				out = append(out, c)
				i++
			}
		case nvDollar:
			if c == '$' && strings.HasPrefix(query[i:], tag) {
				out = append(out, tag...)
				i += len(tag)
				state = nvSQL
			} else {
				out = append(out, c)
				i++
			}
		case nvLine:
			out = append(out, c)
			i++
			if c == '\n' {
				state = nvSQL
			}
		case nvBlock:
			switch {
			case c == '*' && i+1 < len(query) && query[i+1] == '/':
				out = append(out, '*', '/')
				i += 2
				depth--
				if depth == 0 {
					state = nvSQL
				}
			case p.nestedBlockComments && c == '/' && i+1 < len(query) && query[i+1] == '*':
				out = append(out, '/', '*')
				i += 2
				depth++
			default:
				out = append(out, c)
				i++
			}
		}
	}

	if argIdx != len(args) {
		return "", nil, fmt.Errorf("naive: %d placeholder(s) but %d argument(s)", argIdx, len(args))
	}
	if expand {
		return string(out), flat, nil
	}
	return string(out), args, nil
}

func naiveIdentByte(s string, i int) bool {
	if i == 0 {
		return false
	}
	c := s[i-1]
	// Bytes >= 0x80 are UTF-8 identifier material, mirroring identByteBefore.
	return c == '_' || c == '$' || c >= 0x80 ||
		('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')
}

func naiveDashComment(p lexProfile, s string, i int) bool {
	if p.looseDashComment {
		return true
	}
	if i+2 >= len(s) { // -- at end of statement
		return true
	}
	c := s[i+2]
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c < 0x20
}

func naiveEmit(out []byte, style bindStyle, n int) []byte {
	if style == bindDollar {
		return append(out, "$"+strconv.Itoa(n)...)
	}
	return append(out, '?')
}

func naiveSliceElems(arg any) ([]any, bool) {
	if arg == nil {
		return nil, false
	}
	if _, isBytes := arg.([]byte); isBytes {
		return nil, false
	}
	rv := reflect.ValueOf(arg)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	if _, isValuer := arg.(driver.Valuer); isValuer {
		return nil, false
	}
	elems := make([]any, rv.Len())
	for i := range elems {
		elems[i] = rv.Index(i).Interface()
	}
	return elems, true
}

// naiveLiveCount counts live placeholders in sql by brute force: the unique
// argument count naiveRebind accepts. Placeholders cannot outnumber bytes,
// so the trial loop is bounded.
func naiveLiveCount(t *testing.T, p lexProfile, sql string) int {
	t.Helper()
	for k := 0; k <= len(sql)+1; k++ {
		if _, _, err := naiveRebind(p, bindQuestion, sql, make([]any, k)); err == nil {
			return k
		}
	}
	t.Fatalf("naiveLiveCount: no argument count from 0 to %d satisfies %q", len(sql)+1, sql)
	return -1
}

// maxDollarPlaceholder returns the largest $N in s. Callers only use it on
// output whose input contained no $, so every $N found was emitted by rebind
// and cannot hide inside a string or comment.
func maxDollarPlaceholder(s string) int {
	maxN := 0
	for i := 0; i < len(s); i++ {
		if s[i] != '$' {
			continue
		}
		j := i + 1
		v := 0
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			v = v*10 + int(s[j]-'0')
			j++
		}
		if j > i+1 && v > maxN {
			maxN = v
		}
		i = j - 1
	}
	return maxN
}

func FuzzRebind(f *testing.F) {
	seeds := []string{
		"",
		"?",
		"? ?",
		"??",
		"?0?0",
		"?1",
		"??0",
		"SELECT ??",
		"SELECT ???",
		"SELECT ????",
		"SELECT * FROM t WHERE a = ? AND b = ?",
		"SELECT data ?? 'k' FROM t",
		"SELECT '?'",
		"SELECT 'it''s ?' , ?",
		"SELECT '' , ?",
		"SELECT '?",
		`SELECT '\'? '`,
		`'\`,
		"'''?'''",
		`SELECT E'\'?' , ?`,
		`SELECT e'\'?' , ?`,
		`SELECT table_e'\' ?`,
		"E'",
		"e'?'",
		"SELECT $$a?b$$ , ?",
		"SELECT $tag$ ? $tag$ , ?",
		"SELECT col$x$y$ ?",
		"SELECT $y$ ? $y$",
		"SELECT $1 + $2",
		"SELECT $$ ?",
		"SELECT $x$ ? $x",
		"$",
		"$$",
		"$_$?$_$",
		`SELECT "a?b" , ?`,
		`SELECT "a""b?"`,
		`SELECT "?`,
		"SELECT `a?b`",
		"SELECT `?",
		"SELECT [a?b]",
		"SELECT [?",
		"SELECT 1--?x",
		"SELECT 1 -- ?",
		"SELECT 1--",
		"SELECT 1--\t?",
		"SELECT 1 -- ?\n , ?",
		"-",
		"-?",
		"SELECT 1 # ?",
		"#?",
		"SELECT /* ? */ 1",
		"SELECT /* /* ? */ ? */ 1",
		"SELECT /* ?",
		"/*",
		"/**/?",
		"/*/*?*/?*/",
		// CI fuzz regression: a UTF-8 continuation byte before e' is
		// identifier material, so no E-string opens and the ? stays live.
		"\xa0e'\\'?",
	}
	for _, q := range seeds {
		for prof := uint8(0); prof < 3; prof++ {
			for style := uint8(0); style < 2; style++ {
				for _, n := range []uint8{0, 1, 2, 3} {
					f.Add(q, n, prof, style)
				}
			}
		}
	}

	f.Fuzz(func(t *testing.T, query string, nArgs, profileIdx, styleIdx uint8) {
		profiles := [...]lexProfile{pgLex, mysqlLex, sqliteLex}
		styles := [...]bindStyle{bindQuestion, bindDollar}
		p := profiles[int(profileIdx)%len(profiles)]
		style := styles[int(styleIdx)%len(styles)]

		args := make([]any, int(nArgs)%8)
		for i := range args {
			switch i % 4 {
			case 0:
				args[i] = i + 1
			case 1:
				args[i] = "s"
			case 2:
				args[i] = nil
			case 3:
				args[i] = 1.5
			}
		}

		gotSQL, gotArgs, gotErr := rebind(p, style, query, args)
		wantSQL, wantArgs, wantErr := naiveRebind(p, style, query, args)

		if (gotErr != nil) != (wantErr != nil) {
			t.Fatalf("rebind(%q, %d args) err = %v, naive err = %v", query, len(args), gotErr, wantErr)
		}
		if gotErr != nil {
			return
		}
		if gotSQL != wantSQL {
			t.Fatalf("rebind(%q, %d args) sql = %q, naive sql = %q", query, len(args), gotSQL, wantSQL)
		}
		if !reflect.DeepEqual(gotArgs, wantArgs) {
			t.Fatalf("rebind(%q) args = %#v, naive args = %#v", query, gotArgs, wantArgs)
		}

		// Output invariants. Each is gated on inputs that cannot smuggle
		// placeholder lookalikes into the output: pre-existing $N text
		// defeats the max-$N check, and a ?? collapsed to a literal ? is
		// indistinguishable from a live ? in question-style output.
		switch style {
		case bindDollar:
			if !strings.Contains(query, "$") {
				if maxN := maxDollarPlaceholder(gotSQL); maxN != len(gotArgs) {
					t.Fatalf("rebind(%q) = %q: max $N is %d, want %d", query, gotSQL, maxN, len(gotArgs))
				}
			}
		case bindQuestion:
			if !strings.Contains(query, "??") {
				if live := naiveLiveCount(t, p, gotSQL); live != len(gotArgs) {
					t.Fatalf("rebind(%q) = %q: %d live placeholder(s) in output, want %d", query, gotSQL, live, len(gotArgs))
				}
			}
		}
	})
}
