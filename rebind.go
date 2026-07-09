package rio

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
)

// lexProfile is the per-dialect lexical grammar the rebinder needs to walk a
// statement without misreading string, identifier, and comment contents.
// Getting any of these wrong silently corrupts user SQL, so the three
// profiles are pinned by differential fuzzing against a naive reference.
type lexProfile struct {
	backslashEscapes    bool // MySQL default: '\'' does not close the string
	dollarQuote         bool // PostgreSQL: $$...$$ and $tag$...$tag$
	nestedBlockComments bool // PostgreSQL: /* /* */ */ nests
	hashComment         bool // MySQL: # line comment
	bracketIdent        bool // SQLite: [identifier]
	doubleQuoteIsString bool // MySQL: "..." is a string (still skipped whole)
	backtickIdent       bool // MySQL/SQLite: `identifier`
	eStrings            bool // PostgreSQL: E'...' escape strings
	looseDashComment    bool // PG/SQLite: -- always comments; MySQL needs whitespace after
}

var (
	pgLex     = lexProfile{dollarQuote: true, nestedBlockComments: true, eStrings: true, looseDashComment: true}
	mysqlLex  = lexProfile{backslashEscapes: true, hashComment: true, doubleQuoteIsString: true, backtickIdent: true}
	sqliteLex = lexProfile{bracketIdent: true, backtickIdent: true, looseDashComment: true}
)

// bindStyle selects the output placeholder form.
type bindStyle int

const (
	bindQuestion bindStyle = iota // ? as-is (MySQL, SQLite)
	bindDollar                    // $1, $2, ... (PostgreSQL)
)

// rebind rewrites unified ? placeholders into the dialect's form and expands
// slice arguments inside IN (?). It is the single component that rewrites
// user SQL, and it never touches placeholder-lookalikes inside strings,
// quoted identifiers, or comments.
//
// Rules:
//   - ?? collapses to a literal ? on every dialect (PostgreSQL JSONB
//     operators) and consumes no argument.
//   - A single ? consumes one argument. When that argument is a slice or
//     array — except []byte and driver.Valuer implementations — it expands
//     in place to one placeholder per element; empty slices are an error.
//   - Existing $N text passes through untouched; mixing styles is the
//     caller's responsibility.
//   - Placeholder/argument count mismatches error with both counts and the
//     byte offset of the offending placeholder.
func rebind(p lexProfile, style bindStyle, query string, args []any) (string, []any, error) {
	out := make([]byte, 0, len(query)+8)
	outArgs := args // reused verbatim unless a slice expands
	expanded := false
	argIdx := 0
	n := 0 // emitted placeholder count

	emit := func(arg any) {
		n++
		if style == bindDollar {
			out = append(out, '$')
			out = strconv.AppendInt(out, int64(n), 10)
		} else {
			out = append(out, '?')
		}
		if expanded {
			outArgs = append(outArgs, arg)
		}
	}
	// switch to the expanded-args regime, copying what was consumed so far
	startExpanding := func() {
		if !expanded {
			expanded = true
			outArgs = append(make([]any, 0, len(args)+8), args[:argIdx-1]...)
		}
	}

	// copyTo appends the skipped-over region verbatim: quoted and commented
	// text passes through untouched, it is only opaque to the ? scan.
	copyTo := func(end int, from int) int {
		out = append(out, query[from:end]...)
		return end
	}

	i := 0
	for i < len(query) {
		c := query[i]
		switch c {
		case '\'':
			i = copyTo(skipQuoted(query, i, '\'', p.backslashEscapes), i)
			continue
		case '"':
			// Identifier on PG/SQLite, string on MySQL; both pass whole.
			i = copyTo(skipQuoted(query, i, '"', p.backslashEscapes && p.doubleQuoteIsString), i)
			continue
		case '`':
			if p.backtickIdent {
				i = copyTo(skipQuoted(query, i, '`', false), i)
				continue
			}
		case '[':
			if p.bracketIdent {
				i = copyTo(skipUntilByte(query, i+1, ']'), i)
				continue
			}
		case 'E', 'e':
			if p.eStrings && i+1 < len(query) && query[i+1] == '\'' && !identByteBefore(query, i) {
				i = copyTo(skipQuoted(query, i+1, '\'', true), i)
				continue
			}
		case '$':
			if p.dollarQuote && !identByteBefore(query, i) {
				if end, ok := skipDollarQuoted(query, i); ok {
					i = copyTo(end, i)
					continue
				}
			}
		case '-':
			if i+1 < len(query) && query[i+1] == '-' && (p.looseDashComment || dashCommentOK(query, i)) {
				i = copyTo(skipLineComment(query, i), i)
				continue
			}
		case '#':
			if p.hashComment {
				i = copyTo(skipLineComment(query, i), i)
				continue
			}
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				i = copyTo(skipBlockComment(query, i, p.nestedBlockComments), i)
				continue
			}
		case '?':
			if i+1 < len(query) && query[i+1] == '?' {
				out = append(out, '?')
				i += 2
				continue
			}
			// A digit straight after ? would glue onto the emitted $N — "$1"
			// followed by "0" reads back as $10 — and ?N numbered placeholders
			// are not part of the unified syntax, so reject rather than corrupt.
			if i+1 < len(query) && '0' <= query[i+1] && query[i+1] <= '9' {
				return "", nil, fmt.Errorf("rio: placeholder at byte %d is directly followed by a digit; numbered placeholders are not supported", i)
			}
			if argIdx >= len(args) {
				return "", nil, fmt.Errorf("rio: placeholder %d (byte %d) has no argument: %d placeholder(s), %d argument(s)",
					argIdx+1, i, countPlaceholders(p, query), len(args))
			}
			arg := args[argIdx]
			argIdx++
			if elems, ok := sliceElems(arg); ok {
				if len(elems) == 0 {
					return "", nil, fmt.Errorf("rio: empty slice for IN placeholder %d (byte %d)", argIdx, i)
				}
				// Expansion is flat — "IN (?)" keeps its own parentheses and
				// becomes "IN ($1, $2)", the sqlx convention.
				startExpanding()
				for j, e := range elems {
					if j > 0 {
						out = append(out, ", "...)
					}
					emit(e)
				}
				i++
				continue
			}
			if expanded {
				emit(arg)
			} else {
				emit(nil) // counting only; outArgs still aliases args
			}
			i++
			continue
		}
		out = append(out, c)
		i++
	}

	if argIdx != len(args) {
		return "", nil, fmt.Errorf("rio: %d placeholder(s) but %d argument(s)", argIdx, len(args))
	}
	return string(out), outArgs, nil
}

// skipQuoted copies a quoted region starting at the opening quote, honoring
// doubled-quote escapes and, optionally, backslash escapes. It returns the
// index after the closing quote; unterminated regions run to the end (the
// database will reject the statement — rebind must only not miscount).
func skipQuoted(s string, start int, q byte, backslash bool) int {
	i := start + 1
	for i < len(s) {
		switch {
		case backslash && s[i] == '\\':
			i += 2
		case s[i] == q:
			if i+1 < len(s) && s[i+1] == q { // doubled escape
				i += 2
				continue
			}
			return i + 1
		default:
			i++
		}
	}
	// A trailing backslash steps past the end; callers slice with the result.
	return min(i, len(s))
}

func skipUntilByte(s string, start int, b byte) int {
	for i := start; i < len(s); i++ {
		if s[i] == b {
			return i + 1
		}
	}
	return len(s)
}

// identByteBefore reports whether the byte before position i can end an
// identifier — in which case a following $ or E belongs to that identifier
// (PostgreSQL identifiers may contain $: col$x$y is one name, not a quote).
func identByteBefore(s string, i int) bool {
	if i == 0 {
		return false
	}
	c := s[i-1]
	// Bytes >= 0x80 are UTF-8 continuation/lead bytes: PostgreSQL allows
	// non-ASCII identifiers, so treat them as identifier material — never
	// let café$tag$ open a dollar quote mid-word.
	return c == '_' || c == '$' || c >= 0x80 ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// skipDollarQuoted matches $tag$...$tag$ starting at the $ and returns the
// index after the closing delimiter. $1-style placeholders do not match: a
// tag is empty or starts with a letter or underscore.
func skipDollarQuoted(s string, start int) (int, bool) {
	i := start + 1
	for i < len(s) && isTagByte(s[i], i == start+1) {
		i++
	}
	if i >= len(s) || s[i] != '$' {
		return 0, false
	}
	delim := s[start : i+1]
	for j := i + 1; j+len(delim) <= len(s); j++ {
		if s[j] == '$' && s[j:j+len(delim)] == delim {
			return j + len(delim), true
		}
	}
	return len(s), true // unterminated: swallow the rest, arity check reports
}

func isTagByte(c byte, first bool) bool {
	if c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
		return true
	}
	return !first && c >= '0' && c <= '9'
}

// dashCommentOK implements MySQL's rule: -- comments only when followed by
// whitespace, a control character, or the end of the statement.
func dashCommentOK(s string, i int) bool {
	if i+2 >= len(s) {
		return true
	}
	c := s[i+2]
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c < 0x20
}

func skipLineComment(s string, start int) int {
	for i := start; i < len(s); i++ {
		if s[i] == '\n' {
			return i
		}
	}
	return len(s)
}

func skipBlockComment(s string, start int, nested bool) int {
	depth := 1
	i := start + 2
	for i+1 < len(s) {
		switch {
		case s[i] == '*' && s[i+1] == '/':
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		case nested && s[i] == '/' && s[i+1] == '*':
			depth++
			i += 2
		default:
			i++
		}
	}
	return len(s)
}

var valuerType = reflect.TypeFor[driver.Valuer]()

// sliceElems reports whether arg expands inside IN (?). []byte is a scalar
// (BLOB), and driver.Valuer implementations bind as themselves.
func sliceElems(arg any) ([]any, bool) {
	if arg == nil {
		return nil, false
	}
	if _, isBytes := arg.([]byte); isBytes {
		return nil, false
	}
	t := reflect.TypeOf(arg)
	if t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
		return nil, false
	}
	if t.Implements(valuerType) {
		return nil, false
	}
	v := reflect.ValueOf(arg)
	elems := make([]any, v.Len())
	for i := range elems {
		elems[i] = v.Index(i).Interface()
	}
	return elems, true
}

// countPlaceholders is used only for error messages.
func countPlaceholders(p lexProfile, query string) int {
	n := 0
	_, _, _ = rebindCount(p, query, &n)
	return n
}

func rebindCount(p lexProfile, query string, n *int) (string, []any, error) {
	i := 0
	for i < len(query) {
		switch query[i] {
		case '\'':
			i = skipQuoted(query, i, '\'', p.backslashEscapes)
		case '"':
			i = skipQuoted(query, i, '"', p.backslashEscapes && p.doubleQuoteIsString)
		case '`':
			if p.backtickIdent {
				i = skipQuoted(query, i, '`', false)
			} else {
				i++
			}
		case '[':
			if p.bracketIdent {
				i = skipUntilByte(query, i+1, ']')
			} else {
				i++
			}
		case '$':
			if p.dollarQuote && !identByteBefore(query, i) {
				if end, ok := skipDollarQuoted(query, i); ok {
					i = end
					continue
				}
			}
			i++
		case '-':
			if i+1 < len(query) && query[i+1] == '-' && (p.looseDashComment || dashCommentOK(query, i)) {
				i = skipLineComment(query, i)
			} else {
				i++
			}
		case '#':
			if p.hashComment {
				i = skipLineComment(query, i)
			} else {
				i++
			}
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				i = skipBlockComment(query, i, p.nestedBlockComments)
			} else {
				i++
			}
		case 'E', 'e':
			if p.eStrings && i+1 < len(query) && query[i+1] == '\'' && !identByteBefore(query, i) {
				i = skipQuoted(query, i+1, '\'', true)
			} else {
				i++
			}
		case '?':
			if i+1 < len(query) && query[i+1] == '?' {
				i += 2
				continue
			}
			*n++
			i++
		default:
			i++
		}
	}
	return "", nil, nil
}
