package rio

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"unsafe"
)

// lexProfile describes syntax regions in which ? is not a placeholder.
type lexProfile struct {
	backslashEscapes    bool // MySQL/ClickHouse: '\'' does not close the string
	dollarQuote         bool // PostgreSQL: $$...$$ and $tag$...$tag$
	nestedBlockComments bool // PostgreSQL/ClickHouse: /* /* */ */ nests
	hashComment         bool // MySQL: # line comment
	bracketIdent        bool // SQLite: [identifier]
	doubleQuoteIsString bool // MySQL: "..." is a string (still skipped whole)
	backtickIdent       bool // MySQL/SQLite/ClickHouse: `identifier`
	eStrings            bool // PostgreSQL: E'...' escape strings
	looseDashComment    bool // PG/SQLite/ClickHouse: -- always comments; MySQL needs whitespace after

	// ClickHouse lexer rules differ from PostgreSQL heredocs and comments.
	quotedIdentBackslash bool // backslashes escape inside "..." and `...` identifiers
	slashSlashComment    bool // // line comment
	hashSpaceComment     bool // # comments only when followed by ' ' or '!'
	heredoc              bool // $tag$...$tag$ string literals

	// clickhouse-go consumes \? as a literal question mark.
	backslashQuestion bool
}

var (
	pgLex     = lexProfile{dollarQuote: true, nestedBlockComments: true, eStrings: true, looseDashComment: true}
	mysqlLex  = lexProfile{backslashEscapes: true, hashComment: true, doubleQuoteIsString: true, backtickIdent: true}
	sqliteLex = lexProfile{bracketIdent: true, backtickIdent: true, looseDashComment: true}
	chLex     = lexProfile{
		backslashEscapes: true, nestedBlockComments: true, backtickIdent: true, looseDashComment: true,
		quotedIdentBackslash: true, slashSlashComment: true, hashSpaceComment: true, heredoc: true,
		backslashQuestion: true,
	}
)

// bindStyle selects the output placeholder form.
type bindStyle int

const (
	bindQuestion bindStyle = iota // ? as-is (MySQL, SQLite)
	bindDollar                    // $1, $2, ... (PostgreSQL)
	// bindQuestionEsc mirrors ClickHouse's client-side placeholder scanner.
	bindQuestionEsc
)

// rebind rewrites ? placeholders and expands slice arguments without touching
// strings, quoted identifiers, or comments.
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
//
// The unchanged path returns the input and arguments without allocation.
func rebind(p lexProfile, style bindStyle, query string, args []any) (string, []any, error) {
	var out []byte // nil until the first rewrite; query[:copied] already appended
	copied := 0
	outArgs := args // reused verbatim unless a slice expands
	expanded := false
	argIdx := 0
	n := 0 // emitted placeholder count

	// rewriteTo copies the unchanged prefix before a rewrite.
	rewriteTo := func(i int) {
		if out == nil {
			out = make([]byte, 0, len(query)+8)
		}
		out = append(out, query[copied:i]...)
		copied = i
	}
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
	// Copy consumed arguments only when expansion begins.
	startExpanding := func() {
		if !expanded {
			expanded = true
			outArgs = append(make([]any, 0, len(args)+8), args[:argIdx-1]...)
		}
	}

	i := 0
	for i < len(query) {
		c := query[i]
		// Quoted and commented regions are opaque to the placeholder scan.
		switch c {
		case '\'':
			i = skipQuoted(query, i, '\'', p.backslashEscapes)
			continue
		case '"':
			// Identifier or string, depending on dialect.
			i = skipQuoted(query, i, '"', (p.backslashEscapes && p.doubleQuoteIsString) || p.quotedIdentBackslash)
			continue
		case '`':
			if p.backtickIdent {
				i = skipQuoted(query, i, '`', p.quotedIdentBackslash)
				continue
			}
		case '[':
			if p.bracketIdent {
				i = skipUntilByte(query, i+1, ']')
				continue
			}
		case 'E', 'e':
			if p.eStrings && i+1 < len(query) && query[i+1] == '\'' && !identByteBefore(query, i) {
				i = skipQuoted(query, i+1, '\'', true)
				continue
			}
		case '$':
			if p.dollarQuote && !identByteBefore(query, i) {
				if end, ok := skipDollarQuoted(query, i); ok {
					i = end
					continue
				}
			}
			if p.heredoc && !identByteBefore(query, i) {
				if end, ok := skipHeredoc(query, i); ok {
					if err := checkDriverBlindRegion(style, query, i, end, len(args),
						"a $...$ heredoc", "use '...' string syntax or bind the value as an argument"); err != nil {
						return "", nil, err
					}
					i = end
					continue
				}
			}
		case '-':
			if i+1 < len(query) && query[i+1] == '-' && (p.looseDashComment || dashCommentOK(query, i)) {
				i = skipLineComment(query, i)
				continue
			}
		case '#':
			if p.hashComment || hashSpaceCommentAt(p, query, i) {
				i = skipLineComment(query, i)
				continue
			}
		case '/':
			if p.slashSlashComment && i+1 < len(query) && query[i+1] == '/' {
				end := skipLineComment(query, i)
				if err := checkDriverBlindRegion(style, query, i, end, len(args),
					"a // comment", "use a -- comment"); err != nil {
					return "", nil, err
				}
				i = end
				continue
			}
			if i+1 < len(query) && query[i+1] == '*' {
				i = skipBlockComment(query, i, p.nestedBlockComments)
				continue
			}
		case '\\':
			if p.backslashQuestion && i+1 < len(query) && query[i+1] == '?' {
				// Preserve clickhouse-go's literal-question escape.
				i += 2
				continue
			}
		case '?':
			if i+1 < len(query) && query[i+1] == '?' {
				if style == bindQuestionEsc {
					rewriteTo(i) // the ?? becomes the driver's \? escape
					out = append(out, '\\', '?')
				} else {
					rewriteTo(i + 1) // keep the first ? ...
				}
				copied = i + 2 // ... and drop the source pair
				i += 2
				continue
			}
			// A following digit would change a rebound $N placeholder.
			if i+1 < len(query) && '0' <= query[i+1] && query[i+1] <= '9' {
				return "", nil, fmt.Errorf(
					"rio: placeholder at byte %d is directly followed by a digit; "+
						"numbered placeholders are not supported",
					i,
				)
			}
			if argIdx >= len(args) {
				return "", nil, fmt.Errorf("rio: placeholder %d (byte %d) has no argument: %d placeholder(s), %d argument(s)",
					argIdx+1, i, countPlaceholders(p, query), len(args))
			}
			arg := args[argIdx]
			argIdx++
			if elems, ok := sliceValue(arg); ok {
				if elems.Len() == 0 {
					return "", nil, fmt.Errorf("rio: empty slice for IN placeholder %d (byte %d)", argIdx, i)
				}
				// Expansion is flat; callers provide the surrounding parentheses.
				startExpanding()
				rewriteTo(i)
				copied = i + 1 // the single ? is replaced by the expansion
				for j := 0; j < elems.Len(); j++ {
					if j > 0 {
						out = append(out, ", "...)
					}
					emit(elems.Index(j).Interface())
				}
				i++
				continue
			}
			if style == bindDollar {
				rewriteTo(i)
				copied = i + 1 // the ? becomes $N
				emit(arg)
			} else {
				// Question style keeps the ? in place: count it, and under
				// the expanded-args regime collect its argument.
				n++
				if expanded {
					outArgs = append(outArgs, arg)
				}
			}
			i++
			continue
		}
		i++
	}

	if argIdx != len(args) {
		return "", nil, fmt.Errorf("rio: %d placeholder(s) but %d argument(s)", argIdx, len(args))
	}
	if out == nil {
		return query, outArgs, nil // nothing rewrote: reuse the input
	}
	out = append(out, query[copied:]...)
	return byteString(out), outArgs, nil
}

// rebindTemplate rewrites placeholder syntax without concrete argument
// values. Entity CRUD caches use it for fixed statement shapes.
func rebindTemplate(p lexProfile, style bindStyle, query string) (string, int, error) {
	var out []byte
	copied := 0
	n := 0
	var blindErr error

	rewriteTo := func(i int) {
		if out == nil {
			out = make([]byte, 0, len(query)+8)
		}
		out = append(out, query[copied:i]...)
		copied = i
	}
	recordBlind := func(start, end int, region, fix string) {
		if blindErr != nil || style != bindQuestionEsc {
			return
		}
		blindErr = checkDriverBlindRegion(style, query, start, end, 1, region, fix)
	}
	emit := func(i int) error {
		n++
		if blindErr != nil {
			return blindErr
		}
		if style == bindDollar {
			rewriteTo(i)
			out = append(out, '$')
			out = strconv.AppendInt(out, int64(n), 10)
			copied = i + 1
		}
		return nil
	}

	for i := 0; i < len(query); {
		switch query[i] {
		case '\'':
			i = skipQuoted(query, i, '\'', p.backslashEscapes)
		case '"':
			i = skipQuoted(query, i, '"', (p.backslashEscapes && p.doubleQuoteIsString) || p.quotedIdentBackslash)
		case '`':
			if p.backtickIdent {
				i = skipQuoted(query, i, '`', p.quotedIdentBackslash)
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
			if p.heredoc && !identByteBefore(query, i) {
				if end, ok := skipHeredoc(query, i); ok {
					recordBlind(i, end, "a $...$ heredoc", "use '...' string syntax or bind the value as an argument")
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
			if p.hashComment || hashSpaceCommentAt(p, query, i) {
				i = skipLineComment(query, i)
			} else {
				i++
			}
		case '/':
			if p.slashSlashComment && i+1 < len(query) && query[i+1] == '/' {
				end := skipLineComment(query, i)
				recordBlind(i, end, "a // comment", "use a -- comment")
				i = end
			} else if i+1 < len(query) && query[i+1] == '*' {
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
		case '\\':
			if p.backslashQuestion && i+1 < len(query) && query[i+1] == '?' {
				i += 2
			} else {
				i++
			}
		case '?':
			if i+1 < len(query) && query[i+1] == '?' {
				if style == bindQuestionEsc {
					rewriteTo(i)
					out = append(out, '\\', '?')
				} else {
					rewriteTo(i + 1)
				}
				copied = i + 2
				i += 2
				continue
			}
			if i+1 < len(query) && '0' <= query[i+1] && query[i+1] <= '9' {
				if blindErr != nil {
					return "", 0, blindErr
				}
				return "", 0, fmt.Errorf(
					"rio: placeholder at byte %d is directly followed by a digit; "+
						"numbered placeholders are not supported",
					i,
				)
			}
			if err := emit(i); err != nil {
				return "", 0, err
			}
			i++
		default:
			i++
		}
	}
	if n > 0 && blindErr != nil {
		return "", 0, blindErr
	}
	if out == nil {
		return query, n, nil
	}
	out = append(out, query[copied:]...)
	return byteString(out), n, nil
}

// byteString reinterprets b as a string without copying. The caller must
// guarantee b is never modified afterwards; rio uses it only on freshly
// built, function-local render buffers whose last use is this conversion.
func byteString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
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
// tag is empty or starts with a letter (ASCII or not) or underscore.
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
	// Bytes >= 0x80 count as letters, mirroring identByteBefore: PostgreSQL's
	// scanner classes the tag as [A-Za-z\200-\377_] plus digits after the
	// first byte, so $å$...$å$ is a real dollar quote and must be skipped.
	if c == '_' || c >= 0x80 || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
		return true
	}
	return !first && c >= '0' && c <= '9'
}

// skipHeredoc matches ClickHouse's $tag$...$tag$ heredoc starting at the $
// and returns the index after the closing delimiter. Two deliberate
// differences from skipDollarQuoted, both matching the server's Lexer.cpp:
// tags may be empty or start with a digit, and an unterminated heredoc is
// not a heredoc at all — the server lexes the $ as an ordinary token then,
// so the scan must too.
func skipHeredoc(s string, start int) (int, bool) {
	i := start + 1
	for i < len(s) && isWordByte(s[i]) {
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
	return 0, false
}

func isWordByte(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// hashSpaceCommentAt implements ClickHouse's # rule: a comment only when
// followed by a space or '!' — anything else (`#x`, or # at the end) is a
// lexer error server-side, so the scan does not swallow it as a comment.
func hashSpaceCommentAt(p lexProfile, s string, i int) bool {
	return p.hashSpaceComment && i+1 < len(s) && (s[i+1] == ' ' || s[i+1] == '!')
}

// checkDriverBlindRegion guards regions the server lexes as literal text but
// clickhouse-go's client-side binder does not recognize (heredocs, //
// comments): on an argument-carrying statement the driver would substitute a
// ? in there, so rio rejects it with the fix instead of letting the
// statement corrupt. Argument-free statements pass — the driver skips
// binding entirely then.
func checkDriverBlindRegion(style bindStyle, query string, start, end, argc int, region, fix string) error {
	if style != bindQuestionEsc || argc == 0 {
		return nil
	}
	for j := start; j < end; j++ {
		if query[j] == '?' {
			return fmt.Errorf(
				"rio: a ? inside %s (byte %d) would be rewritten by clickhouse-go's client-side binder; %s",
				region,
				j,
				fix,
			)
		}
	}
	return nil
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

// sliceValue reports whether arg expands inside IN (?). []byte is a scalar
// (BLOB), and driver.Valuer implementations bind as themselves.
func sliceValue(arg any) (reflect.Value, bool) {
	if arg == nil {
		return reflect.Value{}, false
	}
	t := reflect.TypeOf(arg)
	if t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
		return reflect.Value{}, false
	}
	if t.Elem().Kind() == reflect.Uint8 {
		// Byte payloads are one value, not a list — named byte slices
		// (json.RawMessage) and [16]byte UUIDs alike. Expanding them would
		// splice a byte-per-placeholder list into "= ?".
		return reflect.Value{}, false
	}
	if t.Implements(valuerType) {
		return reflect.Value{}, false
	}
	return reflect.ValueOf(arg), true
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
			i = skipQuoted(query, i, '"', (p.backslashEscapes && p.doubleQuoteIsString) || p.quotedIdentBackslash)
		case '`':
			if p.backtickIdent {
				i = skipQuoted(query, i, '`', p.quotedIdentBackslash)
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
			if p.heredoc && !identByteBefore(query, i) {
				if end, ok := skipHeredoc(query, i); ok {
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
			if p.hashComment || hashSpaceCommentAt(p, query, i) {
				i = skipLineComment(query, i)
			} else {
				i++
			}
		case '/':
			if p.slashSlashComment && i+1 < len(query) && query[i+1] == '/' {
				i = skipLineComment(query, i)
			} else if i+1 < len(query) && query[i+1] == '*' {
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
		case '\\':
			if p.backslashQuestion && i+1 < len(query) && query[i+1] == '?' {
				i += 2
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
