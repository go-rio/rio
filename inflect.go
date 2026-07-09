package rio

import "strings"

// snakeCase converts an exported Go identifier to its snake_case column
// form, splitting initialisms the way Go names read: within a run of
// uppercase letters, the final letter belongs to the next word when a
// lowercase letter follows (HTTPServer → http_server, ABTest → ab_test,
// UserID → user_id, ID → id). An uppercase letter that follows a lowercase
// letter or a digit also starts a new word, which keeps trailing digits
// attached to their word (HTTPServer2 → http_server2) at the cost of a few
// ugly-but-consistent corners (OAuth2Token → o_auth2_token, IPv4Address →
// i_pv4_address) — rule consistency beats per-case perfection, and tags are
// the escape hatch. Bytes outside [A-Za-z0-9_] are dropped, so for any input
// the result matches [a-z0-9_]* and the function is idempotent. The goldens
// in inflect_test.go freeze this mapping as a compatibility promise.
func snakeCase(name string) string {
	// Boundary detection looks at surviving neighbors, so filter before
	// splitting rather than while splitting.
	kept := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		switch c := name[i]; {
		case isASCIIUpper(c), isASCIILower(c), isASCIIDigit(c), c == '_':
			kept = append(kept, c)
		}
	}

	out := make([]byte, 0, 2*len(kept))
	for i, c := range kept {
		if !isASCIIUpper(c) {
			out = append(out, c)
			continue
		}
		if startsWord(kept, i) && len(out) > 0 && out[len(out)-1] != '_' {
			out = append(out, '_')
		}
		out = append(out, c-'A'+'a')
	}
	return string(out)
}

// startsWord reports whether the uppercase letter at kept[i] begins a new
// word: it follows a lowercase letter or a digit, or it is the last letter
// of an uppercase run and a lowercase letter comes next (the initialism
// boundary rule).
func startsWord(kept []byte, i int) bool {
	if i == 0 {
		return false
	}
	prev := kept[i-1]
	if isASCIILower(prev) || isASCIIDigit(prev) {
		return true
	}
	return isASCIIUpper(prev) && i+1 < len(kept) && isASCIILower(kept[i+1])
}

func isASCIIUpper(c byte) bool { return 'A' <= c && c <= 'Z' }
func isASCIILower(c byte) bool { return 'a' <= c && c <= 'z' }
func isASCIIDigit(c byte) bool { return '0' <= c && c <= '9' }

// pluralize returns the plural table form of a snake_case name. Only the
// final underscore-separated segment is the noun; everything before it is a
// modifier and passes through untouched (user_profile → user_profiles,
// data_point → data_points — "data" being uninflected does not matter
// there). Lookup order on that segment: irregular nouns, uninflected nouns,
// then the spelling rules — consonant+y → ies, the s/sh/ch/x/z endings → es,
// the short consonant+o list → es, and a bare s for everything else. The
// input is already lowercase, as snakeCase produces it, and the output stays
// lowercase.
func pluralize(snake string) string {
	head, noun := "", snake
	if i := strings.LastIndexByte(snake, '_'); i >= 0 {
		head, noun = snake[:i+1], snake[i+1:]
	}
	if noun == "" {
		return snake
	}
	return head + pluralNoun(noun)
}

// pluralNoun pluralizes one lowercase word.
func pluralNoun(noun string) string {
	if p, ok := irregularPlural[noun]; ok {
		return p
	}
	if uninflected[noun] {
		return noun
	}
	if n := len(noun); n >= 2 && noun[n-1] == 'y' && isConsonant(noun[n-2]) {
		return noun[:n-1] + "ies"
	}
	if strings.HasSuffix(noun, "s") || strings.HasSuffix(noun, "x") ||
		strings.HasSuffix(noun, "z") || strings.HasSuffix(noun, "ch") ||
		strings.HasSuffix(noun, "sh") {
		return noun + "es"
	}
	if esOPlural[noun] {
		return noun + "es"
	}
	return noun + "s"
}

// isConsonant reports whether c is a lowercase consonant letter. Digits and
// underscores never count, so names like key2y stay with the default rule.
func isConsonant(c byte) bool {
	switch c {
	case 'a', 'e', 'i', 'o', 'u':
		return false
	}
	return isASCIILower(c)
}

// irregularPlural maps nouns whose plural no spelling rule can derive. The
// table is consulted before every rule, which is what lets quiz → quizzes
// win over the plain z → es rule.
var irregularPlural = map[string]string{
	"person":     "people",
	"child":      "children",
	"man":        "men",
	"woman":      "women",
	"tooth":      "teeth",
	"foot":       "feet",
	"mouse":      "mice",
	"goose":      "geese",
	"ox":         "oxen",
	"datum":      "data",
	"medium":     "media",
	"analysis":   "analyses",
	"basis":      "bases",
	"crisis":     "crises",
	"criterion":  "criteria",
	"phenomenon": "phenomena",
	"leaf":       "leaves",
	"knife":      "knives",
	"wife":       "wives",
	"life":       "lives",
	"half":       "halves",
	"shelf":      "shelves",
	"wolf":       "wolves",
	"thief":      "thieves",
	"quiz":       "quizzes",
}

// uninflected lists nouns whose plural equals the singular. Membership is
// checked on the final segment only, never on modifiers.
var uninflected = map[string]bool{
	"sheep":       true,
	"fish":        true,
	"deer":        true,
	"moose":       true,
	"series":      true,
	"species":     true,
	"equipment":   true,
	"information": true,
	"money":       true,
	"news":        true,
	"data":        true,
	"media":       true,
	"metadata":    true,
	"staff":       true,
	"aircraft":    true,
}

// esOPlural lists the common consonant+o nouns that take es; every other o
// ending appends a bare s (photo → photos, piano → pianos).
var esOPlural = map[string]bool{
	"hero":   true,
	"potato": true,
	"tomato": true,
	"echo":   true,
	"veto":   true,
}

// tableName derives the conventional table name for a struct type as
// pluralize(snakeCase(structName)): User → users, APIKey → api_keys,
// Person → people, UserProfile → user_profiles. Models that want a
// different name declare TableName or use WithTableNamer; the convention
// itself never bends per model.
func tableName(structName string) string {
	return pluralize(snakeCase(structName))
}
