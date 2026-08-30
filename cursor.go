package rio

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"reflect"
	"strconv"
	"time"
)

// SortKey is one column of a structured ordering. It names a mapped column —
// not verbatim SQL — so rio can read its value back out of rows to issue
// cursors.
type SortKey struct {
	Column string
	Desc   bool
}

// Cursor marks a position in a keyset-ordered result: the sort-key values of
// the row it points past, plus a fingerprint of the ordering that issued it.
// The zero Cursor is "no position" and After rejects it. Tokens are opaque
// but not tamper-proof: values bind as parameters, so a forged token can move
// the page window, never change the query.
type Cursor struct {
	fp     uint64
	values []any
}

// IsZero reports whether the cursor marks no position.
func (c Cursor) IsZero() bool { return c.fp == 0 && c.values == nil }

// cursorVersion prefixes every token.
const cursorVersion = byte(1)

// String encodes the cursor as a URL-safe token.
func (c Cursor) String() string {
	b := []byte{cursorVersion}
	b = binary.BigEndian.AppendUint64(b, c.fp)
	for _, v := range c.values {
		var tag byte
		var body string
		switch t := v.(type) {
		case int64:
			tag, body = 'i', strconv.FormatInt(t, 10)
		case uint64:
			tag, body = 'u', strconv.FormatUint(t, 10)
		case float64:
			// Bit-exact: formatting would round-trip imprecisely.
			tag, body = 'f', strconv.FormatUint(math.Float64bits(t), 16)
		case bool:
			tag, body = 'b', strconv.FormatBool(t)
		case string:
			tag, body = 's', t
		case time.Time:
			tag, body = 't', t.UTC().Format(time.RFC3339Nano)
		default:
			// Hand-built cursors with other types encode as their print form
			// and bind as strings.
			tag, body = 's', fmt.Sprint(t)
		}
		b = append(b, tag)
		b = appendCursorString(b, body)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func appendCursorString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

// ParseCursor decodes a token String produced. Malformed input fails here; a
// token for a different ordering fails at After's fingerprint check.
func ParseCursor(s string) (Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, fmt.Errorf("rio: ParseCursor: %w", err)
	}
	if len(raw) < 9 || raw[0] != cursorVersion {
		return Cursor{}, fmt.Errorf("rio: ParseCursor: not a rio cursor token")
	}
	c := Cursor{fp: binary.BigEndian.Uint64(raw[1:9])}
	rest := raw[9:]
	for len(rest) > 0 {
		if len(rest) < 5 {
			return Cursor{}, fmt.Errorf("rio: ParseCursor: truncated token")
		}
		tag := rest[0]
		n := binary.BigEndian.Uint32(rest[1:5])
		rest = rest[5:]
		if uint32(len(rest)) < n {
			return Cursor{}, fmt.Errorf("rio: ParseCursor: truncated token")
		}
		body := string(rest[:n])
		rest = rest[n:]
		switch tag {
		case 'i':
			v, err := strconv.ParseInt(body, 10, 64)
			if err != nil {
				return Cursor{}, fmt.Errorf("rio: ParseCursor: bad integer %q", body)
			}
			c.values = append(c.values, v)
		case 'u':
			v, err := strconv.ParseUint(body, 10, 64)
			if err != nil {
				return Cursor{}, fmt.Errorf("rio: ParseCursor: bad unsigned %q", body)
			}
			c.values = append(c.values, v)
		case 'f':
			bits, err := strconv.ParseUint(body, 16, 64)
			if err != nil {
				return Cursor{}, fmt.Errorf("rio: ParseCursor: bad float %q", body)
			}
			c.values = append(c.values, math.Float64frombits(bits))
		case 'b':
			v, err := strconv.ParseBool(body)
			if err != nil {
				return Cursor{}, fmt.Errorf("rio: ParseCursor: bad bool %q", body)
			}
			c.values = append(c.values, v)
		case 's':
			c.values = append(c.values, body)
		case 't':
			v, err := time.Parse(time.RFC3339Nano, body)
			if err != nil {
				return Cursor{}, fmt.Errorf("rio: ParseCursor: bad time %q", body)
			}
			c.values = append(c.values, v)
		default:
			return Cursor{}, fmt.Errorf("rio: ParseCursor: unknown value tag %q", tag)
		}
	}
	return c, nil
}

// resolvedKey is one sort key bound to its plan field.
type resolvedKey struct {
	f    *field
	desc bool
}

// resolveSortKeys validates the declared keys against the plan and appends
// missing PK columns as the tie-breaker: keyset pagination needs a total order.
func resolveSortKeys(p *plan, s *queryState) ([]resolvedKey, error) {
	if len(s.orderKeys) == 0 {
		return nil, fmt.Errorf("rio: cursor pagination needs OrderKeys (OrderBy is verbatim SQL rio cannot read values back from)")
	}
	if len(s.orders) > 0 {
		return nil, fmt.Errorf("rio: OrderKeys and OrderBy cannot mix; the cursor must own the whole ordering")
	}
	if len(p.pks) == 0 {
		return nil, fmt.Errorf("rio: cursor pagination needs a primary key on %s for its tie-breaker", p.structName)
	}
	keys := make([]resolvedKey, 0, len(s.orderKeys)+len(p.pks))
	seen := make(map[string]bool, len(s.orderKeys)+len(p.pks))
	for _, k := range s.orderKeys {
		f, ok := p.byColumn[k.Column]
		if !ok {
			return nil, fmt.Errorf("rio: OrderKeys: %s has no column %q (expressions go through Raw)", p.structName, k.Column)
		}
		if seen[k.Column] {
			return nil, fmt.Errorf("rio: OrderKeys: column %q declared twice", k.Column)
		}
		seen[k.Column] = true
		if err := checkSortable(p, f); err != nil {
			return nil, err
		}
		keys = append(keys, resolvedKey{f: f, desc: k.Desc})
	}
	tailDesc := keys[len(keys)-1].desc
	for _, pk := range p.pks {
		if !seen[pk.column] {
			keys = append(keys, resolvedKey{f: pk, desc: tailDesc})
		}
	}
	return keys, nil
}

// checkSortable rejects nullable columns (NULL has no portable order) and
// non-scalar columns (no canonical token form).
func checkSortable(p *plan, f *field) error {
	if f.nullable() {
		return fmt.Errorf(
			"rio: OrderKeys: column %q of %s is nullable; keyset comparison over NULL has no portable order — sort a NOT NULL column, or coalesce in Raw",
			f.column, p.structName,
		)
	}
	switch f.code.kind {
	case scanInt, scanUint, scanFloat, scanString, scanBool, scanTime:
		return nil
	}
	return fmt.Errorf(
		"rio: OrderKeys: column %q of %s (%s) has no canonical comparable form for a cursor; sort a scalar column",
		f.column, p.structName, f.typ,
	)
}

// sortKeyFingerprint hashes the resolved ordering so a cursor issued under
// one ordering fails loudly under another.
func sortKeyFingerprint(keys []resolvedKey) uint64 {
	h := fnv.New64a()
	for _, k := range keys {
		_, _ = h.Write([]byte(k.f.column))
		if k.desc {
			_, _ = h.Write([]byte{0, 1})
		} else {
			_, _ = h.Write([]byte{0, 0})
		}
	}
	return h.Sum64()
}

// cursorValue reads one sort-key cell, folded to the cursor's canonical scalars.
func cursorValue(f *field, rv reflect.Value) (any, error) {
	v := rv.FieldByIndex(f.index)
	switch f.code.kind {
	case scanInt:
		return v.Int(), nil
	case scanUint:
		return v.Uint(), nil
	case scanFloat:
		return v.Float(), nil
	case scanString:
		return v.String(), nil
	case scanBool:
		return v.Bool(), nil
	case scanTime:
		// The struct already holds the normalized value the database stores.
		return v.Interface().(time.Time), nil
	}
	return nil, fmt.Errorf("rio: cursor: column %q is not a cursor scalar", f.column)
}

// check verifies the cursor against the query's resolved ordering.
func (c *Cursor) check(keys []resolvedKey) error {
	if c.IsZero() {
		return fmt.Errorf("rio: After: the zero Cursor marks no position; omit After for the first page")
	}
	if c.fp != sortKeyFingerprint(keys) {
		return fmt.Errorf("rio: After: the cursor was issued for a different ordering than this query's OrderKeys")
	}
	if len(c.values) != len(keys) {
		return fmt.Errorf("rio: After: the cursor carries %d value(s) for %d sort key(s)", len(c.values), len(keys))
	}
	return nil
}

// renderAfter appends the expanded keyset predicate, ((k0 > ?) OR (k0 = ? AND
// k1 > ?) OR ...): row-value syntax is neither portable nor mixed-direction.
func renderAfter(b []byte, args []any, d Dialect, table string, keys []resolvedKey, c *Cursor) ([]byte, []any) {
	b = append(b, '(')
	for i, k := range keys {
		if i > 0 {
			b = append(b, " OR "...)
		}
		b = append(b, '(')
		for j := 0; j < i; j++ {
			b = d.quote(b, table)
			b = append(b, '.')
			b = d.quote(b, keys[j].f.column)
			b = append(b, " = ? AND "...)
			args = append(args, c.values[j])
		}
		b = d.quote(b, table)
		b = append(b, '.')
		b = d.quote(b, k.f.column)
		if k.desc {
			b = append(b, " < ?"...)
		} else {
			b = append(b, " > ?"...)
		}
		args = append(args, c.values[i])
		b = append(b, ')')
	}
	b = append(b, ')')
	return b, args
}

// appendOrderKeys renders the resolved structured ordering.
func appendOrderKeys(b []byte, d Dialect, table string, keys []resolvedKey) []byte {
	if len(keys) == 0 {
		return b
	}
	b = append(b, " ORDER BY "...)
	for i, k := range keys {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = d.quote(b, table)
		b = append(b, '.')
		b = d.quote(b, k.f.column)
		if k.desc {
			b = append(b, " DESC"...)
		}
	}
	return b
}

// OrderKeys sets the structured ordering cursor pagination requires, rendered
// as the query's ORDER BY; it cannot mix with verbatim OrderBy. Primary-key
// columns missing from keys are appended as tie-breakers, following the last
// declared direction.
func (q Query[T]) OrderKeys(keys ...SortKey) Query[T] {
	q.cache = nil
	q.s.orderKeys = append(append([]SortKey(nil), q.s.orderKeys...), keys...)
	return q
}

// After resumes past the position c marks: rows strictly after it in the
// OrderKeys ordering. c must come from CursorAfter under the same OrderKeys;
// a different ordering fails loudly. For backward paging, flip every key's
// direction and resume After the first row of the page.
func (q Query[T]) After(c Cursor) Query[T] {
	q.cache = nil
	q.s.after = &c
	return q
}

// CursorAfter issues the cursor marking row's position under the query's
// OrderKeys. The row must hold the values the database stores (any row rio
// scanned does).
func (q Query[T]) CursorAfter(row *T) (Cursor, error) {
	p, err := planOf[T]()
	if err != nil {
		return Cursor{}, err
	}
	keys, err := resolveSortKeys(p, &q.s)
	if err != nil {
		return Cursor{}, err
	}
	rv := reflect.ValueOf(row).Elem()
	c := Cursor{fp: sortKeyFingerprint(keys), values: make([]any, 0, len(keys))}
	for _, k := range keys {
		v, err := cursorValue(k.f, rv)
		if err != nil {
			return Cursor{}, err
		}
		c.values = append(c.values, v)
	}
	return c, nil
}
