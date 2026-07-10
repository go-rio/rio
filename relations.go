package rio

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

type relKind int

const (
	relHasMany relKind = iota + 1
	relHasOne
	relBelongsTo
	relManyToMany
)

func (k relKind) String() string {
	switch k {
	case relHasMany:
		return "HasMany"
	case relHasOne:
		return "HasOne"
	case relBelongsTo:
		return "BelongsTo"
	case relManyToMany:
		return "ManyToMany"
	}
	return "relation"
}

// relContainer is how the mapper recognizes relation fields and how the
// preloader assembles them. It stays unexported: matching happens via
// reflect.Type.Implements, and calls happen through plain type assertions in
// this package (reflect cannot Call unexported methods).
type relContainer interface {
	relKind() relKind
	targetType() reflect.Type
	// setLoaded stores the preloaded value: a []T for HasMany/ManyToMany,
	// a possibly-nil *T for HasOne/BelongsTo.
	setLoaded(v reflect.Value)
}

// relFieldNames maps a container type (HasMany[Post]) to the Go struct field
// name it is declared under, recorded when a plan builds, so notLoadedPanic
// can name the exact With argument — With takes the field name, not the
// target type name (a Posts HasMany[Post] field needs With("Posts")). Two
// models declaring the same container type under different names collapse
// the entry to "" and the panic falls back to generic wording.
var relFieldNames sync.Map // reflect.Type → field name string; "" once ambiguous

func registerRelFieldName(container reflect.Type, name string) {
	if prev, loaded := relFieldNames.LoadOrStore(container, name); loaded && prev.(string) != name {
		relFieldNames.Store(container, "")
	}
}

func notLoadedPanic(kind relKind, container, target reflect.Type) string {
	if name, ok := relFieldNames.Load(container); ok && name.(string) != "" {
		return fmt.Sprintf(
			"rio: %s[%s] accessed before loading; add With(%q) to the query or assemble it manually with Set",
			kind, target.Name(), name)
	}
	// The owning model's plan was never built (or the container type appears
	// under several field names): the exact field name is unknown here.
	return fmt.Sprintf(
		"rio: %s[%s] accessed before loading; add With(\"<the Go field name of this %s[%s] field>\") to the query or assemble it manually with Set",
		kind, target.Name(), kind, target.Name())
}

// HasMany holds the "child rows pointing at this row" side of a one-to-many
// relation. It is a container rather than a bare slice so that "not loaded"
// and "loaded, empty" are different states: rio never returns silently empty
// data and never lazy-loads. Structs containing relation containers are not
// comparable, and cmp.Diff panics on the containers' unexported state — pass
// cmpopts.IgnoreUnexported(rio.HasMany[Post]{}, ...) and compare relation
// contents through the exported accessors (Rows/Row) instead.
type HasMany[T any] struct {
	loaded bool
	rows   []T
}

// Loaded reports whether the relation has been populated by With or Set.
func (r HasMany[T]) Loaded() bool { return r.loaded }

// Rows returns the loaded children. It panics if the relation was never
// loaded — accessing unloaded data is a programming error, not an empty
// result.
func (r HasMany[T]) Rows() []T {
	if !r.loaded {
		panic(notLoadedPanic(relHasMany, reflect.TypeFor[HasMany[T]](), reflect.TypeFor[T]()))
	}
	return r.rows
}

// Set marks the relation loaded with the given rows. Manual assembly (from a
// custom query or fixture) is a supported use.
func (r *HasMany[T]) Set(rows []T) {
	if rows == nil {
		rows = []T{}
	}
	r.loaded, r.rows = true, rows
}

func (HasMany[T]) relKind() relKind             { return relHasMany }
func (HasMany[T]) targetType() reflect.Type     { return reflect.TypeFor[T]() }
func (r *HasMany[T]) setLoaded(v reflect.Value) { r.Set(v.Interface().([]T)) }

// MarshalJSON encodes unloaded relations as null and loaded ones as arrays,
// so API payloads distinguish "not fetched" from "none".
func (r HasMany[T]) MarshalJSON() ([]byte, error) {
	if !r.loaded {
		return []byte("null"), nil
	}
	return json.Marshal(r.rows)
}

// UnmarshalJSON accepts null (leaving the relation unloaded) or an array.
func (r *HasMany[T]) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*r = HasMany[T]{}
		return nil
	}
	var rows []T
	if err := json.Unmarshal(b, &rows); err != nil {
		return err
	}
	r.Set(rows)
	return nil
}

// ManyToMany is HasMany across a join table.
type ManyToMany[T any] struct {
	loaded bool
	rows   []T
}

// Loaded reports whether the relation has been populated by With or Set.
func (r ManyToMany[T]) Loaded() bool { return r.loaded }

// Rows returns the loaded rows, panicking when the relation was never loaded.
func (r ManyToMany[T]) Rows() []T {
	if !r.loaded {
		panic(notLoadedPanic(relManyToMany, reflect.TypeFor[ManyToMany[T]](), reflect.TypeFor[T]()))
	}
	return r.rows
}

// Set marks the relation loaded with the given rows.
func (r *ManyToMany[T]) Set(rows []T) {
	if rows == nil {
		rows = []T{}
	}
	r.loaded, r.rows = true, rows
}

func (ManyToMany[T]) relKind() relKind             { return relManyToMany }
func (ManyToMany[T]) targetType() reflect.Type     { return reflect.TypeFor[T]() }
func (r *ManyToMany[T]) setLoaded(v reflect.Value) { r.Set(v.Interface().([]T)) }

// MarshalJSON behaves like HasMany.MarshalJSON.
func (r ManyToMany[T]) MarshalJSON() ([]byte, error) {
	if !r.loaded {
		return []byte("null"), nil
	}
	return json.Marshal(r.rows)
}

// UnmarshalJSON behaves like HasMany.UnmarshalJSON.
func (r *ManyToMany[T]) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*r = ManyToMany[T]{}
		return nil
	}
	var rows []T
	if err := json.Unmarshal(b, &rows); err != nil {
		return err
	}
	r.Set(rows)
	return nil
}

// HasOne holds the "single child row pointing at this row" side of a
// one-to-one relation.
type HasOne[T any] struct {
	loaded bool
	row    *T
}

// Loaded reports whether the relation has been populated by With or Set.
func (r HasOne[T]) Loaded() bool { return r.loaded }

// Row returns the loaded child, or nil when the parent has none. It panics
// if the relation was never loaded.
func (r HasOne[T]) Row() *T {
	if !r.loaded {
		panic(notLoadedPanic(relHasOne, reflect.TypeFor[HasOne[T]](), reflect.TypeFor[T]()))
	}
	return r.row
}

// Set marks the relation loaded. A nil row means "loaded, has none".
func (r *HasOne[T]) Set(row *T) { r.loaded, r.row = true, row }

func (HasOne[T]) relKind() relKind             { return relHasOne }
func (HasOne[T]) targetType() reflect.Type     { return reflect.TypeFor[T]() }
func (r *HasOne[T]) setLoaded(v reflect.Value) { r.Set(ptrOrNil[T](v)) }

// MarshalJSON encodes unloaded as null; loaded-none also encodes as null.
func (r HasOne[T]) MarshalJSON() ([]byte, error) {
	if !r.loaded || r.row == nil {
		return []byte("null"), nil
	}
	return json.Marshal(r.row)
}

// UnmarshalJSON accepts null (leaving the relation unloaded) or an object.
func (r *HasOne[T]) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*r = HasOne[T]{}
		return nil
	}
	row := new(T)
	if err := json.Unmarshal(b, row); err != nil {
		return err
	}
	r.Set(row)
	return nil
}

// BelongsTo holds the parent row referenced by a foreign key on this row.
// After preloading, a NULL foreign key yields the loaded-nil state — Row
// returns nil instead of panicking, preserving "With makes access safe".
type BelongsTo[T any] struct {
	loaded bool
	row    *T
}

// Loaded reports whether the relation has been populated by With or Set.
func (r BelongsTo[T]) Loaded() bool { return r.loaded }

// Row returns the loaded parent, or nil when the foreign key was NULL. It
// panics if the relation was never loaded.
func (r BelongsTo[T]) Row() *T {
	if !r.loaded {
		panic(notLoadedPanic(relBelongsTo, reflect.TypeFor[BelongsTo[T]](), reflect.TypeFor[T]()))
	}
	return r.row
}

// Set marks the relation loaded. A nil row means "loaded, no parent".
func (r *BelongsTo[T]) Set(row *T) { r.loaded, r.row = true, row }

func (BelongsTo[T]) relKind() relKind             { return relBelongsTo }
func (BelongsTo[T]) targetType() reflect.Type     { return reflect.TypeFor[T]() }
func (r *BelongsTo[T]) setLoaded(v reflect.Value) { r.Set(ptrOrNil[T](v)) }

// MarshalJSON behaves like HasOne.MarshalJSON.
func (r BelongsTo[T]) MarshalJSON() ([]byte, error) {
	if !r.loaded || r.row == nil {
		return []byte("null"), nil
	}
	return json.Marshal(r.row)
}

// UnmarshalJSON behaves like HasOne.UnmarshalJSON.
func (r *BelongsTo[T]) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*r = BelongsTo[T]{}
		return nil
	}
	row := new(T)
	if err := json.Unmarshal(b, row); err != nil {
		return err
	}
	r.Set(row)
	return nil
}

func ptrOrNil[T any](v reflect.Value) *T {
	if !v.IsValid() || v.IsNil() {
		return nil
	}
	return v.Interface().(*T)
}

var relContainerType = reflect.TypeFor[relContainer]()

// isRelContainer reports whether a struct field type is one of the relation
// containers, checking the pointer type so the pointer-receiver setLoaded is
// part of the method set.
func isRelContainer(t reflect.Type) bool {
	return t.Kind() == reflect.Struct && reflect.PointerTo(t).Implements(relContainerType)
}

// containerInfo extracts kind and target type from a zero container value.
func containerInfo(t reflect.Type) (relKind, reflect.Type) {
	c := reflect.New(t).Interface().(relContainer)
	return c.relKind(), c.targetType()
}
