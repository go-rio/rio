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

// relContainer marks relation fields for the mapper and preloader. Calls go
// through type assertions: reflect cannot Call unexported methods.
type relContainer interface {
	relKind() relKind
	targetType() reflect.Type
	loadedLen() (int, bool)
	// setLoaded stores the preloaded value: a []T for HasMany/ManyToMany,
	// a possibly-nil *T for HasOne/BelongsTo.
	setLoaded(v reflect.Value)
}

// relFieldNames records each container type's declared field name so
// notLoadedPanic can name the exact With argument.
var (
	relFieldNames    sync.Map // container reflect.Type → field name; "" once ambiguous
	relContainerType = reflect.TypeFor[relContainer]()
)

// HasMany holds the "child rows pointing at this row" side of a one-to-many
// relation. "Not loaded" and "loaded, empty" are distinct states: rio never
// lazy-loads and never returns silently empty data.
type HasMany[T any] struct {
	loaded bool
	rows   []T
}

// Loaded reports whether the relation has been populated by With or Set.
func (r HasMany[T]) Loaded() bool { return r.loaded }

// Rows returns the loaded children, panicking if the relation was never
// loaded.
func (r HasMany[T]) Rows() []T {
	if !r.loaded {
		panic(notLoadedPanic(relHasMany, reflect.TypeFor[HasMany[T]](), reflect.TypeFor[T]()))
	}
	return r.rows
}

// Set marks the relation loaded with the given rows.
func (r *HasMany[T]) Set(rows []T) {
	if rows == nil {
		rows = []T{}
	}
	r.loaded, r.rows = true, rows
}

// MarshalJSON encodes unloaded relations as null and loaded ones as arrays.
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
// A NULL foreign key preloads as loaded-nil: Row returns nil, no panic.
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
func (HasMany[T]) relKind() relKind             { return relHasMany }
func (HasMany[T]) targetType() reflect.Type     { return reflect.TypeFor[T]() }
func (r HasMany[T]) loadedLen() (int, bool)     { return len(r.rows), r.loaded }
func (r *HasMany[T]) setLoaded(v reflect.Value) { r.Set(v.Interface().([]T)) }

func (ManyToMany[T]) relKind() relKind             { return relManyToMany }
func (ManyToMany[T]) targetType() reflect.Type     { return reflect.TypeFor[T]() }
func (r ManyToMany[T]) loadedLen() (int, bool)     { return len(r.rows), r.loaded }
func (r *ManyToMany[T]) setLoaded(v reflect.Value) { r.Set(v.Interface().([]T)) }

func (HasOne[T]) relKind() relKind             { return relHasOne }
func (HasOne[T]) targetType() reflect.Type     { return reflect.TypeFor[T]() }
func (r HasOne[T]) loadedLen() (int, bool)     { return 0, r.loaded }
func (r *HasOne[T]) setLoaded(v reflect.Value) { r.Set(ptrOrNil[T](v)) }

func (BelongsTo[T]) relKind() relKind             { return relBelongsTo }
func (BelongsTo[T]) targetType() reflect.Type     { return reflect.TypeFor[T]() }
func (r BelongsTo[T]) loadedLen() (int, bool)     { return 0, r.loaded }
func (r *BelongsTo[T]) setLoaded(v reflect.Value) { r.Set(ptrOrNil[T](v)) }

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
	// Plan never built, or ambiguous container type: field name unknown.
	return fmt.Sprintf(
		"rio: %s[%s] accessed before loading; "+
			"add With(\"<the Go field name of this %s[%s] field>\") to the query "+
			"or assemble it manually with Set",
		kind,
		target.Name(),
		kind,
		target.Name(),
	)
}
func ptrOrNil[T any](v reflect.Value) *T {
	if !v.IsValid() || v.IsNil() {
		return nil
	}
	return v.Interface().(*T)
}

// isRelContainer reports whether t is a relation container; it checks the
// pointer type because setLoaded has a pointer receiver.
func isRelContainer(t reflect.Type) bool {
	return t.Kind() == reflect.Struct && reflect.PointerTo(t).Implements(relContainerType)
}

func containerInfo(t reflect.Type) (relKind, reflect.Type) {
	c := reflect.New(t).Interface().(relContainer)
	return c.relKind(), c.targetType()
}
