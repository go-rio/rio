package rio

import (
	"strings"
	"testing"
)

type badValidateModel struct {
	ID int64 `rio:",bogus"`
}

func TestQueryValidateModelAndBuilderState(t *testing.T) {
	if err := From[badValidateModel]().Validate(); err == nil || !strings.Contains(err.Error(), "unknown rio tag option") {
		t.Fatalf("model tag: %v", err)
	}
	err := From[User]().Where("age = ?", 1, 2).Validate()
	if err == nil || !strings.Contains(err.Error(), "1 placeholder(s) but 2 argument(s)") {
		t.Fatalf("condition arity: %v", err)
	}
	if err := From[User]().Limit(-1).Validate(); err == nil || !strings.Contains(err.Error(), "non-negative") {
		t.Fatalf("negative limit: %v", err)
	}
	if err := From[User]().Offset(-1).Validate(); err == nil || !strings.Contains(err.Error(), "non-negative") {
		t.Fatalf("negative offset: %v", err)
	}
}

func TestQueryValidateRelationOptions(t *testing.T) {
	tests := []struct {
		name string
		q    Query[User]
		want string
	}{
		{"With RelWhere missing arg", From[User]().With("Posts", RelWhere("title = ?")), "must bind inline"},
		{"WhereHas RelWhere excess arg", From[User]().WhereHas("Posts", RelWhere("active", true)), "must bind inline"},
		{
			"With RelOrder placeholder",
			From[User]().With("Posts", RelOrder("CASE WHEN title = ? THEN 0 END")),
			"RelOrder has no argument channel",
		},
		{"With negative RelLimit", From[User]().With("Posts", RelLimit(-1)), "RelLimit requires a non-negative value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.q.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestQueryMustUsesValidate(t *testing.T) {
	want := From[User]().With("Posts", RelWhere("title = ?")).Validate()
	if want == nil {
		t.Fatal("Validate unexpectedly succeeded")
	}
	defer func() {
		got := recover()
		err, ok := got.(error)
		if !ok || err.Error() != want.Error() {
			t.Fatalf("Must panic = %#v, want %q", got, want)
		}
	}()
	From[User]().With("Posts", RelWhere("title = ?")).Must()
}

func TestQueryValidateAllowsDeferredMainConditions(t *testing.T) {
	q := From[User]().Where("age >= ?").Having("count(*) > ?")
	if err := q.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := q.Must(); len(got.s.wheres) != 1 ||
		got.s.wheres[0].args != nil ||
		len(got.s.havings) != 1 ||
		got.s.havings[0].args != nil {
		t.Fatalf("Must changed query state: %+v", got.s)
	}
}
