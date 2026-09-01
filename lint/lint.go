// Package lint compares rio model expectations against a live database schema
// and reports the drift: missing tables/columns, nullability and primary-key
// disagreements, and type mismatches the dialect's equivalence classes can rule on.
//
// Check is read-only and reports only what it can decide. It covers PostgreSQL,
// MySQL, and SQLite, and takes table names as unqualified (the connection's
// current schema/database).
package lint

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-rio/rio"
)

// Severity ranks a finding.
type Severity int

const (
	// Notice marks harmless drift worth knowing about.
	Notice Severity = iota
	// Warn marks drift likely to fail at runtime for some data.
	Warn
	// Error marks drift that breaks queries outright.
	Error
)

// String returns the severity's lower-case name: "error", "warn", or "notice".
func (s Severity) String() string {
	switch s {
	case Error:
		return "error"
	case Warn:
		return "warn"
	}
	return "notice"
}

// Finding is one point of drift between a model and the database.
type Finding struct {
	Severity Severity
	// Kind is a stable slug: "missing-table", "missing-column",
	// "extra-column", "nullability", "type", or "primary-key".
	Kind    string
	Model   string
	Table   string
	Column  string // empty for table-level findings
	Message string
}

// Report is Check's outcome.
type Report struct {
	Findings []Finding
}

// Errors returns the Error-severity findings.
func (r *Report) Errors() []Finding {
	var out []Finding
	for _, f := range r.Findings {
		if f.Severity == Error {
			out = append(out, f)
		}
	}
	return out
}

func (r *Report) add(f Finding) { r.Findings = append(r.Findings, f) }

// Check compares each model's mapping under db against the live schema.
// It is read-only and reports only decidable drift.
func Check(ctx context.Context, db *rio.DB, models ...any) (*Report, error) {
	dialect := db.Dialect()
	intro, ok := introspectors[dialect]
	if !ok {
		return nil, fmt.Errorf("lint: this dialect is not supported (postgres, mysql, sqlite)")
	}
	report := &Report{}
	for _, model := range models {
		ts, err := db.DescribeModel(model)
		if err != nil {
			return nil, err
		}
		cols, found, err := intro(ctx, db, ts.Table)
		if err != nil {
			return nil, fmt.Errorf("lint: introspecting %q: %w", ts.Table, err)
		}
		if !found {
			report.add(Finding{
				Severity: Error, Kind: "missing-table", Model: ts.Struct, Table: ts.Table,
				Message: fmt.Sprintf("table %q does not exist; every %s query fails", ts.Table, ts.Struct),
			})
			continue
		}
		checkTable(report, dialect, ts, cols)
	}
	return report, nil
}

func checkTable(report *Report, dialect rio.Dialect, ts *rio.TableSchema, cols []dbColumn) {
	add := func(sev Severity, kind, column, msg string) {
		report.add(Finding{Severity: sev, Kind: kind, Model: ts.Struct, Table: ts.Table, Column: column, Message: msg})
	}
	byName := make(map[string]dbColumn, len(cols))
	for _, c := range cols {
		byName[c.name] = c
	}
	mapped := make(map[string]bool, len(ts.Columns))

	for _, mc := range ts.Columns {
		mapped[mc.Name] = true
		dc, ok := byName[mc.Name]
		if !ok {
			add(Error, "missing-column", mc.Name,
				fmt.Sprintf("column %q is mapped by %s.%s but missing from the table; every query naming it fails", mc.Name, ts.Struct, mc.Field))
			continue
		}
		nullUnhandled := dc.nullable && !mc.Nullable
		needlessPointer := !dc.nullable && mc.Nullable && mc.GoType.Kind() == reflect.Pointer
		switch {
		case nullUnhandled:
			add(Warn, "nullability", mc.Name,
				fmt.Sprintf("column %q is nullable but %s scans it into non-nullable %s; a NULL row fails to scan — use a pointer or add NOT NULL", mc.Name, ts.Struct, mc.GoType))
		case needlessPointer:
			add(Notice, "nullability", mc.Name,
				fmt.Sprintf("column %q is NOT NULL but %s maps it through pointer %s; the nil case cannot occur", mc.Name, ts.Struct, mc.GoType))
		}
		// Unknown types stay silent: no finding beats a guess.
		if verdictFor(dialect, dc.dataType, mc) == verdictMismatch {
			add(Warn, "type", mc.Name,
				fmt.Sprintf("column %q is %s but %s expects %s; values may fail to scan or bind", mc.Name, dc.dataType, ts.Struct, mc.GoType))
		}
		if dc.pk != mc.PrimaryKey {
			state := "is not"
			if dc.pk {
				state = "is"
			}
			add(Error, "primary-key", mc.Name,
				fmt.Sprintf("column %q %s part of the table's primary key but the model disagrees; Find/Update/Delete address the wrong rows", mc.Name, state))
		}
	}
	for _, c := range cols {
		if !mapped[c.name] {
			add(Notice, "extra-column", c.name,
				fmt.Sprintf("table column %q is not mapped by %s; rio never reads or writes it", c.name, ts.Struct))
		}
	}
}
