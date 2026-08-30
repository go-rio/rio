package lint

import (
	"reflect"
	"strings"
	"time"

	"github.com/go-rio/rio"
)

// verdict is a type comparison's outcome. Unknown means the database type is
// outside the known equivalence classes; lint stays silent then — a missing
// finding is honest, a guessed one is not.
type verdict int

const (
	verdictUnknown verdict = iota
	verdictOK
	verdictMismatch
)

// goClass buckets a model column by what it needs from the database.
type goClass int

const (
	classOther goClass = iota // Scanner and friends: undecidable, stay silent
	classInt
	classFloat
	classString
	classBool
	classTime
	classBytes
	classJSON
)

var timeType = reflect.TypeFor[time.Time]()

func classOf(c rio.ColumnSchema) goClass {
	if c.JSON {
		return classJSON
	}
	t := c.GoType
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == timeType {
		return classTime
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return classInt
	case reflect.Float32, reflect.Float64:
		return classFloat
	case reflect.String:
		return classString
	case reflect.Bool:
		return classBool
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return classBytes
		}
	}
	return classOther
}

// acceptable maps, per dialect and Go class, the database types that scan
// and bind cleanly. A type in no class at all stays unknown.
var acceptable = map[string]map[goClass][]string{
	"postgres": {
		classInt:    {"smallint", "integer", "bigint"},
		classFloat:  {"real", "double precision", "numeric"},
		classString: {"text", "character varying", "character", "uuid"},
		classBool:   {"boolean"},
		classTime:   {"timestamp with time zone", "timestamp without time zone", "date"},
		classBytes:  {"bytea"},
		classJSON:   {"json", "jsonb", "text", "character varying"},
	},
	"mysql": {
		classInt:    {"tinyint", "smallint", "mediumint", "int", "bigint"},
		classFloat:  {"float", "double", "decimal"},
		classString: {"char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum"},
		classBool:   {"tinyint"},
		classTime:   {"datetime", "timestamp", "date"},
		classBytes:  {"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob"},
		classJSON:   {"json", "text", "mediumtext", "longtext"},
	},
}

// knownTypes is the union per dialect: a database type outside it is
// unknown, inside it but outside the column's class is a mismatch.
var knownTypes = func() map[string]map[string]bool {
	out := make(map[string]map[string]bool, len(acceptable))
	for dialect, classes := range acceptable {
		set := make(map[string]bool)
		for _, types := range classes {
			for _, t := range types {
				set[t] = true
			}
		}
		out[dialect] = set
	}
	return out
}()

func verdictFor(dialect, dataType string, c rio.ColumnSchema) verdict {
	class := classOf(c)
	if class == classOther {
		return verdictUnknown
	}
	if dialect == "sqlite" {
		// SQLite's type affinity stores anything in any column; a declared
		// type is advisory. Matching declarations confirm, nothing refutes.
		return sqliteVerdict(dataType, class)
	}
	classes, ok := acceptable[dialect]
	if !ok {
		return verdictUnknown
	}
	for _, t := range classes[class] {
		if dataType == t {
			return verdictOK
		}
	}
	if knownTypes[dialect][dataType] {
		return verdictMismatch
	}
	return verdictUnknown
}

// sqliteVerdict matches by declared-type affinity and never returns
// verdictMismatch: any SQLite column can hold any value.
func sqliteVerdict(declared string, class goClass) verdict {
	has := func(subs ...string) bool {
		for _, s := range subs {
			if strings.Contains(declared, s) {
				return true
			}
		}
		return false
	}
	switch class {
	case classInt:
		if has("int") {
			return verdictOK
		}
	case classFloat:
		if has("real", "float", "double", "numeric", "decimal") {
			return verdictOK
		}
	case classString:
		if has("char", "clob", "text") {
			return verdictOK
		}
	case classBool:
		if has("bool", "int") {
			return verdictOK
		}
	case classTime:
		if has("date", "time") {
			return verdictOK
		}
	case classBytes:
		if declared == "" || has("blob") {
			return verdictOK
		}
	case classJSON:
		if has("text", "char", "clob", "json", "blob") {
			return verdictOK
		}
	}
	return verdictUnknown
}
