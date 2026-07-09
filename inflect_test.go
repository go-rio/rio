package rio

import "testing"

// The golden tables below freeze the inflector's published behavior; they
// are a compatibility promise, so entries are only ever appended, never
// edited. A model that dislikes a derived name overrides it with a tag,
// TableName, or WithTableNamer — not by changing the rules.

func TestSnakeCase(t *testing.T) {
	golden := []struct {
		in, want string
	}{
		{"", ""},
		{"A", "a"},
		{"ID", "id"},
		{"UUID", "uuid"},
		{"User", "user"},
		{"User2", "user2"},
		{"UserID", "user_id"},
		{"UserProfile", "user_profile"},
		{"APIKey", "api_key"},
		{"SSHKey", "ssh_key"},
		{"HTTPServer", "http_server"},
		{"HTTPServer2", "http_server2"},
		{"ABTest", "ab_test"},
		{"OAuth2Token", "o_auth2_token"},
		{"IPv4Address", "i_pv4_address"},
		{"DataPoint", "data_point"},
		{"user_id", "user_id"},
	}
	for _, g := range golden {
		if got := snakeCase(g.in); got != g.want {
			t.Errorf("snakeCase(%q) = %q, want %q", g.in, got, g.want)
		}
	}
}

func TestPluralize(t *testing.T) {
	golden := []struct {
		in, want string
	}{
		// Irregular nouns.
		{"person", "people"},
		{"child", "children"},
		{"man", "men"},
		{"woman", "women"},
		{"tooth", "teeth"},
		{"foot", "feet"},
		{"mouse", "mice"},
		{"goose", "geese"},
		{"ox", "oxen"},
		{"datum", "data"},
		{"medium", "media"},
		{"analysis", "analyses"},
		{"basis", "bases"},
		{"crisis", "crises"},
		{"criterion", "criteria"},
		{"phenomenon", "phenomena"},
		{"leaf", "leaves"},
		{"knife", "knives"},
		{"wife", "wives"},
		{"life", "lives"},
		{"half", "halves"},
		{"shelf", "shelves"},
		{"wolf", "wolves"},
		{"thief", "thieves"},
		{"quiz", "quizzes"},
		// Uninflected nouns.
		{"sheep", "sheep"},
		{"fish", "fish"},
		{"deer", "deer"},
		{"moose", "moose"},
		{"series", "series"},
		{"species", "species"},
		{"equipment", "equipment"},
		{"information", "information"},
		{"money", "money"},
		{"news", "news"},
		{"data", "data"},
		{"media", "media"},
		{"metadata", "metadata"},
		{"staff", "staff"},
		{"aircraft", "aircraft"},
		// Spelling rules.
		{"user", "users"},
		{"category", "categories"},
		{"day", "days"},
		{"bus", "buses"},
		{"box", "boxes"},
		{"buzz", "buzzes"},
		{"hero", "heroes"},
		{"potato", "potatoes"},
		{"tomato", "tomatoes"},
		{"echo", "echoes"},
		{"veto", "vetoes"},
		{"photo", "photos"},
		{"piano", "pianos"},
		{"user2", "user2s"},
		// Only the final segment is the noun.
		{"user_profile", "user_profiles"},
		{"api_key", "api_keys"},
		{"data_point", "data_points"},
		// Degenerate input must not panic and must not grow an s.
		{"", ""},
	}
	for _, g := range golden {
		if got := pluralize(g.in); got != g.want {
			t.Errorf("pluralize(%q) = %q, want %q", g.in, got, g.want)
		}
	}
}

func TestTableName(t *testing.T) {
	golden := []struct {
		in, want string
	}{
		{"", ""},
		{"User", "users"},
		{"APIKey", "api_keys"},
		{"Person", "people"},
		{"Child", "children"},
		{"UserProfile", "user_profiles"},
		{"DataPoint", "data_points"},
		{"SSHKey", "ssh_keys"},
		{"Category", "categories"},
		{"Box", "boxes"},
		{"Hero", "heroes"},
		{"Photo", "photos"},
		{"Day", "days"},
		{"Bus", "buses"},
		{"Series", "series"},
		{"Quiz", "quizzes"},
		{"User2", "user2s"},
		{"UUID", "uuids"},
		{"HTTPServer", "http_servers"},
		{"OAuth2Token", "o_auth2_tokens"},
		{"IPv4Address", "i_pv4_addresses"},
	}
	for _, g := range golden {
		if got := tableName(g.in); got != g.want {
			t.Errorf("tableName(%q) = %q, want %q", g.in, got, g.want)
		}
	}
}

// FuzzSnakeCase checks the safety properties snakeCase promises for
// arbitrary input, not just Go identifiers: the output never contains a
// byte outside [a-z0-9_], the function never panics, and it is idempotent,
// so feeding a derived name back through the inflector is harmless.
func FuzzSnakeCase(f *testing.F) {
	for _, seed := range []string{
		"",
		"A",
		"ID",
		"UUID",
		"UserID",
		"APIKey",
		"ABTest",
		"HTTPServer2",
		"OAuth2Token",
		"IPv4Address",
		"SSHKey",
		"SHA256Sum",
		"User2",
		"user_id",
		"_User",
		"a__b",
		"héllo",
		"标识符ID",
		"X Æ A-12",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		got := snakeCase(name)
		for i := 0; i < len(got); i++ {
			c := got[i]
			if c != '_' && !('a' <= c && c <= 'z') && !('0' <= c && c <= '9') {
				t.Fatalf("snakeCase(%q) = %q: byte %q outside [a-z0-9_]", name, got, c)
			}
		}
		if again := snakeCase(got); again != got {
			t.Errorf("snakeCase not idempotent on %q: first %q, second %q", name, got, again)
		}
	})
}
