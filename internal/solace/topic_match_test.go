package solace

import (
	"strings"
	"testing"
)

// matchTopicPattern reports whether topic matches a Solace topic
// subscription pattern. `>` matches one or more trailing levels (only valid
// at the end of the pattern). `*` matches one full level, and as a suffix
// at the end of a level part it acts as a prefix wildcard within that
// level (e.g. `d-*` matches `d-anything`).
func matchTopicPattern(topic, pattern string) bool {
	return matchTopicLevels(strings.Split(topic, "/"), strings.Split(pattern, "/"))
}

func TestMatchTopicPattern(t *testing.T) {
	cases := []struct {
		name    string
		topic   string
		pattern string
		want    bool
	}{
		{"exact match", "foo/bar", "foo/bar", true},
		{"exact mismatch literal", "foo/bar", "foo/baz", false},
		{"topic shorter than pattern", "foo", "foo/bar", false},
		{"topic longer than pattern, no wildcard", "foo/bar/baz", "foo/bar", false},

		{"single wildcard matches one level", "foo/bar", "foo/*", true},
		{"single wildcard does not match empty", "foo", "foo/*", false},
		{"single wildcard does not span levels", "foo/bar/baz", "foo/*", false},
		{"single wildcard in middle", "trades/orders/42", "trades/*/42", true},
		{"single wildcard at start", "abc/orders", "*/orders", true},

		{"multi wildcard matches one trailing level", "foo/bar", "foo/>", true},
		{"multi wildcard matches many trailing levels", "foo/bar/baz/qux", "foo/>", true},
		{"multi wildcard requires at least one level", "foo", "foo/>", false},
		{"multi wildcard not at end is rejected", "foo/x/bar", "foo/>/bar", false},

		{"combined wildcards match", "trades/orders/abc/def", "*/orders/>", true},
		{"combined wildcards mismatch literal", "trades/fills/abc", "*/orders/>", false},

		{"prefix wildcard matches", "foo/d-bar", "foo/d-*", true},
		{"prefix wildcard rejects different prefix", "foo/x-bar", "foo/d-*", false},
		{"prefix wildcard rejects no prefix", "foo/bar", "foo/d-*", false},
		{"prefix wildcard does not span levels", "foo/d-bar/baz", "foo/d-*", false},
		{"prefix wildcard at first level", "d-foo/bar", "d-*/bar", true},
		{"prefix wildcard combined with multi", "trades/d-orders/42", "trades/d-*/>", true},

		{"single-segment topic, > pattern", "foo", ">", true},
		{"two-segment topic, > pattern", "foo/bar", ">", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := matchTopicPattern(tc.topic, tc.pattern)
			if got != tc.want {
				t.Errorf("matchTopicPattern(%q, %q) = %v, want %v", tc.topic, tc.pattern, got, tc.want)
			}
		})
	}
}

func TestTopicTypeIndexLookup(t *testing.T) {
	lookup := func(topic string, mappings map[string]string) (string, error) {
		return newTopicTypeIndex(mappings).Lookup(topic)
	}

	t.Run("empty map", func(t *testing.T) {
		mt, err := lookup("foo/bar", nil)
		if err != nil || mt != "" {
			t.Errorf("got (%q, %v), want (\"\", nil)", mt, err)
		}
	})

	t.Run("no match", func(t *testing.T) {
		m := map[string]string{"a/>": "A"}
		mt, err := lookup("foo/bar", m)
		if err != nil || mt != "" {
			t.Errorf("got (%q, %v), want (\"\", nil)", mt, err)
		}
	})

	t.Run("single match", func(t *testing.T) {
		m := map[string]string{"trades/>": "Trade"}
		mt, err := lookup("trades/orders/42", m)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mt != "Trade" {
			t.Errorf("got %q, want %q", mt, "Trade")
		}
	})

	t.Run("more literal segments wins", func(t *testing.T) {
		m := map[string]string{
			"trades/>":        "Trade",
			"trades/orders/*": "Order",
		}
		mt, err := lookup("trades/orders/42", m)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mt != "Order" {
			t.Errorf("got %q, want %q", mt, "Order")
		}
	})

	t.Run("longer pattern wins on equal literals", func(t *testing.T) {
		// Both patterns have 1 literal segment ("trades"); the longer one
		// is more specific.
		m := map[string]string{
			"trades/>":   "Generic",
			"trades/*/>": "Detailed",
		}
		mt, err := lookup("trades/orders/42", m)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mt != "Detailed" {
			t.Errorf("got %q, want %q", mt, "Detailed")
		}
	})

	t.Run("ambiguous match errors", func(t *testing.T) {
		// Both patterns have 1 literal and 2 segments — fully tied.
		m := map[string]string{
			"trades/*": "A",
			"*/orders": "B",
		}
		mt, err := lookup("trades/orders", m)
		if err == nil {
			t.Fatalf("expected error, got %q", mt)
		}
	})

	t.Run("prefix wildcard beats bare wildcard", func(t *testing.T) {
		m := map[string]string{
			"foo/*":   "Any",
			"foo/d-*": "Detailed",
		}
		mt, err := lookup("foo/d-bar", m)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mt != "Detailed" {
			t.Errorf("got %q, want %q", mt, "Detailed")
		}
	})

	t.Run("full literal beats prefix wildcard", func(t *testing.T) {
		m := map[string]string{
			"foo/d-bar": "Exact",
			"foo/d-*":   "Prefix",
		}
		mt, err := lookup("foo/d-bar", m)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mt != "Exact" {
			t.Errorf("got %q, want %q", mt, "Exact")
		}
	})

	t.Run("most-specific wins over many", func(t *testing.T) {
		m := map[string]string{
			">":                      "Catchall",
			"trades/>":               "Trade",
			"trades/orders/>":        "Order",
			"trades/orders/europe/>": "EUOrder",
		}
		mt, err := lookup("trades/orders/europe/42", m)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mt != "EUOrder" {
			t.Errorf("got %q, want %q", mt, "EUOrder")
		}
	})
}
