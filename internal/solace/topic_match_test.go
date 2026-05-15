package solace

import "testing"

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

func TestLookupTopicType(t *testing.T) {
	t.Run("empty map", func(t *testing.T) {
		mt, err := lookupTopicType("foo/bar", nil)
		if err != nil || mt != "" {
			t.Errorf("got (%q, %v), want (\"\", nil)", mt, err)
		}
	})

	t.Run("no match", func(t *testing.T) {
		m := map[string]string{"a/>": "A"}
		mt, err := lookupTopicType("foo/bar", m)
		if err != nil || mt != "" {
			t.Errorf("got (%q, %v), want (\"\", nil)", mt, err)
		}
	})

	t.Run("single match", func(t *testing.T) {
		m := map[string]string{"trades/>": "Trade"}
		mt, err := lookupTopicType("trades/orders/42", m)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mt != "Trade" {
			t.Errorf("got %q, want %q", mt, "Trade")
		}
	})

	t.Run("more literal segments wins", func(t *testing.T) {
		m := map[string]string{
			"trades/>":         "Trade",
			"trades/orders/*":  "Order",
		}
		mt, err := lookupTopicType("trades/orders/42", m)
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
		mt, err := lookupTopicType("trades/orders/42", m)
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
		mt, err := lookupTopicType("trades/orders", m)
		if err == nil {
			t.Fatalf("expected error, got %q", mt)
		}
	})

	t.Run("most-specific wins over many", func(t *testing.T) {
		m := map[string]string{
			">":                       "Catchall",
			"trades/>":                "Trade",
			"trades/orders/>":         "Order",
			"trades/orders/europe/>":  "EUOrder",
		}
		mt, err := lookupTopicType("trades/orders/europe/42", m)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mt != "EUOrder" {
			t.Errorf("got %q, want %q", mt, "EUOrder")
		}
	})
}
