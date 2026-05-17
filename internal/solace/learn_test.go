package solace

import (
	"reflect"
	"sort"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
)

func newLearnHandler() *learnHandler {
	return &learnHandler{results: make(map[string]*learnState)}
}

func TestLearnHandler_ObserveSample(t *testing.T) {
	t.Run("first sample seeds candidates", func(t *testing.T) {
		h := newLearnHandler()
		h.observeSample("t/1", []string{"A", "B"})
		if got, want := h.results["t/1"].candidates, []string{"A", "B"}; !reflect.DeepEqual(got, want) {
			t.Errorf("candidates = %v, want %v", got, want)
		}
		if got := h.results["t/1"].samples; got != 1 {
			t.Errorf("samples = %d, want 1", got)
		}
	})

	t.Run("intersection narrows to one", func(t *testing.T) {
		h := newLearnHandler()
		h.observeSample("t/1", []string{"A", "B"})
		h.observeSample("t/1", []string{"B", "C"})
		if got, want := h.results["t/1"].candidates, []string{"B"}; !reflect.DeepEqual(got, want) {
			t.Errorf("candidates = %v, want %v", got, want)
		}
	})

	t.Run("empty intersection drops topic", func(t *testing.T) {
		h := newLearnHandler()
		h.observeSample("t/1", []string{"A"})
		h.observeSample("t/1", []string{"B"})
		if !h.results["t/1"].dropped {
			t.Errorf("expected dropped=true after conflicting samples")
		}
	})

	t.Run("dropped topic does not recover", func(t *testing.T) {
		h := newLearnHandler()
		h.observeSample("t/1", []string{"A"})
		h.observeSample("t/1", []string{"B"})
		// Subsequent matching sample must not un-drop.
		h.observeSample("t/1", []string{"A"})
		if !h.results["t/1"].dropped {
			t.Errorf("dropped topic should stay dropped")
		}
	})

	t.Run("empty candidates count as sample but do not narrow", func(t *testing.T) {
		h := newLearnHandler()
		h.observeSample("t/1", []string{"A", "B"})
		h.observeSample("t/1", nil)
		if got, want := h.results["t/1"].candidates, []string{"A", "B"}; !reflect.DeepEqual(got, want) {
			t.Errorf("candidates = %v, want %v (empty sample must not narrow)", got, want)
		}
		if got := h.results["t/1"].samples; got != 2 {
			t.Errorf("samples = %d, want 2", got)
		}
	})

	t.Run("multiple topics are independent", func(t *testing.T) {
		h := newLearnHandler()
		h.observeSample("t/1", []string{"A"})
		h.observeSample("t/2", []string{"B"})
		if got := h.results["t/1"].candidates[0]; got != "A" {
			t.Errorf("t/1 candidate = %q, want A", got)
		}
		if got := h.results["t/2"].candidates[0]; got != "B" {
			t.Errorf("t/2 candidate = %q, want B", got)
		}
	})
}

func TestLearnHandler_Summary(t *testing.T) {
	h := newLearnHandler()
	h.observeSample("resolved/a", []string{"X"})
	h.observeSample("resolved/b", []string{"Y", "Z"})
	h.observeSample("resolved/b", []string{"Y"}) // narrows to Y
	h.observeSample("ambiguous/c", []string{"P", "Q"})
	h.observeSample("ambiguous/c", []string{"P", "Q"}) // stays ambiguous
	h.observeSample("dropped/d", []string{"M"})
	h.observeSample("dropped/d", []string{"N"}) // disjoint -> drop

	resolved, unresolved, dropped := h.summary()

	wantResolved := map[string]string{
		"resolved/a": "X",
		"resolved/b": "Y",
	}
	if !reflect.DeepEqual(resolved, wantResolved) {
		t.Errorf("resolved = %v, want %v", resolved, wantResolved)
	}
	if !reflect.DeepEqual(unresolved, []string{"ambiguous/c"}) {
		t.Errorf("unresolved = %v, want [ambiguous/c]", unresolved)
	}
	if !reflect.DeepEqual(dropped, []string{"dropped/d"}) {
		t.Errorf("dropped = %v, want [dropped/d]", dropped)
	}
}

// TestLearnHandler_InferIntegration runs InferMessageType against a real
// compiled proto payload and confirms it flows through the learn handler
// to a uniquely resolved topic. Uses the same `solaztest.Large` schema as
// the score tests so we don't pull in generated stubs.
func TestLearnHandler_InferIntegration(t *testing.T) {
	reg := newScoreTestRegistry(t)

	largeDesc, err := reg.FindMessage("solaztest.Large")
	if err != nil {
		t.Fatal(err)
	}
	src := dynamicpb.NewMessage(largeDesc)
	if err := protojson.Unmarshal([]byte(`{"id":"x","note":"n"}`), src); err != nil {
		t.Fatalf("protojson.Unmarshal: %v", err)
	}
	payload, err := proto.Marshal(src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	candidates, err := reg.InferMessageType(payload)
	if err != nil {
		t.Fatalf("InferMessageType: %v", err)
	}

	h := &learnHandler{registry: reg, results: make(map[string]*learnState)}
	h.observeSample("trades/large", candidates)

	resolved, unresolved, dropped := h.summary()
	if got := resolved["trades/large"]; got != "solaztest.Large" {
		t.Errorf("resolved[trades/large] = %q, want solaztest.Large (unresolved=%v dropped=%v)",
			got, unresolved, dropped)
	}
}

func TestIntersectSorted(t *testing.T) {
	cases := []struct {
		name    string
		a, b    []string
		want    []string
		ordered bool // true: must match exactly; false: set-equal is enough
	}{
		{"overlap preserves order from a", []string{"A", "B", "C"}, []string{"B", "C"}, []string{"B", "C"}, true},
		{"disjoint", []string{"A"}, []string{"B"}, nil, false},
		{"empty b", []string{"A", "B"}, nil, nil, false},
		{"empty a", nil, []string{"A"}, nil, false},
		{"order-from-a respected even if b reorders", []string{"X", "Y", "Z"}, []string{"Y", "X"}, []string{"X", "Y"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := intersectSorted(tc.a, tc.b)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if tc.ordered {
				if !reflect.DeepEqual(got, tc.want) {
					t.Errorf("intersectSorted = %v, want %v", got, tc.want)
				}
				return
			}
			gotSorted := append([]string(nil), got...)
			sort.Strings(gotSorted)
			wantSorted := append([]string(nil), tc.want...)
			sort.Strings(wantSorted)
			if !reflect.DeepEqual(gotSorted, wantSorted) {
				t.Errorf("intersectSorted = %v, want (any order) %v", got, tc.want)
			}
		})
	}
}
