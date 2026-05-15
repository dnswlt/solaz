package solace

import (
	"fmt"
	"strings"
)

// matchTopicPattern reports whether topic matches a Solace topic
// subscription pattern. `*` matches exactly one level; `>` matches one or
// more trailing levels (only valid at the end of the pattern).
func matchTopicPattern(topic, pattern string) bool {
	pl := strings.Split(pattern, "/")
	tl := strings.Split(topic, "/")
	for i, p := range pl {
		if p == ">" {
			if i != len(pl)-1 {
				return false
			}
			return len(tl) > i
		}
		if i >= len(tl) {
			return false
		}
		if p == "*" {
			continue
		}
		if p != tl[i] {
			return false
		}
	}
	return len(tl) == len(pl)
}

// lookupTopicType returns the message type whose pattern best matches the
// given topic. Specificity is ranked by (literal segments, total segments);
// an unresolvable tie returns an error so the user can disambiguate. No
// match returns an empty string and a nil error.
func lookupTopicType(topic string, mappings map[string]string) (string, error) {
	type match struct {
		pattern  string
		msgType  string
		literals int
		segments int
	}
	var best []match
	for pat, mt := range mappings {
		if !matchTopicPattern(topic, pat) {
			continue
		}
		segs := strings.Split(pat, "/")
		literals := 0
		for _, s := range segs {
			if s != "*" && s != ">" {
				literals++
			}
		}
		m := match{pat, mt, literals, len(segs)}
		if len(best) == 0 {
			best = []match{m}
			continue
		}
		cmp := func(a, b match) int {
			if a.literals != b.literals {
				return a.literals - b.literals
			}
			return a.segments - b.segments
		}
		switch c := cmp(m, best[0]); {
		case c > 0:
			best = []match{m}
		case c == 0:
			best = append(best, m)
		}
	}
	switch len(best) {
	case 0:
		return "", nil
	case 1:
		return best[0].msgType, nil
	default:
		return "", fmt.Errorf("topic %q matches multiple equally-specific topic_types patterns: %q and %q", topic, best[0].pattern, best[1].pattern)
	}
}
