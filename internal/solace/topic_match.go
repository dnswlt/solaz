package solace

import (
	"fmt"
	"sort"
	"strings"
)

// matchTopicLevels is matchTopicPattern over pre-split slices, so callers
// that match the same topic against many patterns can split each only once.
func matchTopicLevels(tl, pl []string) bool {
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
		if strings.HasSuffix(p, "*") {
			if !strings.HasPrefix(tl[i], p[:len(p)-1]) {
				return false
			}
			continue
		}
		if p != tl[i] {
			return false
		}
	}
	return len(tl) == len(pl)
}

// topicTypeIndex precomputes split patterns and specificity scores from a
// topic_types map, sorted from most to least specific. Lookup against an
// incoming topic is then a single pass over the slice with no per-message
// allocation beyond splitting the topic itself.
type topicTypeIndex struct {
	entries []topicTypeEntry
}

type topicTypeEntry struct {
	pattern     string
	msgType     string
	segments    []string
	specificity int
}

// newTopicTypeIndex preprocesses the given mappings. Returns nil for an
// empty map so Lookup on a no-mapping profile is a cheap nil-check.
func newTopicTypeIndex(mappings map[string]string) *topicTypeIndex {
	if len(mappings) == 0 {
		return nil
	}
	entries := make([]topicTypeEntry, 0, len(mappings))
	for pat, mt := range mappings {
		segs := strings.Split(pat, "/")
		spec := 0
		for _, s := range segs {
			switch {
			case s == "*" || s == ">":
				// adds 0
			case strings.HasSuffix(s, "*"):
				spec++
			default:
				spec += 2
			}
		}
		entries = append(entries, topicTypeEntry{
			pattern:     pat,
			msgType:     mt,
			segments:    segs,
			specificity: spec,
		})
	}
	// Most specific first; segment count as tiebreaker. Equal-ranked
	// entries stay adjacent so Lookup can detect ambiguity by scanning
	// the head of the slice.
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].specificity != entries[j].specificity {
			return entries[i].specificity > entries[j].specificity
		}
		return len(entries[i].segments) > len(entries[j].segments)
	})
	return &topicTypeIndex{entries: entries}
}

// Lookup returns the message type whose pattern best matches the topic.
// No match returns ("", nil). A tie between equally-specific patterns
// returns an error so the user can disambiguate.
func (idx *topicTypeIndex) Lookup(topic string) (string, error) {
	if idx == nil {
		return "", nil
	}
	tl := strings.Split(topic, "/")
	var matched *topicTypeEntry
	for i := range idx.entries {
		e := &idx.entries[i]
		if matched != nil &&
			(e.specificity != matched.specificity || len(e.segments) != len(matched.segments)) {
			break
		}
		if !matchTopicLevels(tl, e.segments) {
			continue
		}
		if matched != nil {
			return "", fmt.Errorf("topic %q matches multiple equally-specific topic_types patterns: %q and %q", topic, matched.pattern, e.pattern)
		}
		matched = e
	}
	if matched == nil {
		return "", nil
	}
	return matched.msgType, nil
}
