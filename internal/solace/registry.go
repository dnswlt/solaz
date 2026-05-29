package solace

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/bufbuild/protocompile"
	"github.com/dnswlt/solaz/internal/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	_ "google.golang.org/genproto/googleapis/type/date"
	_ "google.golang.org/genproto/googleapis/type/datetime"
	_ "google.golang.org/genproto/googleapis/type/dayofweek"
	_ "google.golang.org/genproto/googleapis/type/decimal"
	_ "google.golang.org/genproto/googleapis/type/fraction"
	_ "google.golang.org/genproto/googleapis/type/interval"
	_ "google.golang.org/genproto/googleapis/type/latlng"
	_ "google.golang.org/genproto/googleapis/type/money"
	_ "google.golang.org/genproto/googleapis/type/timeofday"
)

// ProtoRegistry holds compiled proto descriptors.
type ProtoRegistry struct {
	Files *protoregistry.Files
}

// NewProtoRegistry compiles all .proto files found in the given paths.
// Missing paths are skipped with a debug log so that a stale directory
// in a profile doesn't break the whole tool. Other errors
// (permission denied, unreadable files, ...) still surface.
//
// When isolated is false, every path joins one shared import-path list
// and one compile pass — files in any root can `import` files from any
// other root. When isolated is true, each path is compiled in its own
// pass with only itself on the import-path list; cross-root imports are
// no longer reachable and must be resolved within their own root (plus
// the standard well-known types and the protobuf global registry
// fallback). All resulting descriptors are merged into one registry, so
// duplicate file paths or duplicate message names across roots still
// fail at registration regardless of mode.
func NewProtoRegistry(paths []string, isolated bool) (*ProtoRegistry, error) {
	existing := make([]string, 0, len(paths))
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				trace.Debugf("proto_paths: skipping %q (not found)", p)
				continue
			}
			return nil, fmt.Errorf("proto_paths %s: %w", p, err)
		}
		existing = append(existing, p)
	}

	reg := &protoregistry.Files{}
	if len(existing) == 0 {
		return &ProtoRegistry{Files: reg}, nil
	}

	if isolated {
		for _, p := range existing {
			if err := compileInto(reg, []string{p}); err != nil {
				return nil, fmt.Errorf("proto_paths %s: %w", p, err)
			}
		}
	} else {
		if err := compileInto(reg, existing); err != nil {
			return nil, err
		}
	}

	return &ProtoRegistry{Files: reg}, nil
}

// compileInto walks every importPath for .proto files (relative to its
// own root), compiles them with importPaths as the resolver's search
// list, and registers the resulting descriptors into reg. Used by both
// the merged and the isolated compilation modes of NewProtoRegistry.
func compileInto(reg *protoregistry.Files, importPaths []string) error {
	var protoFiles []string
	for _, p := range importPaths {
		err := filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && strings.HasSuffix(info.Name(), ".proto") {
				rel, err := filepath.Rel(p, path)
				if err != nil {
					return err
				}
				protoFiles = append(protoFiles, rel)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("walk %s: %w", p, err)
		}
	}

	if len(protoFiles) == 0 {
		return nil
	}

	baseResolver := protocompile.WithStandardImports(&protocompile.SourceResolver{
		ImportPaths: importPaths,
	})

	compiler := protocompile.Compiler{
		Resolver: &globalFallbackResolver{Fallback: baseResolver},
	}

	files, err := compiler.Compile(context.Background(), protoFiles...)
	if err != nil {
		return fmt.Errorf("compile: %w", err)
	}

	for _, f := range files {
		if err := reg.RegisterFile(f); err != nil {
			return fmt.Errorf("register %s: %w", f.Path(), err)
		}
	}
	return nil
}

// MessageNames returns the fully-qualified names of every message type in
// the registry (including nested types), sorted alphabetically.
func (r *ProtoRegistry) MessageNames() []string {
	var names []string
	r.Files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		collectMessageNames(fd.Messages(), &names)
		return true
	})
	sort.Strings(names)
	return names
}

func collectMessageNames(msgs protoreflect.MessageDescriptors, out *[]string) {
	for i := 0; i < msgs.Len(); i++ {
		md := msgs.Get(i)
		// Skip synthetic MapEntry messages: they're an implementation
		// detail of map<K,V> fields, not types users would address by
		// name or want to see in inference results.
		if md.IsMapEntry() {
			continue
		}
		*out = append(*out, string(md.FullName()))
		collectMessageNames(md.Messages(), out)
	}
}

// FindMessage looks up a message descriptor by full name.
func (r *ProtoRegistry) FindMessage(name string) (protoreflect.MessageDescriptor, error) {
	desc, err := r.Files.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, err
	}
	md, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s is not a message", name)
	}
	return md, nil
}

type globalFallbackResolver struct {
	Fallback protocompile.Resolver
}

func (r *globalFallbackResolver) FindFileByPath(path string) (protocompile.SearchResult, error) {
	res, err := r.Fallback.FindFileByPath(path)
	if err == nil {
		return res, nil
	}
	fd, globalErr := protoregistry.GlobalFiles.FindFileByPath(path)
	if globalErr == nil {
		return protocompile.SearchResult{Desc: fd}, nil
	}
	return res, err
}

// InferMessageType attempts to find the most likely message types for a raw payload
// by scoring the unmarshaled structure against all known descriptors.
func (r *ProtoRegistry) InferMessageType(payload []byte) ([]string, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty payload perfectly matches all messages")
	}

	var candidates []string
	maxScore := -1

	names := r.MessageNames()
	for _, name := range names {
		md, err := r.FindMessage(name)
		if err != nil {
			continue
		}

		msg := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(payload, msg); err != nil {
			continue
		}

		score := scoreMessage(msg)
		if score < 0 {
			continue
		}

		if score > maxScore {
			maxScore = score
			candidates = []string{name}
		} else if score == maxScore {
			candidates = append(candidates, name)
		}
	}

	return candidates, nil
}

// scoreMessage calculates a heuristic confidence score for a decoded message.
// It recursively evaluates structural depth and type adherence to differentiate
// between valid payloads and accidental wire-type collisions.
//
// Scoring rules:
//   - Disqualification (-1): Message (or any sub-message) contains unknown
//     fields or invalid UTF-8 sequences in any string field.
//   - Base Score (+1): Awarded for any populated field.
//   - Enum Bonus (+2): Awarded if an integer maps to a known enum value,
//     providing a strong type-safety signal.
//   - Nesting Multiplier (*2): Sub-messages are scored recursively and multiplied
//     to heavily favor deep structural alignment over flat byte arrays.
//
// A sub-message with no populated fields (e.g. an empty google.protobuf.Timestamp
// set on the wire) is not disqualifying — it scores 0 and the parent still gets
// its +1 for the field being present.
//
// Returns a non-negative score, or -1 if the message is disqualified.
func scoreMessage(msg protoreflect.Message) int {
	if len(msg.GetUnknown()) > 0 {
		return -1
	}

	score := 0
	invalid := false

	msg.Range(func(fd protoreflect.FieldDescriptor, val protoreflect.Value) bool {
		score += 1

		switch fd.Kind() {
		case protoreflect.MessageKind:
			// Maps are also reported as MessageKind (their entries are a
			// synthetic MapEntry submessage), but val holds a Map, not a
			// Message — calling val.Message() on it would panic.
			switch {
			case fd.IsMap():
				sub := scoreMapField(fd, val.Map())
				if sub < 0 {
					invalid = true
					return false
				}
				score += sub
			case fd.IsList():
				list := val.List()
				for i := 0; i < list.Len(); i++ {
					subScore := scoreMessage(list.Get(i).Message())
					if subScore < 0 {
						invalid = true
						return false
					}
					score += subScore * 2
				}
			default:
				subScore := scoreMessage(val.Message())
				if subScore < 0 {
					invalid = true
					return false
				}
				score += subScore * 2
			}

		case protoreflect.StringKind:
			if fd.IsList() {
				list := val.List()
				for i := 0; i < list.Len(); i++ {
					if !utf8.ValidString(list.Get(i).String()) {
						invalid = true
						return false
					}
				}
			} else {
				if !utf8.ValidString(val.String()) {
					invalid = true
					return false
				}
			}

		case protoreflect.EnumKind:
			enumVal := fd.Enum().Values().ByNumber(val.Enum())
			if enumVal != nil {
				score += 2
			}
		}
		return true
	})

	if invalid {
		return -1
	}

	return score
}

// scoreMapField returns the score contribution of a populated map field
// (the parent's +1 base for the field itself is added by scoreMessage).
// String keys and string values are checked for valid UTF-8; map values
// of message kind are recursively scored with the same *2 nesting
// multiplier used for repeated messages; known enum values get the usual
// +2 bonus. Returns -1 if any entry disqualifies the parent.
func scoreMapField(fd protoreflect.FieldDescriptor, m protoreflect.Map) int {
	keyFd := fd.MapKey()
	valFd := fd.MapValue()
	keyIsString := keyFd.Kind() == protoreflect.StringKind

	score := 0
	invalid := false
	m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		if keyIsString && !utf8.ValidString(k.String()) {
			invalid = true
			return false
		}
		switch valFd.Kind() {
		case protoreflect.MessageKind:
			sub := scoreMessage(v.Message())
			if sub < 0 {
				invalid = true
				return false
			}
			score += sub * 2
		case protoreflect.StringKind:
			if !utf8.ValidString(v.String()) {
				invalid = true
				return false
			}
		case protoreflect.EnumKind:
			if valFd.Enum().Values().ByNumber(v.Enum()) != nil {
				score += 2
			}
		}
		return true
	})
	if invalid {
		return -1
	}
	return score
}
