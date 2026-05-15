package solace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	_ "google.golang.org/genproto/googleapis/type/date"
	_ "google.golang.org/genproto/googleapis/type/datetime"
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
func NewProtoRegistry(paths []string) (*ProtoRegistry, error) {
	var protoFiles []string
	for _, p := range paths {
		err := filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && strings.HasSuffix(info.Name(), ".proto") {
				// We need the path relative to the import roots for the compiler
				rel, err := filepath.Rel(p, path)
				if err != nil {
					return err
				}
				protoFiles = append(protoFiles, rel)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk %s: %w", p, err)
		}
	}

	if len(protoFiles) == 0 {
		return &ProtoRegistry{Files: &protoregistry.Files{}}, nil
	}

	baseResolver := protocompile.WithStandardImports(&protocompile.SourceResolver{
		ImportPaths: paths,
	})

	compiler := protocompile.Compiler{
		Resolver: &globalFallbackResolver{Fallback: baseResolver},
	}

	files, err := compiler.Compile(context.Background(), protoFiles...)
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}

	reg := &protoregistry.Files{}
	for _, f := range files {
		if err := reg.RegisterFile(f); err != nil {
			return nil, fmt.Errorf("register %s: %w", f.Path(), err)
		}
	}

	return &ProtoRegistry{Files: reg}, nil
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
