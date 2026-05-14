package solace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
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

	compiler := protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{
			ImportPaths: paths,
		},
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
