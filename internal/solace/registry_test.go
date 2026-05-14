package solace_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dnswlt/hackz/solaz/internal/solace"
)

func TestNewProtoRegistry(t *testing.T) {
	// Create a temporary directory for proto files
	tmpDir := t.TempDir()

	// 1. Simple Proto
	proto1 := `syntax = "proto3";
package test.simple;
message SimpleMessage {
	string name = 1;
}
`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "test1.proto"), []byte(proto1), 0644))

	// 2. Proto importing standard protobuf WKT
	proto2 := `syntax = "proto3";
package test.wkt;
import "google/protobuf/timestamp.proto";
message WKTMessage {
	google.protobuf.Timestamp time = 1;
}
`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "test2.proto"), []byte(proto2), 0644))

	// 3. Proto importing Google API type (datetime)
	proto3 := `syntax = "proto3";
package test.api;
import "google/type/datetime.proto";
message APIMessage {
	google.type.DateTime dt = 1;
}
`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "test3.proto"), []byte(proto3), 0644))

	// Instantiate the registry
	reg, err := solace.NewProtoRegistry([]string{tmpDir})
	require.NoError(t, err)
	require.NotNil(t, reg)

	// Validate finding messages
	msg1, err := reg.FindMessage("test.simple.SimpleMessage")
	require.NoError(t, err)
	assert.Equal(t, "test.simple.SimpleMessage", string(msg1.FullName()))

	msg2, err := reg.FindMessage("test.wkt.WKTMessage")
	require.NoError(t, err)
	assert.Equal(t, "test.wkt.WKTMessage", string(msg2.FullName()))

	msg3, err := reg.FindMessage("test.api.APIMessage")
	require.NoError(t, err)
	assert.Equal(t, "test.api.APIMessage", string(msg3.FullName()))

	// Check invalid message
	_, err = reg.FindMessage("test.simple.NotFound")
	require.Error(t, err)
}
