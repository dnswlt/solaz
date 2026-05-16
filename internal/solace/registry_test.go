package solace

import (
	"os"
	"path/filepath"
	"testing"
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
	if err := os.WriteFile(filepath.Join(tmpDir, "test1.proto"), []byte(proto1), 0644); err != nil {
		t.Fatalf("failed to write test1.proto: %v", err)
	}

	// 2. Proto importing standard protobuf WKT
	proto2 := `syntax = "proto3";
package test.wkt;
import "google/protobuf/timestamp.proto";
message WKTMessage {
	google.protobuf.Timestamp time = 1;
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "test2.proto"), []byte(proto2), 0644); err != nil {
		t.Fatalf("failed to write test2.proto: %v", err)
	}

	// 3. Proto importing Google API type (datetime)
	proto3 := `syntax = "proto3";
package test.api;
import "google/type/datetime.proto";
message APIMessage {
	google.type.DateTime dt = 1;
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "test3.proto"), []byte(proto3), 0644); err != nil {
		t.Fatalf("failed to write test3.proto: %v", err)
	}

	// 4. Proto importing Google API type (interval)
	proto4 := `syntax = "proto3";
package test.api2;
import "google/type/interval.proto";
message IntervalMessage {
	google.type.Interval interval = 1;
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "test4.proto"), []byte(proto4), 0644); err != nil {
		t.Fatalf("failed to write test4.proto: %v", err)
	}

	// Instantiate the registry
	reg, err := NewProtoRegistry([]string{tmpDir})
	if err != nil {
		t.Fatalf("NewProtoRegistry failed: %v", err)
	}
	if reg == nil {
		t.Fatal("expected registry to not be nil")
	}

	// Validate finding messages
	msg1, err := reg.FindMessage("test.simple.SimpleMessage")
	if err != nil {
		t.Fatalf("failed to find msg1: %v", err)
	}
	if string(msg1.FullName()) != "test.simple.SimpleMessage" {
		t.Errorf("expected test.simple.SimpleMessage, got %s", msg1.FullName())
	}

	msg2, err := reg.FindMessage("test.wkt.WKTMessage")
	if err != nil {
		t.Fatalf("failed to find msg2: %v", err)
	}
	if string(msg2.FullName()) != "test.wkt.WKTMessage" {
		t.Errorf("expected test.wkt.WKTMessage, got %s", msg2.FullName())
	}

	msg3, err := reg.FindMessage("test.api.APIMessage")
	if err != nil {
		t.Fatalf("failed to find msg3: %v", err)
	}
	if string(msg3.FullName()) != "test.api.APIMessage" {
		t.Errorf("expected test.api.APIMessage, got %s", msg3.FullName())
	}

	msg4, err := reg.FindMessage("test.api2.IntervalMessage")
	if err != nil {
		t.Fatalf("failed to find msg4: %v", err)
	}
	if string(msg4.FullName()) != "test.api2.IntervalMessage" {
		t.Errorf("expected test.api2.IntervalMessage, got %s", msg4.FullName())
	}

	// Check invalid message
	_, err = reg.FindMessage("test.simple.NotFound")
	if err == nil {
		t.Error("expected error finding invalid message, got nil")
	}
}

func TestNewProtoRegistry_SkipsMissingPaths(t *testing.T) {
	tmpDir := t.TempDir()
	proto := `syntax = "proto3";
package test.skip;
message M { string s = 1; }
`
	if err := os.WriteFile(filepath.Join(tmpDir, "m.proto"), []byte(proto), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	missing := filepath.Join(tmpDir, "does-not-exist")
	reg, err := NewProtoRegistry([]string{missing, tmpDir})
	if err != nil {
		t.Fatalf("NewProtoRegistry should tolerate missing paths, got: %v", err)
	}
	if _, err := reg.FindMessage("test.skip.M"); err != nil {
		t.Errorf("message from existing path not found after skipping missing one: %v", err)
	}

	// All missing → empty registry, no error.
	emptyReg, err := NewProtoRegistry([]string{missing})
	if err != nil {
		t.Fatalf("all-missing paths should not error, got: %v", err)
	}
	if len(emptyReg.MessageNames()) != 0 {
		t.Errorf("expected empty registry, got %v", emptyReg.MessageNames())
	}
}
