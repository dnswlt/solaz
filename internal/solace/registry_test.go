package solace

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
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

const scoreTestProtos = `syntax = "proto3";
package solaztest;

enum Status {
  STATUS_UNKNOWN = 0;
  STATUS_ACTIVE = 1;
  STATUS_INACTIVE = 2;
}

message Inner {
  string label = 11;
  int32 value = 12;
}

message Empty {}

message Parent {
  string name = 21;
  Inner inner = 22;
  Empty marker = 23;
  repeated Inner items = 24;
  Status status = 25;
}

message Small {
  string id = 1;
}

message Large {
  string id = 1;
  string note = 2;
}
`

func newScoreTestRegistry(t *testing.T) *ProtoRegistry {
	t.Helper()
	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "score.proto"), []byte(scoreTestProtos), 0644); err != nil {
		t.Fatalf("write proto: %v", err)
	}
	reg, err := NewProtoRegistry([]string{tmpDir})
	if err != nil {
		t.Fatalf("NewProtoRegistry: %v", err)
	}
	return reg
}

func TestScoreMessage(t *testing.T) {
	reg := newScoreTestRegistry(t)

	cases := []struct {
		name    string
		msgType string
		json    string
		want    int
	}{
		{
			name:    "empty message scores 0",
			msgType: "solaztest.Inner",
			json:    `{}`,
			want:    0,
		},
		{
			name:    "single string field",
			msgType: "solaztest.Inner",
			json:    `{"label": "hi"}`,
			want:    1,
		},
		{
			name:    "two scalar fields",
			msgType: "solaztest.Inner",
			json:    `{"label": "hi", "value": 5}`,
			want:    2,
		},
		{
			// Regression: a submessage with no populated fields (here, the
			// declared-empty Empty type) must not disqualify the parent.
			// Originally surfaced with a default google.protobuf.Timestamp
			// on the wire; the bug applies to any all-default submessage.
			name:    "empty submessage does not disqualify parent",
			msgType: "solaztest.Parent",
			json:    `{"name": "n", "marker": {}}`,
			want:    2,
		},
		{
			// Same scenario but the empty sub is Inner — a type that *does*
			// declare fields, just none populated here.
			name:    "empty Inner submessage does not disqualify parent",
			msgType: "solaztest.Parent",
			json:    `{"inner": {}}`,
			want:    1,
		},
		{
			// name +1; inner +1 base; scoreMessage(inner) = 2; *2 multiplier = 4.
			name:    "nested populated submessage with multiplier",
			msgType: "solaztest.Parent",
			json:    `{"name": "n", "inner": {"label": "x", "value": 1}}`,
			want:    6,
		},
		{
			// items +1 base; each element subscore = 1, *2 multiplier each.
			name:    "repeated populated submessages",
			msgType: "solaztest.Parent",
			json:    `{"items": [{"label": "a"}, {"label": "b"}]}`,
			want:    5,
		},
		{
			// status +1 base; +2 enum bonus for a known value.
			name:    "known enum value adds bonus",
			msgType: "solaztest.Parent",
			json:    `{"status": "STATUS_ACTIVE"}`,
			want:    3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			md, err := reg.FindMessage(tc.msgType)
			if err != nil {
				t.Fatalf("FindMessage(%s): %v", tc.msgType, err)
			}
			msg := dynamicpb.NewMessage(md)
			if err := protojson.Unmarshal([]byte(tc.json), msg); err != nil {
				t.Fatalf("protojson.Unmarshal: %v", err)
			}
			if got := scoreMessage(msg); got != tc.want {
				t.Errorf("scoreMessage = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestScoreMessage_AmbiguousAcrossSchemas covers the case where a smaller
// schema and a larger one both match the same payload perfectly. Bytes
// encoding Small{id: "x"} are also a valid wire format for Large{id: "x"}
// (with note unset) — neither side has unknown fields, both have one
// populated field. The heuristic cannot distinguish them and must score
// them equally; that equality is what propagates as "ambiguous" downstream.
func TestScoreMessage_AmbiguousAcrossSchemas(t *testing.T) {
	reg := newScoreTestRegistry(t)

	smallDesc, err := reg.FindMessage("solaztest.Small")
	if err != nil {
		t.Fatal(err)
	}
	src := dynamicpb.NewMessage(smallDesc)
	if err := protojson.Unmarshal([]byte(`{"id": "x"}`), src); err != nil {
		t.Fatalf("protojson.Unmarshal: %v", err)
	}
	payload, err := proto.Marshal(src)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	smallMsg := dynamicpb.NewMessage(smallDesc)
	if err := proto.Unmarshal(payload, smallMsg); err != nil {
		t.Fatalf("proto.Unmarshal Small: %v", err)
	}

	largeDesc, err := reg.FindMessage("solaztest.Large")
	if err != nil {
		t.Fatal(err)
	}
	largeMsg := dynamicpb.NewMessage(largeDesc)
	if err := proto.Unmarshal(payload, largeMsg); err != nil {
		t.Fatalf("proto.Unmarshal Large: %v", err)
	}

	smallScore := scoreMessage(smallMsg)
	largeScore := scoreMessage(largeMsg)

	if smallScore <= 0 || largeScore <= 0 {
		t.Fatalf("expected both scores positive; got Small=%d Large=%d", smallScore, largeScore)
	}
	if smallScore != largeScore {
		t.Errorf("expected equal scores for ambiguous match; got Small=%d Large=%d", smallScore, largeScore)
	}
}

// TestScoreMessage_UnknownFieldsDisqualify covers the mirror case: bytes
// encoding Large{id, note} parsed as Small leave tag 2 as an unknown field,
// which disqualifies Small entirely.
func TestScoreMessage_UnknownFieldsDisqualify(t *testing.T) {
	reg := newScoreTestRegistry(t)

	largeDesc, err := reg.FindMessage("solaztest.Large")
	if err != nil {
		t.Fatal(err)
	}
	src := dynamicpb.NewMessage(largeDesc)
	if err := protojson.Unmarshal([]byte(`{"id": "x", "note": "n"}`), src); err != nil {
		t.Fatalf("protojson.Unmarshal: %v", err)
	}
	payload, err := proto.Marshal(src)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	smallDesc, err := reg.FindMessage("solaztest.Small")
	if err != nil {
		t.Fatal(err)
	}
	smallMsg := dynamicpb.NewMessage(smallDesc)
	if err := proto.Unmarshal(payload, smallMsg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}

	if got := scoreMessage(smallMsg); got != -1 {
		t.Errorf("expected -1 for message with unknown fields, got %d", got)
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
