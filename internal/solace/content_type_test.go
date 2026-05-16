package solace

import "testing"

func TestIsJSONContentType(t *testing.T) {
	cases := []struct {
		ct   string
		want bool
	}{
		{"application/json", true},
		{"application/json; charset=utf-8", true},
		{"Application/JSON", true},
		{"text/json", true},
		{"application/vnd.foo+json", true},
		{"application/vnd.foo+json; v=1", true},
		{"APPLICATION/VND.FOO+JSON", true},

		{"application/x-protobuf", false},
		{"application/vnd.google.protobuf", false},
		{"application/octet-stream", false},
		{"text/plain", false},
		{"", false},
		{"garbage", false},
		{"application/json+xml", false},
	}
	for _, c := range cases {
		if got := isJSONContentType(c.ct); got != c.want {
			t.Errorf("isJSONContentType(%q) = %v, want %v", c.ct, got, c.want)
		}
	}
}

func TestIsProtobufContentType(t *testing.T) {
	cases := []struct {
		ct   string
		want bool
	}{
		{"application/x-protobuf", true},
		{"application/vnd.google.protobuf", true},
		{"application/protobuf", true},
		{"application/x-protobuf; charset=binary", true},
		{"Application/X-Protobuf", true},

		{"application/json", false},
		{"application/octet-stream", false}, // too generic
		{"text/plain", false},
		{"", false},
		{"garbage", false},
	}
	for _, c := range cases {
		if got := isProtobufContentType(c.ct); got != c.want {
			t.Errorf("isProtobufContentType(%q) = %v, want %v", c.ct, got, c.want)
		}
	}
}
