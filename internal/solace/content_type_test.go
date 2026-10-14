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

func TestSniffJSON(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    string // "" means not sniffed as JSON
	}{
		{"object", `{"a": 1}`, `{"a":1}`},
		{"array", `[1, 2]`, `[1,2]`},
		{"leading whitespace", " \n\t{\"a\":1}\n", `{"a":1}`},
		{"multi-line", "{\n  \"a\": [\n    1\n  ]\n}", `{"a":[1]}`},

		{"empty", ``, ""},
		{"whitespace only", "  \n", ""},
		{"number scalar", `42`, ""},
		{"string scalar", `"x"`, ""},
		{"bool scalar", `true`, ""},
		{"truncated object", `{"a":`, ""},
		{"brace then garbage", `{abc`, ""},
		{"plain text", `hello`, ""},
		{"protobuf", "\x0a\x03foo\x10\x2a", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := string(sniffJSON([]byte(c.payload))); got != c.want {
				t.Errorf("sniffJSON(%q) = %q, want %q", c.payload, got, c.want)
			}
		})
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
