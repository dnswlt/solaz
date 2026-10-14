package solace

import (
	"testing"

	"solace.dev/go/messaging/pkg/solace/message"
)

// fakeMessage implements just the InboundMessage methods decode touches;
// calling anything else panics on the nil embedded interface.
type fakeMessage struct {
	message.InboundMessage
	appMessageType string
}

func (m fakeMessage) GetDestinationName() string { return "t/1" }

func (m fakeMessage) GetApplicationMessageType() (string, bool) {
	return m.appMessageType, m.appMessageType != ""
}

func TestPayloadHandlerDecode(t *testing.T) {
	const jsonBody = "{\n  \"a\": 1\n}"
	cases := []struct {
		name      string
		h         payloadHandler
		ct        string
		appType   string
		payload   string
		wantValue string // "" means no value
		wantErr   bool
	}{
		{name: "no content type, JSON object sniffed", payload: jsonBody, wantValue: `{"a":1}`},
		{name: "text/plain, JSON object sniffed", ct: "text/plain", payload: jsonBody, wantValue: `{"a":1}`},
		{name: "sniff beats application_message_type", appType: "com.example.Foo", payload: jsonBody, wantValue: `{"a":1}`},
		{name: "JSON content type", ct: "application/json", payload: jsonBody, wantValue: `{"a":1}`},
		{name: "JSON content type, invalid body", ct: "application/json", payload: `{"a":`, wantErr: true},

		// No registry configured: reaching the proto path yields an error,
		// which proves the sniff was skipped.
		{name: "protobuf content type skips sniff", ct: "application/x-protobuf", payload: jsonBody, wantErr: true},
		{name: "--proto-type skips sniff", h: payloadHandler{protoType: "com.example.Foo"}, payload: jsonBody, wantErr: true},
		{name: "proto hint on non-JSON body", appType: "com.example.Foo", payload: "\x0a\x03foo", wantErr: true},

		{name: "--raw skips sniff", h: payloadHandler{raw: true}, payload: jsonBody},
		{name: "no content type, binary", payload: "\x0a\x03foo"},
		{name: "no content type, JSON scalar", payload: `42`},
		{name: "no content type, invalid JSON", payload: `{abc`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			msg := fakeMessage{appMessageType: c.appType}
			value, _, err := c.h.decode(msg, []byte(c.payload), c.ct)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, c.wantErr)
			}
			if got := string(value); got != c.wantValue {
				t.Errorf("value = %q, want %q", got, c.wantValue)
			}
		})
	}
}
