package solace

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dnswlt/solaz/internal/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"

	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// ReceiveOptions bundles the parameters for receiving a message.
type ReceiveOptions struct {
	Topics      []string
	Timeout     time.Duration
	MaxRuntime  time.Duration
	Registry    *ProtoRegistry
	MessageType string
	TopicTypes  map[string]string
	Mode        string
	Count       int
	Raw         bool // payload mode: emit raw bytes, skip content-type dispatch
	Envelope    bool // payload mode: emit {headers, payload, ...} JSON envelope
}

// Run connects to the messaging service, subscribes to the configured topics,
// and dispatches every received message to a mode-specific handler.
func Run(svc solace.MessagingService, opts ReceiveOptions) error {
	handler, err := newHandler(opts)
	if err != nil {
		return err
	}

	if err := svc.Connect(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	trace.Debugf("connected")
	defer func() {
		if err := svc.Disconnect(); err != nil {
			trace.Warningf("disconnect: %v", err)
			return
		}
		trace.Debugf("disconnected")
	}()

	subs := make([]resource.Subscription, len(opts.Topics))
	for i, t := range opts.Topics {
		subs[i] = resource.TopicSubscriptionOf(t)
	}

	receiver, err := svc.CreateDirectMessageReceiverBuilder().
		WithSubscriptions(subs...).
		Build()
	if err != nil {
		return fmt.Errorf("build receiver: %w", err)
	}
	if err := receiver.Start(); err != nil {
		return fmt.Errorf("start receiver: %w", err)
	}
	trace.Debugf("subscribed to %s; awaiting up to %d messages (per-msg timeout=%s, max-runtime=%s)",
		strings.Join(opts.Topics, ","), opts.Count, opts.Timeout, opts.MaxRuntime)
	defer func() {
		err := receiver.Terminate(1 * time.Second)
		var incomplete *solace.IncompleteMessageDeliveryError
		if err != nil && !errors.As(err, &incomplete) {
			trace.Warningf("terminate receiver: %v", err)
		}
	}()

	var deadline time.Time
	if opts.MaxRuntime > 0 {
		deadline = time.Now().Add(opts.MaxRuntime)
	}

	var received, warnings int
	for i := 0; i < opts.Count; i++ {
		timeout := opts.Timeout
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			if remaining < timeout {
				timeout = remaining
			}
		}
		msg, err := receiver.ReceiveMessage(timeout)
		if err != nil {
			var timeoutErr *solace.TimeoutError
			if errors.As(err, &timeoutErr) && !deadline.IsZero() && !time.Now().Before(deadline) {
				break
			}
			return fmt.Errorf("receive: %w", err)
		}
		received++
		if err := handler.handle(msg); err != nil {
			trace.Warningf("%s: %v", msg.GetDestinationName(), err)
			warnings++
			continue
		}
	}
	trace.Debugf("done: received=%d warnings=%d", received, warnings)

	return handler.finish()
}

// messageHandler processes inbound messages one at a time and may emit a
// summary once the receive loop completes.
type messageHandler interface {
	handle(msg message.InboundMessage) error
	finish() error
}

func newHandler(opts ReceiveOptions) (messageHandler, error) {
	switch opts.Mode {
	case "headers":
		return &headersHandler{}, nil
	case "payload":
		return &payloadHandler{
			registry:    opts.Registry,
			messageType: opts.MessageType,
			topicIndex:  newTopicTypeIndex(opts.TopicTypes),
			raw:         opts.Raw,
			envelope:    opts.Envelope,
		}, nil
	case "stats":
		return &statsHandler{topics: make(map[string]*topicStats)}, nil
	default:
		return nil, fmt.Errorf("unknown mode: %q", opts.Mode)
	}
}

type headersHandler struct{}

func (h *headersHandler) handle(msg message.InboundMessage) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Destination:\t%s\n", msg.GetDestinationName())
	if v, ok := msg.GetApplicationMessageID(); ok {
		fmt.Fprintf(w, "AppMessageID:\t%s\n", v)
	}
	if v, ok := msg.GetApplicationMessageType(); ok {
		fmt.Fprintf(w, "AppMessageType:\t%s\n", v)
	}
	if v, ok := msg.GetHTTPContentType(); ok {
		fmt.Fprintf(w, "HTTPContentType:\t%s\n", v)
	}
	if v, ok := msg.GetHTTPContentEncoding(); ok {
		fmt.Fprintf(w, "HTTPContentEncoding:\t%s\n", v)
	}
	if v, ok := msg.GetCorrelationID(); ok {
		fmt.Fprintf(w, "CorrelationID:\t%s\n", v)
	}
	if v, ok := msg.GetSenderID(); ok {
		fmt.Fprintf(w, "SenderID:\t%s\n", v)
	}
	if v, ok := msg.GetSenderTimestamp(); ok {
		fmt.Fprintf(w, "SenderTimestamp:\t%s\n", v.Format(time.RFC3339Nano))
	}
	if v, ok := msg.GetTimeStamp(); ok {
		fmt.Fprintf(w, "ReceiveTimestamp:\t%s\n", v.Format(time.RFC3339Nano))
	}
	if v, ok := msg.GetSequenceNumber(); ok {
		fmt.Fprintf(w, "SequenceNumber:\t%d\n", v)
	}
	if exp := msg.GetExpiration(); !exp.IsZero() && exp.Unix() != 0 {
		fmt.Fprintf(w, "Expiration:\t%s\n", exp.Format(time.RFC3339Nano))
	}
	if v, ok := msg.GetPriority(); ok {
		fmt.Fprintf(w, "Priority:\t%d\n", v)
	}
	fmt.Fprintf(w, "ClassOfService:\t%d\n", msg.GetClassOfService())
	payload, _ := msg.GetPayloadAsBytes()
	fmt.Fprintf(w, "PayloadBytes:\t%d\n", len(payload))
	if err := w.Flush(); err != nil {
		return err
	}

	if props := msg.GetProperties(); len(props) > 0 {
		fmt.Println("Properties:")
		keys := make([]string, 0, len(props))
		for k := range props {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		pw := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
		for _, k := range keys {
			fmt.Fprintf(pw, "  %s\t=\t%v\n", k, props[k])
		}
		if err := pw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (h *headersHandler) finish() error { return nil }

type payloadHandler struct {
	registry    *ProtoRegistry
	messageType string
	topicIndex  *topicTypeIndex
	raw         bool
	envelope    bool
}

// envelope is the JSON record emitted in --envelope mode. The Payload
// field is either a json.RawMessage (when the payload was successfully
// decoded as JSON or protobuf) or a base64-encoded string (raw bytes or
// failed decode). PayloadEncoding tells the consumer which it got.
type envelope struct {
	Headers         map[string]any `json:"headers"`
	Payload         any            `json:"payload"`
	PayloadEncoding string         `json:"payloadEncoding"`
	PayloadError    string         `json:"payloadError,omitempty"`
}

func (h *payloadHandler) handle(msg message.InboundMessage) error {
	payload, _ := msg.GetPayloadAsBytes()
	ct, _ := msg.GetHTTPContentType()
	value, decodeErr := h.decode(msg, payload, ct)

	if h.envelope {
		return h.emitEnvelope(msg, payload, value, decodeErr)
	}

	// Non-envelope: an attempted-but-failed decode is a per-message warning.
	if decodeErr != nil {
		return decodeErr
	}
	if value != nil {
		fmt.Println(string(value))
		return nil
	}
	trace.Debugf("payload: raw output (%d bytes, content-type=%q)", len(payload), ct)
	_, err := os.Stdout.Write(payload)
	return err
}

// decode runs the content-type / hint dispatch and returns:
//   - (jsonValue, nil) when the payload was successfully decoded to JSON;
//   - (nil, err)       when a decode was attempted and failed;
//   - (nil, nil)       when no decode was attempted (--raw or unknown
//     format with no proto hint).
func (h *payloadHandler) decode(msg message.InboundMessage, payload []byte, ct string) (json.RawMessage, error) {
	if h.raw {
		// --raw is an explicit override — bypass content-type dispatch.
		return nil, nil
	}
	if h.messageType != "" {
		// --type is an explicit override — always decode as protobuf.
		return h.decodeProto(msg, payload, h.messageType)
	}
	if isJSONContentType(ct) {
		return compactJSON(payload, ct)
	}
	msgType, hasHint, err := h.resolveProtoHint(msg)
	if err != nil {
		return nil, err
	}
	if hasHint || isProtobufContentType(ct) {
		return h.decodeProto(msg, payload, msgType)
	}
	return nil, nil
}

// resolveProtoHint reports whether the message carries any signal that
// it should be decoded as protobuf even without an explicit content
// type: a matching topic_types pattern (or an ambiguity error from
// one), or an application_message_type header. The resolved type — if
// known — is returned alongside, so callers don't need a second Lookup
// on the dispatch path. An ambiguity error from topic_types is reported
// as hasHint=true with a non-nil err so it surfaces instead of silently
// degrading to raw output.
func (h *payloadHandler) resolveProtoHint(msg message.InboundMessage) (msgType string, hasHint bool, err error) {
	if h.topicIndex != nil {
		mt, lookupErr := h.topicIndex.Lookup(msg.GetDestinationName())
		if lookupErr != nil {
			return "", true, lookupErr
		}
		if mt != "" {
			return mt, true, nil
		}
	}
	if v, ok := msg.GetApplicationMessageType(); ok {
		return v, true, nil
	}
	return "", false, nil
}

// compactJSON returns payload compacted onto a single line. ct is used
// only to enrich the error message on invalid JSON.
func compactJSON(payload []byte, ct string) (json.RawMessage, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, payload); err != nil {
		return nil, fmt.Errorf("invalid JSON payload (content-type %s): %w", ct, err)
	}
	return buf.Bytes(), nil
}

// decodeProto unmarshals payload using the named protobuf type and
// returns the result as single-line JSON. msgType must already be
// resolved by the caller (`--type`, content-type dispatch, or
// resolveProtoHint); an empty msgType yields a clear "unknown type"
// error so every caller routes through this single function.
func (h *payloadHandler) decodeProto(msg message.InboundMessage, payload []byte, msgType string) (json.RawMessage, error) {
	if h.registry == nil {
		return nil, fmt.Errorf("no proto registry configured (set proto_paths in profile)")
	}
	if msgType == "" {
		return nil, fmt.Errorf("message type is unknown for topic %q (use --type or configure topic_types in the profile)", msg.GetDestinationName())
	}

	if idx := strings.LastIndex(msgType, "/"); idx >= 0 {
		msgType = msgType[idx+1:]
	}

	desc, err := h.registry.FindMessage(msgType)
	if err != nil {
		return nil, fmt.Errorf("message descriptor not found for %s: %w", msgType, err)
	}

	dynMsg := dynamicpb.NewMessage(desc)
	if err := proto.Unmarshal(payload, dynMsg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	return protojson.MarshalOptions{
		Multiline: false,
		Resolver:  dynamicpb.NewTypes(h.registry.Files),
	}.Marshal(dynMsg)
}

// emitEnvelope assembles and prints the {headers, payload, ...} record.
// A successful decode embeds the JSON value verbatim; otherwise the
// payload is base64-encoded and the failure (if any) is captured in
// payloadError so every message produces exactly one output record.
func (h *payloadHandler) emitEnvelope(msg message.InboundMessage, payload []byte, value json.RawMessage, decodeErr error) error {
	env := envelope{
		Headers: collectPayloadHeaders(msg, payload),
	}
	if value != nil && decodeErr == nil {
		env.Payload = value
		env.PayloadEncoding = "json"
	} else {
		env.Payload = base64.StdEncoding.EncodeToString(payload)
		env.PayloadEncoding = "base64"
		if decodeErr != nil {
			env.PayloadError = decodeErr.Error()
		}
	}
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

// collectPayloadHeaders extracts every set message header into a map for
// JSON serialization. Field names match the tabular `headers` mode for
// consistency; JSON key order is alphabetical (encoding/json sorts).
func collectPayloadHeaders(msg message.InboundMessage, payload []byte) map[string]any {
	h := map[string]any{
		"Destination":    msg.GetDestinationName(),
		"ClassOfService": msg.GetClassOfService(),
		"PayloadBytes":   len(payload),
	}
	if v, ok := msg.GetApplicationMessageID(); ok {
		h["AppMessageID"] = v
	}
	if v, ok := msg.GetApplicationMessageType(); ok {
		h["AppMessageType"] = v
	}
	if v, ok := msg.GetHTTPContentType(); ok {
		h["HTTPContentType"] = v
	}
	if v, ok := msg.GetHTTPContentEncoding(); ok {
		h["HTTPContentEncoding"] = v
	}
	if v, ok := msg.GetCorrelationID(); ok {
		h["CorrelationID"] = v
	}
	if v, ok := msg.GetSenderID(); ok {
		h["SenderID"] = v
	}
	if v, ok := msg.GetSenderTimestamp(); ok {
		h["SenderTimestamp"] = v.Format(time.RFC3339Nano)
	}
	if v, ok := msg.GetTimeStamp(); ok {
		h["ReceiveTimestamp"] = v.Format(time.RFC3339Nano)
	}
	if v, ok := msg.GetSequenceNumber(); ok {
		h["SequenceNumber"] = v
	}
	if exp := msg.GetExpiration(); !exp.IsZero() && exp.Unix() != 0 {
		h["Expiration"] = exp.Format(time.RFC3339Nano)
	}
	if v, ok := msg.GetPriority(); ok {
		h["Priority"] = v
	}
	if props := msg.GetProperties(); len(props) > 0 {
		h["Properties"] = props
	}
	return h
}

func (h *payloadHandler) finish() error { return nil }

// isJSONContentType reports whether ct names a JSON media type, including
// the RFC 6839 `+json` structured suffix (e.g. `application/vnd.foo+json`).
// Media-type parameters such as charset are ignored.
func isJSONContentType(ct string) bool {
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	mediaType = strings.ToLower(mediaType)
	return mediaType == "application/json" ||
		mediaType == "text/json" ||
		strings.HasSuffix(mediaType, "+json")
}

// isProtobufContentType reports whether ct names one of the common
// protobuf media types. `application/octet-stream` is deliberately
// excluded — it's too generic to imply protobuf.
func isProtobufContentType(ct string) bool {
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	switch strings.ToLower(mediaType) {
	case "application/x-protobuf",
		"application/vnd.google.protobuf",
		"application/protobuf":
		return true
	}
	return false
}

// topicStats stores aggregated statistics for a single topic.
type topicStats struct {
	Count     int
	TotalSize int64
}

type statsHandler struct {
	topics map[string]*topicStats
}

func (h *statsHandler) handle(msg message.InboundMessage) error {
	topic := msg.GetDestinationName()
	payload, _ := msg.GetPayloadAsBytes()
	s := h.topics[topic]
	if s == nil {
		s = &topicStats{}
		h.topics[topic] = s
	}
	s.Count++
	s.TotalSize += int64(len(payload))
	return nil
}

func (h *statsHandler) finish() error {
	if len(h.topics) == 0 {
		return nil
	}

	names := make([]string, 0, len(h.topics))
	for t := range h.topics {
		names = append(names, t)
	}
	sort.Slice(names, func(i, j int) bool {
		ci, cj := h.topics[names[i]].Count, h.topics[names[j]].Count
		if ci != cj {
			return ci > cj
		}
		return names[i] < names[j]
	})

	var totalCount int
	var totalBytes int64
	for _, s := range h.topics {
		totalCount += s.Count
		totalBytes += s.TotalSize
	}

	fmt.Println()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TOPIC\tCOUNT\tBYTES\tAVG BYTES")
	for _, t := range names {
		s := h.topics[t]
		avg := float64(s.TotalSize) / float64(s.Count)
		fmt.Fprintf(w, "%s\t%d\t%s\t%s\n", t, s.Count, formatBytes(float64(s.TotalSize)), formatBytes(avg))
	}
	avg := float64(totalBytes) / float64(totalCount)
	fmt.Fprintf(w, "TOTAL\t%d\t%s\t%s\n", totalCount, formatBytes(float64(totalBytes)), formatBytes(avg))
	return w.Flush()
}

func formatBytes(b float64) string {
	if b < 1000000 {
		return strings.TrimSuffix(fmt.Sprintf("%.1f", b), ".0")
	}
	const unit = 1024
	if b < unit*unit*unit {
		return fmt.Sprintf("%.1fM", b/(unit*unit))
	}
	return fmt.Sprintf("%.1fG", b/(unit*unit*unit))
}
