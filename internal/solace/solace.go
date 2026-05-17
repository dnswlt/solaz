package solace

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"mime"
	"os"
	"slices"
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
	Topics        []string
	Timeout       time.Duration
	MaxRuntime    time.Duration
	Registry      *ProtoRegistry
	MessageType   string
	TopicTypes    map[string]string
	Mode          string
	Count         int
	Raw           bool   // print mode: emit raw bytes, skip content-type dispatch
	Envelope      bool   // print mode: emit {headers, payload, ...} JSON envelope
	InferType     bool   // print mode: heuristically infer proto type when unknown
	SaveTypesPath string // learn-types mode: if non-empty, merge results into this file
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
	case "print":
		return &payloadHandler{
			registry:    opts.Registry,
			messageType: opts.MessageType,
			topicIndex:  newTopicTypeIndex(opts.TopicTypes),
			raw:         opts.Raw,
			envelope:    opts.Envelope,
			inferType:   opts.InferType,
		}, nil
	case "stats":
		return &statsHandler{topics: make(map[string]*topicStats)}, nil
	case "learn-types":
		if opts.Registry == nil {
			return nil, fmt.Errorf("learn-types requires a proto registry")
		}
		return &learnHandler{
			registry:   opts.Registry,
			topicIndex: newTopicTypeIndex(opts.TopicTypes),
			savePath:   opts.SaveTypesPath,
			results:    make(map[string]*learnState),
		}, nil
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
		keys := slices.Collect(maps.Keys(props))
		slices.Sort(keys)
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
	inferType   bool
}

// envelope is the JSON record emitted in --envelope mode. The Payload
// field is either a json.RawMessage (when the payload was successfully
// decoded as JSON or protobuf) or a base64-encoded string (raw bytes or
// failed decode). PayloadEncoding tells the consumer which it got.
// PayloadType is the fully-qualified protobuf message type used for
// decoding; empty for JSON or raw payloads.
type envelope struct {
	Headers         map[string]any `json:"headers"`
	Payload         any            `json:"payload"`
	PayloadEncoding string         `json:"payloadEncoding"`
	PayloadType     string         `json:"payloadType,omitempty"`
	PayloadError    string         `json:"payloadError,omitempty"`
}

func (h *payloadHandler) handle(msg message.InboundMessage) error {
	payload, _ := msg.GetPayloadAsBytes()
	ct, _ := msg.GetHTTPContentType()
	value, protoType, decodeErr := h.decode(msg, payload, ct)

	if h.envelope {
		return h.emitEnvelope(msg, payload, value, protoType, decodeErr)
	}

	// Non-envelope: an attempted-but-failed decode is a per-message warning.
	if decodeErr != nil {
		return decodeErr
	}
	if value != nil {
		fmt.Println(string(value))
		return nil
	}
	trace.Debugf("print: raw output (%d bytes, content-type=%q)", len(payload), ct)
	_, err := os.Stdout.Write(payload)
	return err
}

// decode runs the content-type / hint dispatch and returns:
//   - (jsonValue, protoType, nil) when the payload was successfully decoded.
//     protoType is the fully-qualified proto message type used; empty for
//     JSON payloads.
//   - (nil, protoType, err) when a decode was attempted and failed.
//     protoType is set when a proto type was resolved before the failure.
//   - (nil, "", nil) when no decode was attempted (--raw or unknown
//     format with no proto hint).
func (h *payloadHandler) decode(msg message.InboundMessage, payload []byte, ct string) (json.RawMessage, string, error) {
	if h.raw {
		// --raw is an explicit override — bypass content-type dispatch.
		return nil, "", nil
	}
	if h.messageType != "" {
		// --type is an explicit override — always decode as protobuf.
		return h.decodeProto(msg, payload, h.messageType)
	}
	if isJSONContentType(ct) {
		v, err := compactJSON(payload, ct)
		return v, "", err
	}
	msgType, hasHint, err := h.resolveProtoHint(msg)
	if err != nil {
		return nil, "", err
	}
	if hasHint || isProtobufContentType(ct) || h.inferType {
		return h.decodeProto(msg, payload, msgType)
	}
	return nil, "", nil
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
// returns the result as single-line JSON along with the resolved type
// name. msgType must already be resolved by the caller (`--type`,
// content-type dispatch, or resolveProtoHint); an empty msgType yields
// a clear "unknown type" error so every caller routes through this
// single function. The returned type is set whenever a type was
// resolved — including failure paths after resolution — so callers
// (the envelope writer in particular) can record which type was used.
func (h *payloadHandler) decodeProto(msg message.InboundMessage, payload []byte, msgType string) (json.RawMessage, string, error) {
	if h.registry == nil {
		return nil, "", fmt.Errorf("no proto registry configured (set proto_paths in profile)")
	}
	if msgType == "" {
		if !h.inferType {
			return nil, "", fmt.Errorf("message type is unknown for topic %q (use --type, --infer, or configure topic_types in the profile)", msg.GetDestinationName())
		}
		started := time.Now()
		candidates, err := h.registry.InferMessageType(payload)
		if err != nil {
			return nil, "", fmt.Errorf("infer message: %w", err)
		}
		if len(candidates) == 0 {
			return nil, "", fmt.Errorf("could not infer message type for topic %q", msg.GetDestinationName())
		}
		if len(candidates) > 1 {
			return nil, "", fmt.Errorf("ambiguous message types for topic %q: %s", msg.GetDestinationName(), strings.Join(candidates, ", "))
		}
		msgType = candidates[0]
		trace.Debugf("inferred %s as %s in %d ms", msg.GetDestinationName(), msgType, time.Since(started).Milliseconds())
	}

	if idx := strings.LastIndex(msgType, "/"); idx >= 0 {
		msgType = msgType[idx+1:]
	}

	desc, err := h.registry.FindMessage(msgType)
	if err != nil {
		return nil, msgType, fmt.Errorf("message descriptor not found for %s: %w", msgType, err)
	}

	dynMsg := dynamicpb.NewMessage(desc)
	if err := proto.Unmarshal(payload, dynMsg); err != nil {
		return nil, msgType, fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	data, err := protojson.MarshalOptions{
		Multiline: false,
		Resolver:  dynamicpb.NewTypes(h.registry.Files),
	}.Marshal(dynMsg)
	return data, msgType, err
}

// emitEnvelope assembles and prints the {headers, payload, ...} record.
// A successful decode embeds the JSON value verbatim; otherwise the
// payload is base64-encoded and the failure (if any) is captured in
// payloadError so every message produces exactly one output record.
// protoType is the proto message type used for decoding (empty for JSON
// or raw payloads); it is recorded even on decode failure to tell the
// consumer which type was attempted.
func (h *payloadHandler) emitEnvelope(msg message.InboundMessage, payload []byte, value json.RawMessage, protoType string, decodeErr error) error {
	env := envelope{
		Headers:     collectPayloadHeaders(msg, payload),
		PayloadType: protoType,
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

	names := slices.Collect(maps.Keys(h.topics))
	slices.SortFunc(names, func(a, b string) int {
		sa, sb := h.topics[a], h.topics[b]
		if sa.Count != sb.Count {
			return cmp.Compare(sb.Count, sa.Count) // descending count
		}
		return cmp.Compare(a, b) // ascending name
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

// learnHandler narrows the protobuf message type for every observed topic
// by intersecting the candidate sets returned by InferMessageType across
// successive samples. Topics already covered by topic_types (from .conf
// or .types) are skipped; so are messages that carry a per-message
// application_message_type hint or a JSON content type, since those can't
// usefully contribute to learning a per-topic mapping.
type learnHandler struct {
	registry   *ProtoRegistry
	topicIndex *topicTypeIndex
	savePath   string
	results    map[string]*learnState
}

// learnState tracks per-topic narrowing. candidates is the running
// intersection of InferMessageType results; nil means "no informative
// sample seen yet". dropped is set when the intersection drops to zero —
// that means two samples disagreed, which under our "one type per topic"
// assumption is a real conflict worth surfacing.
type learnState struct {
	candidates []string
	samples    int
	dropped    bool
}

func (h *learnHandler) handle(msg message.InboundMessage) error {
	topic := msg.GetDestinationName()
	if h.topicIndex != nil {
		if mt, _ := h.topicIndex.Lookup(topic); mt != "" {
			return nil
		}
	}
	if _, ok := msg.GetApplicationMessageType(); ok {
		return nil
	}
	payload, _ := msg.GetPayloadAsBytes()
	if len(payload) == 0 {
		return nil
	}
	ct, _ := msg.GetHTTPContentType()
	if isJSONContentType(ct) {
		return nil
	}
	candidates, err := h.registry.InferMessageType(payload)
	if err != nil {
		return fmt.Errorf("infer: %w", err)
	}
	h.observeSample(topic, candidates)
	return nil
}

// observeSample updates per-topic state from one classification result.
// Empty candidate sets count as samples but contribute no narrowing — a
// single misshapen message must not poison an otherwise-converging topic.
func (h *learnHandler) observeSample(topic string, candidates []string) {
	state := h.results[topic]
	if state == nil {
		state = &learnState{}
		h.results[topic] = state
	}
	state.samples++
	if state.dropped || len(candidates) == 0 {
		return
	}
	if state.candidates == nil {
		state.candidates = append([]string(nil), candidates...)
		return
	}
	state.candidates = intersectSorted(state.candidates, candidates)
	if len(state.candidates) == 0 {
		state.dropped = true
	}
}

// intersectSorted returns the elements of a that also appear in b. The
// order from a is preserved so emit-order is deterministic.
func intersectSorted(a, b []string) []string {
	set := make(map[string]struct{}, len(b))
	for _, x := range b {
		set[x] = struct{}{}
	}
	out := a[:0:0]
	for _, x := range a {
		if _, ok := set[x]; ok {
			out = append(out, x)
		}
	}
	return out
}

// summary partitions the observed topics into resolved (one candidate
// left), unresolved (still ambiguous), and dropped (intersection went to
// zero — samples contradicted each other). Returned slices are sorted.
func (h *learnHandler) summary() (resolved map[string]string, unresolved, dropped []string) {
	resolved = make(map[string]string)
	for topic, state := range h.results {
		switch {
		case state.dropped:
			dropped = append(dropped, topic)
		case len(state.candidates) == 1:
			resolved[topic] = state.candidates[0]
		default:
			// Either still ambiguous (>1 candidates) or never saw an
			// informative sample (candidates == nil). Both are "we couldn't
			// learn a type for this topic" from the caller's perspective.
			unresolved = append(unresolved, topic)
		}
	}
	slices.Sort(unresolved)
	slices.Sort(dropped)
	return resolved, unresolved, dropped
}

func (h *learnHandler) finish() error {
	resolved, unresolved, dropped := h.summary()

	keys := slices.Collect(maps.Keys(resolved))
	slices.Sort(keys)
	for _, topic := range keys {
		fmt.Printf("%s=%s\n", topic, resolved[topic])
	}

	for _, topic := range unresolved {
		state := h.results[topic]
		if len(state.candidates) == 0 {
			trace.Warningf("%s: no informative samples (saw %d messages)", topic, state.samples)
			continue
		}
		trace.Warningf("%s: ambiguous after %d samples: %s",
			topic, state.samples, strings.Join(state.candidates, ", "))
	}
	for _, topic := range dropped {
		trace.Warningf("%s: candidate intersection became empty after %d samples (conflicting types?)",
			topic, h.results[topic].samples)
	}

	if h.savePath != "" && len(resolved) > 0 {
		if err := WriteTypesFile(h.savePath, resolved); err != nil {
			return fmt.Errorf("write types file %s: %w", h.savePath, err)
		}
		trace.Debugf("wrote %d entries to %s", len(resolved), h.savePath)
	}
	return nil
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
