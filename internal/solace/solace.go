package solace

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

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
	defer func() {
		if err := svc.Disconnect(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: disconnect: %v\n", err)
		}
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
	defer func() {
		err := receiver.Terminate(1 * time.Second)
		var incomplete *solace.IncompleteMessageDeliveryError
		if err != nil && !errors.As(err, &incomplete) {
			fmt.Fprintf(os.Stderr, "warning: terminate receiver: %v\n", err)
		}
	}()

	var deadline time.Time
	if opts.MaxRuntime > 0 {
		deadline = time.Now().Add(opts.MaxRuntime)
	}

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
		if err := handler.handle(msg); err != nil {
			return err
		}
	}

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
		return &printJSONHandler{
			registry:    opts.Registry,
			messageType: opts.MessageType,
			topicIndex:  newTopicTypeIndex(opts.TopicTypes),
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

type printJSONHandler struct {
	registry    *ProtoRegistry
	messageType string
	topicIndex  *topicTypeIndex
}

func (h *printJSONHandler) handle(msg message.InboundMessage) error {
	msgType := h.messageType
	if msgType == "" {
		mt, err := h.topicIndex.Lookup(msg.GetDestinationName())
		if err != nil {
			return err
		}
		msgType = mt
	}
	if msgType == "" {
		if v, ok := msg.GetApplicationMessageType(); ok {
			msgType = v
		}
	}

	payload, _ := msg.GetPayloadAsBytes()
	if len(payload) == 0 {
		return fmt.Errorf("message payload is empty")
	}
	if h.registry == nil {
		return fmt.Errorf("no proto registry configured (set proto_paths in profile)")
	}
	if msgType == "" {
		return fmt.Errorf("message type is unknown for topic %q (use --type or configure topic_types in the profile)", msg.GetDestinationName())
	}

	if idx := strings.LastIndex(msgType, "/"); idx >= 0 {
		msgType = msgType[idx+1:]
	}

	desc, err := h.registry.FindMessage(msgType)
	if err != nil {
		return fmt.Errorf("message descriptor not found for %s: %w", msgType, err)
	}

	dynMsg := dynamicpb.NewMessage(desc)
	if err := proto.Unmarshal(payload, dynMsg); err != nil {
		return fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	jsonBytes, err := protojson.MarshalOptions{
		Multiline: false,
		Resolver:  dynamicpb.NewTypes(h.registry.Files),
	}.Marshal(dynMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal to JSON: %w", err)
	}

	fmt.Println(string(jsonBytes))
	return nil
}

func (h *printJSONHandler) finish() error { return nil }

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
