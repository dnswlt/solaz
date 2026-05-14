package solace

import (
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
	Registry    *ProtoRegistry
	MessageType string
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
		if err := receiver.Terminate(5 * time.Second); err != nil {
			fmt.Fprintf(os.Stderr, "warning: terminate receiver: %v\n", err)
		}
	}()

	for i := 0; i < opts.Count; i++ {
		msg, err := receiver.ReceiveMessage(opts.Timeout)
		if err != nil {
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
	case "print":
		return &printJSONHandler{registry: opts.Registry, messageType: opts.MessageType}, nil
	case "stats":
		return &statsHandler{topics: make(map[string]*topicStats)}, nil
	default:
		return nil, fmt.Errorf("unknown mode: %q", opts.Mode)
	}
}

type headersHandler struct{}

func (h *headersHandler) handle(msg message.InboundMessage) error {
	fmt.Printf("Destination:       %s\n", msg.GetDestinationName())
	if v, ok := msg.GetApplicationMessageID(); ok {
		fmt.Printf("AppMessageID:      %s\n", v)
	}
	if v, ok := msg.GetApplicationMessageType(); ok {
		fmt.Printf("AppMessageType:    %s\n", v)
	}
	if v, ok := msg.GetHTTPContentType(); ok {
		fmt.Printf("HTTPContentType:   %s\n", v)
	}
	if v, ok := msg.GetHTTPContentEncoding(); ok {
		fmt.Printf("HTTPContentEncoding:   %s\n", v)
	}
	if v, ok := msg.GetCorrelationID(); ok {
		fmt.Printf("CorrelationID:     %s\n", v)
	}
	if v, ok := msg.GetSenderID(); ok {
		fmt.Printf("SenderID:          %s\n", v)
	}
	if v, ok := msg.GetSenderTimestamp(); ok {
		fmt.Printf("SenderTimestamp:   %s\n", v.Format(time.RFC3339Nano))
	}
	if v, ok := msg.GetTimeStamp(); ok {
		fmt.Printf("ReceiveTimestamp:  %s\n", v.Format(time.RFC3339Nano))
	}
	if v, ok := msg.GetSequenceNumber(); ok {
		fmt.Printf("SequenceNumber:    %d\n", v)
	}
	if exp := msg.GetExpiration(); !exp.IsZero() {
		fmt.Printf("Expiration:        %s\n", exp.Format(time.RFC3339Nano))
	}
	if v, ok := msg.GetPriority(); ok {
		fmt.Printf("Priority:          %d\n", v)
	}
	fmt.Printf("ClassOfService:    %d\n", msg.GetClassOfService())

	if props := msg.GetProperties(); len(props) > 0 {
		fmt.Println("Properties:")
		for k, v := range props {
			fmt.Printf("  %s = %v\n", k, v)
		}
	}

	payload, _ := msg.GetPayloadAsBytes()
	fmt.Printf("PayloadBytes:      %d\n", len(payload))
	return nil
}

func (h *headersHandler) finish() error { return nil }

type printJSONHandler struct {
	registry    *ProtoRegistry
	messageType string
}

func (h *printJSONHandler) handle(msg message.InboundMessage) error {
	msgType := h.messageType
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
		return fmt.Errorf("message type is unknown (use --type to specify explicitly)")
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
		fmt.Fprintf(w, "%s\t%d\t%d\t%.1f\n", t, s.Count, s.TotalSize, avg)
	}
	avg := float64(totalBytes) / float64(totalCount)
	fmt.Fprintf(w, "TOTAL\t%d\t%d\t%.1f\n", totalCount, totalBytes, avg)
	return w.Flush()
}
