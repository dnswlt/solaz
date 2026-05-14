package solace

import (
	"fmt"
	"os"
	"strings"
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

// Run connects to the messaging service, subscribes to the given topic,
// waits for a single message within the timeout, and prints its details.
func Run(svc solace.MessagingService, opts ReceiveOptions) error {
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
		if err := PrintMessage(msg, opts); err != nil {
			return fmt.Errorf("print message: %w", err)
		}
	}
	return nil
}

// PrintMessage prints message details to stdout.
func PrintMessage(msg message.InboundMessage, opts ReceiveOptions) error {
	switch opts.Mode {
	case "headers":
		return printHeaders(msg)
	case "print":
		return printPayloadJSON(msg, opts)
	default:
		return fmt.Errorf("unknown mode: %q", opts.Mode)
	}
}

func printHeaders(msg message.InboundMessage) error {
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

func printPayloadJSON(msg message.InboundMessage, opts ReceiveOptions) error {
	var msgType string
	if opts.MessageType != "" {
		msgType = opts.MessageType
	}
	if v, ok := msg.GetApplicationMessageType(); ok {
		if msgType == "" {
			msgType = v
		}
	}

	payload, _ := msg.GetPayloadAsBytes()
	if len(payload) == 0 {
		return fmt.Errorf("message payload is empty")
	}
	if opts.Registry == nil {
		return fmt.Errorf("no proto registry configured (set proto_paths in profile)")
	}
	if msgType == "" {
		return fmt.Errorf("message type is unknown (use --type to specify explicitly)")
	}

	if idx := strings.LastIndex(msgType, "/"); idx >= 0 {
		msgType = msgType[idx+1:]
	}

	desc, err := opts.Registry.FindMessage(msgType)
	if err != nil {
		return fmt.Errorf("message descriptor not found for %s: %w", msgType, err)
	}

	dynMsg := dynamicpb.NewMessage(desc)
	if err := proto.Unmarshal(payload, dynMsg); err != nil {
		return fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	jsonBytes, err := protojson.MarshalOptions{
		Multiline: false,
		Resolver:  dynamicpb.NewTypes(opts.Registry.Files),
	}.Marshal(dynMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal to JSON: %w", err)
	}

	fmt.Println(string(jsonBytes))
	return nil
}
