package solace

import (
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"

	solacemsg "solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// ReceiveOptions bundles the parameters for receiving a message.
type ReceiveOptions struct {
	Topic       string
	Timeout     time.Duration
	Registry    *ProtoRegistry
	MessageType string
}

// Run connects to the messaging service, subscribes to the given topic,
// waits for a single message within the timeout, and prints its details.
func Run(svc solacemsg.MessagingService, opts ReceiveOptions) error {
	if err := svc.Connect(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		if err := svc.Disconnect(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: disconnect: %v\n", err)
		}
	}()

	sub := resource.TopicSubscriptionOf(opts.Topic)
	receiver, err := svc.CreateDirectMessageReceiverBuilder().
		WithSubscriptions(sub).
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

	msg, err := receiver.ReceiveMessage(opts.Timeout)
	if err != nil {
		return fmt.Errorf("receive: %w", err)
	}

	PrintMessage(msg, opts)
	return nil
}

// PrintMessage prints message details to stdout.
func PrintMessage(msg message.InboundMessage, opts ReceiveOptions) {
	fmt.Printf("Destination:       %s\n", msg.GetDestinationName())
	var msgType string
	if opts.MessageType != "" {
		msgType = opts.MessageType
	}
	if v, ok := msg.GetApplicationMessageID(); ok {
		fmt.Printf("AppMessageID:      %s\n", v)
	}
	if v, ok := msg.GetApplicationMessageType(); ok {
		fmt.Printf("AppMessageType:    %s\n", v)
		if msgType == "" {
			msgType = v
		}
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

	if opts.Registry != nil && len(payload) > 0 && msgType != "" {
		if idx := strings.LastIndex(msgType, "/"); idx >= 0 {
			msgType = msgType[idx+1:]
		}

		desc, err := opts.Registry.FindMessage(msgType)
		if err == nil {
			dynMsg := dynamicpb.NewMessage(desc)
			if err := proto.Unmarshal(payload, dynMsg); err == nil {
				jsonBytes, err := protojson.MarshalOptions{
					Multiline: true,
					Resolver:  dynamicpb.NewTypes(opts.Registry.Files),
				}.Marshal(dynMsg)
				if err == nil {
					fmt.Println("Payload JSON:")
					fmt.Println(string(jsonBytes))
				} else {
					fmt.Printf("Failed to marshal to JSON: %v\n", err)
				}
			} else {
				fmt.Printf("Failed to unmarshal protobuf: %v\n", err)
			}
		} else {
			fmt.Printf("Message descriptor not found for %s: %v\n", msgType, err)
		}
	}
}
