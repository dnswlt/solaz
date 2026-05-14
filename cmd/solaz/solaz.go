package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// Profile holds the connection settings for a Solace broker.
// It is loaded from a JSON file (default: ~/.solaz.conf).
type Profile struct {
	Host           string `json:"host"`             // e.g. "tcps://broker.example.com:55443"
	VPN            string `json:"vpn"`              // message VPN name
	ClientCertFile string `json:"client_cert_file"` // PEM-encoded client cert
	ClientKeyFile  string `json:"client_key_file"`  // PEM-encoded private key
	ClientKeyPass  string `json:"client_key_pass"`  // optional password for encrypted key
	TrustStoreDir  string `json:"trust_store_dir"`  // dir with CA certs to trust
	ClientName     string `json:"client_name"`      // optional client name
}

func defaultProfilePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".solaz.conf"
	}
	return filepath.Join(home, ".solaz.conf")
}

func loadProfile(path string) (*Profile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p Profile
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if p.Host == "" || p.VPN == "" || p.ClientCertFile == "" || p.ClientKeyFile == "" {
		return nil, fmt.Errorf("%s: host, vpn, client_cert_file and client_key_file are required", path)
	}
	return &p, nil
}

func buildService(p *Profile) (solace.MessagingService, error) {
	props := config.ServicePropertyMap{
		config.TransportLayerPropertyHost:                              p.Host,
		config.ServicePropertyVPNName:                                  p.VPN,
		config.AuthenticationPropertyScheme:                            config.AuthenticationSchemeClientCertificate,
		config.AuthenticationPropertySchemeSSLClientCertFile:           p.ClientCertFile,
		config.AuthenticationPropertySchemeSSLClientPrivateKeyFile:     p.ClientKeyFile,
	}
	if p.ClientKeyPass != "" {
		props[config.AuthenticationPropertySchemeClientCertPrivateKeyFilePassword] = p.ClientKeyPass
	}
	if p.TrustStoreDir != "" {
		props[config.TransportLayerSecurityPropertyTrustStorePath] = p.TrustStoreDir
	}
	if p.ClientName != "" {
		props[config.ClientPropertyName] = p.ClientName
	}

	return messaging.NewMessagingServiceBuilder().
		FromConfigurationProvider(props).
		Build()
}

func main() {
	var (
		profileFlag = flag.String("profile", "", "path to profile file (default: ~/.solaz.conf)")
		topicFlag   = flag.String("topic", "", "topic subscription pattern (required)")
		timeoutFlag = flag.Duration("timeout", 30*time.Second, "max time to wait for a message")
	)
	flag.Parse()

	if *topicFlag == "" {
		log.Fatal("--topic is required")
	}

	profilePath := *profileFlag
	if profilePath == "" {
		profilePath = defaultProfilePath()
	}
	profile, err := loadProfile(profilePath)
	if err != nil {
		log.Fatalf("profile: %v", err)
	}

	svc, err := buildService(profile)
	if err != nil {
		log.Fatalf("build messaging service: %v", err)
	}
	if err := svc.Connect(); err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer func() {
		if err := svc.Disconnect(); err != nil {
			log.Printf("disconnect: %v", err)
		}
	}()

	sub := resource.TopicSubscriptionOf(*topicFlag)
	receiver, err := svc.CreateDirectMessageReceiverBuilder().
		WithSubscriptions(sub).
		Build()
	if err != nil {
		log.Fatalf("build receiver: %v", err)
	}
	if err := receiver.Start(); err != nil {
		log.Fatalf("start receiver: %v", err)
	}
	defer func() {
		if err := receiver.Terminate(5 * time.Second); err != nil {
			log.Printf("terminate receiver: %v", err)
		}
	}()

	fmt.Fprintf(os.Stderr, "Subscribed to %q on %s/%s. Waiting up to %s for one message...\n",
		*topicFlag, profile.Host, profile.VPN, *timeoutFlag)

	msg, err := receiver.ReceiveMessage(*timeoutFlag)
	if err != nil {
		log.Fatalf("receive: %v", err)
	}

	fmt.Printf("Destination:       %s\n", msg.GetDestinationName())
	if v, ok := msg.GetApplicationMessageID(); ok {
		fmt.Printf("AppMessageID:      %s\n", v)
	}
	if v, ok := msg.GetApplicationMessageType(); ok {
		fmt.Printf("AppMessageType:    %s\n", v)
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
}
