package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// Config is the top-level structure of the solaz config file
// (default: ~/.solaz.conf, JSON-encoded). It holds a list of named
// broker profiles.
type Config struct {
	Profiles []Profile `json:"profiles"`
}

// Profile holds the connection settings for a single Solace broker.
type Profile struct {
	Name               string `json:"name"`                 // unique within the config
	Host               string `json:"host"`                 // e.g. "tcps://broker.example.com:55443"
	VPN                string `json:"vpn"`                  // message VPN name
	ClientCertFile     string `json:"client_cert_file"`     // PEM-encoded client cert
	ClientKeyFile      string `json:"client_key_file"`      // PEM-encoded private key
	ClientKeyPass      string `json:"client_key_pass"`      // optional password for encrypted key
	ClientCertUserName string `json:"client_cert_username"` // optional username for client-certificate auth
	TrustStoreDir      string `json:"trust_store_dir"`      // dir with CA certs to trust
	ClientName         string `json:"client_name"`          // optional client name
	InsecureSkipVerify bool   `json:"insecure_skip_verify"` // dev-only: disable broker cert validation
}

func defaultConfigPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".solaz.conf"
	}
	return filepath.Join(home, ".solaz.conf")
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if len(c.Profiles) == 0 {
		return nil, fmt.Errorf("%s: no profiles defined", path)
	}
	seen := map[string]bool{}
	for i, p := range c.Profiles {
		if p.Name == "" {
			return nil, fmt.Errorf("%s: profile #%d has no name", path, i)
		}
		if seen[p.Name] {
			return nil, fmt.Errorf("%s: duplicate profile name %q", path, p.Name)
		}
		seen[p.Name] = true
	}
	return &c, nil
}

// selectProfile picks a profile by name, or falls back to the first
// profile if no name is given.
func selectProfile(c *Config, name, configPath string) (*Profile, error) {
	var p *Profile
	if name == "" {
		p = &c.Profiles[0]
	} else {
		for i := range c.Profiles {
			if c.Profiles[i].Name == name {
				p = &c.Profiles[i]
				break
			}
		}
		if p == nil {
			return nil, fmt.Errorf("profile %q not found in %s (available: %s)", name, configPath, profileNames(c))
		}
	}
	if p.Host == "" || p.VPN == "" || p.ClientCertFile == "" || p.ClientKeyFile == "" {
		return nil, fmt.Errorf("%s: profile %q: host, vpn, client_cert_file and client_key_file are required", configPath, p.Name)
	}
	return p, nil
}

func profileNames(c *Config) string {
	names := make([]string, len(c.Profiles))
	for i, p := range c.Profiles {
		names[i] = p.Name
	}
	return strings.Join(names, ", ")
}

func buildService(p *Profile) (solace.MessagingService, error) {
	props := config.ServicePropertyMap{
		config.TransportLayerPropertyHost:                          p.Host,
		config.ServicePropertyVPNName:                              p.VPN,
		config.AuthenticationPropertyScheme:                        config.AuthenticationSchemeClientCertificate,
		config.AuthenticationPropertySchemeSSLClientCertFile:       p.ClientCertFile,
		config.AuthenticationPropertySchemeSSLClientPrivateKeyFile: p.ClientKeyFile,
	}
	if p.ClientKeyPass != "" {
		props[config.AuthenticationPropertySchemeClientCertPrivateKeyFilePassword] = p.ClientKeyPass
	}
	if p.ClientCertUserName != "" {
		props[config.AuthenticationPropertySchemeClientCertUserName] = p.ClientCertUserName
	}
	if p.TrustStoreDir != "" && !p.InsecureSkipVerify {
		props[config.TransportLayerSecurityPropertyTrustStorePath] = p.TrustStoreDir
	}
	if p.ClientName != "" {
		props[config.ClientPropertyName] = p.ClientName
	}
	if p.InsecureSkipVerify {
		fmt.Fprintf(os.Stderr, "WARNING: profile %q has insecure_skip_verify=true; broker certificate will NOT be validated.\n", p.Name)
		props[config.TransportLayerSecurityPropertyCertValidated] = false
		props[config.TransportLayerSecurityPropertyCertValidateServername] = false
	}

	return messaging.NewMessagingServiceBuilder().
		FromConfigurationProvider(props).
		Build()
}

func main() {
	var (
		configFlag  = flag.String("config", "", "path to config file (default: ~/.solaz.conf)")
		profileFlag = flag.String("profile", "", "profile name to use (defaults to the first profile in the config)")
		topicFlag   = flag.String("topic", "", "topic subscription pattern (required)")
		timeoutFlag = flag.Duration("timeout", 30*time.Second, "max time to wait for a message")
	)
	flag.Parse()

	if *topicFlag == "" {
		log.Fatal("--topic is required")
	}

	configPath := *configFlag
	if configPath == "" {
		configPath = defaultConfigPath()
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	profile, err := selectProfile(cfg, *profileFlag, configPath)
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

	fmt.Fprintf(os.Stderr, "[%s] subscribed to %q on %s/%s. Waiting up to %s for one message...\n",
		profile.Name, *topicFlag, profile.Host, profile.VPN, *timeoutFlag)

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
