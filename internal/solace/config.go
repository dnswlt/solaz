package solace

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"solace.dev/go/messaging"
	solacemsg "solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
)

// Config is the top-level structure of the solaz config file
// (default: ~/.solaz.conf, JSON-encoded). It holds a list of named
// broker profiles.
type Config struct {
	Profiles []Profile `json:"profiles"`
}

// Profile holds the connection settings for a single Solace broker.
type Profile struct {
	Name               string   `json:"name"`                 // unique within the config
	Host               string   `json:"host"`                 // e.g. "tcps://broker.example.com:55443"
	VPN                string   `json:"vpn"`                  // message VPN name
	ClientCertFile     string   `json:"client_cert_file"`     // PEM-encoded client cert
	ClientKeyFile      string   `json:"client_key_file"`      // PEM-encoded private key
	ClientKeyPass      string   `json:"client_key_pass"`      // optional password for encrypted key
	ClientCertUserName string   `json:"client_cert_username"` // optional username for client-certificate auth
	TrustStoreDir      string   `json:"trust_store_dir"`      // dir with CA certs to trust
	ClientName         string   `json:"client_name"`          // optional client name
	InsecureSkipVerify bool     `json:"insecure_skip_verify"` // dev-only: disable broker cert validation
	ProtoPaths         []string `json:"proto_paths"`          // paths to search for .proto files

	// TopicTypes maps a Solace topic subscription pattern (with `*` and
	// `>` wildcards) to a fully-qualified protobuf message type. At
	// decode time the `payload` command matches each message's concrete
	// destination against these patterns; the most specific match wins.
	// An entry here takes precedence over the message's
	// application_message_type header, but is overridden by an explicit
	// --type flag.
	TopicTypes map[string]string `json:"topic_types"`
}

func DefaultConfigPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".solaz.conf"
	}
	return filepath.Join(home, ".solaz.conf")
}

func LoadConfig(path string) (*Config, error) {
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

// SelectProfile picks a profile by name, or falls back to the first
// profile if no name is given.
func SelectProfile(c *Config, name, configPath string) (*Profile, error) {
	if name == "" {
		return &c.Profiles[0], nil
	}
	for i := range c.Profiles {
		if c.Profiles[i].Name == name {
			return &c.Profiles[i], nil
		}
	}
	return nil, fmt.Errorf("profile %q not found in %s (available: %s)", name, configPath, ProfileNames(c))
}

func ValidateProfile(p *Profile, configPath string) error {
	if p.Host == "" || p.VPN == "" || p.ClientCertFile == "" || p.ClientKeyFile == "" {
		return fmt.Errorf("%s: profile %q: host, vpn, client_cert_file and client_key_file are required", configPath, p.Name)
	}
	return nil
}

// ExpandProfile substitutes ${var} / $var placeholders in every string
// field of p using vars. Unresolved placeholders cause an error listing
// the missing names.
func ExpandProfile(p *Profile, vars map[string]string) error {
	var missing []string
	seen := map[string]bool{}
	mapper := func(key string) string {
		if v, ok := vars[key]; ok {
			return v
		}
		if !seen[key] {
			seen[key] = true
			missing = append(missing, key)
		}
		return ""
	}
	fields := []*string{
		&p.Name, &p.Host, &p.VPN,
		&p.ClientCertFile, &p.ClientKeyFile, &p.ClientKeyPass,
		&p.ClientCertUserName, &p.TrustStoreDir, &p.ClientName,
	}
	for _, f := range fields {
		*f = os.Expand(*f, mapper)
	}
	for i, f := range p.ProtoPaths {
		p.ProtoPaths[i] = os.Expand(f, mapper)
	}
	if len(p.TopicTypes) > 0 {
		expanded := make(map[string]string, len(p.TopicTypes))
		for k, v := range p.TopicTypes {
			expanded[os.Expand(k, mapper)] = os.Expand(v, mapper)
		}
		p.TopicTypes = expanded
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing template variables: %s", strings.Join(missing, ", "))
	}
	return nil
}

func ProfileNames(c *Config) string {
	names := make([]string, len(c.Profiles))
	for i, p := range c.Profiles {
		names[i] = p.Name
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func BuildService(p *Profile) (solacemsg.MessagingService, error) {
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
		props[config.TransportLayerSecurityPropertyCertValidated] = false
		props[config.TransportLayerSecurityPropertyCertValidateServername] = false
	}

	return messaging.NewMessagingServiceBuilder().
		FromConfigurationProvider(props).
		Build()
}
