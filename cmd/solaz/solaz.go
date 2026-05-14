package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dnswlt/hackz/solaz/internal/solace"
)

// varsFlag collects repeated --var KEY=VALUE flags into a map.
type varsFlag struct{ m map[string]string }

func (v *varsFlag) String() string {
	if v == nil || len(v.m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(v.m))
	for k := range v.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + v.m[k]
	}
	return strings.Join(parts, ",")
}

func (v *varsFlag) Set(s string) error {
	k, val, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return fmt.Errorf("invalid --var %q, want KEY=VALUE", s)
	}
	if v.m == nil {
		v.m = map[string]string{}
	}
	v.m[k] = val
	return nil
}

func main() {
	var (
		configFlag  = flag.String("config", "", "path to config file (default: ~/.solaz.conf)")
		profileFlag = flag.String("profile", "", "profile name to use (defaults to the first profile in the config)")
		topicFlag   = flag.String("topic", "", "topic subscription pattern (required)")
		timeoutFlag = flag.Duration("timeout", 30*time.Second, "max time to wait for a message")
		typeFlag    = flag.String("type", "", "protobuf message type to use for decoding")
		vars        = &varsFlag{}
	)
	flag.Var(vars, "var", "template variable KEY=VALUE; may be repeated. Expands ${KEY} placeholders in profile fields")
	flag.Parse()

	if *topicFlag == "" {
		log.Fatal("--topic is required")
	}

	configPath := *configFlag
	if configPath == "" {
		configPath = solace.DefaultConfigPath()
	}
	cfg, err := solace.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	profile, err := solace.SelectProfile(cfg, *profileFlag, configPath)
	if err != nil {
		log.Fatalf("profile: %v", err)
	}
	if err := solace.ExpandProfile(profile, vars.m); err != nil {
		log.Fatalf("profile %q: %v", profile.Name, err)
	}
	if err := solace.ValidateProfile(profile, configPath); err != nil {
		log.Fatalf("profile: %v", err)
	}

	svc, err := solace.BuildService(profile)
	if err != nil {
		log.Fatalf("build messaging service: %v", err)
	}

	fmt.Fprintf(os.Stderr, "[%s] subscribed to %q on %s/%s. Waiting up to %s for one message...\n",
		profile.Name, *topicFlag, profile.Host, profile.VPN, *timeoutFlag)

	var registry *solace.ProtoRegistry
	if len(profile.ProtoPaths) > 0 {
		registry, err = solace.NewProtoRegistry(profile.ProtoPaths)
		if err != nil {
			log.Fatalf("proto registry: %v", err)
		}
	}

	opts := solace.ReceiveOptions{
		Topic:       *topicFlag,
		Timeout:     *timeoutFlag,
		Registry:    registry,
		MessageType: *typeFlag,
	}
	if err := solace.Run(svc, opts); err != nil {
		log.Fatal(err)
	}
}
