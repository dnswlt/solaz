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

// topicsFlag collects repeated --topic flags into a slice.
type topicsFlag []string

func (t *topicsFlag) String() string {
	if t == nil {
		return ""
	}
	return strings.Join(*t, ",")
}

func (t *topicsFlag) Set(s string) error {
	*t = append(*t, s)
	return nil
}

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
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> [flags]\n\nCommands:\n  headers   Print message headers and payload size\n  print     Print message payloads as single-line JSON\n  stats     Aggregate metrics across messages\n", os.Args[0])
		os.Exit(1)
	}

	cmd := os.Args[1]
	if cmd == "-h" || cmd == "--help" {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> [flags]\n\nCommands:\n  headers   Print message headers and payload size\n  print     Print message payloads as single-line JSON\n  stats     Aggregate metrics across messages\n", os.Args[0])
		os.Exit(0)
	}

	var (
		configPath  string
		profileName string
		timeout     time.Duration
		msgType     string
		vars        = &varsFlag{}
		topics      topicsFlag
	)

	setupFlags := func(fs *flag.FlagSet) {
		fs.StringVar(&configPath, "config", "", "path to config file (default: ~/.solaz.conf)")
		fs.StringVar(&profileName, "profile", "", "profile name to use (defaults to the first profile in the config)")
		fs.DurationVar(&timeout, "timeout", 30*time.Second, "max time to wait for a message")
		fs.StringVar(&msgType, "type", "", "protobuf message type to use for decoding")
		fs.Var(vars, "var", "template variable KEY=VALUE; may be repeated. Expands ${KEY} placeholders in profile fields")
		fs.Var(&topics, "topic", "topic subscription pattern (required, may be repeated)")
	}

	mode := "headers"
	count := 1

	switch cmd {
	case "headers":
		fs := flag.NewFlagSet("headers", flag.ExitOnError)
		setupFlags(fs)
		fs.Parse(os.Args[2:])
	case "print":
		mode = "print"
		fs := flag.NewFlagSet("print", flag.ExitOnError)
		setupFlags(fs)
		fs.IntVar(&count, "count", 1, "number of messages to print")
		fs.Parse(os.Args[2:])
	case "stats":
		mode = "stats"
		fs := flag.NewFlagSet("stats", flag.ExitOnError)
		setupFlags(fs)
		fs.IntVar(&count, "count", 100, "number of messages to aggregate")
		fs.Parse(os.Args[2:])
	default:
		log.Fatalf("unknown command %q (expected 'headers', 'print', or 'stats')", cmd)
	}

	if len(topics) == 0 {
		log.Fatal("--topic is required")
	}

	if configPath == "" {
		configPath = solace.DefaultConfigPath()
	}
	cfg, err := solace.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	profile, err := solace.SelectProfile(cfg, profileName, configPath)
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

	fmt.Fprintf(os.Stderr, "[%s] subscribed to %q on %s/%s. Waiting up to %s for messages...\n",
		profile.Name, strings.Join(topics, ","), profile.Host, profile.VPN, timeout)

	var registry *solace.ProtoRegistry
	if len(profile.ProtoPaths) > 0 {
		registry, err = solace.NewProtoRegistry(profile.ProtoPaths)
		if err != nil {
			log.Fatalf("proto registry: %v", err)
		}
	}

	opts := solace.ReceiveOptions{
		Topics:      topics,
		Timeout:     timeout,
		Registry:    registry,
		MessageType: msgType,
		Mode:        mode,
		Count:       count,
	}
	if err := solace.Run(svc, opts); err != nil {
		log.Fatal(err)
	}
}
