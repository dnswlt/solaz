package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dnswlt/hackz/solaz/internal/solace"
)

const usage = `Usage: %s <command> [flags]

Commands:
  headers   Print message headers and payload size
  payload   Print message payloads as single-line JSON
  stats     Aggregate metrics across messages
  types     List protobuf message types known to the proto registry
`

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}

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
		fmt.Fprintf(os.Stderr, usage, os.Args[0])
		os.Exit(1)
	}

	cmd := os.Args[1]
	if cmd == "-h" || cmd == "--help" {
		fmt.Fprintf(os.Stderr, usage, os.Args[0])
		os.Exit(0)
	}

	args := os.Args[2:]
	switch cmd {
	case "headers":
		runHeaders(args)
	case "payload":
		runPayload(args)
	case "stats":
		runStats(args)
	case "types":
		runTypes(args)
	default:
		fatalf("unknown command %q (expected 'headers', 'payload', 'stats', or 'types')", cmd)
	}
}

// loadProfile resolves a profile from --config / --profile / --var. When
// validate is true it additionally checks that the connection fields
// required to reach a broker are populated.
func loadProfile(configPath, profileName string, vars map[string]string, validate bool) *solace.Profile {
	if configPath == "" {
		configPath = solace.DefaultConfigPath()
	}
	cfg, err := solace.LoadConfig(configPath)
	if err != nil {
		fatalf("config: %v", err)
	}
	profile, err := solace.SelectProfile(cfg, profileName, configPath)
	if err != nil {
		fatalf("profile: %v", err)
	}
	if err := solace.ExpandProfile(profile, vars); err != nil {
		fatalf("profile %q: %v", profile.Name, err)
	}
	if validate {
		if err := solace.ValidateProfile(profile, configPath); err != nil {
			fatalf("profile: %v", err)
		}
	}
	return profile
}

// runReceive builds a messaging service for the profile and runs the
// receive loop with the supplied options.
func runReceive(profile *solace.Profile, opts solace.ReceiveOptions) {
	svc, err := solace.BuildService(profile)
	if err != nil {
		fatalf("build messaging service: %v", err)
	}

	fmt.Fprintf(os.Stderr, "[%s] subscribed to %q on %s/%s. Waiting up to %s for messages...\n",
		profile.Name, strings.Join(opts.Topics, ","), profile.Host, profile.VPN, opts.Timeout)

	if err := solace.Run(svc, opts); err != nil {
		fatalf("%v", err)
	}
}

func runHeaders(args []string) {
	var (
		configPath, profileName string
		timeout, maxRuntime     time.Duration
		vars                    = &varsFlag{}
		topics                  topicsFlag
	)
	fs := flag.NewFlagSet("headers", flag.ExitOnError)
	fs.StringVar(&configPath, "config", "", "path to config file (default: ~/.solaz.conf)")
	fs.StringVar(&profileName, "profile", "", "profile name to use (defaults to the first profile in the config)")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "max time to wait for a single message")
	fs.DurationVar(&maxRuntime, "max-runtime", 0, "max total time to spend receiving messages (0 disables)")
	fs.Var(vars, "var", "template variable KEY=VALUE; may be repeated. Expands ${KEY} placeholders in profile fields")
	fs.Var(&topics, "topic", "topic subscription pattern (required, may be repeated)")
	fs.Parse(args)

	if len(topics) == 0 {
		fatalf("--topic is required")
	}
	profile := loadProfile(configPath, profileName, vars.m, true)

	runReceive(profile, solace.ReceiveOptions{
		Topics:     topics,
		Timeout:    timeout,
		MaxRuntime: maxRuntime,
		Mode:       "headers",
		Count:      1,
	})
}

func runPayload(args []string) {
	var (
		configPath, profileName string
		timeout, maxRuntime     time.Duration
		msgType                 string
		count                   int
		vars                    = &varsFlag{}
		topics                  topicsFlag
	)
	fs := flag.NewFlagSet("payload", flag.ExitOnError)
	fs.StringVar(&configPath, "config", "", "path to config file (default: ~/.solaz.conf)")
	fs.StringVar(&profileName, "profile", "", "profile name to use (defaults to the first profile in the config)")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "max time to wait for a single message")
	fs.DurationVar(&maxRuntime, "max-runtime", 0, "max total time to spend receiving messages (0 disables)")
	fs.StringVar(&msgType, "type", "", "protobuf message type to use for decoding")
	fs.IntVar(&count, "count", 1, "number of messages to print")
	fs.Var(vars, "var", "template variable KEY=VALUE; may be repeated. Expands ${KEY} placeholders in profile fields")
	fs.Var(&topics, "topic", "topic subscription pattern (required, may be repeated)")
	fs.Parse(args)

	if len(topics) == 0 {
		fatalf("--topic is required")
	}
	profile := loadProfile(configPath, profileName, vars.m, true)

	var registry *solace.ProtoRegistry
	if len(profile.ProtoPaths) > 0 {
		var err error
		registry, err = solace.NewProtoRegistry(profile.ProtoPaths)
		if err != nil {
			fatalf("proto registry: %v", err)
		}
	}

	runReceive(profile, solace.ReceiveOptions{
		Topics:      topics,
		Timeout:     timeout,
		MaxRuntime:  maxRuntime,
		Registry:    registry,
		MessageType: msgType,
		Mode:        "payload",
		Count:       count,
	})
}

func runStats(args []string) {
	var (
		configPath, profileName string
		timeout, maxRuntime     time.Duration
		count                   int
		vars                    = &varsFlag{}
		topics                  topicsFlag
	)
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	fs.StringVar(&configPath, "config", "", "path to config file (default: ~/.solaz.conf)")
	fs.StringVar(&profileName, "profile", "", "profile name to use (defaults to the first profile in the config)")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "max time to wait for a single message")
	fs.DurationVar(&maxRuntime, "max-runtime", 0, "max total time to spend receiving messages (0 disables)")
	fs.IntVar(&count, "count", 100, "number of messages to aggregate")
	fs.Var(vars, "var", "template variable KEY=VALUE; may be repeated. Expands ${KEY} placeholders in profile fields")
	fs.Var(&topics, "topic", "topic subscription pattern (required, may be repeated)")
	fs.Parse(args)

	if len(topics) == 0 {
		fatalf("--topic is required")
	}
	profile := loadProfile(configPath, profileName, vars.m, true)

	runReceive(profile, solace.ReceiveOptions{
		Topics:     topics,
		Timeout:    timeout,
		MaxRuntime: maxRuntime,
		Mode:       "stats",
		Count:      count,
	})
}

// runTypes loads the proto registry from the selected profile and prints
// every known protobuf message type. It does not connect to a broker.
func runTypes(args []string) {
	var (
		configPath, profileName string
		vars                    = &varsFlag{}
	)
	fs := flag.NewFlagSet("types", flag.ExitOnError)
	fs.StringVar(&configPath, "config", "", "path to config file (default: ~/.solaz.conf)")
	fs.StringVar(&profileName, "profile", "", "profile name to use (defaults to the first profile in the config)")
	fs.Var(vars, "var", "template variable KEY=VALUE; may be repeated. Expands ${KEY} placeholders in profile fields")
	fs.Parse(args)

	profile := loadProfile(configPath, profileName, vars.m, false)
	if len(profile.ProtoPaths) == 0 {
		fatalf("profile %q has no proto_paths configured", profile.Name)
	}
	registry, err := solace.NewProtoRegistry(profile.ProtoPaths)
	if err != nil {
		fatalf("proto registry: %v", err)
	}
	for _, name := range registry.MessageNames() {
		fmt.Println(name)
	}
}
