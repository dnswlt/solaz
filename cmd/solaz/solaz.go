package main

import (
	"flag"
	"fmt"
	"maps"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dnswlt/solaz/internal/solace"
	"github.com/dnswlt/solaz/internal/trace"
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

// stringListFlag collects repeated flags into a slice.
type stringListFlag []string

func (s *stringListFlag) String() string {
	if s == nil {
		return ""
	}
	return strings.Join(*s, ",")
}

func (s *stringListFlag) Set(val string) error {
	*s = append(*s, val)
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

// loadProfile resolves a profile from --config / --profile / --var. Vars
// from the companion `.vars` file (default ~/.solaz.vars, or the --config
// path with its extension swapped) are merged in as a baseline; CLI --var
// flags override them. When validate is true the connection fields
// required to reach a broker are also checked.
func loadProfile(configPath, profileName string, vars map[string]string, validate bool) *solace.Profile {
	if configPath == "" {
		configPath = solace.DefaultConfigPath()
	}
	varsPath := solace.VarsPath(configPath)
	fileVars, err := solace.LoadVarsFile(varsPath)
	if err != nil {
		fatalf("vars: %v", err)
	}
	merged := make(map[string]string)
	maps.Copy(merged, fileVars)
	maps.Copy(merged, vars)

	cfg, err := solace.LoadConfig(configPath)
	if err != nil {
		fatalf("config: %v", err)
	}
	profile, err := solace.SelectProfile(cfg, profileName, configPath)
	if err != nil {
		fatalf("profile: %v", err)
	}
	if err := solace.ExpandProfile(profile, merged); err != nil {
		fatalf("profile %q: %v", profile.Name, err)
	}
	if validate {
		if err := solace.ValidateProfile(profile, configPath); err != nil {
			fatalf("profile: %v", err)
		}
	}
	trace.Debugf("loaded profile %q from %s (%d vars)", profile.Name, configPath, len(merged))
	return profile
}

// runReceive builds a messaging service for the profile and runs the
// receive loop with the supplied options.
func runReceive(profile *solace.Profile, opts solace.ReceiveOptions) {
	svc, err := solace.BuildService(profile)
	if err != nil {
		fatalf("build messaging service: %v", err)
	}

	trace.Debugf("connecting to %s/%s", profile.Host, profile.VPN)

	if err := solace.Run(svc, opts); err != nil {
		fatalf("%v", err)
	}
}

func runHeaders(args []string) {
	var (
		configPath, profileName string
		timeout, maxRuntime     time.Duration
		verbose                 bool
		vars                    = &varsFlag{}
		topics                  stringListFlag
	)
	fs := flag.NewFlagSet("headers", flag.ExitOnError)
	fs.StringVar(&configPath, "config", "", "path to config file (default: ~/.solaz.conf)")
	fs.StringVar(&profileName, "profile", "", "profile name to use (defaults to the first profile in the config)")
	fs.DurationVar(&timeout, "timeout", 60*time.Second, "max time to wait for a single message")
	fs.DurationVar(&maxRuntime, "max-runtime", 0, "max total time to spend receiving messages (0 disables)")
	fs.BoolVar(&verbose, "verbose", false, "enable debug logging to stderr")
	fs.BoolVar(&verbose, "v", false, "shorthand for --verbose")
	fs.Var(vars, "var", "template variable KEY=VALUE; may be repeated. Expands ${KEY} placeholders in profile fields")
	fs.Var(&topics, "topic", "topic subscription pattern (required, may be repeated)")
	fs.Parse(args)
	trace.SetVerbose(verbose)

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
		configPath, profileName          string
		timeout, maxRuntime              time.Duration
		msgType                          string
		count                            int
		verbose, raw, envelope, identify bool
		vars                             = &varsFlag{}
		topics, protoPaths               stringListFlag
	)
	fs := flag.NewFlagSet("payload", flag.ExitOnError)
	fs.StringVar(&configPath, "config", "", "path to config file (default: ~/.solaz.conf)")
	fs.StringVar(&profileName, "profile", "", "profile name to use (defaults to the first profile in the config)")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "max time to wait for a single message")
	fs.DurationVar(&maxRuntime, "max-runtime", 0, "max total time to spend receiving messages (0 disables)")
	fs.StringVar(&msgType, "type", "", "protobuf message type to use for decoding")
	fs.IntVar(&count, "count", 1, "number of messages to print")
	fs.BoolVar(&raw, "raw", false, "write payloads to stdout as raw bytes, bypassing content-type decoding")
	fs.BoolVar(&envelope, "envelope", false, "emit {headers, payload, payloadEncoding, ...} JSON envelopes; every message produces one record")
	fs.BoolVar(&identify, "identify", false, "heuristically identify the protobuf message type when no --type, topic_types, or application_message_type is set")
	fs.BoolVar(&verbose, "verbose", false, "enable debug logging to stderr")
	fs.BoolVar(&verbose, "v", false, "shorthand for --verbose")
	fs.Var(vars, "var", "template variable KEY=VALUE; may be repeated. Expands ${KEY} placeholders in profile fields")
	fs.Var(&topics, "topic", "topic subscription pattern (required, may be repeated)")
	fs.Var(&protoPaths, "proto-path", "additional path to search for .proto files (may be repeated)")
	fs.Parse(args)
	trace.SetVerbose(verbose)

	if len(topics) == 0 {
		fatalf("--topic is required")
	}
	if raw && msgType != "" {
		fatalf("--raw and --type are mutually exclusive")
	}
	if identify && raw {
		fatalf("--identify and --raw are mutually exclusive")
	}
	if identify && msgType != "" {
		fatalf("--identify and --type are mutually exclusive")
	}
	profile := loadProfile(configPath, profileName, vars.m, true)
	profile.ProtoPaths = append(profile.ProtoPaths, protoPaths...)

	if identify && len(profile.ProtoPaths) == 0 {
		fatalf("--identify requires proto_paths (in the profile) or --proto-path")
	}

	var registry *solace.ProtoRegistry
	if !raw && len(profile.ProtoPaths) > 0 {
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
		TopicTypes:  profile.TopicTypes,
		Mode:        "payload",
		Count:       count,
		Raw:         raw,
		Envelope:    envelope,
		Identify:    identify,
	})
}

func runStats(args []string) {
	var (
		configPath, profileName string
		timeout, maxRuntime     time.Duration
		count                   int
		verbose                 bool
		vars                    = &varsFlag{}
		topics                  stringListFlag
	)
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	fs.StringVar(&configPath, "config", "", "path to config file (default: ~/.solaz.conf)")
	fs.StringVar(&profileName, "profile", "", "profile name to use (defaults to the first profile in the config)")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "max time to wait for a single message")
	fs.DurationVar(&maxRuntime, "max-runtime", 0, "max total time to spend receiving messages (0 disables)")
	fs.IntVar(&count, "count", 100, "number of messages to aggregate")
	fs.BoolVar(&verbose, "verbose", false, "enable debug logging to stderr")
	fs.BoolVar(&verbose, "v", false, "shorthand for --verbose")
	fs.Var(vars, "var", "template variable KEY=VALUE; may be repeated. Expands ${KEY} placeholders in profile fields")
	fs.Var(&topics, "topic", "topic subscription pattern (required, may be repeated)")
	fs.Parse(args)
	trace.SetVerbose(verbose)

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
		verbose                 bool
		vars                    = &varsFlag{}
		protoPaths              stringListFlag
	)
	fs := flag.NewFlagSet("types", flag.ExitOnError)
	fs.StringVar(&configPath, "config", "", "path to config file (default: ~/.solaz.conf)")
	fs.StringVar(&profileName, "profile", "", "profile name to use (defaults to the first profile in the config)")
	fs.BoolVar(&verbose, "verbose", false, "enable debug logging to stderr")
	fs.BoolVar(&verbose, "v", false, "shorthand for --verbose")
	fs.Var(vars, "var", "template variable KEY=VALUE; may be repeated. Expands ${KEY} placeholders in profile fields")
	fs.Var(&protoPaths, "proto-path", "additional path to search for .proto files (may be repeated)")
	fs.Parse(args)
	trace.SetVerbose(verbose)

	profile := loadProfile(configPath, profileName, vars.m, false)
	profile.ProtoPaths = append(profile.ProtoPaths, protoPaths...)

	if len(profile.ProtoPaths) == 0 {
		fatalf("profile %q has no proto_paths configured and no --proto-path provided", profile.Name)
	}
	registry, err := solace.NewProtoRegistry(profile.ProtoPaths)
	if err != nil {
		fatalf("proto registry: %v", err)
	}
	for _, name := range registry.MessageNames() {
		fmt.Println(name)
	}
}
