# CLAUDE.md

Guidance for Claude when working in this repo. Keep it lean — if something is
already obvious from the code or `README.md`, don't restate it here.

## What this is

`solaz` is a CLI "swiss army knife" for Solace messaging: subscribe to topics,
inspect headers, decode protobuf payloads to JSON, aggregate per-topic stats,
list known proto types. It is meant for professional day-to-day use by
engineers who already live in a unix shell — not a service, not a UI tool.

User-facing surface is documented in [README.md](README.md). This file is
about *how* the code should be shaped, not *what* it does.

## Layout

- [cmd/solaz/solaz.go](cmd/solaz/solaz.go) — entrypoint, flag parsing, one
  `run<Command>` per subcommand. Thin glue; real work lives in `internal/`.
- [internal/solace/solace.go](internal/solace/solace.go) — receive loop,
  per-mode `messageHandler` implementations (`headers`, `payload`, `stats`).
- [internal/solace/config.go](internal/solace/config.go) — config/profile
  loading, `${var}` expansion, `.vars` companion file, broker service build.
- [internal/solace/registry.go](internal/solace/registry.go) — protobuf
  descriptor compilation from `proto_paths`.
- [internal/solace/topic_match.go](internal/solace/topic_match.go) — Solace
  topic pattern matching (`*` single-level / prefix-within-level, `>` trailing
  multi-level), and the `topic_types` specificity index.
- [internal/trace/trace.go](internal/trace/trace.go) — stderr output helpers:
  `Debugf` (gated by `--verbose` / `SetVerbose`) and `Warningf` (unconditional,
  auto-prefixed with `warning:`). All non-stdout output should go through
  one of these — see principles 2 and 3.

Tests sit next to their target file as `*_test.go`, table-driven, standard
`testing` package. Mix of internal (`package solace`) and external
(`package solace_test`) — pick whichever the assertion needs.

## Build / test

```sh
go build ./cmd/solaz       # produces ./solaz (gitignored)
go test ./...              # all tests, no external deps needed
go vet ./...
```

No broker is contacted by the test suite. Anything that needs a real Solace
connection lives behind manual invocation.

## Design principles

These are the non-negotiables. New code and refactors should reinforce them.

### 1. Composable output

Subcommands fall into two categories:

- **Pipeable** (machine-readable, one record per line, no decoration):
  `payload` emits single-line JSON; `types` emits one FQ name per line.
  These must stay `jq`/`grep`/`awk`-friendly. No headers, no summary lines,
  no progress chatter on stdout. Ever.
- **Human-readable** (tabular, decorated): `headers`, `stats`. These use
  `text/tabwriter` and are fine to format for human eyes. Don't pipe them.

If you add a new subcommand, decide which bucket it's in *first* and commit
to it. Don't mix — a command that prints a JSON line and then a "done!"
banner is broken for both audiences.

### 2. Robust against per-message failures

A single bad message must not abort a run of 100. Decode errors, missing
proto types, unknown topics, etc. are **per-message warnings**, not fatal
errors:

```go
trace.Warningf("%s: %v", topic, err)
// continue the loop
```

`trace.Warningf` (in [internal/trace/trace.go](internal/trace/trace.go))
prepends a `warning:` prefix and writes unconditionally to stderr — use
it instead of raw `fmt.Fprintf(os.Stderr, ...)` so the prefix stays
consistent across the binary.

Fatal-and-exit is reserved for setup-time problems (bad config, can't
connect to broker, can't build the proto registry, can't subscribe). Once
the receive loop is running, errors from individual messages should be
logged to stderr and skipped.

The receive loop in [internal/solace/solace.go](internal/solace/solace.go)
implements this: handler errors become `warning: <topic>: <err>` lines and
the loop continues. Don't add new fatal paths inside handlers. If a handler
ever needs to truly abort the run (e.g. unrecoverable I/O state), introduce
an explicit classified-error sentinel rather than `return`ing a raw error.

### 3. Silent by default, opt-in verbose

Like `grep`, `curl -s`, `jq`: produce *only* the requested output on
stdout, *nothing* on stderr unless something went wrong (or `--verbose`
is set). No "connected to broker", no "subscribed to X", no progress
counters.

Every subcommand accepts `--verbose` / `-v`, which flips a package-level
toggle via `trace.SetVerbose`. Debug output goes through `trace.Debugf`
in [internal/trace/trace.go](internal/trace/trace.go) — plain
`fmt.Fprintf(os.Stderr, ...)`, no timestamp, no level prefix. This is a
CLI, not a service: no `2026-05-15T12:34:56Z INFO` prefixes, just terse
human-readable lines.

When you add a new code path that does something a user might want to
trace ("connecting", "subscribed", "loaded N protos"), reach for
`trace.Debugf`. For unconditional warnings, use `trace.Warningf` (see
principle 2). Raw `fmt.Fprintf(os.Stderr, ...)` should not appear outside
`internal/trace` — everything else routes through the helpers.

### 4. Concise, professional UX

- Error messages: lowercase, no trailing punctuation, lead with the
  context (`fatalf("profile %q: %v", ...)` rather than `"Error: ..."`).
- Flag names: long form only, hyphenated (`--max-runtime`, not
  `--maxruntime`). Match `README.md`'s flag table.
- Help output: terse. The existing `usage` constant is the model — one
  line per command, no decoration.
- No emoji. No colors. No spinners. No interactive prompts.

## Conventions

- **Profile templating**: use `os.Expand` (handles both `${VAR}` and `$VAR`).
  Expansion runs *before* validation so missing vars fail with a clear
  "missing template variables: ..." rather than a downstream TLS error.
- **`fatalf`** in `cmd/solaz` is the single exit-with-message helper.
  Use it; don't sprinkle `os.Exit(1)` calls.
- **Topic matching**: always go through `matchTopicLevels` / `topicTypeIndex`.
  Don't reimplement Solace wildcard semantics ad-hoc — the edge cases
  (`*` as prefix-within-level, `>` only at the end) are subtle and covered
  by [internal/solace/topic_match_test.go](internal/solace/topic_match_test.go).
- **Proto registry**: lazy — only build it when a command actually needs it
  (`payload`, `types`). `headers` and `stats` must work without
  `proto_paths` configured.
- **No new dependencies** without a good reason. The current set (Solace
  SDK, `protobuf`, `protocompile`, `genproto/googleapis/type/*`) is
  deliberate; everything else is stdlib.

## When in doubt

Bias toward deleting code over adding it. This tool's value is in being
small, sharp, and pipeable. New flags, new output formats, new abstractions
need to justify themselves against that bar.
