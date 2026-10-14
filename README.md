# solaz

A simple Solace subscriber CLI.

## Getting started

Install the latest release (`linux/amd64` or `darwin/arm64`):

```sh
curl -fsSL https://raw.githubusercontent.com/dnswlt/solaz/main/install.sh | sh
```

Prebuilt binaries are also on the
[releases page](https://github.com/dnswlt/solaz/releases). On other
platforms, build from source with `go build ./cmd/solaz`.

Drop a broker profile into `~/.solaz.conf` (see [Config](#config)), then:

```sh
solaz headers --topic 'foo/>'
```

## Usage

```sh
solaz <command> [flags]
```

### Commands

- `headers` — print message headers and payload byte size for each received message.
- `print` — print message payloads, one record per message, dispatched
  on the message's `Content-Type` (and explicit overrides):
  - `--raw` set → write raw payload bytes to stdout unchanged, bypassing
    content-type dispatch.
  - `--proto-type` set → always decode as protobuf with that type.
  - JSON content type (`application/json`, `text/json`, or any `+json`
    structured-suffix type) → compact onto a single line and print as-is.
  - No protobuf content type, and the payload is a valid JSON object or
    array → treated as JSON, same as above.
  - Protobuf content type (`application/x-protobuf`,
    `application/vnd.google.protobuf`, `application/protobuf`), or any
    proto hint (matching `topic_types` entry, or an
    `application_message_type` header) → decode as protobuf using the
    descriptors from the profile's proto set and print as single-line
    JSON.
  - Anything else → write the raw payload bytes to stdout unchanged. No
    framing, no trailing newline — pipe into a downstream decoder of your
    choice. Use `--count 1` (the default) when consuming raw output, or
    handle framing yourself for larger batches.
- `stats`   — aggregate per-topic count, total bytes, and average size across
  messages.
- `types`   — list all protobuf message types known to the proto registry
  (compiled from the profile's proto set). Does not connect to a broker;
  useful for discovering the right value to pass to `--proto-type`.
- `profiles` — list profile names from the config file, one per line, in
  file order (the first is the default). Does not connect to a broker.
- `learn-types` — for each subscribed topic, narrow down the protobuf
  message type carried on it by intersecting the candidate sets from
  successive messages, and print one `topic=type` line per resolved
  topic. Topics already covered by `topic_types` (from `.conf` or the
  companion `.types` file) are skipped. With `--save`, the new mappings
  are merged into the `.types` file alongside the config. See
  [Learning topic types](#learning-topic-types).

### Flags

Flags common to all commands:

- `--topic PATTERN` — subscription pattern (required, may be repeated for
  multiple subscriptions).
- `--profile NAME` — profile from the config file (defaults to the first
  entry).
- `--config PATH` — config file path (default `~/.solaz.conf`).
- `--duration DUR` — total time to receive messages before stopping; `0`
  disables (default `0`).
- `--idle-timeout DUR` — max wait for the next message before erroring
  out; `0` waits indefinitely (default `30s`, or `0` with `--duration`).
- `--var KEY=VALUE` — template variable for profile expansion (repeatable).
- `--proto-type FQNAME` — protobuf message type for `print`; overrides the
  per-message hint.

Per-command flags:

- `print`: `--count N` (default `1`) — number of messages to print.
  `--raw` — emit raw payload bytes, bypassing content-type dispatch
  (mutually exclusive with `--proto-type`).
  `--envelope` — emit one `{headers, payload, payloadEncoding, ...}` JSON
  object per message (see [Envelope mode](#envelope-mode)).
  `--infer` — heuristically infer the protobuf message type when
  no other hint resolves one.
- `stats`: `--count N` (default `100`) — number of messages to aggregate.
- `learn-types`: `--count N` (default `1000`), `--save` — merge resolved
  mappings into the companion `.types` file rather than only printing
  them.

The loop stops when `--count` is reached *or* `--duration` elapses,
whichever comes first. `--count 0` means no limit. With `--duration`
set, `--count` and `--idle-timeout` default to `0`, so a time-bounded run
needs nothing else:

```sh
solaz stats --topic 'trades/>' --duration 10m
```

Ctrl-C (or SIGTERM) ends a run the same way: `stats` still prints its
summary, `learn-types --save` still writes, and the exit status is 0. A
second Ctrl-C exits immediately.

### Examples

```sh
# First profile, headers for one message
solaz headers --topic 'foo/>'

# Aggregate up to 1000 messages or 2 minutes, whichever first
solaz stats --topic 'foo/>' --count 1000 --duration 2m

# Print 5 payloads as JSON, forcing a specific proto type
solaz print --topic 'a/b/*' --count 5 --proto-type com.example.MyMessage

# Subscribe to multiple topics at once
solaz stats --topic 'foo/>' --topic 'bar/baz/*'

# Use a named profile and a longer custom receive timeout
solaz print --topic 'x/>' --profile prod --idle-timeout 1m

# List every protobuf message type the registry knows about
solaz types --profile prod
```

The `types` command accepts only `--config`, `--profile`, and `--var`;
receive-related flags don't apply.

The CLI connects with client-certificate auth, runs the receive loop, and
disconnects when done.

### Learning topic types

`learn-types` listens to live traffic and narrows down the protobuf type
on each topic by intersecting candidates across successive messages.
Resolved mappings print to stdout as `topic=type` lines; with `--save`
they are merged into a companion `.types` file (default `~/.solaz.types`).

```sh
# Print the mapping for whatever is flowing on the orders subtree
solaz learn-types --topic 'trades/orders/>' --profile prod

# Same, but persist so future runs pick it up automatically
solaz learn-types --topic 'trades/>' --save --duration 5m
```

The `.types` file is auto-consulted by every command. Its format mirrors
`.vars` (one `topic=type` per line). A proto set's `topic_types` wins on
key collision, so a manually-curated mapping always overrides a learned
one. To re-learn a topic, delete its line from `.types`.

### Envelope mode

With `--envelope`, the `print` command emits one JSON object per message
that wraps the headers and the payload together. Every message produces a
record — payloads that can't be decoded are base64-encoded rather than
dropped — so the output stream is safe to feed straight into `jq` or a log
indexer without losing messages.

Schema:

```json
{
  "headers": {
    "Destination": "trades/orders/btc",
    "AppMessageType": "com.example.Order",
    "HTTPContentType": "application/json",
    "ReceiveTimestamp": "2026-05-15T12:34:56.789Z",
    "ClassOfService": 1,
    "PayloadBytes": 124
  },
  "payload": { "price": 50000, "side": "buy" },
  "payloadEncoding": "json"
}
```

- `headers` — every set header field, named as in the tabular `headers`
  mode. JSON key order is alphabetical.
- `payload` — the decoded JSON value (from a JSON payload or a
  protobuf decode), or a base64-encoded string of the raw bytes.
- `payloadEncoding` — `"json"` or `"base64"`, so consumers don't have to
  guess.
- `protoType` — fully-qualified protobuf message type used for
  decoding; omitted for JSON or raw payloads.
- `payloadError` — only present when a decode was attempted and failed
  (e.g. `--proto-type` set but the descriptor is missing); the original bytes
  are still available under `payload` as base64.

`--envelope` composes with `--proto-type` (force-decode, base64 on failure) and
`--raw` (always base64, no decode attempted).

```sh
# Tail 100 orders with headers, pretty-printed
solaz print --envelope --topic 'trades/orders/>' --count 100 | jq .

# Pull just the destinations + sequence numbers
solaz print --envelope --topic 'foo/>' --count 50 \
  | jq -r '[.headers.Destination, .headers.SequenceNumber] | @tsv'
```

## Config

`solaz` reads a JSON config file (default: `~/.solaz.conf`, override with
`--config PATH`). The file holds a list of named broker profiles and a map
of named proto sets. Pick a profile with `--profile NAME`, or omit the flag
to use the first entry. Unknown keys are rejected.

Example `~/.solaz.conf`:

```json
{
  "proto_sets": {
    "trading": {
      "paths": ["/home/me/src/trading-api/proto"],
      "topic_types": {
        "trades/orders/>": "com.example.Order",
        "trades/fills/>":  "com.example.Fill"
      }
    }
  },
  "profiles": [
    {
      "name": "dev",
      "host": "tcps://broker.dev.example.com:55443",
      "vpn": "dev-vpn",
      "client_cert_file": "/etc/solaz/dev.crt",
      "client_key_file":  "/etc/solaz/dev.key",
      "trust_store_dir":  "/etc/solaz/ca/",
      "client_name":      "solaz-dev",
      "proto_set":        "trading"
    },
    {
      "name": "prod",
      "host": "tcps://broker.prod.example.com:55443",
      "vpn": "prod-vpn",
      "client_cert_file":     "/etc/solaz/prod.crt",
      "client_key_file":      "/etc/solaz/prod.key",
      "client_key_pass":      "hunter2",
      "client_cert_username": "svc-solaz-prod",
      "trust_store_dir":      "/etc/solaz/ca/",
      "proto_set":            "trading"
    }
  ]
}
```

One profile per broker environment, all sharing one proto set, keeps
switching environments down to `--profile NAME`.

Required profile fields: `name`, `host`, `vpn`, plus the credentials for
the chosen `auth_scheme` (see below).

Optional profile fields:

- `auth_scheme` — `client_cert` (default, client-certificate auth) or
  `basic` (username/password). When omitted, the scheme is derived from
  which fields are populated; set it explicitly to disambiguate when
  fields from both schemes are present.
- `client_key_pass` — password for an encrypted private key.
- `client_cert_username` — Solace VPN username to authenticate as. Omit to
  let the broker derive it from the cert (typically the CN, per its
  certificate-matching rule). Set this when the broker requires an
  explicit username that doesn't match what it would extract.
- `basic_username`, `basic_password` — credentials for `auth_scheme=basic`.
  Don't inline the password; expand it from the environment via
  `"basic_password": "${SOLAZ_BROKER_PW}"` and supply the value through a
  gitignored `.vars` companion file or `--var`.
- `trust_store_dir` — directory of CA certs in OpenSSL hashed-directory
  format, used to validate the broker's server certificate. See
  [Populating the trust store](#populating-the-trust-store). Ignored when
  `insecure_skip_verify` is true.
- `client_name` — display name for the connection in the broker's
  connected-clients list. Cosmetic; not used for authentication.
- `insecure_skip_verify` — dev-only. Disables broker certificate
  validation (chain *and* hostname).
- `proto_set` — name of an entry in `proto_sets`. Required for decoding
  protobuf payloads (or pass `--proto-path`).

### Proto sets

`proto_sets` maps a name to a schema that profiles share. Fields:

- `paths` (required) — directories searched for `.proto` files used to
  decode protobuf payloads to JSON. `--proto-path` adds more for a
  single run.
- `topic_types` — map from a Solace topic subscription pattern (with
  `*` matching one level, `*` as a suffix to a level part acting as a
  prefix wildcard within that level — e.g. `d-*` matches `d-anything` —
  and `>` matching one or more trailing levels) to a fully-qualified
  protobuf message type. When `--proto-type` is not set,
  the `print` command matches each message's concrete destination
  against these patterns to pick a type; the most specific match wins
  (more literal segments first, then more segments overall), and an
  unresolvable tie produces a per-message error. With the example
  above, a single run can subscribe to several topics that carry
  different types:

  ```sh
  solaz print --topic 'trades/orders/>' --topic 'trades/fills/>'
  ```

  A `topic_types` match takes precedence over the message's
  `application_message_type` header. `--proto-type`, when set, overrides
  everything.

### Templated profiles

Any string field in a profile may contain `${VAR}` (or `$VAR`)
placeholders. Provide values on the command line with `--var KEY=VALUE`
(repeatable); unresolved placeholders fail fast with a list of missing
names. This lets one profile cover many brokers/environments without
duplicating entries:

```json
{
  "profiles": [
    {
      "name": "openshift",
      "host":             "tcps://${namespace}-broker.internal:55443",
      "vpn":              "${vpn}",
      "client_cert_file": "${creds}/${namespace}.crt",
      "client_key_file":  "${creds}/${namespace}.key",
      "trust_store_dir":  "${creds}/ca",
      "client_name":      "solaz-cli",
      "insecure_skip_verify": true
    }
  ]
}
```

```sh
solaz headers --topic 'foo/>' \
      --var namespace=dev-payments-1 \
      --var vpn=payments-dev \
      --var creds=$HOME/solaz-creds
```

Expansion happens before validation, so a templated profile with no
`--var`s supplied errors with `missing template variables: namespace, vpn, creds`
rather than failing later at TLS time. Profiles with no placeholders are
unaffected.

### Vars file

For frequently-used template values, drop them into a companion vars file
next to the config (default `~/.solaz.vars`, or the `--config` path with
its extension swapped for `.vars`). Each line is `KEY=VALUE`; blank lines
and `#`-prefixed lines are ignored. CLI `--var` flags override matching
keys from the file. Example `~/.solaz.vars`:

```sh
# default expansion values for ~/.solaz.conf
namespace = dev-payments-1
vpn       = payments-dev
creds     = /home/me/solaz-creds
```

With that in place, `solaz headers --topic 'foo/>'` works without any
`--var` flags; you can still override one ad-hoc with
`--var namespace=qa-payments-1`.

### Populating the trust store

Only needed if you set `trust_store_dir`. For dev brokers you can skip
this entirely with `insecure_skip_verify: true`. If your org already
ships a trust store, point `trust_store_dir` at it and you're done.

`trust_store_dir` is a directory of CA certs in OpenSSL hashed-directory
format (filenames are `<subject-hash>.0`, not arbitrary `*.pem`). As an
ad-hoc bootstrap when you don't have one, pull the broker's chain from a
live connection and rehash it:

```sh
HOST=broker.example.com
PORT=55443
TRUST_DIR=/etc/solaz/ca       # = your profile's trust_store_dir

mkdir -p "$TRUST_DIR"
cd "$TRUST_DIR"

# Split the broker's full chain into cert-1.pem (leaf), cert-2.pem (intermediate), ...
openssl s_client -connect "$HOST:$PORT" -showcerts </dev/null 2>/dev/null \
  | awk '/-----BEGIN CERTIFICATE-----/{n++; f="cert-"n".pem"}
         n>0 {print > f}
         /-----END CERTIFICATE-----/{close(f)}'

# Drop the leaf — you trust *issuers*, not the broker's own cert
rm -f cert-1.pem

# Inspect what you kept
for f in cert-*.pem; do
  echo "== $f =="
  openssl x509 -in "$f" -noout -subject -issuer
done

# Build the <hash>.0 symlinks Solace looks up by
openssl rehash .
```

Verify with plain openssl before re-running `solaz`:

```sh
openssl s_client -connect "$HOST:$PORT" -CApath "$TRUST_DIR" </dev/null 2>&1 \
  | grep 'Verify return code'
# Want: "Verify return code: 0 (ok)"
```

If you still see `20 (unable to get local issuer certificate)`, the broker
isn't sending its intermediate(s) in the handshake. Obtain the missing CA
PEM out-of-band (broker admin, your org's PKI), drop it into `$TRUST_DIR`,
and re-run `openssl rehash`.
