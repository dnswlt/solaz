# solaz

A simple Solace subscriber CLI.

## Usage

```sh
solaz <command> [flags]
```

### Commands

- `headers` — print message headers and payload byte size for each received message.
- `payload` — print message payloads, one record per message, dispatched
  on the message's `Content-Type` (and explicit overrides):
  - `--raw` set → write raw payload bytes to stdout unchanged, bypassing
    content-type dispatch.
  - `--type` set → always decode as protobuf with that type.
  - JSON content type (`application/json`, `text/json`, or any `+json`
    structured-suffix type) → compact onto a single line and print as-is.
  - Protobuf content type (`application/x-protobuf`,
    `application/vnd.google.protobuf`, `application/protobuf`), or any
    proto hint (matching `topic_types` entry, or an
    `application_message_type` header) → decode as protobuf using the
    descriptors from `proto_paths` and print as single-line JSON.
  - Anything else → write the raw payload bytes to stdout unchanged. No
    framing, no trailing newline — pipe into a downstream decoder of your
    choice. Use `--count 1` (the default) when consuming raw output, or
    handle framing yourself for larger batches.
- `stats`   — aggregate per-topic count, total bytes, and average size across
  messages.
- `types`   — list all protobuf message types known to the proto registry
  (compiled from `proto_paths`). Does not connect to a broker; useful for
  discovering the right value to pass to `--type`.

### Flags

Flags common to all commands:

- `--topic PATTERN` — subscription pattern (required, may be repeated for
  multiple subscriptions).
- `--profile NAME` — profile from the config file (defaults to the first
  entry).
- `--config PATH` — config file path (default `~/.solaz.conf`).
- `--timeout DUR` — max wait for a single message before erroring out
  (default `30s`).
- `--max-runtime DUR` — max total time spent receiving; `0` disables
  (default `0`).
- `--var KEY=VALUE` — template variable for profile expansion (repeatable).
- `--type FQNAME` — protobuf message type for `payload`; overrides the
  per-message hint.

Per-command flags:

- `payload`: `--count N` (default `1`) — number of messages to print.
  `--raw` — emit raw payload bytes, bypassing content-type dispatch
  (mutually exclusive with `--type`).
  `--envelope` — emit one `{headers, payload, payloadEncoding, ...}` JSON
  object per message (see [Envelope mode](#envelope-mode)).
  `--infer` — heuristically infer the protobuf message type when
  no other hint resolves one.
- `stats`: `--count N` (default `100`) — number of messages to aggregate.

The loop stops when `--count` is reached *or* `--max-runtime` elapses,
whichever comes first.

### Examples

```sh
# First profile, headers for one message
solaz headers --topic 'foo/>'

# Aggregate up to 1000 messages or 2 minutes, whichever first
solaz stats --topic 'foo/>' --count 1000 --max-runtime 2m

# Print 5 payloads as JSON, forcing a specific proto type
solaz payload --topic 'a/b/*' --count 5 --type com.example.MyMessage

# Subscribe to multiple topics at once
solaz stats --topic 'foo/>' --topic 'bar/baz/*'

# Use a named profile and a longer custom receive timeout
solaz payload --topic 'x/>' --profile prod --timeout 1m

# List every protobuf message type the registry knows about
solaz types --profile prod
```

The `types` command accepts only `--config`, `--profile`, and `--var`;
receive-related flags don't apply.

The CLI connects with client-certificate auth, runs the receive loop, and
disconnects when done.

### Envelope mode

With `--envelope`, the `payload` command emits one JSON object per message
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
- `payload` — the decoded JSON value (from a JSON content type or a
  protobuf decode), or a base64-encoded string of the raw bytes.
- `payloadEncoding` — `"json"` or `"base64"`, so consumers don't have to
  guess.
- `payloadType` — fully-qualified protobuf message type used for
  decoding; omitted for JSON or raw payloads.
- `payloadError` — only present when a decode was attempted and failed
  (e.g. `--type` set but the descriptor is missing); the original bytes
  are still available under `payload` as base64.

`--envelope` composes with `--type` (force-decode, base64 on failure) and
`--raw` (always base64, no decode attempted).

```sh
# Tail 100 orders with headers, pretty-printed
solaz payload --envelope --topic 'trades/orders/>' --count 100 | jq .

# Pull just the destinations + sequence numbers
solaz payload --envelope --topic 'foo/>' --count 50 \
  | jq -r '[.headers.Destination, .headers.SequenceNumber] | @tsv'
```

## Config

`solaz` reads a JSON config file (default: `~/.solaz.conf`, override with
`--config PATH`). The file holds a list of named broker profiles; pick one
with `--profile NAME`, or omit the flag to use the first entry.

Example `~/.solaz.conf`:

```json
{
  "profiles": [
    {
      "name": "dev",
      "host": "tcps://broker.dev.example.com:55443",
      "vpn": "dev-vpn",
      "client_cert_file": "/etc/solaz/dev.crt",
      "client_key_file":  "/etc/solaz/dev.key",
      "trust_store_dir":  "/etc/solaz/ca/",
      "client_name":      "solaz-dev"
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
      "proto_paths":          ["/etc/solaz/protos"]
    }
  ]
}
```

Required fields: `name`, `host`, `vpn`, `client_cert_file`, `client_key_file`.

Optional fields:

- `client_key_pass` — password for an encrypted private key.
- `client_cert_username` — Solace VPN username to authenticate as. Omit to
  let the broker derive it from the cert (typically the CN, per its
  certificate-matching rule). Set this when the broker requires an
  explicit username that doesn't match what it would extract.
- `trust_store_dir` — directory of CA certs in OpenSSL hashed-directory
  format, used to validate the broker's server certificate. See
  [Populating the trust store](#populating-the-trust-store). Ignored when
  `insecure_skip_verify` is true.
- `client_name` — display name for the connection in the broker's
  connected-clients list. Cosmetic; not used for authentication.
- `insecure_skip_verify` — dev-only. Disables broker certificate
  validation (chain *and* hostname). Prints a warning to stderr.
- `proto_paths` — list of directories searched for `.proto` files used by
  the `payload` command to decode protobuf payloads to JSON. Required for
  `payload` if message payloads are protobuf-encoded.
- `topic_types` — map from a Solace topic subscription pattern (with
  `*` matching one level, `*` as a suffix to a level part acting as a
  prefix wildcard within that level — e.g. `d-*` matches `d-anything` —
  and `>` matching one or more trailing levels) to a fully-qualified
  protobuf message type. When `--type` is not set,
  the `payload` command matches each message's concrete destination
  against these patterns to pick a type; the most specific match wins
  (more literal segments first, then more segments overall), and an
  unresolvable tie produces a per-message error. This lets a single run
  subscribe to several topics that carry different types:

  ```sh
  solaz payload --topic 'trades/orders/>' --topic 'trades/fills/>'
  ```

  with the profile:

  ```json
  "topic_types": {
    "trades/orders/>": "com.example.Order",
    "trades/fills/>":  "com.example.Fill"
  }
  ```

  A `topic_types` match takes precedence over the message's
  `application_message_type` header. `--type`, when set, overrides
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

For frequently-used values, drop them into a companion `vars` file: it
sits next to the config (default `~/.solaz.vars`, or the `--config` path
with its extension swapped for `.vars`). Each line is `KEY=VALUE`; blank
lines and `#`-prefixed lines are ignored. CLI `--var` flags override
matching keys from the file. Example `~/.solaz.vars`:

```sh
# default expansion values for ~/.solaz.conf
namespace = dev-payments-1
vpn       = payments-dev
creds     = /home/me/solaz-creds
```

with that in place, `solaz headers --topic 'foo/>'` works without any
`--var` flags; you can still override one ad-hoc with
`--var namespace=qa-payments-1`.

### Populating the trust store

`trust_store_dir` is a directory of CA certs in OpenSSL hashed-directory
format (filenames are `<subject-hash>.0`, not arbitrary `*.pem`). The
quickest way to seed it from a live broker is to pull its chain and rehash:

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
