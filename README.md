# solaz

A simple Solace subscriber CLI.

## Usage

```sh
solaz <command> [flags]
```

### Commands

- `headers` — print message headers and payload byte size for each received message.
- `payload` — print message payloads as single-line JSON. Decodes protobuf
  payloads using the descriptors loaded from `proto_paths`.
- `stats`   — aggregate per-topic count, total bytes, and average size across
  messages.

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
```

The CLI connects with client-certificate auth, runs the receive loop, and
disconnects when done.

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
