# solaz

A simple Solace subscriber CLI.

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
      "trust_store_dir":      "/etc/solaz/ca/"
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

## Populating the trust store

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

## Usage

```sh
solaz --topic 'foo/>'                       # first profile
solaz --topic 'a/b/*' --profile prod        # named profile
solaz --topic 'foo/>' --timeout 1m          # custom receive timeout
solaz --config ./other.conf --topic 'x/>'   # alternate config file
```

The CLI connects with client-certificate auth, subscribes to the topic
pattern, waits for one message, prints its headers and payload byte size,
then disconnects.
