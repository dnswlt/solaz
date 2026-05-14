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
      "client_cert_file": "/etc/solaz/prod.crt",
      "client_key_file":  "/etc/solaz/prod.key",
      "client_key_pass":  "hunter2",
      "trust_store_dir":  "/etc/solaz/ca/"
    }
  ]
}
```

Required fields: `name`, `host`, `vpn`, `client_cert_file`, `client_key_file`.
Optional: `client_key_pass`, `trust_store_dir`, `client_name`.

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
