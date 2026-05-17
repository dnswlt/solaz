#!/bin/sh
# Install solaz from a GitHub release.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/dnswlt/solaz/main/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/dnswlt/solaz/main/install.sh | VERSION=v0.1.0 sh
#   curl -fsSL https://raw.githubusercontent.com/dnswlt/solaz/main/install.sh | DEST=$HOME/.local/bin sh

set -eu

# Wrapped in a function so a partial download from `curl | sh` can't execute
# half the logic.
main() {
  REPO="dnswlt/solaz"
  BIN="solaz"
  VERSION="${VERSION:-latest}"
  DEST="${DEST:-/usr/local/bin}"

  OS=$(uname -s | tr '[:upper:]' '[:lower:]')
  ARCH=$(uname -m)
  case "$ARCH" in
    x86_64|amd64)   ARCH="amd64" ;;
    aarch64|arm64)  ARCH="arm64" ;;
    *) echo "solaz: unsupported arch: $ARCH" >&2; exit 1 ;;
  esac

  case "$OS/$ARCH" in
    linux/amd64|darwin/arm64) ;;
    *)
      echo "solaz: no prebuilt binary for $OS/$ARCH" >&2
      echo "       available: linux/amd64, darwin/arm64" >&2
      echo "       build from source: https://github.com/$REPO" >&2
      exit 1 ;;
  esac

  if [ "$VERSION" = "latest" ]; then
    VERSION=$(curl -sSI "https://github.com/$REPO/releases/latest" \
      | grep -i '^location:' \
      | sed 's|.*/tag/||' | tr -d '[:space:]')
    if [ -z "$VERSION" ]; then
      echo "solaz: could not resolve latest version" >&2
      exit 1
    fi
  fi

  ARCHIVE="${BIN}_${VERSION}_${OS}_${ARCH}.tar.gz"
  URL="https://github.com/$REPO/releases/download/${VERSION}/${ARCHIVE}"

  echo "Installing $BIN $VERSION ($OS/$ARCH) -> $DEST/$BIN"

  TMP=$(mktemp -d)
  trap 'rm -rf "$TMP"' EXIT

  curl -fSL  "$URL"        -o "$TMP/$ARCHIVE"
  curl -fsSL "$URL.sha256" -o "$TMP/$ARCHIVE.sha256"

  (cd "$TMP" && {
    if command -v sha256sum >/dev/null 2>&1; then
      sha256sum -c "$ARCHIVE.sha256"
    else
      shasum -a 256 -c "$ARCHIVE.sha256"
    fi
  }) >/dev/null

  tar -xzf "$TMP/$ARCHIVE" -C "$TMP"

  if [ -w "$DEST" ]; then
    mv "$TMP/$BIN" "$DEST/$BIN"
  else
    echo "  (using sudo to write to $DEST)"
    sudo mv "$TMP/$BIN" "$DEST/$BIN"
  fi

  echo "Installed $DEST/$BIN"

  if [ "$OS" = "darwin" ] \
     && [ ! -e /opt/homebrew/opt/openssl ] \
     && [ ! -e /usr/local/opt/openssl ]; then
    echo
    echo "Note: solaz on macOS dynamically links Homebrew's OpenSSL."
    echo "      If you see a dyld error on first run: brew install openssl"
  fi
}

main "$@"
