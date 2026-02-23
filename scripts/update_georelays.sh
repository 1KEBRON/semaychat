#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RELAYS_DIR="$ROOT_DIR/relays"
OUTPUT_FILE="$RELAYS_DIR/online_relays_gps.csv"
SOURCE_URL="${SEMAY_GEORELAYS_SOURCE_URL:-https://raw.githubusercontent.com/permissionlesstech/georelays/refs/heads/main/nostr_relays.csv}"

mkdir -p "$RELAYS_DIR"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
tmp_file="$tmp_dir/nostr_relays.csv"

if command -v curl >/dev/null 2>&1; then
  curl -fsSL "$SOURCE_URL" -o "$tmp_file"
elif command -v wget >/dev/null 2>&1; then
  wget -q "$SOURCE_URL" -O "$tmp_file"
else
  echo "error: neither curl nor wget is available" >&2
  exit 1
fi

if [[ ! -s "$tmp_file" ]]; then
  echo "error: downloaded relay file is empty" >&2
  exit 1
fi

mv "$tmp_file" "$OUTPUT_FILE"
echo "updated $OUTPUT_FILE from $SOURCE_URL"
