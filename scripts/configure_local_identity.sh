#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_CONFIG_PATH="$ROOT_DIR/Configs/Local.xcconfig"
LOCAL_CONFIG_TEMPLATE="$ROOT_DIR/Configs/Local.xcconfig.example"

TEAM_ID="${DEVELOPMENT_TEAM:-}"
BUNDLE_ID=""
APP_GROUP_ID=""

print_usage() {
  cat <<'EOF'
Usage:
  configure_local_identity.sh [options]

Options:
  --team-id <id>      Apple Developer Team ID (optional)
  --bundle-id <id>    Bundle identifier override
  --app-group <id>    App group identifier override
  --help              Show this help

Defaults:
- If --bundle-id is omitted and --team-id is provided:
    bundle id => chat.bitchat.<team-id>
- If --app-group is omitted:
    app group => group.<bundle-id>
EOF
}

fail() {
  echo "error: $*" >&2
  exit 1
}

upsert_setting() {
  local file="$1"
  local key="$2"
  local value="$3"
  local tmp
  tmp="$(mktemp)"

  awk -v key="$key" -v value="$value" '
    BEGIN { updated = 0 }
    {
      if ($0 ~ "^[[:space:]]*" key "[[:space:]]*=") {
        print key " = " value
        updated = 1
      } else {
        print $0
      }
    }
    END {
      if (updated == 0) {
        print key " = " value
      }
    }
  ' "$file" >"$tmp"

  mv "$tmp" "$file"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --team-id)
      TEAM_ID="${2:-}"
      shift 2
      ;;
    --bundle-id)
      BUNDLE_ID="${2:-}"
      shift 2
      ;;
    --app-group)
      APP_GROUP_ID="${2:-}"
      shift 2
      ;;
    --help|-h)
      print_usage
      exit 0
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
done

if [[ ! -f "$LOCAL_CONFIG_PATH" ]]; then
  [[ -f "$LOCAL_CONFIG_TEMPLATE" ]] || fail "missing template: $LOCAL_CONFIG_TEMPLATE"
  cp "$LOCAL_CONFIG_TEMPLATE" "$LOCAL_CONFIG_PATH"
fi

if [[ -z "$BUNDLE_ID" ]]; then
  if [[ -n "$TEAM_ID" ]]; then
    BUNDLE_ID="chat.bitchat.$TEAM_ID"
  else
    BUNDLE_ID="$(
      awk -F'=' '/^[[:space:]]*PRODUCT_BUNDLE_IDENTIFIER[[:space:]]*=/{gsub(/[[:space:]]/, "", $2); print $2; exit}' "$LOCAL_CONFIG_PATH"
    )"
  fi
fi
[[ -n "$BUNDLE_ID" ]] || fail "bundle id is empty; pass --bundle-id or --team-id"

if [[ -z "$APP_GROUP_ID" ]]; then
  APP_GROUP_ID="group.$BUNDLE_ID"
fi

if [[ -n "$TEAM_ID" ]]; then
  upsert_setting "$LOCAL_CONFIG_PATH" "DEVELOPMENT_TEAM" "$TEAM_ID"
fi
upsert_setting "$LOCAL_CONFIG_PATH" "PRODUCT_BUNDLE_IDENTIFIER" "$BUNDLE_ID"
upsert_setting "$LOCAL_CONFIG_PATH" "SEMAY_APP_GROUP_IDENTIFIER" "$APP_GROUP_ID"

echo "Updated: $LOCAL_CONFIG_PATH"
echo "DEVELOPMENT_TEAM=${TEAM_ID:-<unchanged>}"
echo "PRODUCT_BUNDLE_IDENTIFIER=$BUNDLE_ID"
echo "SEMAY_APP_GROUP_IDENTIFIER=$APP_GROUP_ID"
