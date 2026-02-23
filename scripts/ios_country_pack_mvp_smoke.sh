#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_PATH="$ROOT_DIR/bitchat.xcodeproj"
SCHEME_NAME="${SEMAY_IOS_SCHEME:-bitchat (iOS)}"
SIM_ID="${SEMAY_IOS_SIM_ID:-}"
LOG_DIR="${SEMAY_SMOKE_LOG_DIR:-$ROOT_DIR/build/ios-country-pack-smoke-$(date +%Y%m%d-%H%M%S)}"

if [[ -z "$SIM_ID" ]]; then
  SIM_ID="$(xcrun simctl list devices available | awk -F '[()]' '/iPhone 16e/ {print $2; exit}')"
fi

if [[ -z "$SIM_ID" ]]; then
  SIM_ID="$(xcrun simctl list devices available | awk -F '[()]' '/iPhone/ {print $2; exit}')"
fi

if [[ -z "$SIM_ID" ]]; then
  echo "Could not resolve a simulator ID. Set SEMAY_IOS_SIM_ID and retry." >&2
  exit 2
fi

mkdir -p "$LOG_DIR"

TEST_SUITES=(
  "bitchatTests_iOS/SemayMapSurfacePolicyTests"
  "bitchatTests_iOS/OfflineTileCountryPackSelectionTests"
  "bitchatTests_iOS/OfflineTilePackSelectorTests"
  "bitchatTests_iOS/OfflineTilePackIntegrityTests"
  "bitchatTests_iOS/OfflineTileHubInstallPlanTests"
  "bitchatTests_iOS/SemayServiceContributionShareTests"
  "bitchatTests_iOS/SemayServiceDirectoryEntryDecodingTests"
  "bitchatTests_iOS/SemayServiceDirectoryUXTests"
  "bitchatTests_iOS/SemayLocalizationCompletenessTests"
)

echo "Using simulator: $SIM_ID"
echo "Logs: $LOG_DIR"

xcrun simctl boot "$SIM_ID" >/dev/null 2>&1 || true
xcrun simctl bootstatus "$SIM_ID" -b

failures=0

for suite in "${TEST_SUITES[@]}"; do
  safe_name="${suite//\//_}"
  safe_name="${safe_name//:/_}"
  log_file="$LOG_DIR/$safe_name.log"

  echo "Running $suite"
  if ! xcodebuild test \
    -project "$PROJECT_PATH" \
    -scheme "$SCHEME_NAME" \
    -destination "id=$SIM_ID" \
    -only-testing:"$suite" \
    | tee "$log_file"; then
    failures=$((failures + 1))
    echo "Suite failed: $suite"
  fi
done

if [[ "$failures" -gt 0 ]]; then
  echo "$failures suite(s) failed. See logs in $LOG_DIR" >&2
  exit 1
fi

echo "All country-pack MVP smoke suites passed."
