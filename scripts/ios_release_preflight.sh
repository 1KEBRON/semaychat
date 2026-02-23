#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${SEMAY_PREFLIGHT_LOG_DIR:-$ROOT_DIR/build/ios-release-preflight-$(date +%Y%m%d-%H%M%S)}"

mkdir -p "$LOG_DIR"

TEST_FILTERS=(
  "SeedPhraseServiceICloudAvailabilityTests"
  "SemayLocalizationCompletenessTests"
  "SemayTranslationServiceTests"
  "SemayMapSurfacePolicyTests"
  "SemayServiceContributionShareTests"
  "SemayServiceDirectoryUXTests"
  "OfflineTileHubInstallPlanTests"
)

echo "Semay iOS release preflight"
echo "Root: $ROOT_DIR"
echo "Logs: $LOG_DIR"

failures=0

for filter in "${TEST_FILTERS[@]}"; do
  safe_name="${filter//\//_}"
  log_file="$LOG_DIR/$safe_name.log"

  echo ""
  echo "==> swift test --filter $filter"
  if ! (cd "$ROOT_DIR" && swift test --filter "$filter" | tee "$log_file"); then
    failures=$((failures + 1))
    echo "FAILED: $filter"
  fi
done

if [[ "${SEMAY_RUN_IOS_SIM_SMOKE:-0}" == "1" ]]; then
  smoke_log="$LOG_DIR/ios_country_pack_mvp_smoke.log"
  echo ""
  echo "==> scripts/ios_country_pack_mvp_smoke.sh"
  if ! (cd "$ROOT_DIR" && ./scripts/ios_country_pack_mvp_smoke.sh | tee "$smoke_log"); then
    failures=$((failures + 1))
    echo "FAILED: iOS simulator smoke suite"
  fi
fi

if [[ "$failures" -gt 0 ]]; then
  echo ""
  echo "Preflight failed with $failures failing step(s). Review logs in: $LOG_DIR" >&2
  exit 1
fi

echo ""
echo "Preflight passed. Logs written to: $LOG_DIR"
