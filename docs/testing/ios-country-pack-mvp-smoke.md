# iOS Country-Pack MVP Smoke

Run this before TestFlight/App Store submissions when map country-pack behavior or contribution lifecycle changes.

## What It Covers
- Map surface trust policy states.
- Country-pack catalog selection and dependency planning.
- Pack integrity checks (hash/signature policy paths).
- Offline pack selector behavior.
- Install success/failure metrics instrumentation (`install_metrics` log lines).
- Personal-first contribution lifecycle and deterministic replay behavior.
- TI/AM localization completeness for release-critical listing actions.

## Command
```bash
cd /Users/kebz/arki/semayhub/semaychat
./scripts/ios_country_pack_mvp_smoke.sh
```

## Optional Environment Overrides
- `SEMAY_IOS_SIM_ID`: specific simulator UUID.
- `SEMAY_IOS_SCHEME`: Xcode scheme (default: `bitchat (iOS)`).
- `SEMAY_SMOKE_LOG_DIR`: output directory for suite logs.

## Expected Result
- Script exits `0`.
- Output ends with `All country-pack MVP smoke suites passed.`
- Per-suite logs are written to `build/ios-country-pack-smoke-<timestamp>/`.

## If It Fails
1. Re-run the failing suite directly from the generated log command context.
2. If failure is simulator process instability, reboot the simulator and rerun.
3. If failure is deterministic, block release until fixed and re-validated.

## Install Metrics Logging
- Each install attempt records persistent counters in `OfflineTileStore`.
- Success/failure outcomes emit lines like:
  `"[OfflineTileStore] install_metrics context=success ..."` or
  `"[OfflineTileStore] install_metrics context=failure ..."`
- Use these counters to compute install success rate across sessions.
