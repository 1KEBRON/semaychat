# iOS Release Preflight (Local)

This is the default testing path for release validation.

## Run

```bash
./scripts/ios_release_preflight.sh
```

## Optional Simulator Smoke

Run with iOS simulator smoke suites included:

```bash
SEMAY_RUN_IOS_SIM_SMOKE=1 ./scripts/ios_release_preflight.sh
```

## Output

- Logs are written to `build/ios-release-preflight-<timestamp>/`.
- Non-zero exit means at least one preflight step failed.

## Included Checks

- `SeedPhraseServiceICloudAvailabilityTests`
- `SemayLocalizationCompletenessTests`
- `SemayTranslationServiceTests`
- `SemayMapSurfacePolicyTests`
- `SemayServiceContributionShareTests`
- `SemayServiceDirectoryUXTests`
- `OfflineTileHubInstallPlanTests`

When `SEMAY_RUN_IOS_SIM_SMOKE=1`:

- `scripts/ios_country_pack_mvp_smoke.sh`
