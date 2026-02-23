# Local Testing Commands

## Default release validation

```bash
./scripts/ios_release_preflight.sh
```

## Include simulator smoke checks

```bash
SEMAY_RUN_IOS_SIM_SMOKE=1 ./scripts/ios_release_preflight.sh
```

## Refresh relay dataset manually

```bash
./scripts/update_georelays.sh
```
