# Upstream Sync (Private Semay Chat)

This repo is private (`origin`). Upstream source is public Bitchat.

- Private remote: `origin`
- Public source remote: `upstream` (`https://github.com/permissionlesstech/bitchat.git`)

## Sync flow

Run from repo root:

```bash
./scripts/sync_upstream.sh --open-pr
```

What this does:
1. Verifies a clean working tree.
2. Fetches `origin` and `upstream`.
3. Creates a sync branch from `main`.
4. Merges upstream default branch into that sync branch.
5. Pushes sync branch to private `origin`.
6. Optionally opens a PR in private repo.

## Privacy guarantee

This workflow **never pushes to upstream**. Your private Semay code remains private unless you explicitly push it to a public remote.
