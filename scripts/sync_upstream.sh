#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME=$(basename "$0")

usage() {
  cat <<USAGE
Usage: $SCRIPT_NAME [options]

Sync public upstream changes into this private repo without publishing private code.

Options:
  --upstream-name NAME   Remote name for public source (default: upstream)
  --upstream-url URL     Add upstream remote if missing (default for Semay Chat: https://github.com/permissionlesstech/bitchat.git)
  --base-branch BRANCH   Private base branch to sync into (default: main)
  --no-push              Do not push sync branch to origin
  --open-pr              Open a GitHub PR after push (private origin only)
  -h, --help             Show this help

Examples:
  $SCRIPT_NAME
  $SCRIPT_NAME --open-pr
  $SCRIPT_NAME --base-branch main --no-push
USAGE
}

UPSTREAM_NAME="upstream"
UPSTREAM_URL="https://github.com/permissionlesstech/bitchat.git"
BASE_BRANCH="main"
DO_PUSH=1
OPEN_PR=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --upstream-name)
      UPSTREAM_NAME="$2"
      shift 2
      ;;
    --upstream-url)
      UPSTREAM_URL="$2"
      shift 2
      ;;
    --base-branch)
      BASE_BRANCH="$2"
      shift 2
      ;;
    --no-push)
      DO_PUSH=0
      shift
      ;;
    --open-pr)
      OPEN_PR=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Working tree is not clean. Commit or stash changes before syncing upstream." >&2
  exit 2
fi

if ! git remote get-url origin >/dev/null 2>&1; then
  echo "Missing origin remote." >&2
  exit 2
fi

if ! git remote get-url "$UPSTREAM_NAME" >/dev/null 2>&1; then
  if [[ -z "$UPSTREAM_URL" ]]; then
    echo "Missing $UPSTREAM_NAME remote and no --upstream-url provided." >&2
    exit 2
  fi
  echo "Adding $UPSTREAM_NAME -> $UPSTREAM_URL"
  git remote add "$UPSTREAM_NAME" "$UPSTREAM_URL"
fi

echo "Fetching origin and $UPSTREAM_NAME..."
git fetch origin --prune --tags
git fetch "$UPSTREAM_NAME" --prune --tags

UPSTREAM_HEAD_REF=$(git symbolic-ref --quiet --short "refs/remotes/$UPSTREAM_NAME/HEAD" 2>/dev/null || true)
if [[ -n "$UPSTREAM_HEAD_REF" ]]; then
  UPSTREAM_BRANCH=${UPSTREAM_HEAD_REF#"$UPSTREAM_NAME"/}
else
  UPSTREAM_BRANCH=$(git remote show "$UPSTREAM_NAME" | sed -n '/HEAD branch/s/.*: //p' | tr -d '[:space:]')
fi

if [[ -z "$UPSTREAM_BRANCH" ]]; then
  UPSTREAM_BRANCH="main"
fi

if ! git show-ref --verify --quiet "refs/remotes/$UPSTREAM_NAME/$UPSTREAM_BRANCH"; then
  echo "Could not resolve $UPSTREAM_NAME/$UPSTREAM_BRANCH after fetch." >&2
  exit 2
fi

if git show-ref --verify --quiet "refs/heads/$BASE_BRANCH"; then
  git checkout "$BASE_BRANCH"
else
  git checkout -b "$BASE_BRANCH" "origin/$BASE_BRANCH"
fi

git pull --ff-only origin "$BASE_BRANCH"

if git merge-base --is-ancestor "$UPSTREAM_NAME/$UPSTREAM_BRANCH" "$BASE_BRANCH"; then
  echo "Already up to date: $BASE_BRANCH already contains $UPSTREAM_NAME/$UPSTREAM_BRANCH"
  exit 0
fi

STAMP=$(date +%Y%m%d-%H%M%S)
SYNC_BRANCH="sync/${UPSTREAM_NAME}-${UPSTREAM_BRANCH}-${STAMP}"
SYNC_BRANCH=${SYNC_BRANCH//\//-}

git checkout -b "$SYNC_BRANCH"

echo "Merging $UPSTREAM_NAME/$UPSTREAM_BRANCH into $SYNC_BRANCH..."
if ! git merge --no-ff --no-edit "$UPSTREAM_NAME/$UPSTREAM_BRANCH"; then
  echo
  echo "Merge has conflicts. Resolve conflicts, run tests, then push manually:" >&2
  echo "  git add <files> && git commit" >&2
  echo "  git push -u origin $SYNC_BRANCH" >&2
  exit 3
fi

if [[ "$DO_PUSH" -eq 1 ]]; then
  git push -u origin "$SYNC_BRANCH"
  if [[ "$OPEN_PR" -eq 1 ]]; then
    if command -v gh >/dev/null 2>&1; then
      gh pr create \
        --base "$BASE_BRANCH" \
        --head "$SYNC_BRANCH" \
        --title "Sync $UPSTREAM_NAME/$UPSTREAM_BRANCH into $BASE_BRANCH" \
        --body "Automated upstream sync.\n\n- Source: \\`$UPSTREAM_NAME/$UPSTREAM_BRANCH\\`\n- Target: \\`$BASE_BRANCH\\`\n\nThis PR merges public upstream fixes into private origin without publishing private code."
    else
      echo "gh CLI not found; skipping PR creation."
    fi
  fi
else
  echo "Skipping push (--no-push)."
fi

echo "Done. Synced public upstream into private branch: $SYNC_BRANCH"
