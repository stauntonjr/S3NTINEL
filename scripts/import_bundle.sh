#!/usr/bin/env bash
# File: scripts/import_bundle.sh

set -euo pipefail

BUNDLE_PATH="${1:-s3ntinel.bundle}"
REMOTE_NAME="${2:-handoff}"

if [[ ! -f "${BUNDLE_PATH}" ]]; then
  echo "Bundle not found: ${BUNDLE_PATH}" >&2
  exit 1
fi

if git remote get-url "${REMOTE_NAME}" >/dev/null 2>&1; then
  git remote remove "${REMOTE_NAME}"
fi

git remote add "${REMOTE_NAME}" "${BUNDLE_PATH}"
echo "Fetching from bundle remote '${REMOTE_NAME}'"
git fetch "${REMOTE_NAME}" --tags
echo "Bundle import complete. Inspect refs with: git branch -a"
