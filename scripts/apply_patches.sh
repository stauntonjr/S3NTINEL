#!/usr/bin/env bash
# File: scripts/apply_patches.sh

set -euo pipefail

PATCH_DIR="${1:-patches}"

if [[ ! -d "${PATCH_DIR}" ]]; then
  echo "Patch directory not found: ${PATCH_DIR}" >&2
  exit 1
fi

shopt -s nullglob
PATCHES=("${PATCH_DIR}"/*.patch)

if [[ ${#PATCHES[@]} -eq 0 ]]; then
  echo "No patch files found in ${PATCH_DIR}" >&2
  exit 1
fi

echo "Applying ${#PATCHES[@]} patches from ${PATCH_DIR}"
git am "${PATCHES[@]}"
echo "Patch apply complete"
