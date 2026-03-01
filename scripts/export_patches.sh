#!/usr/bin/env bash
# File: scripts/export_patches.sh

set -euo pipefail

BASE_REF="${1:-origin/main}"
OUTPUT_DIR="${2:-patches}"

mkdir -p "${OUTPUT_DIR}"

echo "Exporting patches from ${BASE_REF}..HEAD into ${OUTPUT_DIR}"
git format-patch "${BASE_REF}..HEAD" -o "${OUTPUT_DIR}"
echo "Patch export complete: ${OUTPUT_DIR}"
