#!/usr/bin/env bash
# File: scripts/export_bundle.sh

set -euo pipefail

OUTPUT_PATH="${1:-s3ntinel.bundle}"
REFSPEC="${2:---all}"

echo "Creating git bundle: ${OUTPUT_PATH} (refspec: ${REFSPEC})"
git bundle create "${OUTPUT_PATH}" "${REFSPEC}"
echo "Bundle created at: ${OUTPUT_PATH}"
