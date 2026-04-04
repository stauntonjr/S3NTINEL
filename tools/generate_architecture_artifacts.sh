#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-.}"
OUT_DIR="${2:-docs/architecture}"
ANNOTATIONS="${3:-docs/architecture/annotations.yaml}"

python tools/architecture_workflow.py \
  --root "$ROOT" \
  --output-dir "$OUT_DIR" \
  --annotations "$ANNOTATIONS" \
  render

echo "Wrote architecture artifacts to:"
echo "  $OUT_DIR"

