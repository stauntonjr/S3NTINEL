#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-.}"
OUT_DIR="${2:-repo_maps}"

mkdir -p "$OUT_DIR"

python tools/repo_schematic.py "$ROOT" \
  -o "$OUT_DIR/repo_schematic.txt" \
  --imports \
  --exclude .codex

python tools/module_deps.py "$ROOT" \
  -o "$OUT_DIR/module_deps.txt" \
  --only-internal \
  --exclude .codex

python tools/module_deps.py "$ROOT" \
  -o "$OUT_DIR/reverse_deps.txt" \
  --reverse \
  --only-internal \
  --exclude .codex

python tools/module_deps.py "$ROOT" \
  -o "$OUT_DIR/module_edges.txt" \
  --edges \
  --only-internal \
  --exclude .codex

echo "Wrote:"
echo "  $OUT_DIR/repo_schematic.txt"
echo "  $OUT_DIR/module_deps.txt"
echo "  $OUT_DIR/reverse_deps.txt"
echo "  $OUT_DIR/module_edges.txt"
