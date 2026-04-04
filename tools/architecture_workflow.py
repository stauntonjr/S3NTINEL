#!/usr/bin/env python3
"""Repo architecture extraction and rendering workflow."""

from __future__ import annotations

import argparse
import difflib
import json
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from libs.architecture.ai_review import AI_REVIEW_FILENAMES, write_ai_review_bundle
from libs.architecture.extract import build_architecture_bundle
from libs.architecture.render import RENDERED_FILENAMES, write_render_outputs
from tools import module_deps, repo_schematic


RAW_FILENAMES = (
    "raw/repo_schematic.txt",
    "raw/repo_schematic.json",
    "raw/module_deps.txt",
    "raw/module_deps.json",
    "raw/reverse_deps.txt",
    "raw/module_edges.txt",
)
FACT_FILENAME = "architecture_facts.json"
MANIFEST_FILENAME = "generation_manifest.json"


def _relative_posix(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _repo_map_modules(root: Path, focus_paths: tuple[str, ...]):
    excludes = set(repo_schematic.DEFAULT_EXCLUDES) | {".codex"}
    files = [
        path
        for path in repo_schematic.iter_python_files(root, excludes)
        if _relative_posix(path, root).startswith(focus_paths)
    ]
    module_index = module_deps.build_module_index(root, excludes)
    schematic_modules = [repo_schematic.parse_module(path, root) for path in files]
    dependency_modules = [module_deps.parse_module_deps(root, path, module_index) for path in files]
    return schematic_modules, dependency_modules


def _write_raw_maps(bundle, output_dir: Path) -> list[str]:
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    root = Path(bundle.root)
    schematic_modules, dependency_modules = _repo_map_modules(root, bundle.annotations.focus_paths)

    repo_schematic_txt = "\n\n".join(
        repo_schematic.render_module(
            module,
            show_doc=True,
            show_imports=True,
            show_constants=True,
            show_function_docs=False,
            show_method_docs=False,
        )
        for module in schematic_modules
    ).rstrip() + "\n"
    repo_schematic_json = json.dumps(
        {
            "root": bundle.root,
            "modules": [repo_schematic.module_payload(module) for module in schematic_modules],
        },
        indent=2,
        sort_keys=True,
    ) + "\n"

    module_deps_txt = module_deps.render_by_module(
        dependency_modules,
        only_internal=True,
        include_unresolved=False,
        sort_deps=True,
    )
    module_deps_json = json.dumps(
        {
            "root": bundle.root,
            "modules": module_deps.module_payload(
                dependency_modules,
                include_unresolved=False,
                sort_deps=True,
            ),
        },
        indent=2,
        sort_keys=True,
    ) + "\n"

    raw_files = {
        "raw/repo_schematic.txt": repo_schematic_txt,
        "raw/repo_schematic.json": repo_schematic_json,
        "raw/module_deps.txt": module_deps_txt,
        "raw/module_deps.json": module_deps_json,
        "raw/reverse_deps.txt": module_deps.render_reverse(dependency_modules, sort_deps=True),
        "raw/module_edges.txt": module_deps.render_edges(dependency_modules, sort_deps=True),
    }

    for relative_path, content in raw_files.items():
        path = output_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    return list(raw_files)


def _write_bundle_payload(bundle, output_dir: Path) -> list[str]:
    (output_dir / FACT_FILENAME).write_text(
        json.dumps(bundle.to_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return [FACT_FILENAME]


def _write_manifest(
    output_dir: Path,
    *,
    raw_files: list[str],
    rendered_files: list[str],
    include_ai_review: bool,
) -> list[str]:
    manifest = {
        "raw_files": sorted(raw_files),
        "fact_files": [FACT_FILENAME],
        "rendered_files": sorted(rendered_files),
        "ai_review_files": list(AI_REVIEW_FILENAMES) if include_ai_review else [],
    }
    (output_dir / MANIFEST_FILENAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return [MANIFEST_FILENAME]


def run_extract(root: Path, output_dir: Path, annotations_path: Path) -> list[str]:
    bundle = build_architecture_bundle(root, annotations_path)
    raw_files = _write_raw_maps(bundle, output_dir)
    fact_files = _write_bundle_payload(bundle, output_dir)
    manifest_files = _write_manifest(output_dir, raw_files=raw_files, rendered_files=[], include_ai_review=False)
    return raw_files + fact_files + manifest_files


def run_render(root: Path, output_dir: Path, annotations_path: Path) -> list[str]:
    bundle = build_architecture_bundle(root, annotations_path)
    raw_files = _write_raw_maps(bundle, output_dir)
    fact_files = _write_bundle_payload(bundle, output_dir)
    rendered_files = write_render_outputs(bundle, output_dir)
    manifest_files = _write_manifest(
        output_dir,
        raw_files=raw_files,
        rendered_files=rendered_files,
        include_ai_review=False,
    )
    return raw_files + fact_files + rendered_files + manifest_files


def run_ai_draft(root: Path, output_dir: Path, annotations_path: Path) -> list[str]:
    bundle = build_architecture_bundle(root, annotations_path)
    ai_dir = output_dir / "ai_review"
    written = write_ai_review_bundle(bundle, ai_dir)
    return [f"ai_review/{name}" for name in written]


def _expected_check_files() -> tuple[str, ...]:
    return RAW_FILENAMES + (FACT_FILENAME,) + RENDERED_FILENAMES + (MANIFEST_FILENAME,)


def run_check(root: Path, output_dir: Path, annotations_path: Path) -> int:
    expected_files = _expected_check_files()
    with tempfile.TemporaryDirectory(prefix="architecture_check_") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        run_render(root, temp_dir, annotations_path)

        diffs: list[str] = []
        for relative_path in expected_files:
            existing = output_dir / relative_path
            candidate = temp_dir / relative_path
            if not existing.exists():
                diffs.append(f"missing checked-in file: {relative_path}")
                continue
            if existing.read_text(encoding="utf-8") == candidate.read_text(encoding="utf-8"):
                continue
            diff = "\n".join(
                difflib.unified_diff(
                    existing.read_text(encoding="utf-8").splitlines(),
                    candidate.read_text(encoding="utf-8").splitlines(),
                    fromfile=str(existing),
                    tofile=str(candidate),
                    lineterm="",
                )
            )
            diffs.append(f"drift detected for {relative_path}\n{diff}")

        if diffs:
            raise SystemExit("\n\n".join(diffs))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate repo architecture artifacts.")
    parser.add_argument(
        "--root",
        default=".",
        help="Repo root to scan (default: current directory).",
    )
    parser.add_argument(
        "--output-dir",
        default="docs/architecture",
        help="Directory for checked-in architecture artifacts.",
    )
    parser.add_argument(
        "--annotations",
        default="docs/architecture/annotations.yaml",
        help="Path to the checked-in architecture annotations YAML.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("extract", help="Write raw repo maps and normalized architecture facts.")
    subparsers.add_parser("render", help="Write raw maps, facts, DSL, docs, and export assets.")
    subparsers.add_parser("ai-draft", help="Write optional AI review input and prompt packaging.")
    subparsers.add_parser("check", help="Fail if checked-in generated artifacts drift from current sources.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    root = Path(args.root).resolve()
    output_dir = (root / args.output_dir).resolve()
    annotations_path = (root / args.annotations).resolve()

    if args.command == "extract":
        run_extract(root, output_dir, annotations_path)
        return 0
    if args.command == "render":
        run_render(root, output_dir, annotations_path)
        return 0
    if args.command == "ai-draft":
        run_ai_draft(root, output_dir, annotations_path)
        return 0
    if args.command == "check":
        return run_check(root, output_dir, annotations_path)
    raise SystemExit(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
