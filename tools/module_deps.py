#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


DEFAULT_EXCLUDES = {
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "node_modules",
    "dist",
    "build",
}


@dataclass(frozen=True)
class ImportRef:
    raw: str
    resolved: str | None
    kind: str  # "import" | "from"


@dataclass
class ModuleDeps:
    module: str
    path: Path
    imports: list[ImportRef] = field(default_factory=list)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def should_skip(path: Path, excludes: set[str]) -> bool:
    return any(part in excludes for part in path.parts)


def iter_python_files(root: Path, excludes: set[str]) -> Iterable[Path]:
    for path in sorted(root.rglob("*.py")):
        if should_skip(path.relative_to(root), excludes):
            continue
        yield path


def path_to_module(root: Path, path: Path) -> str:
    rel = path.relative_to(root)
    parts = list(rel.parts)
    if parts[-1] == "__init__.py":
        parts = parts[:-1]
    else:
        parts[-1] = parts[-1][:-3]
    return ".".join(parts)


def build_module_index(root: Path, excludes: set[str]) -> dict[str, Path]:
    index: dict[str, Path] = {}
    for path in iter_python_files(root, excludes):
        index[path_to_module(root, path)] = path
    return index


def parent_module(module: str) -> str:
    if "." not in module:
        return ""
    return module.rsplit(".", 1)[0]


def package_of_module(module: str) -> str:
    return module if module and not module.endswith(".__init__") else module


def resolve_relative_base(current_module: str, level: int) -> str:
    """
    For `from .x import y`, level=1 means current package.
    For `from ..x import y`, level=2 means parent package, etc.
    """
    parts = current_module.split(".")
    if not parts:
        return ""

    # current module -> containing package
    if parts:
        parts = parts[:-1]

    for _ in range(level - 1):
        if parts:
            parts = parts[:-1]

    return ".".join(parts)


def best_existing_prefix(name: str, module_index: dict[str, Path]) -> str | None:
    """
    Return the longest existing module/package prefix in the repo.
    Example:
      import libs.reporting.base -> libs.reporting.base
      import libs.reporting -> libs.reporting
      import libs.reporting.base.Report -> libs.reporting.base (if present)
    """
    candidate = name
    while candidate:
        if candidate in module_index:
            return candidate
        if "." not in candidate:
            break
        candidate = candidate.rsplit(".", 1)[0]
    return None


def resolve_from_import(
    current_module: str,
    level: int,
    module: str | None,
    imported_name: str,
    module_index: dict[str, Path],
) -> str | None:
    base = resolve_relative_base(current_module, level) if level else ""
    if module:
        full_base = f"{base}.{module}" if base else module
    else:
        full_base = base

    full_base = full_base.strip(".")

    # Try module itself first.
    if full_base:
        existing = best_existing_prefix(full_base, module_index)
        if existing:
            # If imported_name refers to a submodule, prefer that if it exists.
            sub = f"{full_base}.{imported_name}"
            sub_existing = best_existing_prefix(sub, module_index)
            return sub_existing or existing

    # Handle `from pkg import mod`
    if full_base:
        candidate = f"{full_base}.{imported_name}"
        existing = best_existing_prefix(candidate, module_index)
        if existing:
            return existing

    return None


def resolve_import(name: str, module_index: dict[str, Path]) -> str | None:
    return best_existing_prefix(name, module_index)


class ImportVisitor(ast.NodeVisitor):
    def __init__(self, current_module: str, module_index: dict[str, Path]) -> None:
        self.current_module = current_module
        self.module_index = module_index
        self.imports: list[ImportRef] = []

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            raw = alias.name
            resolved = resolve_import(alias.name, self.module_index)
            self.imports.append(ImportRef(raw=raw, resolved=resolved, kind="import"))

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        mod = node.module
        level = node.level or 0

        for alias in node.names:
            if alias.name == "*":
                raw = f"from {'.' * level}{mod or ''} import *"
                resolved = resolve_from_import(
                    self.current_module,
                    level,
                    mod,
                    "",
                    self.module_index,
                )
            else:
                raw = f"from {'.' * level}{mod or ''} import {alias.name}"
                resolved = resolve_from_import(
                    self.current_module,
                    level,
                    mod,
                    alias.name,
                    self.module_index,
                )
            self.imports.append(ImportRef(raw=raw, resolved=resolved, kind="from"))


def parse_module_deps(
    root: Path,
    path: Path,
    module_index: dict[str, Path],
) -> ModuleDeps:
    module_name = path_to_module(root, path)
    deps = ModuleDeps(module=module_name, path=path.relative_to(root))

    try:
        tree = ast.parse(read_text(path), filename=str(path))
    except SyntaxError:
        return deps

    visitor = ImportVisitor(module_name, module_index)
    visitor.visit(tree)
    deps.imports = visitor.imports
    return deps


def render_by_module(
    modules: list[ModuleDeps],
    *,
    only_internal: bool,
    include_unresolved: bool,
    sort_deps: bool,
) -> str:
    lines: list[str] = []

    for mod in modules:
        lines.append(f"{mod.module}  ({mod.path})")

        deps: list[str] = []
        unresolved: list[str] = []

        for imp in mod.imports:
            if imp.resolved:
                deps.append(imp.resolved)
            elif include_unresolved:
                unresolved.append(imp.raw)

        if sort_deps:
            deps = sorted(set(deps))
            unresolved = sorted(set(unresolved))

        if deps:
            lines.append("  -> internal:")
            for dep in deps:
                if dep != mod.module:
                    lines.append(f"     - {dep}")

        if include_unresolved and unresolved and not only_internal:
            lines.append("  -> external/unresolved:")
            for raw in unresolved:
                lines.append(f"     - {raw}")

        if len(lines) and lines[-1] == f"{mod.module}  ({mod.path})":
            lines.append("  -> none")

        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def render_reverse(
    modules: list[ModuleDeps],
    *,
    sort_deps: bool,
) -> str:
    reverse: dict[str, list[str]] = defaultdict(list)

    for mod in modules:
        seen: set[str] = set()
        for imp in mod.imports:
            if imp.resolved and imp.resolved != mod.module and imp.resolved not in seen:
                reverse[imp.resolved].append(mod.module)
                seen.add(imp.resolved)

    keys = sorted(reverse) if sort_deps else list(reverse)

    lines: list[str] = []
    for target in keys:
        lines.append(target)
        users = sorted(set(reverse[target])) if sort_deps else reverse[target]
        for user in users:
            lines.append(f"  <- {user}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def render_edges(modules: list[ModuleDeps], *, sort_deps: bool) -> str:
    edges: set[tuple[str, str]] = set()
    for mod in modules:
        for imp in mod.imports:
            if imp.resolved and imp.resolved != mod.module:
                edges.add((mod.module, imp.resolved))

    ordered = sorted(edges) if sort_deps else list(edges)
    return "\n".join(f"{src} -> {dst}" for src, dst in ordered) + ("\n" if ordered else "")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a module dependency sketch for a Python repo."
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=".",
        help="Repo root to scan (default: current directory).",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Write output to this file instead of stdout.",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Directory/file segment to exclude. Can be provided multiple times.",
    )
    parser.add_argument(
        "--only-internal",
        action="store_true",
        help="Show only dependencies resolved to repo modules.",
    )
    parser.add_argument(
        "--include-unresolved",
        action="store_true",
        help="Also show imports that do not resolve to repo modules.",
    )
    parser.add_argument(
        "--reverse",
        action="store_true",
        help="Show reverse dependencies (who imports a module).",
    )
    parser.add_argument(
        "--edges",
        action="store_true",
        help="Output as flat 'src -> dst' edges instead of grouped blocks.",
    )
    parser.add_argument(
        "--modules-prefix",
        action="append",
        default=[],
        help="Only include modules whose dotted name starts with this prefix. Repeatable.",
    )
    parser.add_argument(
        "--paths-prefix",
        action="append",
        default=[],
        help="Only include files whose relative path starts with this prefix. Repeatable.",
    )
    parser.add_argument(
        "--no-sort",
        action="store_true",
        help="Do not sort dependency lists.",
    )

    args = parser.parse_args()
    root = Path(args.root).resolve()
    excludes = set(DEFAULT_EXCLUDES) | set(args.exclude)

    module_index = build_module_index(root, excludes)

    modules = [
        parse_module_deps(root, path, module_index)
        for path in iter_python_files(root, excludes)
    ]

    if args.modules_prefix:
        prefixes = tuple(args.modules_prefix)
        modules = [m for m in modules if m.module.startswith(prefixes)]

    if args.paths_prefix:
        path_prefixes = tuple(args.paths_prefix)
        modules = [m for m in modules if str(m.path).startswith(path_prefixes)]

    sort_deps = not args.no_sort

    if args.edges:
        output = render_edges(modules, sort_deps=sort_deps)
    elif args.reverse:
        output = render_reverse(modules, sort_deps=sort_deps)
    else:
        output = render_by_module(
            modules,
            only_internal=args.only_internal,
            include_unresolved=args.include_unresolved,
            sort_deps=sort_deps,
        )

    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
    else:
        print(output, end="")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
