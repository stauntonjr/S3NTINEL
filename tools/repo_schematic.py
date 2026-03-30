#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
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


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def unparse(node: ast.AST | None) -> str:
    if node is None:
        return ""
    try:
        return ast.unparse(node)
    except Exception:
        return "<expr>"


def format_arg(arg: ast.arg, default: ast.AST | None = None) -> str:
    text = arg.arg
    if arg.annotation:
        text += f": {unparse(arg.annotation)}"
    if default is not None:
        text += f" = {unparse(default)}"
    return text


def format_arguments(args: ast.arguments) -> str:
    parts: list[str] = []

    posonly = list(args.posonlyargs)
    regular = list(args.args)

    defaults = list(args.defaults)
    all_pos = posonly + regular
    default_offset = len(all_pos) - len(defaults)
    default_map: dict[int, ast.AST] = {
        i + default_offset: default for i, default in enumerate(defaults)
    }

    for i, arg in enumerate(posonly):
        parts.append(format_arg(arg, default_map.get(i)))
    if posonly:
        parts.append("/")

    for i, arg in enumerate(regular, start=len(posonly)):
        parts.append(format_arg(arg, default_map.get(i)))

    if args.vararg:
        vararg = "*" + format_arg(args.vararg)
        parts.append(vararg)
    elif args.kwonlyargs:
        parts.append("*")

    for kwarg, default in zip(args.kwonlyargs, args.kw_defaults):
        parts.append(format_arg(kwarg, default))

    if args.kwarg:
        parts.append("**" + format_arg(args.kwarg))

    return ", ".join(parts)


def format_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    args = format_arguments(node.args)
    ret = f" -> {unparse(node.returns)}" if node.returns else ""
    prefix = "async def" if isinstance(node, ast.AsyncFunctionDef) else "def"
    return f"{prefix} {node.name}({args}){ret}"


def get_doc_first_line(node: ast.AST) -> str | None:
    doc = ast.get_docstring(node)
    if not doc:
        return None
    line = doc.strip().splitlines()[0].strip()
    return line or None


def decorator_names(node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) -> list[str]:
    return [unparse(d) for d in node.decorator_list]


def target_names(target: ast.AST) -> list[str]:
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        names: list[str] = []
        for elt in target.elts:
            names.extend(target_names(elt))
        return names
    return []


@dataclass
class FunctionInfo:
    signature: str
    decorators: list[str] = field(default_factory=list)
    doc: str | None = None


@dataclass
class ClassInfo:
    name: str
    decorators: list[str] = field(default_factory=list)
    bases: list[str] = field(default_factory=list)
    doc: str | None = None
    class_attributes: list[str] = field(default_factory=list)
    methods: list[FunctionInfo] = field(default_factory=list)


@dataclass
class ModuleInfo:
    path: Path
    doc: str | None = None
    imports: list[str] = field(default_factory=list)
    constants: list[str] = field(default_factory=list)
    functions: list[FunctionInfo] = field(default_factory=list)
    classes: list[ClassInfo] = field(default_factory=list)
    parse_error: str | None = None


class ModuleVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.info = ModuleInfo(path=Path("."))

    def visit_Import(self, node: ast.Import) -> None:
        names = []
        for alias in node.names:
            if alias.asname:
                names.append(f"{alias.name} as {alias.asname}")
            else:
                names.append(alias.name)
        self.info.imports.append(f"import {', '.join(names)}")

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = "." * node.level + (node.module or "")
        names = []
        for alias in node.names:
            if alias.asname:
                names.append(f"{alias.name} as {alias.asname}")
            else:
                names.append(alias.name)
        self.info.imports.append(f"from {module} import {', '.join(names)}")

    def visit_Assign(self, node: ast.Assign) -> None:
        if all(isinstance(t, ast.Name) and t.id.isupper() for t in node.targets):
            for t in node.targets:
                self.info.constants.extend(target_names(t))

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if isinstance(node.target, ast.Name) and node.target.id.isupper():
            self.info.constants.append(node.target.id)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.info.functions.append(
            FunctionInfo(
                signature=format_signature(node),
                decorators=decorator_names(node),
                doc=get_doc_first_line(node),
            )
        )

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.info.functions.append(
            FunctionInfo(
                signature=format_signature(node),
                decorators=decorator_names(node),
                doc=get_doc_first_line(node),
            )
        )

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        c = ClassInfo(
            name=node.name,
            decorators=decorator_names(node),
            bases=[unparse(b) for b in node.bases],
            doc=get_doc_first_line(node),
        )

        for item in node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                c.methods.append(
                    FunctionInfo(
                        signature=format_signature(item),
                        decorators=decorator_names(item),
                        doc=get_doc_first_line(item),
                    )
                )
            elif isinstance(item, ast.Assign):
                for target in item.targets:
                    for name in target_names(target):
                        c.class_attributes.append(name)
            elif isinstance(item, ast.AnnAssign):
                if isinstance(item.target, ast.Name):
                    c.class_attributes.append(item.target.id)

        self.info.classes.append(c)


def parse_module(path: Path, root: Path) -> ModuleInfo:
    text = read_text(path)
    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError as e:
        return ModuleInfo(
            path=path.relative_to(root),
            parse_error=f"{e.__class__.__name__}: {e}",
        )

    visitor = ModuleVisitor()
    visitor.info.path = path.relative_to(root)
    visitor.info.doc = get_doc_first_line(tree)

    for node in tree.body:
        visitor.visit(node)

    return visitor.info


def should_skip(path: Path, excludes: set[str]) -> bool:
    return any(part in excludes for part in path.parts)


def iter_python_files(root: Path, excludes: set[str]) -> Iterable[Path]:
    for path in sorted(root.rglob("*.py")):
        if should_skip(path.relative_to(root), excludes):
            continue
        yield path


def render_module(
    info: ModuleInfo,
    *,
    show_doc: bool,
    show_imports: bool,
    show_constants: bool,
    show_function_docs: bool,
    show_method_docs: bool,
) -> str:
    lines: list[str] = []
    lines.append(f"{info.path}")

    if info.parse_error:
        lines.append(f"  !! {info.parse_error}")
        return "\n".join(lines)

    if show_doc and info.doc:
        lines.append(f"  doc: {info.doc}")

    if show_imports and info.imports:
        lines.append("  imports:")
        for imp in info.imports:
            lines.append(f"    - {imp}")

    if show_constants and info.constants:
        lines.append("  constants:")
        for name in sorted(set(info.constants)):
            lines.append(f"    - {name}")

    if info.functions:
        lines.append("  functions:")
        for fn in info.functions:
            for dec in fn.decorators:
                lines.append(f"    - @{dec}")
            lines.append(f"    - {fn.signature}")
            if show_function_docs and fn.doc:
                lines.append(f"      doc: {fn.doc}")

    if info.classes:
        lines.append("  classes:")
        for cls in info.classes:
            dec_prefix = " ".join(f"@{d}" for d in cls.decorators)
            bases = f"({', '.join(cls.bases)})" if cls.bases else ""
            if dec_prefix:
                lines.append(f"    - {dec_prefix} class {cls.name}{bases}")
            else:
                lines.append(f"    - class {cls.name}{bases}")
            if show_doc and cls.doc:
                lines.append(f"      doc: {cls.doc}")
            if cls.class_attributes:
                lines.append("      attributes:")
                for attr in cls.class_attributes:
                    lines.append(f"        - {attr}")
            if cls.methods:
                lines.append("      methods:")
                for method in cls.methods:
                    for dec in method.decorators:
                        lines.append(f"        - @{dec}")
                    lines.append(f"        - {method.signature}")
                    if show_method_docs and method.doc:
                        lines.append(f"          doc: {method.doc}")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a concise schematic of a Python repo."
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
        "--no-doc",
        action="store_true",
        help="Hide module/class doc first lines.",
    )
    parser.add_argument(
        "--imports",
        action="store_true",
        help="Include import lines.",
    )
    parser.add_argument(
        "--constants",
        action="store_true",
        help="Include top-level ALL_CAPS constants.",
    )
    parser.add_argument(
        "--function-docs",
        action="store_true",
        help="Include first docstring line for top-level functions.",
    )
    parser.add_argument(
        "--method-docs",
        action="store_true",
        help="Include first docstring line for methods.",
    )

    args = parser.parse_args()
    root = Path(args.root).resolve()
    excludes = set(DEFAULT_EXCLUDES) | set(args.exclude)

    modules = [parse_module(path, root) for path in iter_python_files(root, excludes)]
    rendered = [
        render_module(
            m,
            show_doc=not args.no_doc,
            show_imports=args.imports,
            show_constants=args.constants,
            show_function_docs=args.function_docs,
            show_method_docs=args.method_docs,
        )
        for m in modules
    ]
    output = "\n\n".join(rendered).rstrip() + "\n"

    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
    else:
        print(output, end="")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
