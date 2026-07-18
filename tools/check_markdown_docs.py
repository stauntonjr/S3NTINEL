"""Offline validation for repository-owned Markdown documentation."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote


EXCLUDED_DIRECTORIES = {".git", ".specstory", "__pycache__"}
EXCLUDED_PATH_PREFIXES = ("docs/architecture/", "docs/archive/")
INLINE_LINK_RE = re.compile(r"(?<!!)\[[^\]]*\]\(([^)]+)\)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")


@dataclass(frozen=True)
class Finding:
    path: Path
    line: int
    message: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.message}"


def is_excluded(path: Path, root: Path) -> bool:
    relative = path.relative_to(root)
    relative_text = relative.as_posix()
    # Generated snapshots and historical captures are not active documentation contracts.
    return any(part in EXCLUDED_DIRECTORIES for part in relative.parts) or relative_text.startswith(
        EXCLUDED_PATH_PREFIXES
    )


def markdown_paths(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.md") if not is_excluded(path, root))


def github_anchor(text: str) -> str:
    """Create the GitHub-compatible anchor used by this repository's headings."""
    normalized = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text).strip().lower()
    normalized = re.sub(r"[^\w\s-]", "", normalized)
    return re.sub(r"[\s-]+", "-", normalized).strip("-")


def headings(path: Path) -> tuple[list[tuple[int, int]], set[str]]:
    h1_lines: list[tuple[int, int]] = []
    anchors: set[str] = set()
    in_fence = False
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if line.lstrip().startswith(("```", "~~~")):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        match = HEADING_RE.match(line)
        if not match:
            continue
        level = len(match.group(1))
        if level == 1:
            h1_lines.append((line_number, level))
        anchors.add(github_anchor(match.group(2)))
    return h1_lines, anchors


def split_target(raw_target: str) -> tuple[str, str | None]:
    target = raw_target.strip()
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1]
    target = target.split(maxsplit=1)[0]
    if "#" not in target:
        return target, None
    path_part, anchor = target.split("#", maxsplit=1)
    return path_part, unquote(anchor)


def is_external(target: str) -> bool:
    return target.startswith(("http://", "https://", "mailto:", "tel:"))


def resolve_link(path: Path, root: Path, target_path: str) -> Path:
    if target_path.startswith("/"):
        return root / target_path.lstrip("/")
    return path.parent / target_path


def check_markdown_file(path: Path, root: Path) -> list[Finding]:
    findings: list[Finding] = []
    h1_lines, _ = headings(path)
    if len(h1_lines) > 1:
        for line_number, _ in h1_lines[1:]:
            findings.append(Finding(path, line_number, "duplicate top-level heading"))

    relative = path.relative_to(root)
    if relative.parts[:2] == ("docs", "plans") and path.name != "README.md":
        first_lines = path.read_text(encoding="utf-8").splitlines()[:8]
        if not any(line in {"Status: Plan", "Status: Completed"} for line in first_lines):
            findings.append(Finding(path, 1, "plan is missing a valid status line in its header"))
        authority = "Authority: Non-authoritative roadmap. Use package READMEs and docs/current/ for current behavior."
        normalized_first_lines = [line.replace("`", "") for line in first_lines]
        if authority not in normalized_first_lines:
            findings.append(Finding(path, 1, "plan is missing the required authority line in its header"))

    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        for match in INLINE_LINK_RE.finditer(line):
            target, anchor = split_target(match.group(1))
            if not target and not anchor:
                findings.append(Finding(path, line_number, "empty Markdown link target"))
                continue
            if is_external(target):
                continue
            if target.startswith("/") and target.startswith("/home/"):
                findings.append(Finding(path, line_number, "absolute filesystem link is not portable"))
                continue

            target_path = path if not target else resolve_link(path, root, target)
            if not target_path.exists():
                findings.append(
                    Finding(path, line_number, f"local link target does not exist: {match.group(1)}")
                )
                continue
            if anchor:
                _, target_anchors = headings(target_path)
                if github_anchor(anchor) not in target_anchors:
                    findings.append(
                        Finding(path, line_number, f"local link anchor does not exist: {match.group(1)}")
                    )
    return findings


def check_repository(root: Path) -> list[Finding]:
    root = root.resolve()
    findings: list[Finding] = []
    for path in markdown_paths(root):
        findings.extend(check_markdown_file(path, root))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="repository root to check")
    args = parser.parse_args()
    findings = check_repository(args.root)
    for finding in findings:
        print(finding)
    if findings:
        print(f"Markdown documentation check failed with {len(findings)} finding(s).")
        return 1
    print("Markdown documentation check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
