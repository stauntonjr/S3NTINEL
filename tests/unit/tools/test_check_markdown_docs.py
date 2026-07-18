from pathlib import Path

from tools.check_markdown_docs import check_repository


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_check_repository_accepts_document_and_root_relative_anchors(tmp_path: Path) -> None:
    write(tmp_path / "README.md", "# Root\n\n[Guide](docs/guide.md#an-anchor)\n")
    write(tmp_path / "docs/guide.md", "# Guide\n\n## An Anchor\n\n[Root](/README.md)\n")

    assert check_repository(tmp_path) == []


def test_check_repository_reports_missing_targets_and_anchors(tmp_path: Path) -> None:
    write(tmp_path / "README.md", "# Root\n\n[Missing](missing.md)\n[Bad](#unknown)\n")

    messages = [finding.message for finding in check_repository(tmp_path)]

    assert "local link target does not exist: missing.md" in messages
    assert "local link anchor does not exist: #unknown" in messages


def test_check_repository_excludes_generated_and_archived_docs_and_checks_plan_headers(tmp_path: Path) -> None:
    write(tmp_path / "docs/architecture/stale.md", "# Stale\n\n[Missing](missing.md)\n")
    write(tmp_path / "docs/archive/prior-chat.md", "# Historical\n\n[Missing](missing.md)\n")
    write(tmp_path / "docs/plans/example.md", "# Plan\n")

    messages = [finding.message for finding in check_repository(tmp_path)]

    assert "plan is missing a valid status line in its header" in messages
    assert "plan is missing the required authority line in its header" in messages
    assert all("missing.md" not in message for message in messages)
