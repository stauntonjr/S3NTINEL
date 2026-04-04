from __future__ import annotations

from pathlib import Path

from tools import repo_schematic


def test_parse_module_tracks_file_class_and_function_loc_spans(tmp_path):
    root = tmp_path
    module_path = root / "pkg" / "mod.py"
    module_path.parent.mkdir(parents=True, exist_ok=True)
    module_path.write_text(
        "\n".join(
            [
                '"""Example module."""',
                "",
                "VALUE = 1",
                "",
                "def top_level(x: int) -> int:",
                '    """Double the input."""',
                "    return x * 2",
                "",
                "class Example:",
                '    """Simple example."""',
                "",
                "    FLAG = True",
                "    count: int = 1",
                "    label: str = 'demo'",
                "",
                "    def method(self, value: int) -> int:",
                "        return value + 1",
                "",
            ]
        ),
        encoding="utf-8",
    )

    info = repo_schematic.parse_module(module_path, root)

    assert info.span_loc == 17
    assert info.functions[0].name == "top_level"
    assert info.functions[0].span_loc == 3
    assert info.classes[0].name == "Example"
    assert info.classes[0].span_loc == 9
    assert [field.name for field in info.classes[0].fields] == ["count", "label"]
    assert info.classes[0].fields[0].annotation == "int"
    assert info.classes[0].fields[0].default == "1"
    assert info.classes[0].methods[0].name == "method"
    assert info.classes[0].methods[0].span_loc == 2
