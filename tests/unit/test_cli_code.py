# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6a9d41-7c83-4e15-a0b8-5d3e1c94f2a7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``provisa code check`` (REQ-1984): exit codes, JSON equal to manifest_json, warnings."""

# Requirements: REQ-1984
from __future__ import annotations

import json
import sys
import textwrap
from pathlib import Path

import pytest

from provisa import cli, cli_code
from provisa.codesource.manifest import PYTHON_RULES, manifest_json, read_manifest

pytestmark = pytest.mark.unit

_GOOD = """
from typing import TypedDict

class Row(TypedDict):
    id: int
    name: str

def customers(region: str) -> list[Row]:
    \"\"\"@table\"\"\"
    return []

def helper() -> int:
    return 1
"""


def _bundle(tmp_path: Path, source: str) -> Path:
    code = tmp_path / "bundle"
    code.mkdir()
    (code / "mod.py").write_text(textwrap.dedent(source))
    return code


def _run(*argv: str) -> int:
    args = cli.build_parser().parse_args(["code", "check", *argv])
    return args.func(args)


def test_a_clean_bundle_exits_0_and_prints_the_manifest(tmp_path, capsys):
    code = _bundle(tmp_path, _GOOD)
    assert _run(str(code), "--python", sys.executable) == 0
    out = capsys.readouterr().out
    assert "table customers" in out
    assert "param region: text (required)" in out
    assert "manifest_hash " in out


def test_json_is_exactly_manifest_json(tmp_path, capsys):
    code = _bundle(tmp_path, _GOOD)
    assert _run(str(code), "--python", sys.executable, "--json") == 0
    printed = json.loads(capsys.readouterr().out)
    _manifest, raw, _warnings = cli_code.check(code, None, None, Path(sys.executable))
    assert printed == manifest_json(read_manifest(raw, PYTHON_RULES))


def test_an_untagged_public_function_is_a_warning_and_exit_0(tmp_path, capsys):
    code = _bundle(tmp_path, _GOOD)
    assert _run(str(code), "--python", sys.executable) == 0
    err = capsys.readouterr().err
    assert "warning: helper: a public function with no Provisa tag" in err


def test_a_wrong_interpreter_version_is_a_warning(tmp_path, capsys):
    code = _bundle(tmp_path, _GOOD)
    _run(str(code), "--python", sys.executable)
    err = capsys.readouterr().err
    if sys.version_info[:2] == cli_code.PINNED_PYTHON:
        assert "Provisa runs code under" not in err
    else:
        assert "Provisa runs code under 3.12" in err


def test_a_refused_manifest_exits_1_naming_every_problem(tmp_path, capsys):
    code = _bundle(
        tmp_path,
        """
        from typing import Any

        def a() -> list[dict]:
            \"\"\"@table\"\"\"
            return []

        def b() -> list[Any]:
            \"\"\"@table\"\"\"
            return []
        """,
    )
    assert _run(str(code), "--python", sys.executable) == 1
    lines = [ln for ln in capsys.readouterr().err.splitlines() if ln]
    assert any(ln.startswith("a:") for ln in lines)
    assert any(ln.startswith("b:") for ln in lines)


def test_an_unknown_tag_exits_1(tmp_path, capsys):
    code = _bundle(
        tmp_path,
        """
        def a() -> list[int]:
            \"\"\"@table
            @bogus\"\"\"
            return []
        """,
    )
    assert _run(str(code), "--python", sys.executable) == 1
    assert "unknown tag @bogus" in capsys.readouterr().err


def test_a_missing_directory_exits_2(tmp_path, capsys):
    assert _run(str(tmp_path / "nope"), "--python", sys.executable) == 2
    assert "is not a directory" in capsys.readouterr().err


def test_a_missing_interpreter_exits_2(tmp_path, capsys):
    code = _bundle(tmp_path, _GOOD)
    assert _run(str(code)) == 2
    assert "pass --python" in capsys.readouterr().err


def test_no_entry_exits_2(tmp_path, capsys):
    code = tmp_path / "empty"
    code.mkdir()
    assert _run(str(code), "--python", sys.executable) == 2
    assert "no top-level Python module" in capsys.readouterr().err


def test_an_ambiguous_entry_exits_2_and_naming_one_reads(tmp_path, capsys):
    code = _bundle(tmp_path, _GOOD)
    (code / "other.py").write_text("")
    assert _run(str(code), "--python", sys.executable) == 2
    assert "2 top-level modules" in capsys.readouterr().err
    assert _run(str(code), "--python", sys.executable, "--entry", "mod") == 0


def test_typescript_without_declarations_exits_2(tmp_path, capsys, monkeypatch):
    code = tmp_path / "ts"
    code.mkdir()
    (code / "package.json").write_text("{}")
    assert _run(str(code)) == 2
    assert "no TypeScript declarations" in capsys.readouterr().err


def test_typescript_is_read_through_the_node_reader(tmp_path, capsys, monkeypatch):
    code = tmp_path / "ts"
    code.mkdir()
    (code / "index.d.ts").write_text("")
    raw = {
        "exports": [
            {
                "name": "ping",
                "tags": ["command"],
                "tag_arguments": [],
                "signatures": [{"parameters": [], "returns": {"kind": "string"}}],
            }
        ],
        "types": {},
        "diagnostics": [],
    }
    monkeypatch.setattr(cli_code, "_read_typescript", lambda _code: (raw, []))
    assert _run(str(code), "--json") == 0
    assert json.loads(capsys.readouterr().out)["exports"][0]["name"] == "ping"


def test_a_reader_diagnostic_exits_1(tmp_path, capsys, monkeypatch):
    code = tmp_path / "ts"
    code.mkdir()
    (code / "index.d.ts").write_text("")
    raw = {"exports": [], "types": {}, "diagnostics": ["index.d.ts(1,1): error TS1005"]}
    monkeypatch.setattr(cli_code, "_read_typescript", lambda _code: (raw, []))
    assert _run(str(code)) == 1
    assert "do not type-check" in capsys.readouterr().err


def test_the_typescript_launcher_runs_uvx_provisa_at_the_packages_version():
    import shutil
    import subprocess

    node = shutil.which("node")
    assert node is not None, "node is required for the launcher test"
    package = Path(__file__).resolve().parents[2] / "provisa-sdk" / "typescript"
    version = json.loads((package / "package.json").read_text())["version"]
    script = (
        f"import {{ command }} from {json.dumps(str(package / 'bin' / 'check.js'))};"
        "console.log(JSON.stringify(command(['check','proj','--json'], process.argv[1])));"
    )
    done = subprocess.run(
        [node, "--input-type=module", "-e", script, version],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(done.stdout) == [
        "uvx",
        f"provisa=={version}",
        "code",
        "check",
        "proj",
        "--json",
    ]
