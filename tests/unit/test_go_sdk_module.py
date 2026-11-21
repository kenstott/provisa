# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's Go SDK and the reader agree (REQ-1984): the module is the vanity path
provisa.dev/sdk/go, which the site's go-import page maps to the repository's provisa-sdk/go; the
reader looks for that path and the SDK's spelling; every exported function of the SDK is a
registration or an option the reader reads, each a marker whose body only returns; and the SDK has
no dependency and no test helper."""

# Requirements: REQ-1984
from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]
SDK = REPO / "provisa-sdk" / "go"
READER = REPO / "provisa" / "golang" / "reader" / "main.go"
PAGE = REPO / "site" / "sdk" / "go.html"
MODULE = "provisa.dev/sdk/go"

_FUNC = re.compile(r"^func (?P<name>[A-Z]\w*)(?:\[[^\]]*\])?\((?P<rest>.*)$", re.M)


def _reader_names(table: str) -> set[str]:
    body = re.search(
        rf"{table} = map\[string\]string\{{(?P<body>.*?)\n\t\}}", READER.read_text(), re.S
    )
    assert body is not None, table
    return set(re.findall(r'"([A-Za-z]+)": "', body["body"]))


def _go_files() -> list[Path]:
    return sorted(SDK.glob("*.go"))


def test_the_module_is_the_vanity_path_and_requires_nothing():
    text = (SDK / "go.mod").read_text()
    assert re.search(rf"^module {re.escape(MODULE)}$", text, re.M)
    assert "require" not in text
    assert not (SDK / "go.sum").exists()


class _Metas(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.found: dict[str, str] = {}

    def handle_starttag(self, tag, attrs):
        given = dict(attrs)
        name = given.get("name")
        if tag == "meta" and name:
            self.found[name] = given.get("content") or ""


def test_the_sites_go_import_page_maps_the_path_to_the_sdk_directory():
    metas = _Metas()
    metas.feed(PAGE.read_text())
    prefix, vcs, repo, subdir = metas.found["go-import"].split()
    assert (prefix, vcs, repo) == (MODULE, "git", "https://github.com/kenstott/provisa")
    assert REPO / subdir == SDK


def test_the_reader_reads_the_sdks_path_and_spelling():
    reader = READER.read_text()
    assert f'sdkPath     = "{MODULE}"' in reader
    sdk_spelling = re.search(r"^const Spelling = (\d+)$", (SDK / "provisa.go").read_text(), re.M)
    reader_spelling = re.search(r"sdkSpelling = (\d+)", reader)
    assert sdk_spelling and reader_spelling
    assert sdk_spelling.group(1) == reader_spelling.group(1)


def test_every_exported_function_is_a_registration_or_option_the_reader_reads():
    exported = {m["name"] for path in _go_files() for m in _FUNC.finditer(path.read_text())}
    assert exported == _reader_names("sdkRegistrations") | _reader_names("sdkOptions")


def test_every_function_is_a_marker_that_only_returns():
    for path in _go_files():
        lines = path.read_text().splitlines()
        for at, line in enumerate(lines):
            if not line.startswith("func "):
                continue
            if line.endswith("{"):
                assert lines[at + 1 : at + 3] == ["\treturn Decl{}", "}"], line
            else:
                assert re.search(r"\{ return \w+\{\} \}$|\{\}$", line), line


def test_the_sdk_imports_only_the_standard_library_and_has_no_test_helpers():
    for path in _go_files():
        imports = re.findall(r'^\t"([^"]+)"$', path.read_text(), re.M)
        assert all("." not in p.split("/")[0] for p in imports), imports
    assert not list(SDK.rglob("*_test.go"))
    assert [p.name for p in _go_files()] == ["provisa.go"]
