# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Code entered here (REQ-1993, amended 2026-11-25): code written in the source form, kept in the
source's mapping, laid out by its language as a bundle, and hashed, read and run on the uploaded
archive's path, in every code source language."""

# Requirements: REQ-1993, REQ-1984, REQ-2200, REQ-2227
from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import textwrap
import tomllib
from pathlib import Path

import pytest

from provisa.codesource import entered
from provisa.codesource.bundle import BundleRefused, content_hash, unpack
from provisa.codesource.install import UploadCode, ensure_installed
from provisa.codesource.languages import LANGUAGES
from provisa.codesource.manifest import PYTHON_RULES, read_manifest
from provisa.codesource.source import _code_reference, config_of

pytestmark = pytest.mark.unit

PYTHON_CODE = textwrap.dedent(
    '''
    from dataclasses import dataclass


    @dataclass
    class Rate:
        currency: str
        rate: float


    def rates() -> list[Rate]:
        """Today's rates.

        @table
        """
        return [Rate("EUR", 1.08), Rate("GBP", 1.27)]
    '''
)

CODE = {
    "python": PYTHON_CODE,
    "node": "/** @table\n * @returns {{currency: string}[]} */\nexport function rates() {\n"
    '  return [{ currency: "EUR" }];\n}\n',
    "go": "package entered\n\n// Rates are today's rates.\n//\n// @table\nfunc Rates() []string "
    '{ return []string{"EUR"} }\n',
    "rust": '/// @table\npub fn rates() -> Vec<String> { vec!["EUR".into()] }\n',
}
DEPENDENCIES = {
    "python": ["httpx==0.27.0"],
    "node": ["@acme/rates@1.2.0"],
    "go": ["github.com/acme/rates v1.2.0"],
    "rust": ["serde@1.0.200"],
}


def _mapping(language: str, **code: object) -> dict:
    runtime = LANGUAGES[language].runtimes[0]  # Node names one of its three
    return {"runtime": runtime, "code": {"kind": entered.ENTERED, "source": CODE[language], **code}}


def _unpacked(upload: UploadCode, dest: Path) -> Path:
    assert upload.filename == entered.ARCHIVE
    return unpack(upload.filename, upload.data, dest)


# -- storing: the mapping ----------------------------------------------------------------------


@pytest.mark.parametrize("language", sorted(CODE))
def test_entered_code_is_a_code_source_reference_in_every_language(language):
    config = config_of(language, _mapping(language, dependencies=DEPENDENCIES[language]))
    assert config.code["kind"] == "entered"
    assert config.code["source"] == CODE[language]
    assert config.code["dependencies"] == DEPENDENCIES[language]


@pytest.mark.parametrize("language", sorted(CODE))
def test_entered_code_with_no_text_is_refused_by_name(language):
    with pytest.raises(BundleRefused) as refused:
        config_of(language, {**_mapping(language), "code": {"kind": "entered", "source": ""}})
    assert refused.value.code == "codesource.code_incomplete"
    assert refused.value.params == {"missing": "source"}


def test_a_dependency_list_that_is_not_lines_is_refused_by_name():
    with pytest.raises(BundleRefused) as refused:
        config_of("python", _mapping("python", dependencies="httpx==0.27.0"))
    assert refused.value.code == "codesource.entered_dependencies"
    with pytest.raises(BundleRefused):
        config_of("python", _mapping("python", dependencies=["httpx", "  "]))


@pytest.mark.parametrize("line", ["rates", "rates@", "@rates", "@acme/rates"])
def test_a_name_at_version_line_without_both_is_refused_by_name(line):
    with pytest.raises(BundleRefused) as refused:
        entered.name_at_version(line)
    assert refused.value.code == "codesource.entered_dependency"


def test_a_go_dependency_is_a_module_and_a_version():
    with pytest.raises(BundleRefused) as refused:
        code = _mapping("go", dependencies=["github.com/acme/rates"])["code"]
        entered.code_of(LANGUAGES["go"], code)
    assert refused.value.code == "codesource.entered_dependency"


# -- hashing ---------------------------------------------------------------------------------


@pytest.mark.parametrize("language", sorted(CODE))
def test_the_same_entered_code_archives_and_hashes_the_same_and_an_edit_changes_it(
    language, tmp_path
):
    code = _mapping(language)["code"]
    first = entered.code_of(LANGUAGES[language], code)
    again = entered.code_of(LANGUAGES[language], dict(code))
    assert first.data == again.data
    edited = entered.code_of(LANGUAGES[language], {**code, "source": code["source"] + "\n"})
    hashes = {
        content_hash(_unpacked(u, tmp_path / name))
        for name, u in (("a", first), ("b", again), ("c", edited))
    }
    assert len(hashes) == 2


@pytest.mark.parametrize("language", sorted(CODE))
def test_entered_code_is_installed_and_its_hash_recorded_on_the_uploads_path(language, tmp_path):
    config = config_of(language, _mapping(language))
    # Entered code reads nothing from the context: it is in the mapping.
    upload = asyncio.run(_code_reference(config, None))  # pyright: ignore[reportArgumentType]
    assert isinstance(upload, UploadCode)
    installed = ensure_installed(
        source_id="entered-src",
        runtime=language,
        fetch=lambda dest: _unpacked(upload, dest),
        install=lambda root: None,
        expected_hash=None,
        cache_root=tmp_path / "cache",
    )
    assert installed.content_hash == content_hash(_unpacked(upload, tmp_path / "again"))
    # Saved, the recorded hash holds it: the same text is accepted, an edited one refused.
    ensure_installed(
        source_id="entered-src",
        runtime=language,
        fetch=lambda dest: _unpacked(upload, dest),
        install=lambda root: None,
        expected_hash=installed.content_hash,
        cache_root=tmp_path / "cache",
    )
    edited = entered.code_of(
        LANGUAGES[language], {**_mapping(language)["code"], "source": CODE[language] + "//\n"}
    )
    with pytest.raises(BundleRefused) as refused:
        ensure_installed(
            source_id="entered-src",
            runtime=language,
            fetch=lambda dest: _unpacked(edited, dest),
            install=lambda root: None,
            expected_hash=installed.content_hash,
            cache_root=tmp_path / "cache",
        )
    assert refused.value.code == "codesource.content_changed"


# -- loading: each language's bundle -----------------------------------------------------------


def test_python_entered_code_is_the_module_main_read_by_the_python_reader(tmp_path):
    from provisa.python.source import READER, code_path, entry_module

    upload = entered.code_of(LANGUAGES["python"], _mapping("python")["code"])
    root = code_path(_unpacked(upload, tmp_path))
    assert sorted(p.name for p in root.iterdir()) == ["main.py"]
    entry = entry_module(root)
    assert entry == "main"
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(root), entry], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    manifest = read_manifest(json.loads(done.stdout), PYTHON_RULES)
    assert [(e.name, e.kind) for e in manifest.exports] == [("rates", "table")]


def test_python_entered_dependencies_are_its_projects_and_need_a_lockfile(tmp_path):
    from provisa.python.install import lockfile

    code = _mapping("python", dependencies=DEPENDENCIES["python"])["code"]
    root = _unpacked(entered.code_of(LANGUAGES["python"], code), tmp_path)
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    assert project["dependencies"] == ["httpx==0.27.0"]
    with pytest.raises(BundleRefused) as refused:
        lockfile(root)  # as an uploaded project that declares dependencies with no lockfile is
    assert refused.value.code == "codesource.no_lockfile"


def test_node_entered_code_is_an_es_module_its_declarations_read_from_itself(tmp_path):
    from provisa.node.install import needs_install
    from provisa.node.reader import entry_declarations
    from provisa.node.worker import entry_module

    root = _unpacked(entered.code_of(LANGUAGES["node"], _mapping("node")["code"]), tmp_path / "a")
    assert entry_declarations(root) == (root / "index.js").resolve()
    assert entry_module(root).resolve() == (root / "index.js").resolve()
    assert json.loads((root / "package.json").read_text())["type"] == "module"
    assert needs_install(root, "node") is False
    code = _mapping("node", dependencies=DEPENDENCIES["node"])["code"]
    with_deps = _unpacked(entered.code_of(LANGUAGES["node"], code), tmp_path / "b")
    assert json.loads((with_deps / "package.json").read_text())["dependencies"] == {
        "@acme/rates": "1.2.0"
    }
    with pytest.raises(BundleRefused) as refused:
        needs_install(with_deps, "node")
    assert refused.value.code == "codesource.no_lockfile"


def test_go_entered_code_is_the_root_package_of_its_own_module(tmp_path):
    from provisa.golang.build import directives, module_path
    from provisa.golang.install import check_lockfile

    root = _unpacked(entered.code_of(LANGUAGES["go"], _mapping("go")["code"]), tmp_path / "a")
    assert module_path(root) == "provisa.local/entered"
    assert (root / "entered.go").read_text() == CODE["go"]
    check_lockfile(root)  # requires nothing, so needs no go.sum
    code = _mapping("go", dependencies=DEPENDENCIES["go"])["code"]
    with_deps = _unpacked(entered.code_of(LANGUAGES["go"], code), tmp_path / "b")
    assert directives((with_deps / "go.mod").read_text(), "require") == [
        ["github.com/acme/rates", "v1.2.0"]
    ]
    with pytest.raises(BundleRefused) as refused:
        check_lockfile(with_deps)
    assert refused.value.code == "codesource.lockfile_missing"


def test_rust_entered_code_is_the_library_of_its_own_crate(tmp_path):
    from provisa.rustlang.reader import crate_dir, crate_manifest, library_root

    code = _mapping("rust", dependencies=DEPENDENCIES["rust"])["code"]
    root = _unpacked(entered.code_of(LANGUAGES["rust"], code), tmp_path)
    crate = crate_dir(root, "")
    manifest = crate_manifest(crate, "1.90.0")
    assert manifest["package"]["name"] == "entered"
    assert manifest["dependencies"] == {"serde": "1.0.200"}
    assert library_root(crate, manifest).read_text() == CODE["rust"]
