# Copyright (c) 2026 Kenneth Stott
# Canary: 524360a8-7494-416f-ab03-898646d9e7c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source read from a directory on the server (REQ-2121): only under the operator's
allowed directories and never on a server of many organizations; copied without what the
developer's tools make; read as it is now, so an edit is read by the next query; never pinned."""

# Requirements: REQ-2121

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.codesource import install
from provisa.codesource import source as code_source
from provisa.codesource.bundle import BundleRefused

pytestmark = pytest.mark.unit


@pytest.fixture
def roots(monkeypatch, tmp_path):
    allowed = tmp_path / "allowed"
    (allowed / "rates").mkdir(parents=True)
    (allowed / "rates" / "rates.py").write_text("def rate(): return 1\n")
    monkeypatch.setattr("provisa.core.source_downloads.download_roots", lambda: [str(allowed)])
    monkeypatch.setattr("provisa.core.secrets_providers.many_organisations", lambda: False)
    return allowed


def _mapping(path) -> dict:
    return {"runtime": "python", "code": {"kind": "directory", "path": str(path)}}


def test_a_directory_under_the_allowed_ones_is_its_code_and_is_never_pinned(roots):
    config = code_source.config_of("python", {**_mapping(roots / "rates"), "content_hash": "h"})
    assert config.code == {"kind": "directory", "path": str((roots / "rates").resolve())}
    assert config.content_hash is None


@pytest.mark.parametrize(
    "where, code",
    [
        ("outside", "codesource.directory_outside_roots"),
        ("relative", "codesource.directory_relative"),
        ("missing", "codesource.directory_missing"),
    ],
)
def test_a_directory_it_may_not_read_is_refused_by_name(roots, tmp_path, where, code):
    path = {
        "outside": tmp_path,
        "relative": "rates",
        "missing": roots / "gone",
    }[where]
    with pytest.raises(BundleRefused) as refused:
        code_source.config_of("python", _mapping(path))
    assert refused.value.code == code


def test_a_server_of_many_organizations_runs_no_code_from_its_own_disk(roots, monkeypatch):
    monkeypatch.setattr("provisa.core.secrets_providers.many_organisations", lambda: True)
    with pytest.raises(BundleRefused) as refused:
        code_source.config_of("python", _mapping(roots / "rates"))
    assert refused.value.code == "codesource.directory_refused"


def test_its_copy_leaves_out_what_the_developers_tools_make(roots, tmp_path):
    folder = roots / "rates"
    for made in ("node_modules", ".venv", "__pycache__", ".git"):
        (folder / made).mkdir()
        (folder / made / "x").write_text("x")
    (folder / "pkg" / "__pycache__").mkdir(parents=True)
    (folder / "pkg" / "__pycache__" / "y.pyc").write_text("y")
    copied = install.fetch_directory(install.DirectoryCode(str(folder)), tmp_path / "copy")
    assert sorted(p.relative_to(copied).as_posix() for p in copied.rglob("*")) == [
        "pkg",
        "rates.py",
    ]


async def test_an_edit_in_the_directory_is_read_by_the_next_query(roots, monkeypatch):
    built: list[str] = []

    def _build(source_id, config, code, lock, ctx):
        from provisa.codesource.bundle import content_hash

        found = content_hash(roots / "rates", install.DIRECTORY_SKIPPED)
        built.append(found)
        return SimpleNamespace(content_hash=found)

    monkeypatch.setattr(code_source, "_prepare_python", _build)
    monkeypatch.setattr(code_source, "_prepared", {})
    ctx = SimpleNamespace(org_id="acme", admin_db=None)
    mapping = _mapping(roots / "rates")
    first = await code_source.prepare("rates", "python", mapping, ctx)
    again = await code_source.prepare("rates", "python", mapping, ctx)
    assert again is first and len(built) == 1  # unchanged: prepared once
    (roots / "rates" / "rates.py").write_text("def rate(): return 2\n")
    edited = await code_source.prepare("rates", "python", mapping, ctx)
    assert edited is not first and len(built) == 2


async def test_saving_a_directory_source_pins_no_hash(roots, monkeypatch):
    from provisa.api.admin import _code_source_registration as registration

    async def _prepare(*_args):
        return SimpleNamespace(content_hash="h-now")

    async def _context(_state):
        return None

    monkeypatch.setattr(code_source, "prepare", _prepare)
    monkeypatch.setattr(code_source, "context_for", _context)
    mapping = _mapping(roots / "rates")
    assert await registration.record_code("rates", "python", mapping) is None
    assert "content_hash" not in mapping
    assert registration.unpinned({"rates": {"type": "python", "mapping": mapping}}) == []
