# Copyright (c) 2026 Kenneth Stott
# Canary: e33302a1-8893-44a6-86be-520460c0ce57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1873: staging the bundled extensions into an operator's own Postgres.

These cover the decisions that need no server: reading the target's facts, choosing the bundle,
refusing a mismatch or a corrupted file before anything is written, and extending
shared_preload_libraries without replacing it. tests/integration/test_pg_ext_install.py installs
into a real Postgres container.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from provisa.pg_extensions.external_install import (
    BundleUnavailable,
    TargetFacts,
    install,
    preload_with,
    read_facts,
    select_bundle,
    verify_bundle,
)


def _bundle(root: Path, os_: str, arch: str, pg_major: str, *, corrupt: bool = False) -> Path:
    tree = root / f"{os_}-{arch}"
    (tree / "lib").mkdir(parents=True)
    (tree / "share" / "extension").mkdir(parents=True)
    artifacts = []
    for name in ("postgres_fdw", "pg_duckdb", "libduckdb", "libpq"):  # the last two: support libs
        blob = f"{name} for {os_}-{arch} pg{pg_major}".encode()
        (tree / "lib" / f"{name}.so").write_bytes(blob)
        digest = hashlib.sha256(b"something else" if corrupt and name == "pg_duckdb" else blob)
        artifacts.append(
            {"name": name, "key": name, "file": f"lib/{name}.so", "sha256": digest.hexdigest()}
        )
    for ext in ("postgres_fdw", "pg_duckdb"):  # the extensions; the libraries have no control file
        (tree / "share" / "extension" / f"{ext}.control").write_text("comment = 'x'\n")
    (tree / "manifest.json").write_text(
        json.dumps({"os": os_, "arch": arch, "pg_major": pg_major, "artifacts": artifacts})
    )
    return root


class _FakeTarget:
    """A target whose commands answer like a Postgres host's. Records what it was asked to do."""

    label = "fake"

    def __init__(self, version="PostgreSQL 16.4", uname=("Linux", "x86_64")) -> None:
        self.answers = {
            ("pg_config", "--version"): version,
            ("pg_config", "--pkglibdir"): "/usr/lib/postgresql/16/lib",
            ("pg_config", "--sharedir"): "/usr/share/postgresql/16",
            ("uname", "-s"): uname[0],
            ("uname", "-m"): uname[1],
        }
        self.written: list[str] = []

    def run(self, cmd: list[str]) -> str:
        key = tuple(cmd)
        if key in self.answers:
            return self.answers[key]
        if cmd[0] in ("mkdir", "mv", "rm"):
            self.written.append(" ".join(cmd))
            return ""
        raise AssertionError(f"unexpected command {cmd}")

    def copy_in(self, local: Path, remote: str) -> None:
        self.written.append(f"copy {local.name} -> {remote}")


class TestFacts:
    @pytest.mark.parametrize(
        ("version", "major"),
        [("PostgreSQL 16.4", 16), ("PostgreSQL 15.8 (Debian 15.8-1.pgdg120+1)", 15)],
    )
    def test_the_major_comes_from_pg_config(self, version, major):
        assert read_facts(_FakeTarget(version=version)).pg_major == major

    @pytest.mark.parametrize(
        ("uname", "platform"),
        [
            (("Linux", "x86_64"), "linux-x64"),
            (("Linux", "aarch64"), "linux-arm64"),
            (("Darwin", "arm64"), "darwin-arm64"),
        ],
    )
    def test_the_platform_is_the_targets_own(self, uname, platform):
        assert read_facts(_FakeTarget(uname=uname)).platform == platform

    def test_an_unrecognised_platform_is_refused_by_name(self):
        with pytest.raises(BundleUnavailable, match="SunOS"):
            read_facts(_FakeTarget(uname=("SunOS", "sparc")))


class TestSelection:
    def test_the_matching_bundle_is_chosen(self, tmp_path):
        _bundle(tmp_path, "linux", "x64", "16")
        facts = TargetFacts(16, "linux-x64", "/lib", "/share")
        assert select_bundle(tmp_path, facts) == tmp_path / "linux-x64"

    def test_another_major_is_refused_naming_what_exists(self, tmp_path):
        _bundle(tmp_path, "linux", "x64", "16")
        _bundle(tmp_path, "darwin", "arm64", "16")
        with pytest.raises(BundleUnavailable) as refused:
            select_bundle(tmp_path, TargetFacts(15, "linux-x64", "/lib", "/share"))
        assert "PostgreSQL 15 on linux-x64" in str(refused.value)
        assert "PostgreSQL 16 on darwin-arm64" in str(refused.value)
        assert "PostgreSQL 16 on linux-x64" in str(refused.value)

    def test_another_platform_is_refused(self, tmp_path):
        _bundle(tmp_path, "linux", "x64", "16")
        with pytest.raises(BundleUnavailable, match="linux-arm64"):
            select_bundle(tmp_path, TargetFacts(16, "linux-arm64", "/lib", "/share"))

    def test_a_corrupted_artifact_is_refused_by_name(self, tmp_path):
        _bundle(tmp_path, "linux", "x64", "16", corrupt=True)
        with pytest.raises(BundleUnavailable, match="pg_duckdb"):
            verify_bundle(tmp_path / "linux-x64")


class TestNothingWrittenOnRefusal:
    def test_a_mismatch_writes_nothing(self, tmp_path):
        _bundle(tmp_path, "linux", "x64", "16")
        target = _FakeTarget(version="PostgreSQL 15.8")
        with pytest.raises(BundleUnavailable):
            install(target, tmp_path)
        assert target.written == []

    def test_a_corrupted_bundle_writes_nothing(self, tmp_path):
        _bundle(tmp_path, "linux", "x64", "16", corrupt=True)
        target = _FakeTarget()
        with pytest.raises(BundleUnavailable):
            install(target, tmp_path)
        assert target.written == []

    def test_a_match_stages_then_moves_into_place(self, tmp_path):
        _bundle(tmp_path, "linux", "x64", "16")
        target = _FakeTarget()
        installed = install(target, tmp_path)
        assert installed.extensions == ["pg_duckdb", "postgres_fdw"]
        moves = [w for w in target.written if w.startswith("mv ")]
        assert any(m.endswith("/usr/lib/postgresql/16/lib/") for m in moves)
        assert any(m.endswith("/usr/share/postgresql/16/extension/") for m in moves)
        copies = [w for w in target.written if w.startswith("copy ")]
        assert copies and all("/tmp/provisa-pg-ext-" in c for c in copies)


class TestPreload:
    @pytest.mark.parametrize(
        ("current", "expected"),
        [
            ("", "pg_duckdb"),
            ("pg_stat_statements", "pg_stat_statements,pg_duckdb"),
            ("pg_stat_statements, pg_duckdb", None),
        ],
    )
    def test_pg_duckdb_is_appended_never_replacing(self, current, expected):
        assert preload_with(current, "pg_duckdb") == expected
