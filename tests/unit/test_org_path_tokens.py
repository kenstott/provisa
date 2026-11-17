# Copyright (c) 2026 Kenneth Stott
# Canary: aae5a714-6afc-40ae-b223-8e11fcaac3f1
#
# This source code is licensed under the Business Source License 1.1
"""REQ-1997, the maintainer's ruling: "all uploaded file paths must be relative to the org path
(in saas in particular we don't want to reveal the physical location of the file)" -- "have some
org-level {} statements that resolve to org locations". A file source's path written with
{files} or {org} is kept and shown so, resolved where the file is read, and refused when it
leaves the org's folder; on a server of many organizations a path on its own disk is refused."""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest

import provisa.core.settings_catalog  # noqa: F401 -- declares the settings read here
from provisa.core import org_files, secrets_providers


@pytest.fixture
def org(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    folder = org_files.folder("acme")
    (folder / "files" / "survey").mkdir(parents=True)
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)
    return folder


def test_a_token_path_resolves_under_the_org_folder(org):
    assert org_files.resolve_path("{files}/survey/s.sqlite", "acme") == str(
        org / "files" / "survey" / "s.sqlite"
    )
    assert org_files.resolve_path("{org}/notes/a.csv", "acme") == str(org / "notes" / "a.csv")
    assert org_files.resolve_path("{files}", "acme") == str(org / "files")
    # Any other path is left as it is.
    assert org_files.resolve_path("/data/a.csv", "acme") == "/data/a.csv"
    assert org_files.resolve_path("s3://b/a.csv", "acme") == "s3://b/a.csv"
    assert not org_files.is_org_path("{filesx}/a") and org_files.is_org_path("{files}/a")


def test_a_token_path_that_leaves_the_org_folder_is_refused_by_name(org, tmp_path):
    with pytest.raises(org_files.OrgPathRefused, match=r"\{files\}/\.\./\.\./x leaves"):
        org_files.resolve_path("{files}/../../x", "acme")
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    (org / "files" / "survey" / "link").symlink_to(outside, target_is_directory=True)
    with pytest.raises(org_files.OrgPathRefused, match="leaves the organization's folder"):
        org_files.resolve_path("{files}/survey/link/a.csv", "acme")


def test_a_path_in_the_org_folder_is_shown_as_its_token(org):
    assert org_files.token_path(str(org / "files" / "survey" / "s.sqlite"), "acme") == (
        "{files}/survey/s.sqlite"
    )
    assert org_files.token_path(str(org / "notes" / "a.csv"), "acme") == "{org}/notes/a.csv"
    assert org_files.token_path("/data/a.csv", "acme") == "/data/a.csv"


@contextmanager
def _held_elsewhere(code: str) -> Iterator[None]:
    """Another process running ``code``, which takes its hold and says so, then waits."""
    other = subprocess.Popen(
        [sys.executable, "-c", code + "\nprint('held', flush=True)\nimport time; time.sleep(60)"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        assert other.stdout is not None
        line = other.stdout.readline().strip()
        assert line == "held", line
        yield
    finally:
        other.kill()
        other.wait()


def _input(type_: str, path: str | None) -> Any:
    return SimpleNamespace(
        id="survey",
        type=type_,
        path=path,
        sentinel_path=None,
        mapping_json=None,
        federation_hints_json=None,
    )


def test_the_save_keeps_a_path_in_the_org_folder_as_its_token(org, monkeypatch):
    from provisa.api.admin.schema_common import file_source_path

    monkeypatch.setattr("provisa.core.org_files.current_org_segment", lambda: "acme")
    saved = _input("sqlite", str(org / "files" / "survey" / "s.sqlite"))
    assert file_source_path(saved) is None
    assert saved.path == "{files}/survey/s.sqlite"  # an upload saved before, re-saved as a token
    typed = _input("csv", "{files}/survey/a.csv")
    assert file_source_path(typed) is None and typed.path == "{files}/survey/a.csv"
    refused = file_source_path(_input("parquet", "{files}/../../etc/x"))
    assert refused is not None and refused.code == "schema.file_path_leaves_org"
    # A path elsewhere on the server, and a remote location, stand on a server of one org.
    elsewhere = _input("duckdb", "/data/shop.duckdb")
    assert file_source_path(elsewhere) is None and elsewhere.path == "/data/shop.duckdb"
    assert file_source_path(_input("csv", "s3://bucket/a.csv")) is None
    assert file_source_path(_input("postgresql", "/anything")) is None


def test_a_server_of_many_organizations_refuses_a_path_on_its_own_disk(org, monkeypatch):
    from provisa.api.admin.schema_common import file_source_path

    from provisa.core.request_context import current_org
    from provisa.security import sandbox_egress

    monkeypatch.setattr("provisa.core.org_files.current_org_segment", lambda: "acme")
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: True)
    # The rule is asked of the bound org, which is not the deployment's own.
    monkeypatch.setattr(sandbox_egress, "_deployment_org", lambda: "default")
    token = current_org.set("acme")
    try:
        refused = file_source_path(_input("sqlite", "/data/shop.sqlite"))
    finally:
        current_org.reset(token)
    assert refused is not None and refused.code == "sources.local_path_refused"
    assert "{files}" in refused.message
    # A token path and a remote location stand.
    assert file_source_path(_input("sqlite", "{files}/survey/s.sqlite")) is None
    assert file_source_path(_input("sqlite", "https://example.com/s.sqlite")) is None


def test_the_file_connectors_read_a_token_path_where_it_resolves(org, monkeypatch):
    from provisa.federation.connector_duckdb import DuckDBCsvConnector, DuckDBDuckdbConnector

    monkeypatch.setattr("provisa.core.org_files.current_org_segment", lambda: "acme")
    csv = DuckDBCsvConnector().details(
        SimpleNamespace(id="c", type="csv", path="{files}/survey/a.csv", federation_hints={})
    )
    assert str(org / "files" / "survey" / "a.csv") in csv["view_ddl"]
    attach = DuckDBDuckdbConnector().details(
        SimpleNamespace(id="d", type="duckdb", path="{files}/survey/d.duckdb", federation_hints={})
    )["attach"]
    assert str(org / "files" / "survey" / "d.duckdb") in attach


async def test_the_duckdb_driver_opens_a_token_path_and_names_it_as_written(org, monkeypatch):
    from provisa.executor.drivers.duckdb_driver import DuckDBDriver
    from provisa.federation import file_locks

    monkeypatch.setattr("provisa.core.org_files.current_org_segment", lambda: "acme")
    path = org / "files" / "survey" / "d.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1)")
    con.close()
    driver = DuckDBDriver()
    await driver.connect("", 0, "{files}/survey/d.duckdb", "", "")
    try:
        assert (await driver.execute("SELECT count(*) FROM t")).rows == [(1,)]
        # Another process holds the file; the refusal names it as the source does, never its
        # physical place.
        with _held_elsewhere(f"import duckdb\nc = duckdb.connect({str(path)!r}, read_only=True)"):
            with pytest.raises(file_locks.FileLocked) as refused:
                await driver.execute("INSERT INTO t VALUES (2)")
        assert "{files}/survey/d.duckdb" in str(refused.value)
        assert str(org) not in str(refused.value)
    finally:
        await driver.close()


async def test_an_upload_answers_its_path_as_the_org_token(org, monkeypatch):
    from provisa.core import source_uploads

    monkeypatch.setattr("provisa.core.org_files.current_org_segment", lambda: "acme")

    async def chunks():
        yield b"x"

    path, _size = await source_uploads.receive("survey", "s.sqlite", chunks(), 1)
    assert org_files.token_path(str(path)) == "{files}/survey/s.sqlite"
    assert source_uploads.is_upload("survey", "{files}/survey/s.sqlite")
    assert source_uploads.is_upload("survey", str(path))  # one saved before, absolute
    assert json.dumps({"path": org_files.token_path(str(path))}).find(str(org)) == -1
