# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e6c1d-93f4-4a27-8d1e-6f2a7c9b4e30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1997: in a held organization (a sandbox org on any install; every org but the deployment's
own on a server of many organizations) no source reads a place on the server's own disk outside
the organization's folder, whatever its type and however the place is written. Refused where the
source is saved and where its location is opened; an install of one organization is unchanged,
and a demo's own source reads its shipped file in a sandbox."""

# Requirements: REQ-1997, REQ-2006

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import provisa.core.settings_catalog  # noqa: F401 -- declares the settings read here
from provisa.core import org_files, secrets_providers
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.security import sandbox_egress as egress

_SANDBOX = "sbx0123456789ab"
_REFUSED = "sources.local_path_refused"
_SPEC = {"openapi": "3.0.0", "info": {"title": "t", "version": "1"}, "paths": {}}

#: Every way a source names a place on the server's own disk: ``(source type, field)``. A field
#: with a dot is a key of that bag.
_PLACES = [
    ("sqlite", "path"),
    ("duckdb", "path"),
    ("csv", "path"),
    ("parquet", "path"),
    ("jdbc", "path"),
    ("files", "path"),
    ("iceberg", "path"),
    ("delta_lake", "path"),
    ("openapi", "path"),
    ("grpc_remote", "path"),
    ("grpc_remote", "federation_hints.import_paths"),
    ("csv", "sentinel_path"),
    ("bigquery", "federation_hints.credentials_path"),
    ("sharepoint", "mapping.certificate_path"),
]

#: The ways out of the organization's folder, by name (built by :func:`_bad`).
_WAYS_OUT = [
    "absolute",
    "relative",
    "token_dotdot",
    "symlink",
    "token_symlink",
    "file_url",
    "file_url_host",
    "file_url_escaped",
    "other_org",
    "looks_remote",
    "looks_remote_host",
    "remote_dotdot",
]


@pytest.fixture
def disk(tmp_path, monkeypatch):
    """A server's disk: two organizations' folders, a file of the server's own holding a
    specification, and a link out of the first organization's folder."""
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.chdir(tmp_path)
    for org in ("acme", "other", _SANDBOX, "default"):
        (org_files.folder(org) / "files").mkdir(parents=True)
    outside = tmp_path / "server"
    outside.mkdir()
    (outside / "secret.json").write_text(json.dumps(_SPEC))
    (org_files.folder("other") / "files" / "theirs.json").write_text(json.dumps(_SPEC))
    for org in ("acme", _SANDBOX):
        (org_files.folder(org) / "files" / "link").symlink_to(outside, target_is_directory=True)
        (org_files.folder(org) / "files" / "own.json").write_text(json.dumps(_SPEC))
    return SimpleNamespace(root=tmp_path, outside=outside)


def _bind(monkeypatch, org: str, *, many: bool):
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: many)
    monkeypatch.setattr(egress, "_deployment_org", lambda: "default")
    monkeypatch.setattr("provisa.core.org_files.current_org_segment", lambda: org)
    return set_current_org(org)


@pytest.fixture(params=["many_organizations", "sandbox"])
def held(request, disk, monkeypatch):
    """A held organization, bound: an ordinary org on a server of many, or a sandbox org on an
    install of one."""
    org = "acme" if request.param == "many_organizations" else _SANDBOX
    token = _bind(monkeypatch, org, many=request.param == "many_organizations")
    yield SimpleNamespace(org=org, folder=org_files.folder(org), **vars(disk))
    reset_current_org(token)
    egress.forget(org)


@pytest.fixture
def one_org(disk, monkeypatch):
    token = _bind(monkeypatch, "acme", many=False)
    yield SimpleNamespace(org="acme", folder=org_files.folder("acme"), **vars(disk))
    reset_current_org(token)


def _bad(way: str, ctx: Any) -> str:
    secret = ctx.outside / "secret.json"
    return {
        "absolute": str(secret),
        "relative": os.path.relpath(secret, ctx.root),
        "token_dotdot": "{files}/../../../../server/secret.json",
        "symlink": str(ctx.folder / "files" / "link" / "secret.json"),
        "token_symlink": "{files}/link/secret.json",
        "file_url": f"file://{secret}",
        "file_url_host": f"file://localhost{secret}",
        "file_url_escaped": f"file://{ctx.outside}/%73ecret.json",
        "other_org": str(org_files.folder("other") / "files" / "theirs.json"),
        # Not addresses: a reader that takes each for a path climbs to the server's file.
        "looks_remote": "http://../server/secret.json",
        "looks_remote_host": "weird://x/../../server/secret.json",
        "remote_dotdot": "https://example.com/../../server/secret.json",
    }[way]


def _fields(field: str, value: str) -> dict:
    bag, _, key = field.partition(".")
    if not key:
        return {bag: value}
    return {bag: {key: value}}


def _input(source_type: str, field: str, value: str) -> Any:
    """What the admin API's save is given (``SourceInput``), as far as the path rule reads it."""
    fields = _fields(field, value)
    return SimpleNamespace(
        id="probe",
        type=source_type,
        path=fields.get("path"),
        sentinel_path=fields.get("sentinel_path"),
        mapping_json=json.dumps(fields.get("mapping") or {}),
        federation_hints_json=json.dumps(fields.get("federation_hints") or {}),
    )


def _source(source_type: str, field: str, value: str) -> Any:
    from provisa.core.models import Source, SourceType

    return Source(id="probe", type=SourceType(source_type), **_fields(field, value))


def _code(exc: BaseException) -> str | None:
    return getattr(exc, "code", None)


# --- refused where it is saved ----------------------------------------------------------------


@pytest.mark.parametrize("way", _WAYS_OUT)
@pytest.mark.parametrize(("source_type", "field"), _PLACES)
def test_the_admin_save_refuses_a_place_outside_the_orgs_folder(held, source_type, field, way):
    from provisa.api.admin.schema_common import file_source_path

    refused = file_source_path(_input(source_type, field, _bad(way, held)))
    assert refused is not None and not refused.success
    assert refused.code == _REFUSED
    assert (refused.params or {})["source"] == "probe"


@pytest.mark.parametrize("way", _WAYS_OUT)
@pytest.mark.parametrize(("source_type", "field"), _PLACES)
def test_every_save_of_a_source_refuses_it(held, source_type, field, way):
    """``pinned_source`` is asked by ``repositories.source.upsert`` and ``create``: the admin
    API, a configuration load and an import all save through them."""
    with pytest.raises(PermissionError) as refused:
        egress.pinned_source(_source(source_type, field, _bad(way, held)))
    assert _code(refused.value) == _REFUSED
    assert refused.value.params == {"path": _bad(way, held)}  # type: ignore[attr-defined]


def test_the_two_save_checks_stand_in_the_repository():
    import inspect

    from provisa.core.repositories import source as repo

    assert "pinned_source(source)" in inspect.getsource(repo.upsert)
    assert "pinned_source(source)" in inspect.getsource(repo.create)


# --- refused where a row that already holds it is opened ---------------------------------------


@pytest.mark.parametrize("way", _WAYS_OUT)
@pytest.mark.parametrize(("source_type", "field"), _PLACES)
def test_an_engine_attach_of_a_saved_row_refuses_it(held, source_type, field, way):
    """``Engine.resolve`` is where every engine attaches a source; it asks ``pinned_source``."""
    from provisa.federation.engine import FederationEngine

    with pytest.raises(PermissionError) as refused:
        FederationEngine.resolve(SimpleNamespace(), _source(source_type, field, _bad(way, held)))
    assert _code(refused.value) == _REFUSED


@pytest.mark.parametrize("way", _WAYS_OUT)
def test_the_file_drivers_resolver_refuses_it(held, way):
    """``org_files.resolve_path`` is what every reader of a SQLite, DuckDB, CSV, Parquet or JDBC
    file opens the path through (the DuckDB connector, both file drivers, the SQLite loader)."""
    with pytest.raises(PermissionError) as refused:
        org_files.resolve_path(_bad(way, held))
    assert _code(refused.value) == _REFUSED or isinstance(refused.value, org_files.OrgPathRefused)


@pytest.mark.parametrize("way", _WAYS_OUT)
def test_an_openapi_specification_outside_the_folder_is_not_read(held, way):
    from provisa.openapi.loader import load_spec

    # The file is there and holds a specification: a read would return it.
    with pytest.raises(PermissionError) as refused:
        load_spec(_bad(way, held))
    assert _code(refused.value) == _REFUSED


@pytest.mark.parametrize("way", _WAYS_OUT)
def test_a_proto_file_outside_the_folder_is_not_read(held, way):
    from provisa.grpc_remote.loader import load_proto

    with pytest.raises(PermissionError) as refused:
        asyncio.run(load_proto(_bad(way, held)))
    assert _code(refused.value) == _REFUSED


def test_proto_import_directories_outside_the_folder_are_refused(held):
    from provisa.grpc_remote.loader import compile_proto_stubs, load_proto

    inside = held.folder / "files" / "a.proto"
    inside.write_text('syntax = "proto3";')
    with pytest.raises(PermissionError) as refused:
        asyncio.run(load_proto(str(inside), import_paths=[str(held.outside)]))
    assert _code(refused.value) == _REFUSED
    with pytest.raises(PermissionError) as refused:
        compile_proto_stubs('syntax = "proto3";', import_paths=[str(held.outside)])
    assert _code(refused.value) == _REFUSED


def test_a_sentinel_marker_outside_the_folder_is_not_read(held):
    from provisa.events.sentinel_probe import build_sentinel_probe

    probe = build_sentinel_probe(f"file://{held.outside}/secret.json")

    async def read() -> str | None:
        return await probe()

    with pytest.raises(PermissionError) as refused:
        asyncio.run(read())
    assert _code(refused.value) == _REFUSED


# --- what still works ---------------------------------------------------------------------------


@pytest.mark.parametrize(("source_type", "field"), _PLACES)
def test_a_place_in_the_orgs_own_folder_is_saved_and_opened(held, source_type, field):
    from provisa.api.admin.schema_common import file_source_path

    for written in (str(held.folder / "files" / "own.json"), "{files}/own.json"):
        assert file_source_path(_input(source_type, field, written)) is None
        egress.pinned_source(_source(source_type, field, written))


def test_a_file_in_the_orgs_own_folder_is_read(held):
    from provisa.openapi.loader import load_spec

    own = held.folder / "files" / "own.json"
    assert org_files.resolve_path(str(own)) == str(own)
    assert load_spec(str(own)) == _SPEC
    assert load_spec("{files}/own.json") == _SPEC


@pytest.mark.parametrize(
    "location", ["https://example.com/a.csv", "s3://bucket/a.parquet", "gs://b/t", "az://c/t"]
)
@pytest.mark.parametrize("source_type", ["csv", "parquet", "files", "iceberg", "delta_lake"])
def test_a_remote_location_is_not_a_place_on_this_disk(held, source_type, location):
    from provisa.api.admin.schema_common import file_source_path
    from provisa.core.source_uploads import require_source_own_disk

    assert file_source_path(_input(source_type, "path", location)) is None
    require_source_own_disk(source_type, {"path": location})


def test_a_branded_specification_is_no_location(held):
    from provisa.core.source_uploads import require_source_own_disk

    require_source_own_disk("openapi", {"path": "brand:stripe"})
    require_source_own_disk("openapi", {"path": "servicenow:companion"})


@pytest.mark.parametrize("way", [w for w in _WAYS_OUT if not w.startswith("token_")])
@pytest.mark.parametrize(("source_type", "field"), _PLACES)
def test_an_install_of_one_organization_reads_a_local_path_as_before(
    one_org, source_type, field, way
):
    from provisa.api.admin.schema_common import file_source_path

    from provisa.core.source_uploads import UPLOAD_TYPES

    written = _bad(way, one_org)
    if way == "looks_remote_host" and field == "path" and source_type in UPLOAD_TYPES:
        # As before: an upload type's path at a scheme no reader knows is refused by name.
        with pytest.raises(ValueError, match="reads no file at a weird:// location"):
            file_source_path(_input(source_type, field, written))
    else:
        assert file_source_path(_input(source_type, field, written)) is None
    assert egress.pinned_source(_source(source_type, field, written)) is not None
    assert org_files.resolve_path(written) == written


def test_an_install_of_one_organization_reads_a_local_specification(one_org):
    from provisa.openapi.loader import load_spec

    assert load_spec(str(one_org.outside / "secret.json")) == _SPEC


def test_the_deployments_own_org_on_a_server_of_many_reads_a_local_path(disk, monkeypatch):
    from provisa.openapi.loader import load_spec

    token = _bind(monkeypatch, "default", many=True)
    try:
        written = str(disk.outside / "secret.json")
        assert egress.pinned_source(_source("files", "path", written)) is not None
        assert load_spec(written) == _SPEC
    finally:
        reset_current_org(token)


@pytest.mark.parametrize("source_type", ["sqlite", "files", "openapi"])
def test_a_demos_own_source_reads_its_shipped_file_in_a_sandbox(disk, monkeypatch, source_type):
    """A demo's sources are the operator's: its template's configuration names them, and each
    sandbox org's runtime records them (``set_demo_destinations``), as it does their hosts."""
    from provisa.openapi.loader import load_spec

    shipped = str(disk.outside / "secret.json")
    token = _bind(monkeypatch, _SANDBOX, many=False)
    try:
        demo = _source(source_type, "path", shipped)
        egress.set_demo_destinations(_SANDBOX, [egress.source_fields(demo)])
        assert egress.pinned_source(demo) is not None
        assert org_files.resolve_path(shipped) == shipped
        assert load_spec(shipped) == _SPEC
        # Another file beside the demo's is not the demo's.
        other = Path(disk.outside / "other.json")
        other.write_text("{}")
        with pytest.raises(PermissionError) as refused:
            egress.pinned_source(_source(source_type, "path", str(other)))
        assert _code(refused.value) == _REFUSED
    finally:
        reset_current_org(token)
        egress.forget(_SANDBOX)
