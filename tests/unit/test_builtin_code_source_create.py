# Copyright (c) 2026 Kenneth Stott
# Canary: 4c8e2f17-a930-4d6b-9e15-7b0d3a6c2e48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2267 (amended 2026-11-24): an OpenAPI or remote GraphQL source saved through the admin
create path is a built-in code source. Its save writes the built-in's code into a server-owned
directory for the source, records the code as the ``builtin`` kind and declares the built-in's
credential as the source's secret reference, never storing a literal; the code-source runtime
serves it, reading a registered table's rows as the in-process source read them.

The code runs in a real Python host process under Provisa's own Python; the OpenAPI and GraphQL
remotes are the fixtures of test_openapi_code_source and test_graphql_code_source, real HTTP
servers on ephemeral loopback ports."""

# Requirements: REQ-2267
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.codesource import source as code_source
from provisa.codesource import table as code_table
from provisa.codesource import worker as w
from provisa.core.models import GraphQLRemoteConfig
from tests.unit.test_graphql_code_source import (
    _code_table,
    _plain,
    _registration_rows,
    remote,  # noqa: F401 -- the fixture remote GraphQL endpoint
)
from tests.unit.test_openapi_code_source import (
    SPEC,
    _existing_rows,
    _project,
    _table,
    api,  # noqa: F401 -- the fixture OpenAPI server
)

pytestmark = pytest.mark.unit


@pytest.fixture
def saving(tmp_path, monkeypatch):
    """A save in the org ``acme``: its folder ready under a data directory of its own, and the
    deployment's graphql_remote limits."""
    from provisa.api.app import state
    from provisa.core import org_files

    monkeypatch.setattr(org_files, "data_dir", lambda: tmp_path / "data")
    (tmp_path / "data" / "orgs" / "acme").mkdir(parents=True)
    ctx = code_source.Context(
        cache_root=tmp_path / "cache", registries=[], egress=[], admin_db=None, org_id="acme"
    )

    async def _context(_state):
        return ctx

    monkeypatch.setattr(code_source, "context_for", _context)
    monkeypatch.setattr(code_source, "_prepared", {})
    monkeypatch.setattr(state, "config", SimpleNamespace(graphql_remote=GraphQLRemoteConfig()))
    yield SimpleNamespace(ctx=ctx, code=tmp_path / "data" / "orgs" / "acme" / "code" / "builtin")
    w.pool.stop_source("pets-src")
    w.pool.stop_source("gql-src")


@pytest.fixture
def spec_file(tmp_path, api):  # noqa: F811 -- the imported fixture
    """The fixture spec on disk, its server the fixture API."""
    path = tmp_path / "petstore.json"
    path.write_text(json.dumps({**SPEC, "servers": [{"url": api.url}]}))
    return path


async def _save(source_id: str, source_type: str, mapping: dict, path: str):
    from provisa.api.admin._code_source_registration import record_code

    return await record_code(source_id, source_type, mapping, path)


def _registered(monkeypatch, source_id: str, source_type: str, mapping: dict) -> None:
    """The saved source as the registry holds it, its type and mapping as saved."""
    source = SimpleNamespace(id=source_id, type=SimpleNamespace(value=source_type), mapping=mapping)

    async def _sources(_state):
        return [source]

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)


def _written(directory: Path) -> str:
    return "".join(p.read_text() for p in sorted(directory.rglob("*")) if p.is_file())


# -- the save: a built-in code source, its credential a declared secret ----------------------------


async def test_an_openapi_source_saved_is_a_builtin_code_source_with_its_declared_secret(
    saving, spec_file, monkeypatch
):
    monkeypatch.setenv("PETS_TOKEN", "tok-from-env")
    mapping = {"auth": {"type": "bearer", "token": "${env:PETS_TOKEN}"}}
    assert await _save("pets-src", "openapi", mapping, str(spec_file)) is None
    assert mapping["code"] == {"kind": "builtin"}
    assert mapping["secrets"] == {"credential": "${env:PETS_TOKEN}"}
    directory = saving.code / "pets-src"
    from provisa.codesource.bundle import content_hash

    assert mapping["content_hash"] == content_hash(directory)
    written = _written(directory)
    assert "${env:PETS_TOKEN}" not in written and "tok-from-env" not in written
    held = json.loads((directory / "source.json").read_text())["auth"]
    assert held == {"type": "bearer", "token": {"$secret": "credential"}}
    # The runtime serves it: prepared from the code its save wrote.
    ready = await code_source.prepare("pets-src", "openapi", mapping, saving.ctx)
    assert ready.content_hash == mapping["content_hash"]
    assert ready.manifest.export("listPets") is not None


async def test_a_graphql_remote_source_saved_is_a_builtin_code_source_with_its_declared_secret(
    saving,
    remote,  # noqa: F811 -- the imported fixture
    monkeypatch,
):
    monkeypatch.setenv("GQL_TOKEN", "tok-from-env")
    mapping = {"auth": {"type": "bearer", "token": "${env:GQL_TOKEN}"}, "namespace": ""}
    assert await _save("gql-src", "graphql_remote", mapping, remote.url) is None
    assert mapping["code"] == {"kind": "builtin"}
    assert mapping["secrets"] == {"credential": "${env:GQL_TOKEN}"}
    directory = saving.code / "gql-src"
    written = _written(directory)
    assert "${env:GQL_TOKEN}" not in written and "tok-from-env" not in written
    config = json.loads((directory / "graphql_source.json").read_text())
    assert config["auth"] == {"type": "bearer", "token": {"$secret": "credential"}}
    assert config["schema"]["queryType"]  # introspected from the endpoint at the save
    ready = await code_source.prepare("gql-src", "graphql_remote", mapping, saving.ctx)
    assert ready.manifest.export("authors") is not None


@pytest.mark.parametrize("source_type", ["openapi", "graphql_remote"])
async def test_a_literal_credential_is_refused_by_name_and_nothing_is_stored(
    saving,
    spec_file,
    remote,  # noqa: F811 -- the imported fixture
    source_type,
):
    path = str(spec_file) if source_type == "openapi" else remote.url
    source_id = "pets-src" if source_type == "openapi" else "gql-src"
    mapping = {"auth": {"type": "bearer", "token": "tok-literal"}}
    refused = await _save(source_id, source_type, mapping, path)
    assert refused is not None and not refused.success
    assert refused.code == "codesource.credential_not_reference"
    assert "tok-literal" not in refused.message
    assert "code" not in mapping and "secrets" not in mapping
    assert not (saving.code / source_id).exists()


async def test_a_builtin_source_whose_code_was_changed_is_refused_by_both_hashes(saving, spec_file):
    mapping: dict = {}
    assert await _save("pets-src", "openapi", mapping, str(spec_file)) is None
    (saving.code / "pets-src" / "spec.json").write_text("{}")
    code_source.forget("pets-src")
    with pytest.raises(code_source.BundleRefused) as refused:
        await code_source.prepare("pets-src", "openapi", mapping, saving.ctx)
    assert refused.value.code == "codesource.content_changed"


# -- the runtime: a registered table reads the rows the in-process source read ---------------------


@pytest.mark.parametrize(
    "table, inputs",
    [
        ("listPets", {}),
        ("listPets", {"status": "sold"}),
        ("listOwners", {}),
        ("listOwnerVisits", {"ownerId": 1}),
    ],
)
async def test_an_openapi_sources_table_reads_the_rows_the_in_process_source_read(
    saving,
    spec_file,
    api,  # noqa: F811 -- the imported fixture
    monkeypatch,
    table,
    inputs,
):
    mapping: dict = {}
    assert await _save("pets-src", "openapi", mapping, str(spec_file)) is None
    _registered(monkeypatch, "pets-src", "openapi", mapping)
    ready = await code_source.prepare("pets-src", "openapi", mapping, saving.ctx)
    read = await code_table.read(None, _table(ready.manifest, table), inputs)
    columns = [f.name for f in ready.manifest.types[f"{table}Row"].fields]
    existing = await _existing_rows(api.url, table, table, inputs)
    assert existing, "the fixture answers rows"
    assert _project(read.rows, columns) == _project(existing, columns)


@pytest.mark.parametrize(
    "name, inputs, variables",
    [
        ("authors", {}, None),
        ("books", {}, None),
        ("publisher_by_code", {"code": "p1"}, {"code": "p1"}),
    ],
)
async def test_a_graphql_remote_sources_table_reads_the_rows_the_in_process_source_read(
    saving,
    remote,  # noqa: F811 -- the imported fixture
    monkeypatch,
    name,
    inputs,
    variables,
):
    mapping: dict = {"namespace": ""}
    assert await _save("gql-src", "graphql_remote", mapping, remote.url) is None
    _registered(monkeypatch, "gql-src", "graphql_remote", mapping)
    ready = await code_source.prepare("gql-src", "graphql_remote", mapping, saving.ctx)
    read = await code_table.read(None, _code_table(ready.manifest, name), inputs)
    expected = await _registration_rows(remote.url, name, variables)
    assert expected
    got = [{k: v for k, v in r.items() if not k.startswith("_nf_")} for r in read.rows]
    assert _plain(got) == _plain(expected)
