# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2b8e47-1f9a-4c3e-b0d5-7a4e91c2f8b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1159 (amended 2026-11-27): a command written in the command form as entered code.

The ``test`` instance: an isolated server on DuckDB with its own control plane. The command form
saves what it does with the mutations that already exist: the code goes into a Python code source
of the command's name as entered code (REQ-1993), saved by createSource and changed by
updateSource, and the declared @query is registered as the command by naming it
(``POST /admin/actions/functions``, REQ-2226), the form sending no arguments and no columns. The
command's arguments and output columns are the declaration's; it runs through the one pipeline;
changing the code and saving the command again reads the declaration again."""

# Requirements: REQ-1159, REQ-1993, REQ-2226

from __future__ import annotations

import json

import httpx
import pytest
import yaml

from tests.integration.test_code_sources_e2e import _ROLE, _admin, _config, _rows, _sql

pytestmark = [pytest.mark.integration]

_SOURCE = "greet_cmd"

_CODE = '''
from dataclasses import dataclass


@dataclass
class Greeting:
    name: str
    times: int


def greet(name: str, times: int = 1) -> list[Greeting]:
    """Greets a name.

    @query
    """
    return [Greeting(name, times)]
'''

#: The same function, changed to take a greeting word too.
_CHANGED = '''
from dataclasses import dataclass


@dataclass
class Greeting:
    word: str
    name: str


def greet(word: str, name: str) -> list[Greeting]:
    """Greets a name with a word.

    @query
    """
    return [Greeting(word, name)]
'''


def _mapping(code: str) -> str:
    """The source's mapping as the command form writes it (codeSource.codeMappingJson)."""
    return json.dumps(
        {"runtime": "python", "code": {"kind": "entered", "source": code, "dependencies": []}}
    )


def _source_input(code: str) -> str:
    """The source as the command form saves it: no table of its own, so no landing clock."""
    return (
        f'{{id: "{_SOURCE}", type: "python", host: "", port: 0, database: "", '
        f"mappingJson: {json.dumps(_mapping(code))}}}"
    )


def _command_body() -> dict:
    """What the command form sends: the function named, no arguments and no columns."""
    return {
        "name": "greet",
        "sourceId": _SOURCE,
        "schemaName": "code",
        "functionName": "greet",
        "implKind": "code_command",
        "arguments": [],
        "outputColumns": None,
        "domainId": "fx",
        "visibleTo": [_ROLE],
    }


def _registered(srv) -> dict:
    response = httpx.get(
        f"{srv.base_url}/admin/actions",
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 30,
    )
    assert response.status_code == 200, response.text
    (fn,) = [f for f in response.json()["functions"] if f["name"] == "greet"]
    return fn


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("command_entered_code")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    srv = IsolatedServer(
        "command_entered_code",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        env={"PROVISA_RUNTIME_DEPS_CACHE": str(tmp_path_factory.mktemp("runtime-deps"))},
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


@pytest.fixture(scope="module")
def saved(server):
    """The command saved as the command form saves it: its code, then the command."""
    _admin(server, f"createSource(input: {_source_input(_CODE)})")
    response = httpx.post(
        f"{server.base_url}/admin/actions/functions",
        json=_command_body(),
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    return _registered(server)


def test_its_arguments_and_columns_are_the_declarations(saved):
    assert saved["implKind"] == "code_command"
    assert saved["kind"] == "query"
    assert [(a["name"], a["type"]) for a in saved["arguments"]] == [
        ("name", "text"),
        ("times", "bigint"),
    ]
    assert [(c["name"], c["type"]) for c in saved["outputColumns"]] == [
        ("name", "text"),
        ("times", "bigint"),
    ]


def test_it_runs_through_the_pipeline(server, saved):
    del saved
    rows = _rows(_sql(server, "SELECT name, times FROM greet('Ada', 3)"))
    assert rows == [{"name": "Ada", "times": 3}]


def test_the_code_is_kept_as_entered_code(server, saved):
    del saved
    response = httpx.post(
        f"{server.base_url}/admin/graphql",
        json={"query": "{ sources { id mappingJson } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 30,
    )
    assert response.status_code == 200, response.text
    (source,) = [s for s in response.json()["data"]["sources"] if s["id"] == _SOURCE]
    code = json.loads(source["mappingJson"])["code"]
    assert code["kind"] == "entered"
    assert code["source"] == _CODE


def test_changed_code_saved_again_is_read_again(server, saved):
    del saved
    _admin(server, f"updateSource(input: {_source_input(_CHANGED)})")
    response = httpx.put(
        f"{server.base_url}/admin/actions/functions/greet",
        json=_command_body(),
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    fn = _registered(server)
    assert [a["name"] for a in fn["arguments"]] == ["word", "name"]
    assert [c["name"] for c in fn["outputColumns"]] == ["word", "name"]
    rows = _rows(_sql(server, "SELECT word, name FROM greet('Hello', 'Ada')"))
    assert rows == [{"word": "Hello", "name": "Ada"}]
