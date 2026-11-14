# Copyright (c) 2026 Kenneth Stott
# Canary: 1ddc9d34-7ec1-40b1-a2ae-e7ed7b0a7c94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-2234: Register Table reads the columns of every table it lists, one after another.

An OpenAPI table's columns were read by parsing the source's whole spec again at each table. The
Stripe spec has 273 read operations and took seconds to parse, so a table far down its list could
not be picked for minutes. A spec is parsed once per source and loaded spec; a spec loaded again
(the source re-registered) is parsed again.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import schema_query
from provisa.openapi import mapper


def _spec(*operation_ids: str) -> dict:
    item = {
        "type": "array",
        "items": {"type": "object", "properties": {"id": {"type": "integer"}}},
    }
    return {
        "openapi": "3.0.0",
        "paths": {
            f"/{op}": {
                "get": {
                    "operationId": op,
                    "responses": {
                        "200": {"content": {"application/json": {"schema": item}}},
                    },
                }
            }
            for op in operation_ids
        },
    }


@pytest.fixture
def parses(monkeypatch):
    """Counts the spec parses, and serves a source `api` whose spec is set by the test."""
    import provisa.api.app as app

    calls: list[dict] = []
    real = mapper.parse_spec

    def counted(spec, *args, **kwargs):
        calls.append(spec)
        return real(spec, *args, **kwargs)

    monkeypatch.setattr(mapper, "parse_spec", counted)
    state = SimpleNamespace(source_types={"api": "openapi"}, openapi_specs={})
    monkeypatch.setattr(app, "state", state)
    monkeypatch.setattr(schema_query, "_engine_reaches_live", lambda _s, _t: False)
    return SimpleNamespace(calls=calls, state=state)


def _load(state, spec: dict) -> None:
    state.openapi_specs["api"] = {"spec": spec, "base_url": "https://api.test"}


async def test_each_table_read_parses_the_spec_once(parses):
    spec = _spec("listA", "listB", "listC")
    _load(parses.state, spec)
    for table in ("listA", "listB", "listC"):
        columns = await schema_query._introspect_available_columns("api", "openapi", table)
        assert [c.name for c in columns] == ["id"]
    assert parses.calls == [spec]


async def test_a_spec_loaded_again_is_parsed_again(parses):
    first = _spec("listA")
    _load(parses.state, first)
    await schema_query._introspect_available_columns("api", "openapi", "listA")
    second = _spec("listA", "listNew")
    _load(parses.state, second)
    columns = await schema_query._introspect_available_columns("api", "openapi", "listNew")
    assert [c.name for c in columns] == ["id"]
    assert parses.calls == [first, second]
