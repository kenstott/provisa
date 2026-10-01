# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7a9c41-5b2d-4f86-9a10-c8d4e6f2b735
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An OpenAPI source's spec is parsed once, not once per statement (REQ-1730, REQ-1877).

``_lookup_openapi_table`` answers "is this table name an OpenAPI operation?" for every table of
every statement the pipeline routes (``would_materialize_optimize``). It re-parsed every
registered spec on each call — measured 0.1-0.56 ms per request on every raw-SQL and compiled
surface. The parsed operations are kept while the spec entry they came from is the same object,
the rule every kept plan follows."""

# Requirements: REQ-1730, REQ-1877

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.data import materialization
from provisa.openapi import mapper

_SPEC = {
    "openapi": "3.0.0",
    "paths": {
        "/pets": {"get": {"operationId": "listPets", "responses": {"200": {"description": "ok"}}}},
        "/pets/{id}": {
            "get": {
                "operationId": "getPetById",
                "parameters": [{"name": "id", "in": "path", "required": True}],
                "responses": {"200": {"description": "ok"}},
            }
        },
    },
}


@pytest.fixture
def parses(monkeypatch):
    seen = {"n": 0}
    real = mapper.parse_spec

    def _counting(spec, operation_overrides=None):
        seen["n"] += 1
        return real(spec, operation_overrides=operation_overrides)

    monkeypatch.setattr(mapper, "parse_spec", _counting)
    return seen


def _state():
    return SimpleNamespace(openapi_specs={"petstore": {"spec": _SPEC, "base_url": "http://x"}})


def test_repeated_lookups_parse_each_spec_once(parses):
    state = _state()
    first = materialization._lookup_openapi_table(state, "listPets")
    assert first[0] == "petstore" and first[2].operation_id == "listPets"
    for name in ("listPets", "list_pets", "getPetById", "orders", "customers") * 20:
        source_id, _entry, query = materialization._lookup_openapi_table(state, name)
        assert (source_id is None) == (name in ("orders", "customers"))
        assert query is None or query.operation_id in ("listPets", "getPetById")
    assert parses["n"] == 1


def test_a_replaced_spec_entry_is_parsed_again(parses):
    state = _state()
    materialization._lookup_openapi_table(state, "listPets")
    renamed = {"openapi": "3.0.0", "paths": {"/cats": {"get": {"operationId": "listCats"}}}}
    state.openapi_specs["petstore"] = {"spec": renamed, "base_url": "http://x"}  # re-registered
    assert materialization._lookup_openapi_table(state, "listPets") == (None, None, None)
    assert materialization._lookup_openapi_table(state, "listCats")[0] == "petstore"
    assert parses["n"] == 2
    # an operation override is part of what the parse depends on
    state.openapi_specs["petstore"] = {
        "spec": renamed,
        "base_url": "http://x",
        "operation_overrides": {"listCats": "mutation"},
    }
    assert materialization._lookup_openapi_table(state, "listCats") == (None, None, None)
    assert parses["n"] == 3


def test_two_states_share_nothing(parses):
    a, b = _state(), _state()
    b.openapi_specs["petstore"] = {"spec": {"openapi": "3.0.0", "paths": {}}, "base_url": "y"}
    assert materialization._lookup_openapi_table(a, "listPets")[0] == "petstore"
    assert materialization._lookup_openapi_table(b, "listPets") == (None, None, None)


def test_a_spec_that_does_not_parse_is_skipped_and_retried(monkeypatch):
    calls = {"n": 0}

    def _boom(spec, operation_overrides=None):
        calls["n"] += 1
        raise ValueError("bad spec")

    monkeypatch.setattr(mapper, "parse_spec", _boom)
    state = _state()
    for _ in range(3):
        assert materialization._lookup_openapi_table(state, "listPets") == (None, None, None)
    assert calls["n"] == 3
