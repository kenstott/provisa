# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1865: row_materialize needs a row fetch by primary keys, and each source type declares
whether it has one (``core.source_registry.ROW_FETCH_BY_KEYS``).

The declaration is held to the code: every entry points at a fetch that exists, and the declared
types are exactly the ones ``SourceRowLoader.load_keys`` is wired to fetch by key, plus the lakes
the engine reads in place. A table whose source's type declares none is refused row_materialize
by name, at config load and on save."""

# Requirements: REQ-1865

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.core import source_registry
from provisa.core.config_loader import _validate_row_materialize
from provisa.core.models import SourceType
from provisa.core.source_registry import ROW_FETCH_BY_KEYS, supports_row_fetch_by_keys
from tests.unit.test_row_materialize_model import _FakeConfig, _FakeSource, _table

_EVERY_TYPE = sorted(t.value for t in SourceType)


def _resolve(pointer: str):
    module, attribute = pointer.split(":")
    target = importlib.import_module(module)
    for part in attribute.split("."):
        target = getattr(target, part)
    return target


@pytest.mark.parametrize("source_type", sorted(ROW_FETCH_BY_KEYS))
def test_every_declared_fetch_exists(source_type):
    assert callable(_resolve(ROW_FETCH_BY_KEYS[source_type]))


def _wired() -> dict[str, str]:
    """Each type ``build_keyed_adapter_loaders`` wires a key fetch for, with the factory that
    made it -- for an engine that attaches nothing, over a state holding a source of every type."""
    from provisa.events.app_wiring import build_keyed_adapter_loaders

    sources = [SimpleNamespace(id=f"s_{t}", type=SimpleNamespace(value=t)) for t in _EVERY_TYPE]
    state = SimpleNamespace(
        config=SimpleNamespace(sources=sources),
        source_types={s.id: t for s, t in zip(sources, _EVERY_TYPE)},
        source_dialects={},
    )
    with patch("provisa.federation.strategy.engine_attaches", lambda _engine, _type: False):
        loaders = build_keyed_adapter_loaders(state, None)
    return {t: fetch.__qualname__.split(".")[0] for t, fetch in loaders.items()}


def test_the_declared_types_are_the_ones_fetched_by_key():
    """Both directions: a type declared without a wired fetch would be accepted and then never
    load; a type fetched by key but not declared would be refused for no reason."""
    from provisa.events.source_loader import _ADAPTER_FETCH_ONLY

    wired = _wired()
    terminal = {t for t, p in ROW_FETCH_BY_KEYS.items() if p.endswith("SourceRowLoader.load_keys")}
    assert set(ROW_FETCH_BY_KEYS) == set(wired) | terminal
    # Each wired type's declaration names the factory that wired it.
    for source_type, factory in wired.items():
        assert ROW_FETCH_BY_KEYS[source_type].endswith(f":{factory}"), source_type
    # The lakes the engine reads in place have no loader of their own and are not adapter-only.
    assert terminal == {"delta_lake", "hive", "iceberg"}
    assert not terminal & (set(wired) | _ADAPTER_FETCH_ONLY)


def test_a_live_only_type_declares_no_fetch_and_is_not_wired():
    assert not supports_row_fetch_by_keys("qdrant")
    assert "qdrant" not in _wired()


def _validate(source_type: str) -> None:
    table = _table(row_materialize=True, cache_ttl=60)
    _validate_row_materialize(_FakeConfig([table], [_FakeSource(type=source_type)]))


@pytest.mark.parametrize(
    "source_type",
    sorted(t for t in ROW_FETCH_BY_KEYS if t not in {"govdata", "neo4j"}),
)
def test_a_type_with_a_fetch_by_keys_is_accepted(source_type):
    _validate(source_type)  # does not raise


_DISTINCT = {"openapi", "graphql_remote", "grpc_remote", "govdata"}


@pytest.mark.parametrize(
    "source_type",
    sorted(t for t in _EVERY_TYPE if t not in ROW_FETCH_BY_KEYS and t not in _DISTINCT),
)
def test_a_type_without_one_is_refused_by_name(source_type):
    with pytest.raises(ValueError) as refused:
        _validate(source_type)
    assert str(refused.value) == (
        f"orders: row_materialize needs row fetch by primary keys, which {source_type} sources "
        "don't support"
    )


@pytest.mark.parametrize("source_type", sorted(_DISTINCT))
def test_a_type_refused_for_its_own_reason_says_that_reason(source_type):
    with pytest.raises(ValueError, match=f"refused for source type '{source_type}'"):
        _validate(source_type)


def test_the_maintainers_list_is_refused():
    """REST/OpenAPI, GraphQL-remote, gRPC-remote, SPARQL, code sources and Qdrant."""
    for source_type in ("openapi", "graphql_remote", "grpc_remote", "sparql", "node", "python"):
        assert source_registry.row_materialize_refusal("t", source_type) is not None
    assert source_registry.row_materialize_refusal("t", "qdrant") == (
        "t: row_materialize needs row fetch by primary keys, which qdrant sources don't support"
    )


@pytest.mark.asyncio
async def test_saving_a_table_refuses_it_by_its_sources_stored_type(monkeypatch):
    from provisa.api.admin import _landing_ttl

    async def _stored(_conn, source_id):
        return {"svc": "openapi", "graph": "sparql", "pg": "postgresql"}[source_id]

    monkeypatch.setattr(_landing_ttl, "_stored_type", _stored)
    refusal = _landing_ttl.row_materialize_save_refusal
    assert await refusal(None, "pg", "orders", True) is None
    assert await refusal(None, "graph", "orders", False) is None  # not asked for
    sparql = await refusal(None, "graph", "orders", True)
    assert sparql is not None and sparql.success is False
    assert sparql.message == (
        "orders: row_materialize needs row fetch by primary keys, which sparql sources don't "
        "support"
    )
    api = await refusal(None, "svc", "orders", True)
    assert api is not None and "filtered by its own parameters" in api.message
