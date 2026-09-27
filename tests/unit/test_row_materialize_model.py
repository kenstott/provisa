# Copyright (c) 2026 Kenneth Stott
# Canary: 2863f86e-399e-4ff6-96ec-5c4708956966
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1865: Table.row_materialize validation — PK required, conflict with materialize (CTAS),
and the cache_ttl-resolution + query_template $keys checks in config_loader."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from provisa.core.config_loader import _validate_row_materialize
from provisa.core.models import Column, Table


def _pk_column(name: str = "id") -> Column:
    return Column(name=name, visible_to=["public"], data_type="integer", is_primary_key=True)


def _plain_column(name: str = "status") -> Column:
    return Column(name=name, visible_to=["public"], data_type="text")


def _table(**kwargs) -> Table:
    defaults = dict(
        source_id="src1",
        domain_id="dom1",
        schema_name="public",
        table_name="orders",
        columns=[_pk_column(), _plain_column()],
    )
    defaults.update(kwargs)
    return Table(**defaults)


def test_row_materialize_requires_primary_key():
    with pytest.raises(ValidationError, match="requires at least one is_primary_key column"):
        Table(
            source_id="src1",
            domain_id="dom1",
            schema_name="public",
            table_name="orders",
            columns=[_plain_column()],
            row_materialize=True,
        )


def test_row_materialize_conflicts_with_materialize():
    with pytest.raises(ValidationError, match="mutually exclusive"):
        _table(row_materialize=True, materialize=True)


def test_row_materialize_with_pk_and_no_materialize_ok():
    t = _table(row_materialize=True)
    assert t.row_materialize is True


class _FakeSource:
    def __init__(self, cache_ttl=None, type="neo4j"):
        self.id = "src1"
        self.cache_ttl = cache_ttl
        self.type = type


class _FakeConfig:
    def __init__(self, tables, sources):
        self.tables = tables
        self.sources = sources


def test_config_loader_rejects_row_materialize_with_no_resolvable_cache_ttl():
    table = _table(row_materialize=True, cache_ttl=None)
    config = _FakeConfig([table], [_FakeSource(cache_ttl=None)])
    with pytest.raises(ValueError, match="resolved cache_ttl"):
        _validate_row_materialize(config)


def test_config_loader_accepts_row_materialize_with_table_own_cache_ttl():
    table = _table(row_materialize=True, cache_ttl=60)
    config = _FakeConfig([table], [_FakeSource(cache_ttl=None)])
    _validate_row_materialize(config)  # does not raise


def test_config_loader_accepts_row_materialize_inheriting_source_cache_ttl():
    table = _table(row_materialize=True, cache_ttl=None)
    config = _FakeConfig([table], [_FakeSource(cache_ttl=300)])
    _validate_row_materialize(config)  # does not raise


def test_config_loader_rejects_query_template_not_projecting_the_pk():
    """The PK's own column name ("id") is never returned by this template -- the keyed-fetch
    filter can only bind to a property the template already projects, never an invented one."""
    table = _table(
        row_materialize=True,
        cache_ttl=60,
        query_template="MATCH (o:Order) RETURN o.status AS status",
    )
    config = _FakeConfig([table], [_FakeSource(cache_ttl=None, type="neo4j")])
    with pytest.raises(ValueError, match="projected elements"):
        _validate_row_materialize(config)


def test_config_loader_accepts_query_template_projecting_the_pk():
    table = _table(
        row_materialize=True,
        cache_ttl=60,
        query_template="MATCH (o:Order) RETURN o.id AS id, o.status AS status",
    )
    config = _FakeConfig([table], [_FakeSource(cache_ttl=None, type="neo4j")])
    _validate_row_materialize(config)  # does not raise


def test_config_loader_rejects_query_template_on_non_neo4j_source():
    """sparql has no parser/AST to safely resolve a projected property or splice a filter against
    -- row_materialize on a query_template table is only wired for neo4j today."""
    table = _table(
        row_materialize=True,
        cache_ttl=60,
        query_template="SELECT ?id ?status WHERE { ?o :id ?id ; :status ?status }",
    )
    config = _FakeConfig([table], [_FakeSource(cache_ttl=None, type="sparql")])
    with pytest.raises(ValueError, match="no keyed-fetch translation"):
        _validate_row_materialize(config)
