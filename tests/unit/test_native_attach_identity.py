# Copyright (c) 2026 Kenneth Stott
# Canary: 3b4c4d4a-a5c0-4f00-9fac-206c05cdad00
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A native engine attaches every registered table, each by its identity (REQ-1674).

The walk that attaches registered tables remembered what it had attached by ``schema.table``:
two sources that both hold ``public.orders`` were one entry, and the second source's table was
skipped as already attached — never queryable on the engine."""

# Requirements: REQ-1674, REQ-841

from __future__ import annotations

from types import SimpleNamespace

from provisa.federation.native_backend import NativeEngineBackend


class _Runtime:
    def __init__(self) -> None:
        self.attached: list[tuple[str, str, str]] = []

    def attach_source(self, source) -> None:
        self.attached.append((source.id, source.schema_name, source.table_name))


class _Backend(NativeEngineBackend):
    def __init__(self) -> None:  # no engine wiring: the walk alone is under test
        self._runtime = _Runtime()
        # The walk attaches only a source its engine reads in place (a postgresql attach here).
        self.engine = SimpleNamespace(
            name="duckdb", connector_for=lambda _type: SimpleNamespace(reads_in_place=True)
        )
        self._attached = set()
        self._detached = set()
        self._refused = set()


def _src(source_id: str):
    return SimpleNamespace(
        id=source_id,
        type=SimpleNamespace(value="postgresql"),
        host="h",
        port=5432,
        database="d",
        username="u",
        password="p",
        path=None,
        base_url=None,
        federation_hints={},
        mapping={},
    )


def test_two_sources_same_named_tables_are_each_attached(monkeypatch):
    monkeypatch.setattr("provisa.core.operator_floor.floor_setting", lambda _src: None)
    backend = _Backend()
    config = SimpleNamespace(
        sources=[_src("sales"), _src("crm")],
        tables=[
            SimpleNamespace(source_id="sales", schema_name="public", table_name="orders"),
            SimpleNamespace(source_id="crm", schema_name="public", table_name="orders"),
        ],
    )
    state = SimpleNamespace(
        runtime_sources={},
        tables=[],
        model_db=None,
        tenant_db=None,
        source_catalogs={"sales": "sales", "crm": "crm"},
    )

    assert backend._walk_registry(state, config) is True
    assert sorted(backend._runtime.attached) == [
        ("crm", "public", "orders"),
        ("sales", "public", "orders"),
    ]


def test_one_source_is_attached_under_each_environments_catalog(monkeypatch):
    """REQ-1529, REQ-1942: one engine serves an org's environments, each attaching the same source
    under its own catalog -- prod's attach is not the environment's."""
    monkeypatch.setattr("provisa.core.operator_floor.floor_setting", lambda _src: None)
    backend = _Backend()
    attached: list[str] = []
    backend._runtime.attach_source = lambda source: attached.append(source.catalog)
    config = SimpleNamespace(
        sources=[_src("sales")],
        tables=[SimpleNamespace(source_id="sales", schema_name="public", table_name="orders")],
    )
    for catalog in ("org_acme__sales", "org_acme_env_qa__sales"):
        state = SimpleNamespace(
            runtime_sources={},
            tables=[],
            model_db=None,
            tenant_db=None,
            source_catalogs={"sales": catalog},
        )
        assert backend._walk_registry(state, config) is True
    assert attached == ["org_acme__sales", "org_acme_env_qa__sales"]


def test_the_duckdb_runtime_names_each_environments_catalog_as_the_compiler_does():
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    handed = SimpleNamespace(id="sales-pg", catalog="org_acme_env_qa__sales_pg")
    assert DuckDBFederationRuntime._catalog_of(handed) == "org_acme_env_qa__sales_pg"
    # An introspection attach hands no catalog: prod's, the source id normalized.
    assert DuckDBFederationRuntime._catalog_of(SimpleNamespace(id="sales-pg")) == "sales_pg"


def test_a_source_reached_through_another_connection_is_refused():
    """REQ-1529: the native engine keeps one connection per source id; an environment binding the
    source to another database is refused rather than read through the first."""
    import pytest

    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    ran: list[str] = []
    runtime = SimpleNamespace(
        _sqlite_loaded=True,
        _pg_ext_loaded=True,
        _con=SimpleNamespace(execute=ran.append),
        _raw_attached=set(),
        _raw_attach_ddl={},
        _ext_loaded=set(),
        _engine=SimpleNamespace(connector_for=lambda _type: SimpleNamespace(extension=None)),
    )
    pg = SimpleNamespace(id="sales-pg", type=SimpleNamespace(value="postgresql"))
    prod = {"attach": "ATTACH 'host=prod' AS \"_src_sales-pg\"", "raw_alias": "_src_sales-pg"}
    assert DuckDBFederationRuntime._attach_raw(runtime, pg, prod) == "_src_sales-pg"
    assert (
        DuckDBFederationRuntime._attach_raw(runtime, pg, prod) == "_src_sales-pg"
    )  # inherited: shared
    assert ran == [prod["attach"]]
    own = {"attach": "ATTACH 'host=dev' AS \"_src_sales-pg\"", "raw_alias": "_src_sales-pg"}
    with pytest.raises(RuntimeError, match="another connection"):
        DuckDBFederationRuntime._attach_raw(runtime, pg, own)
