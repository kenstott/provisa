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


def test_each_environments_connection_to_a_source_has_its_own_attach():
    """REQ-1529, REQ-1942 on DuckDB: an environment inheriting the connection shares the attach
    already made (the ATTACH is the same); one whose own binding points elsewhere -- a dev lane
    repointed, a synthetic lane bound to its store -- gets an attach named for its catalog; a
    changed binding lets the old connection go."""
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
        _catalog_of=DuckDBFederationRuntime._catalog_of,
    )

    def source(catalog: str):
        return SimpleNamespace(
            id="sales-pg", type=SimpleNamespace(value="postgresql"), catalog=catalog
        )

    def details(host: str):
        return {
            "attach": f"ATTACH 'host={host}' AS \"_src_sales-pg\"",
            "raw_alias": "_src_sales-pg",
        }

    attach = DuckDBFederationRuntime._attach_raw
    assert attach(runtime, source("sales_pg"), details("prod")) == "_src_sales-pg"
    # Inherited: the same connection, the same attach.
    assert attach(runtime, source("org_x_env_qa__sales_pg"), details("prod")) == "_src_sales-pg"
    # Repointed to the lane's own database: an attach of its own.
    dev = attach(runtime, source("org_x_env_dev__sales_pg"), details("dev"))
    assert dev == "_src_sales-pg__org_x_env_dev__sales_pg"
    # A synthetic lane bound to its store: likewise.
    store = attach(runtime, source("org_x_env_syn__sales_pg"), details("store"))
    assert store == "_src_sales-pg__org_x_env_syn__sales_pg"
    assert ran == [
        "ATTACH 'host=prod' AS \"_src_sales-pg\"",
        f"ATTACH 'host=dev' AS \"{dev}\"",
        f"ATTACH 'host=store' AS \"{store}\"",
    ]
    # The dev lane rebound to another database: its old connection is let go first.
    assert attach(runtime, source("org_x_env_dev__sales_pg"), details("dev2")) == dev
    assert ran[-2:] == [f'DETACH "{dev}"', f"ATTACH 'host=dev2' AS \"{dev}\""]
