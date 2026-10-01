# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4e2a71-3c5b-4f86-b1e0-7a2c8d5f9e13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SECURITY (REQ-001, REQ-266): governed raw SQL reads only registered relations.

Every raw-SQL surface (pgwire, Flight SQL, /data/sql, the Cypher/NL SQL tabs) goes through
``validate_sql`` in the one pipeline. A relation there that is not a registered table — a DuckDB
system/meta table function (``duckdb_secrets()``, ``duckdb_settings()``, ``duckdb_databases()``),
a file reader (``read_csv('/etc/passwd')``) or a quoted file path DuckDB answers with a
replacement scan — is engine internals or the engine host's filesystem, not governed data. A role
with ``domain_access: ["*"]`` skipped the unregistered-relation check entirely, so such SQL reached
the engine: on DuckDB ``duckdb_secrets()`` prints http-secret headers (the ClickHouse
X-ClickHouse-Key) in clear text, and a quoted ``".../x.csv"`` reads a host file.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta

pytestmark = pytest.mark.asyncio

_ORDERS = TableMeta(
    table_id=1,
    field_name="orders",
    type_name="Orders",
    source_id="pg",
    catalog_name="pg",
    schema_name="sales",
    table_name="orders",
    domain_id="sales",
)
_TABLES = [
    {
        "id": 1,
        "source_id": "pg",
        "schema_name": "sales",
        "table_name": "orders",
        "domain_id": "sales",
        "columns": [{"column_name": "id", "data_type": "integer", "visible_to": ["analyst"]}],
    }
]


def _state(domain_access: list[str]) -> SimpleNamespace:
    ctx = CompilationContext()
    # Keyed by field name too: a domain_prefix schema exposes the table as ``sa__orders``.
    ctx.tables = {"orders": _ORDERS, "sa__orders": _ORDERS}
    return SimpleNamespace(
        admin_db=None,  # as AppState without a control plane: no debug-trace settings
        contexts={"analyst": ctx},
        rls_contexts={"analyst": RLSContext.empty()},
        roles={"analyst": {"id": "analyst", "capabilities": [], "domain_access": domain_access}},
        masking_rules={},
        tables=_TABLES,
        source_types={"pg": "postgresql"},
        source_catalogs={},
        relationships=[],
        metrics={},
        federation_engine=None,
        view_sql_map=None,
        security_high=False,
        schema_boot_id="test-boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        routing_cache=CompiledQueryCache(),
    )


class _Routed(Exception):
    """Raised by the routing stand-in: the statement cleared governance and reached routing."""


async def _govern(monkeypatch, sql: str, domain_access: list[str]) -> None:
    """Run the statement through the pipeline top. Raises ``_Routed`` when it cleared
    governance (routing is where the engine would take it), else what governance raised."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", _state(domain_access), raising=False)

    async def _routing(*_args, **_kwargs):
        raise _Routed

    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_routing)):
        await _pipeline._govern_and_route(sql, "analyst")


_ENGINE_INTERNALS = [
    "SELECT * FROM duckdb_secrets()",
    "SELECT name, value FROM duckdb_settings()",
    "SELECT path FROM duckdb_databases()",
    "SELECT * FROM read_csv('/etc/passwd')",
    'SELECT * FROM "/etc/hosts.csv"',
    "SELECT o.id, (SELECT count(*) FROM duckdb_secrets()) AS n FROM sales.orders o",
    "SELECT o.id FROM sales.orders o JOIN duckdb_databases() d ON d.path = 'x'",
]


@pytest.mark.parametrize("domain_access", [["*"], ["sales"]])
@pytest.mark.parametrize("sql", _ENGINE_INTERNALS)
async def test_engine_internals_are_refused_before_routing(monkeypatch, sql, domain_access):
    with pytest.raises(PermissionError, match="V006"):
        await _govern(monkeypatch, sql, domain_access)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT o.id FROM sales.orders o",
        "SELECT o.id FROM sa__orders o",
        "WITH x AS (SELECT o.id FROM sales.orders o) SELECT id FROM x",
        "SELECT g FROM generate_series(1, 3) AS t(g)",
    ],
)
async def test_registered_relations_ctes_and_generators_still_route(monkeypatch, sql):
    with pytest.raises(_Routed):
        await _govern(monkeypatch, sql, ["*"])
