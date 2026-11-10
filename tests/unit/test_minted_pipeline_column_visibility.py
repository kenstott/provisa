# Copyright (c) 2026 Kenneth Stott
# Canary: 4768f909-4db6-41b9-915c-9377bed2e780
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A minted session's statement is governed by the one pipeline as the calling role (REQ-2226):
a column the role's grant does not name is refused however the statement names it, a star reads
only what the role is served, and a grant to an ancestor reaches the child. The listener adds no
check of its own; these run the statement through ``_govern_and_route`` with the route stubbed."""

# Requirements: REQ-2226, REQ-039

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from provisa.compiler.introspect import ColumnMetadata

ORDERS = 11
_READER = "ledger_analyst"


def _col(name: str, *roles: str) -> dict:
    return {
        "column_name": name,
        "data_type": "varchar",
        "visible_to": list(roles),
        "native_filter_type": None,
    }


def _tables(secret_to: tuple[str, ...]) -> list[dict]:
    return [
        {
            "id": ORDERS,
            "domain_id": "fx",
            "source_id": "ledger",
            "schema_name": "default",
            "table_name": "orders",
            "alias": None,
            "columns": [
                _col("id", _READER, "org_admin"),
                _col("region", _READER, "org_admin"),
                _col("secret", *secret_to),
            ],
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
        }
    ]


class _Routed(Exception):
    """The statement passed governance; carries what would have run."""


def _state(monkeypatch, secret_to: tuple[str, ...]):
    monkeypatch.setenv("PROVISA_SINGLE_DOMAIN", "false")
    import provisa.api.app as appmod
    from provisa.api.app_loaders import _build_and_register_schemas
    from provisa.core.db import standard_role_dicts
    from provisa.security.inheritance import expand_column_grants, role_chains

    st = appmod.state
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(st, name, {}, raising=False)
    for name in ("meta_roles", "meta_resolved_at", "masking_rules"):
        monkeypatch.setattr(st._active_runtime(), name, {}, raising=False)
    monkeypatch.setattr(st, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(st, "source_types", {"ledger": "python"}, raising=False)
    monkeypatch.setattr(st, "source_catalogs", {"ledger": "ledger"}, raising=False)
    monkeypatch.setattr(st, "runtime_sources", {"ledger": {"mapping": {}}}, raising=False)
    roles = [{**r, "description": "t"} for r in standard_role_dicts()] + [
        {
            "id": _READER,
            "description": "t",
            "parents": ["developer"],
            "capabilities": ["usage", "query_development", "command_execution", "full_results"],
            "domain_access": ["*"],
        }
    ]
    # As the runtime build folds them (api.app): a grant to an ancestor reaches its descendants.
    tables = _tables(secret_to)
    expand_column_grants(tables, role_chains(roles))
    _build_and_register_schemas(
        roles=roles,
        tables=tables,
        relationships=[],
        col_types_converted={
            ORDERS: [
                ColumnMetadata(column_name=c, data_type="varchar", is_nullable=True)
                for c in ("id", "region", "secret")
            ]
        },
        naming_rules=[],
        domains=[{"id": "fx"}],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=[],
    )
    monkeypatch.setattr(st, "tables", tables, raising=False)
    return st


async def _govern(sql: str, role: str) -> str:
    """The governed statement ``sql`` routes as for ``role``; PermissionError when refused."""
    from provisa.pgwire import _pipeline

    async def _route(*args, **kwargs):
        raise _Routed(str(kwargs.get("governed_sql") or (args[0] if args else "")))

    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_route)):
        try:
            await _pipeline._govern_and_route(sql, role)
        except _Routed as routed:
            return str(routed)
    raise AssertionError(f"{sql!r} neither routed nor was refused")


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT "secret" FROM "fx"."orders"',
        "SELECT o.secret FROM fx.orders AS o",
        "SELECT (SELECT o.secret FROM fx.orders AS i WHERE i.id = o.id) FROM fx.orders AS o",
    ],
)
async def test_a_column_the_role_is_not_granted_is_refused(monkeypatch, sql):
    _state(monkeypatch, ("org_admin",))
    with pytest.raises(PermissionError, match="secret"):
        await _govern(sql, _READER)


async def test_a_granted_column_is_read(monkeypatch):
    _state(monkeypatch, (_READER,))
    governed = await _govern('SELECT "secret" FROM "fx"."orders"', _READER)
    assert "secret" in governed


async def test_a_star_reads_only_the_columns_the_role_is_served(monkeypatch):
    _state(monkeypatch, ("org_admin",))
    governed = await _govern("SELECT * FROM fx.orders", _READER)
    assert "secret" not in governed
    assert "region" in governed


async def test_a_grant_to_an_ancestor_reaches_the_child(monkeypatch):
    _state(monkeypatch, ("developer",))
    governed = await _govern('SELECT "secret" FROM "fx"."orders"', _READER)
    assert "secret" in governed


async def test_a_qualifier_naming_no_governed_relation_is_refused(monkeypatch):
    _state(monkeypatch, ("org_admin",))
    with pytest.raises(PermissionError):
        await _govern("SELECT x.secret FROM fx.orders AS o", _READER)


def _v003(sql: str) -> list[str]:
    """The column check's (V003) refusals of ``sql`` over fx.orders, ``secret`` hidden."""
    import sqlglot

    from provisa.compiler.cte_utils import cte_names
    from provisa.compiler.sql_validator import _check_column_visibility
    from provisa.compiler.stage2 import GovernanceContext

    gov = GovernanceContext()
    gov.table_map = {"fx.orders": ORDERS, "orders": ORDERS}
    gov.all_columns = {ORDERS: [(c, "varchar") for c in ("id", "region", "secret")]}
    gov.visible_columns = {ORDERS: frozenset({"id", "region"})}
    tree = sqlglot.parse_one(sql, read="postgres")
    return [v.message for v in _check_column_visibility(tree, gov, cte_names(tree))]


def test_a_correlated_reference_to_a_hidden_column_is_refused_by_the_column_check():
    """An outer table's hidden column read from a subquery is judged where it resolves, the
    outer table, not skipped because the subquery's own FROM does not hold it (REQ-039)."""
    refused = _v003(
        "SELECT o.id FROM fx.orders AS o "
        "WHERE EXISTS (SELECT 1 FROM fx.orders AS i WHERE i.id = 1 AND o.secret IS NOT NULL)"
    )
    assert refused == []  # a WHERE is another rule's; the select list is this one's
    # A lateral derived table's select list reads the outer table: no enclosing select list
    # reaches the column, so only resolving the qualifier outward judges it.
    refused = _v003("SELECT d.x FROM fx.orders AS o, LATERAL (SELECT o.secret AS x) AS d")
    assert any("o.secret" in m for m in refused), refused
    assert _v003("SELECT d.x FROM fx.orders AS o, LATERAL (SELECT o.region AS x) AS d") == []


def test_a_qualifier_naming_a_derived_relation_is_judged_in_its_own_select():
    assert _v003("SELECT d.id FROM (SELECT id FROM fx.orders) AS d") == []
    assert _v003("WITH d AS (SELECT id FROM fx.orders) SELECT d.id FROM d") == []
    assert _v003("SELECT g.n FROM generate_series(1, 3) AS g(n)") == []
