# Copyright (c) 2026 Kenneth Stott
# Canary: 4b9d1f72-6e05-4a38-b2c9-8f0a5d7e3c16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin catalog reads answer by the caller's rights and reach (REQ-1958).

``tables``, ``relationships``, ``domains``, ``sources`` and ``metrics`` used to answer every
signed-in caller with the whole registered catalog, grant lists included. A catalog
administrator (``table_registration`` or a hiding right, REQ-1944) reads the catalog of the
domains those roles reach; anyone else reads what the role the request acts as is served, from
the one narrowing the Flight and MCP catalogs use; grant lists and mask settings need
``view_governance`` (REQ-1134).

The model: two domains. ``sales`` holds ``orders`` (id, region, margin) and ``customers`` (id,
name), with orders.customer_id → customers.id; ``hr`` holds ``staff`` (id, salary), with
orders.rep_id → staff.id. The analyst is served orders(id, region, customer_id) and customers.
"""

# Requirements: REQ-1958, REQ-1134, REQ-1944, REQ-1337, REQ-1327

from __future__ import annotations

import types

import pytest

import provisa.api.app as appmod
from provisa.api.admin import schema_query
from provisa.api.admin.catalog_scope import catalog_scope
from provisa.api.admin.schema_query import Query

_TABLES = [
    {"id": 1, "domain_id": "sales", "source_id": "pg", "table_name": "orders"},
    {"id": 2, "domain_id": "sales", "source_id": "pg", "table_name": "customers"},
    {"id": 3, "domain_id": "hr", "source_id": "hrdb", "table_name": "staff"},
]
_COLUMNS = {
    1: ["id", "region", "margin", "customer_id", "rep_id"],
    2: ["id", "name"],
    3: ["id", "salary"],
}
_ANALYST_SERVED = {1: ["id", "region", "customer_id"], 2: ["id", "name"]}
_ROLES = {
    "analyst": {"capabilities": ["query_development"], "domain_access": ["sales"]},
    "registrar": {"capabilities": ["table_registration"], "domain_access": ["sales"]},
    "masker": {"capabilities": ["masking_config"], "domain_access": ["hr"]},
    "auditor": {
        "capabilities": ["query_development", "view_governance"],
        "domain_access": ["sales"],
    },
    "registrar_all": {
        "capabilities": ["table_registration", "view_governance", "source_registration"],
        "domain_access": ["*"],
    },
    "platform": {"capabilities": ["cross_org"], "domain_access": ["*"]},
}
_METRICS = [
    {"name": "everyone", "visible_to": ["*"]},
    {"name": "analysts", "visible_to": ["analyst"]},
    {"name": "maskers", "visible_to": ["masker"]},
]


def _context(served: dict[int, list[str]]):
    return types.SimpleNamespace(
        tables={f"t{i}": types.SimpleNamespace(table_id=i) for i in served},
        physical_to_sql={(i, c): c for i, cols in served.items() for c in cols},
    )


class _Rows:
    def __init__(self, rows):
        self._rows = [types.SimpleNamespace(_mapping=r, **r) for r in rows]

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


def _relationship(rel_id, source, source_column, target, target_column, **extra):
    return {
        "id": rel_id,
        "source_table_id": source,
        "target_table_id": target,
        "source_column": source_column,
        "target_column": target_column,
        "via_table_id": None,
        **extra,
    }


_RELATIONSHIPS = [
    _relationship("orders-customers", 1, "customer_id", 2, "id"),
    _relationship("orders-staff", 1, "rep_id", 3, "id"),
    _relationship("orders-fn", 1, "id", None, None),  # to a function: no target table
]


class _Conn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def execute_core(self, stmt):
        sql = str(stmt)
        if "FROM relationships" in sql:
            return _Rows(_RELATIONSHIPS)
        if "FROM sources" in sql:
            wanted = [{"id": "pg"}, {"id": "hrdb"}]
            params = stmt.compile().params
            if params:
                wanted = [s for s in wanted if s["id"] in params.values()]
            return _Rows(wanted)
        if "FROM domains" in sql:
            return _Rows([{"id": "hr"}, {"id": "marketing"}, {"id": "sales"}])
        return _Rows(_TABLES)


class _Pool:
    def acquire(self):
        return _Conn()


@pytest.fixture(autouse=True)
def _model(monkeypatch):
    contexts = {
        "analyst": _context(_ANALYST_SERVED),
        "auditor": _context(_ANALYST_SERVED),
        "registrar": _context(_ANALYST_SERVED),
        "registrar_all": _context(_COLUMNS),
        "masker": _context({3: ["id"]}),
        # "platform" has none: a control-plane role is given no data surface (REQ-1327).
    }
    for name, value in (("roles", _ROLES), ("contexts", contexts)):
        monkeypatch.setattr(appmod.state, name, value, raising=False)
    monkeypatch.setattr("provisa.core.domain_policy.single_domain", lambda: False)
    monkeypatch.setattr(schema_query, "_get_pool", _pool)
    monkeypatch.setattr(schema_query, "_resolve_admin_context", lambda info: None)

    async def _table(conn, row, all_tables=None, user_can_deploy=True):  # noqa: ARG001
        return types.SimpleNamespace(
            id=row["id"],
            domain_id=row["domain_id"],
            table_name=row["table_name"],
            view_sql="SELECT 1",
            columns=[
                types.SimpleNamespace(
                    column_name=c,
                    visible_to=["analyst"],
                    writable_by=[],
                    unmasked_to=["registrar"],
                    mask_type="constant",
                    mask_pattern=None,
                    mask_replace=None,
                    mask_value="***",
                    mask_precision=None,
                )
                for c in _COLUMNS[row["id"]]
            ],
        )

    monkeypatch.setattr(schema_query, "_fetch_table_with_columns", _table)
    monkeypatch.setattr(
        schema_query, "_rel_from_row", lambda row, convention=None: types.SimpleNamespace(**row)
    )

    async def _no_synthetic(conn):  # noqa: ARG001
        return []

    monkeypatch.setattr(schema_query, "_has_table_synthetic_relationships", _no_synthetic)
    monkeypatch.setattr(
        schema_query, "_source_from_row", lambda row, connection: types.SimpleNamespace(**row)
    )
    monkeypatch.setattr(schema_query, "_domain_from_row", lambda row: types.SimpleNamespace(**row))

    async def _metrics(conn):  # noqa: ARG001
        return [
            {
                **m,
                "expression": "COUNT(*)",
                "datatype": None,
                "description": None,
                "ai_context": None,
                "from_fact": None,
            }
            for m in _METRICS
        ]

    monkeypatch.setattr("provisa.core.repositories.metric.list_all", _metrics)
    monkeypatch.setattr(
        appmod.state, "global_gql_naming_convention", "apollo_graphql", raising=False
    )


async def _pool():
    return _Pool()


def _caller(*roles: str, acting: str | None = None, anonymous: bool = False):
    """The GraphQL ``info`` of a signed-in caller holding ``roles``, acting as ``acting``."""
    if anonymous:
        identity = types.SimpleNamespace(user_id="anonymous", roles=[])
    else:
        identity = types.SimpleNamespace(user_id="u1", roles=list(roles))
    state = types.SimpleNamespace(identity=identity, role=acting or (roles[0] if roles else None))
    return types.SimpleNamespace(context={"request": types.SimpleNamespace(state=state)})


async def _tables(info) -> dict[str, list[str]]:
    return {t.table_name: [c.column_name for c in t.columns] for t in await Query().tables(info)}


# --- tables ---------------------------------------------------------------------------------------


async def test_an_analyst_is_answered_the_tables_and_columns_its_role_is_served():
    assert await _tables(_caller("analyst")) == {
        "orders": ["id", "region", "customer_id"],  # not margin, not rep_id
        "customers": ["id", "name"],
    }


async def test_a_registrar_is_answered_the_whole_catalog_of_the_domains_it_reaches():
    assert await _tables(_caller("registrar")) == {
        "orders": _COLUMNS[1],
        "customers": _COLUMNS[2],
    }
    assert await _tables(_caller("registrar_all")) == {
        "orders": _COLUMNS[1],
        "customers": _COLUMNS[2],
        "staff": _COLUMNS[3],
    }


async def test_a_hiding_right_administers_only_the_domains_its_own_role_reaches():
    """REQ-1944: masking_config in hr is not masking_config in sales, whatever else is held."""
    assert await _tables(_caller("masker")) == {"staff": _COLUMNS[3]}
    # Holding the analyst role too adds what the analyst is SERVED in sales — not all of sales.
    assert await _tables(_caller("masker", "analyst", acting="analyst")) == {
        "orders": ["id", "region", "customer_id"],
        "customers": ["id", "name"],
        "staff": _COLUMNS[3],
    }


async def test_a_control_plane_role_is_answered_no_data_catalog():
    assert await _tables(_caller("platform")) == {}


async def test_with_no_auth_provider_nothing_is_narrowed():
    info = _caller(anonymous=True)
    assert await _tables(info) == {
        "orders": _COLUMNS[1],
        "customers": _COLUMNS[2],
        "staff": _COLUMNS[3],
    }
    (orders, *_rest) = await Query().tables(info)
    assert orders.columns[0].visible_to == ["analyst"] and orders.view_sql == "SELECT 1"


# --- grant lists and mask settings (REQ-1134) -----------------------------------------------------


@pytest.mark.parametrize("role", ["analyst", "registrar", "masker"])
async def test_grant_lists_and_masks_are_withheld_without_view_governance(role):
    for table in await Query().tables(_caller(role)):
        assert table.view_sql is None
        for column in table.columns:
            assert column.visible_to is None and column.writable_by is None
            assert column.unmasked_to is None
            assert column.mask_type is None and column.mask_value is None


@pytest.mark.parametrize("role", ["auditor", "registrar_all"])
async def test_view_governance_reads_them(role):
    tables = await Query().tables(_caller(role))
    assert tables, role
    for table in tables:
        assert table.view_sql == "SELECT 1"
        for column in table.columns:
            assert column.visible_to == ["analyst"] and column.unmasked_to == ["registrar"]
            assert column.mask_type == "constant" and column.mask_value == "***"


async def test_view_governance_does_not_widen_what_is_listed():
    assert await _tables(_caller("auditor")) == await _tables(_caller("analyst"))


# --- relationships --------------------------------------------------------------------------------


async def _relationship_ids(info, everything: bool = False) -> list[str]:
    query = Query().all_relationships if everything else Query().relationships
    return [r.id for r in await query(info)]


async def test_an_analyst_is_answered_the_relationships_among_what_it_is_served():
    # orders.rep_id → staff is not: the analyst is served neither rep_id nor staff.
    assert await _relationship_ids(_caller("analyst")) == ["orders-customers", "orders-fn"]
    assert await _relationship_ids(_caller("analyst"), everything=True) == [
        "orders-customers",
        "orders-fn",
    ]


async def test_a_registrar_is_answered_the_relationships_within_its_domains():
    # It administers sales, so it sees rep_id — but staff (hr) is not in its catalog.
    assert await _relationship_ids(_caller("registrar")) == ["orders-customers", "orders-fn"]
    assert await _relationship_ids(_caller("registrar_all")) == [
        "orders-customers",
        "orders-staff",
        "orders-fn",
    ]


async def test_a_control_plane_role_is_answered_no_relationships():
    assert await _relationship_ids(_caller("platform")) == []


# --- domains, sources, metrics -------------------------------------------------------------------


async def test_domains_are_the_ones_the_callers_roles_reach():
    async def _ids(info):
        return [d.id for d in await Query().domains(info)]

    assert await _ids(_caller("analyst")) == ["sales"]
    assert await _ids(_caller("masker")) == ["hr"]
    assert await _ids(_caller("registrar_all")) == ["hr", "marketing", "sales"]
    assert await _ids(_caller(anonymous=True)) == ["hr", "marketing", "sales"]


async def test_a_source_is_named_to_a_caller_answered_a_table_from_it():
    async def _ids(info):
        return [s.id for s in await Query().sources(info)]

    assert await _ids(_caller("analyst")) == ["pg"]
    assert await _ids(_caller("masker")) == ["hrdb"]
    assert await _ids(_caller("platform")) == []
    # Its registrar reads every source.
    assert await _ids(_caller("registrar_all")) == ["pg", "hrdb"]
    assert (await Query().source(_caller("analyst"), "pg")).id == "pg"
    assert await Query().source(_caller("analyst"), "hrdb") is None


async def test_a_metric_is_listed_by_its_own_visibility():
    async def _names(info):
        return [m.name for m in await Query().metrics(info)]

    assert await _names(_caller("analyst")) == ["everyone", "analysts"]
    assert await _names(_caller("masker")) == ["everyone", "maskers"]
    assert await _names(_caller("platform")) == []  # no data surface: not "everyone" either
    assert await _names(_caller("registrar_all")) == ["everyone", "analysts", "maskers"]
    # Who a metric is granted to is a grant list.
    assert all(m.visible_to is None for m in await Query().metrics(_caller("analyst")))
    assert (await Query().metrics(_caller("registrar_all")))[1].visible_to == ["analyst"]


# --- the rule itself ------------------------------------------------------------------------------


def test_the_scope_is_decided_by_rights_and_reach_never_by_a_role_id(monkeypatch):
    """REQ-1337: rename every role and the answers do not change."""
    renamed = {f"r{i}": definition for i, definition in enumerate(_ROLES.values())}
    by_old = dict(zip(_ROLES, renamed))
    monkeypatch.setattr(appmod.state, "roles", renamed, raising=False)
    monkeypatch.setattr(
        appmod.state,
        "contexts",
        {by_old[old]: ctx for old, ctx in appmod.state.contexts.items()},
        raising=False,
    )
    registrar = catalog_scope(_caller(by_old["registrar"]))
    assert registrar.admin and registrar.admin_reach == frozenset({"sales"})
    analyst = catalog_scope(_caller(by_old["analyst"]))
    assert not analyst.admin and set(analyst.served) == {1, 2}
    platform = catalog_scope(_caller(by_old["platform"]))
    assert not platform.admin and platform.served == {}
