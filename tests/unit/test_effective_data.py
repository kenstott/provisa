# Copyright (c) 2026 Kenneth Stott
# Canary: 513779f1-908f-4c6f-aaf9-445091bac47c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A role's data access, as its panel shows it (REQ-2161): per table it reaches and per column, what
it reads and writes, masked or row-filtered, with the grant that serves it and how it inherits it --
decided by the one column rule (rights.column_served) over the grants the build expanded, and capped
by what the viewer may read (REQ-2152)."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.security.effective_rights import data_view

PARENTS = {"clinical_reviewer": ["analyst"], "analyst": ["viewer"], "viewer": [], "steward": []}
CHAIN = ["clinical_reviewer", "analyst", "viewer"]
# A grant to Viewer, as the build expands it down to the roles built on it (REQ-2154).
EVERYONE = ["viewer", "analyst", "clinical_reviewer"]


def _col(name, visible_to, scope="domain"):
    return {"column_name": name, "visible_to": visible_to, "scope": scope}


# The build's tables: grants expanded down the chains (a grant to analyst is held by its child).
TABLES = [
    {
        "id": 1,
        "domain_id": "claims",
        "table_name": "claims",
        "alias": None,
        "columns": [
            _col("diagnosis", ["steward", "analyst", "clinical_reviewer"], "restricted"),
            _col("amount", EVERYONE),
            _col("ssn", ["steward"], "restricted"),
        ],
        "write_grants": [
            {"role": "clinical_reviewer", "operation": "update", "columns": ["diagnosis"]},
            {"role": "steward", "operation": "update", "columns": ["amount"]},
        ],
    },
    {
        "id": 2,
        "domain_id": "billing",
        "table_name": "invoices",
        "alias": "inv",
        "columns": [_col("total", EVERYONE, "public"), _col("internal_note", EVERYONE)],
        "write_grants": [],
    },
    {
        "id": 3,
        "domain_id": "hr",
        "table_name": "salaries",
        "alias": None,
        "columns": [_col("pay", EVERYONE)],
        "write_grants": [],
    },
]
# What was granted, before the expansion.
STORED = {
    (1, "diagnosis"): ["steward", "analyst"],
    (1, "amount"): ["viewer"],
    (1, "ssn"): ["steward"],
}
MASKS = {1: {"amount": SimpleNamespace(mask_type=SimpleNamespace(value="regex"))}}


def view(readable=lambda _t, _c: True):
    return data_view(
        "clinical_reviewer",
        reach=["claims"],
        chain=CHAIN,
        parents=PARENTS,
        tables=TABLES,
        stored=STORED,
        masks=MASKS,
        row_rule=lambda tid, _domain: "region = 'eu'" if tid == 1 else None,
        readable=readable,
    )


def test_each_column_says_what_the_role_does_with_it_and_the_grant_behind_it():
    claims = next(t for t in view() if t["table_id"] == 1)
    assert claims["name"] == "claims.claims"
    assert claims["reach"] == "domain"
    cols = {c["name"]: c for c in claims["columns"]}
    assert cols["diagnosis"] == {
        "name": "diagnosis",
        "scope": "restricted",
        "read": True,
        "write": ["update"],
        "mask": None,
        "row_rule": "region = 'eu'",
        "granted_by": "analyst",
        "via": ["clinical_reviewer", "analyst"],
        "reach": "domain",
    }
    # A domain column granted to Viewer is every role's built on it; masked, still read.
    assert cols["amount"]["granted_by"] == "viewer"
    assert cols["amount"]["via"] == ["clinical_reviewer", "analyst", "viewer"]
    assert cols["amount"]["mask"] == {"type": "regex"}
    # Restricted to another role: not the role's, and not listed.
    assert "ssn" not in cols


def test_another_domain_shows_only_its_public_columns():
    tables = {t["table_id"]: t for t in view()}
    assert tables[2]["reach"] == "across"
    assert tables[2]["name"] == "billing.inv"
    assert [c["name"] for c in tables[2]["columns"]] == ["total"]
    assert tables[2]["columns"][0]["reach"] == "public"
    # A domain it does not reach with nothing public is not listed at all.
    assert 3 not in tables


def test_the_viewer_sees_only_what_it_may_read_with_nothing_marking_the_rest():
    tables = {t["table_id"]: t for t in view(readable=lambda t, c: (t, c) != (1, "diagnosis"))}
    assert [c["name"] for c in tables[1]["columns"]] == ["amount"]


# --- from what the runtime serves -----------------------------------------------------------------

from provisa.compiler.rls import RLSContext  # noqa: E402
from provisa.security.effective_rights import data_view_from_state  # noqa: E402


def _state():
    return SimpleNamespace(
        tables=TABLES,
        roles={"clinical_reviewer": {"id": "clinical_reviewer", "domain_access": ["claims"]}},
        role_chains={"clinical_reviewer": CHAIN},
        meta_roles={},
        meta_qualifiers={},
        masking_rules={
            (1, "clinical_reviewer"): {
                "amount": (SimpleNamespace(mask_type=SimpleNamespace(value="regex")), "numeric")
            },
            (1, "steward"): {
                "diagnosis": (SimpleNamespace(mask_type=SimpleNamespace(value="hash")), "text")
            },
        },
        rls_contexts={"clinical_reviewer": RLSContext(rules={}, domain_rules={"claims": "x > 0"})},
    )


def test_the_runtime_s_masks_and_row_rules_for_the_role_are_its_own():
    view = data_view_from_state(
        _state(),
        "clinical_reviewer",
        parents=PARENTS,
        stored=STORED,
        readable=lambda _t, _c: True,
        governance=True,
    )
    cols = {c["name"]: c for c in next(t for t in view if t["table_id"] == 1)["columns"]}
    assert cols["amount"]["mask"] == {"type": "regex"}
    assert cols["diagnosis"]["mask"] is None
    assert cols["diagnosis"]["row_rule"] == "x > 0"


def test_without_the_governance_right_the_grants_behind_each_column_are_not_named():
    view = data_view_from_state(
        _state(),
        "clinical_reviewer",
        parents=PARENTS,
        stored=STORED,
        readable=lambda _t, _c: True,
        governance=False,
    )
    col = next(t for t in view if t["table_id"] == 1)["columns"][0]
    assert (col["mask"], col["row_rule"], col["granted_by"], col["via"]) == (None, None, None, [])
    assert col["read"] is True


# --- who can read a table's columns (REQ-2162) ---------------------------------------------------

from provisa.security.effective_rights import table_access  # noqa: E402


def _two_role_state():
    state = _state()
    state.roles = {
        "clinical_reviewer": {"id": "clinical_reviewer", "domain_access": ["claims"]},
        "steward": {"id": "steward", "domain_access": ["claims"]},
        "viewer": {"id": "viewer", "domain_access": []},
    }
    state.role_chains = {"clinical_reviewer": CHAIN, "steward": ["steward"], "viewer": ["viewer"]}
    state.rls_contexts = {}
    return state


def test_each_column_lists_every_role_that_reads_or_writes_it_with_the_chain_behind_it():
    access = table_access(
        _two_role_state(),
        1,
        roles=["clinical_reviewer", "steward", "viewer"],
        parents=PARENTS,
        stored=STORED,
        readable=lambda _t, _c: True,
        governance=True,
        standard={"viewer"},
    )
    assert access["name"] == "claims.claims"
    cols = {c["name"]: c for c in access["columns"]}
    diagnosis = {a["role"]: a for a in cols["diagnosis"]["access"]}
    # The scenario: diagnosis granted to steward and, through analyst, to clinical_reviewer.
    assert set(diagnosis) == {"clinical_reviewer", "steward"}
    assert diagnosis["clinical_reviewer"]["granted_by"] == "analyst"
    assert diagnosis["clinical_reviewer"]["via"] == ["clinical_reviewer", "analyst"]
    assert diagnosis["clinical_reviewer"]["write"] == ["update"]
    assert diagnosis["steward"]["standard"] is False
    # Each role reads through its own mask: the steward's is not the reviewer's.
    assert diagnosis["steward"]["mask"] == {"type": "hash"}
    assert diagnosis["clinical_reviewer"]["mask"] is None
    # ssn: steward alone.
    assert [a["role"] for a in cols["ssn"]["access"]] == ["steward"]


def test_only_columns_the_viewer_may_read_can_be_chosen():
    access = table_access(
        _two_role_state(),
        1,
        roles=["clinical_reviewer", "steward"],
        parents=PARENTS,
        stored=STORED,
        readable=lambda _t, c: c == "amount",
        governance=True,
        standard=set(),
    )
    assert [c["name"] for c in access["columns"]] == ["amount"]


# --- GET /admin/tables/{table_id}/access --------------------------------------------------------

import pytest  # noqa: E402
from fastapi import Request  # noqa: E402

from provisa.api.admin import roles_router, table_access_router  # noqa: E402
from provisa.api.errors import ApiError  # noqa: E402


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return [SimpleNamespace(**r, _mapping=r) for r in self._rows]


class _Conn:
    async def execute_core(self, statement):
        if "table_columns" in str(statement):
            return _Result(
                [
                    {"table_id": 1, "column_name": c, "visible_to": v}
                    for (_t, c), v in STORED.items()
                ]
            )
        return _Result([{"id": rid, "parents": ps} for rid, ps in PARENTS.items()])


class _Pool:
    def acquire(self):
        class _Ctx:
            async def __aenter__(self):
                return _Conn()

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


@pytest.fixture
def wired(monkeypatch):
    def wire(served, visible):
        monkeypatch.setattr(roles_router, "_pool", lambda _r: _Pool())
        monkeypatch.setattr("provisa.api.app.state", _two_role_state())
        monkeypatch.setattr(
            "provisa.api.admin.catalog_scope.catalog_scope",
            lambda _info: SimpleNamespace(whole=False, served=served, governance=True),
        )
        monkeypatch.setattr(
            table_access_router, "role_definitions_visible", lambda *_a: lambda r: r in visible
        )

    return wire


def _request() -> Request:
    request = Request({"type": "http", "headers": [], "method": "GET", "path": "/admin/tables/1"})
    request.state.active_org_id = "acme"
    return request


@pytest.mark.asyncio
async def test_the_endpoint_lists_the_roles_the_caller_may_read_on_the_columns_it_reads(wired):
    wired({1: {"diagnosis"}}, {"clinical_reviewer"})
    access = await table_access_router.table_access_endpoint(1, _request())
    assert [c["name"] for c in access["columns"]] == ["diagnosis"]
    assert [a["role"] for a in access["columns"][0]["access"]] == ["clinical_reviewer"]


@pytest.mark.asyncio
async def test_a_table_the_caller_reads_nothing_of_is_not_found(wired):
    wired({}, {"clinical_reviewer", "steward"})
    with pytest.raises(ApiError) as refused:
        await table_access_router.table_access_endpoint(1, _request())
    assert refused.value.code == "schema.table_not_found"


def test_a_write_grant_outside_the_role_s_reach_is_not_its_write():
    tables = [
        {
            "id": 9,
            "domain_id": "billing",
            "table_name": "ledger",
            "alias": None,
            "columns": [_col("entry", EVERYONE), _col("total", EVERYONE, "public")],
            "write_grants": [
                {"role": "clinical_reviewer", "operation": "update", "columns": ["entry", "total"]}
            ],
        }
    ]
    view = data_view(
        "clinical_reviewer",
        reach=["claims"],
        chain=CHAIN,
        parents=PARENTS,
        tables=tables,
        stored={},
        masks={},
        row_rule=lambda _t, _d: None,
        readable=lambda _t, _c: True,
    )
    # Only the public column, read across domains, and no write.
    [table] = view
    assert [(c["name"], c["read"], c["write"], c["reach"]) for c in table["columns"]] == [
        ("total", True, [], "public")
    ]


# --- a meta-role's data access is what serving builds for it ---------------------------------------

from provisa.compiler.rls import RLSContext as _RLS  # noqa: E402
from provisa.security.masking import MaskingRule, MaskType  # noqa: E402
from tests.role_state import RoleState  # noqa: E402


@pytest.fixture
def _no_surface_build(monkeypatch):
    def _register(state, role, rls):
        state.rls_contexts[role["id"]] = rls

    monkeypatch.setattr("provisa.api.app_loaders.register_role_surface", _register)


def test_a_meta_role_reads_the_union_with_each_member_s_mask_and_the_or_of_their_rows(
    _no_surface_build,
):
    # east is granted region, west email; email is masked to east... and west alike.
    tables = [
        {
            "id": 1,
            "source_id": "pg",
            "domain_id": "sales",
            "table_name": "customers",
            "alias": None,
            "columns": [
                {"column_name": "region", "visible_to": ["east"], "scope": "restricted"},
                {"column_name": "email", "visible_to": ["west"], "scope": "restricted"},
            ],
            "write_grants": [],
        }
    ]
    role = lambda rid: {  # noqa: E731
        "id": rid,
        "description": rid,
        "capabilities": [],
        "domain_access": ["sales"],
        "max_rows": None,
        "rate_limit": None,
        "session_vars": {},
    }
    state = RoleState(
        roles={"east": role("east"), "west": role("west")},
        rls_contexts={
            "east": _RLS(rules={1: "region = 'east'"}),
            "west": _RLS(rules={1: "region = 'west'"}),
        },
        contexts={
            r: SimpleNamespace(tables={"1": SimpleNamespace(table_id=1)}) for r in ("east", "west")
        },
        masking_rules={(1, "west"): {"email": (MaskingRule(MaskType.constant), "varchar")}},
        masked_columns=[
            (1, "email", [{"to": ["west"], "mask": {"type": "constant", "value": None}}], "varchar")
        ],
        tracked_functions={},
        tracked_webhooks={},
        meta_roles={},
        meta_resolved_at={},
        meta_qualifiers={},
        role_chains={"east": ["east"], "west": ["west"]},
        role_build_inputs={
            "role_chains": {"east": ["east"], "west": ["west"]},
            "tables": tables,
            "metrics": [],
            "functions": [],
            "webhooks": [],
        },
    )
    # The served tables are the build inputs' own list, as app.py publishes them.
    state.tables = state.role_build_inputs["tables"]
    view = data_view_from_state(
        state,
        "meta:east+west",
        parents={"east": [], "west": []},
        stored={(1, "region"): ["east"], (1, "email"): ["west"]},
        readable=lambda _t, _c: True,
        governance=True,
    )
    cols = {c["name"]: c for c in view[0]["columns"]}
    # The union of the members' columns, each granted by the member that holds it.
    assert set(cols) == {"region", "email"}
    assert cols["region"]["granted_by"] == "east"
    assert cols["email"]["granted_by"] == "west"
    assert cols["email"]["mask"] == {"type": "constant"}
    # Each row read as the member whose filter admitted it: the OR of their rules.
    assert "region = 'east'" in cols["region"]["row_rule"]
    assert "region = 'west'" in cols["region"]["row_rule"]
