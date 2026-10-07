# Copyright (c) 2026 Kenneth Stott
# Canary: 0e6b4d29-7c18-4a53-9f2e-b8d1a5c3e740
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly's profiler, fakes, synthetic-dataset and config-export tools (REQ-1934, REQ-1494,
REQ-1939, REQ-1919).

Each tool calls the admin route the UI calls, with the caller's request, so the route's own guard
decides: a role without the right is refused by name, before anything is read. The happy paths run
the real routes over a stand-in database; a table change runs the real fake check and reaches the
``updateTable`` mutation with the whole table as the editor would send it.
"""

# Requirements: REQ-1934, REQ-1494, REQ-1939, REQ-1919, REQ-1857

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from provisa.api.admin import types as admin_types
from provisa.api.errors import ApiError
from provisa.api.mcp import chat, model_tool_specs, model_tools
from provisa.profiler.governance import ColumnRule

_ROLES = {
    "viewer": {"capabilities": ["query_development"]},
    "steward": {
        "capabilities": ["table_registration", "source_registration", "environment_management"]
    },
    "org_admin": {"capabilities": ["user_management", "table_registration"]},
}


# -- a stand-in model database ----------------------------------------------------------------


class _Row(tuple):
    def __new__(cls, mapping: dict):
        row = super().__new__(cls, tuple(mapping.values()))
        row._mapping = mapping  # type: ignore[attr-defined]
        return row

    def __getattr__(self, name: str) -> Any:
        try:
            return self._mapping[name]  # type: ignore[attr-defined]
        except KeyError as exc:
            raise AttributeError(name) from exc


class _Result:
    def __init__(self, rows: list[dict]):
        self._rows = [_Row(r) for r in rows]

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    """Answers a select by the name of the table it reads: ``by_table[name]`` -> rows."""

    def __init__(self, by_table: dict[str, list[dict]]):
        self.by_table = by_table

    async def execute_core(self, stmt):
        froms = getattr(stmt, "get_final_froms", None)
        if froms is None:  # DDL (CREATE TABLE IF NOT EXISTS of a result relation)
            return _Result([])
        name = froms()[0].name
        return _Result(self.by_table.get(name, []))


class _Pool:
    def __init__(self, conn: _Conn):
        self.conn = conn

    def acquire(self):
        conn = self.conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


@pytest.fixture
def app_state(monkeypatch):
    from provisa.api.app import state

    monkeypatch.setattr(state, "roles", _ROLES, raising=False)
    monkeypatch.setattr(
        state,
        "contexts",
        {"viewer": object(), "steward": object(), "org_admin": object()},
        raising=False,
    )
    return state


def _use_db(monkeypatch, app_state, by_table: dict[str, list[dict]]) -> _Conn:
    conn = _Conn(by_table)
    monkeypatch.setattr(app_state, "model_db", _Pool(conn), raising=False)
    return conn


def _request(role: str, org: str = "acme"):
    identity = SimpleNamespace(user_id="u1", roles=[role])
    return SimpleNamespace(state=SimpleNamespace(identity=identity, active_org_id=org, role=role))


# -- governance: a role without the right is refused by name -----------------------------------

_TABLE_RIGHT = "Missing capability: 'table_registration'"
_ENV_RIGHT = "Missing capability: 'environment_management'"
_DATA_RIGHT = "Missing capability: 'environment_data'"

_REFUSALS = [
    ("find_table_id", {"domain": "sales", "table": "orders"}, _TABLE_RIGHT),
    ("list_profilers", {}, _TABLE_RIGHT),
    ("set_table_profiler", {"table_id": 1, "profiler_source_id": "prof"}, _TABLE_RIGHT),
    ("run_profiler", {"source_id": "prof"}, "Missing capability: 'source_registration'"),
    ("run_table_profile", {"table_id": 1}, _TABLE_RIGHT),
    ("list_profile_runs", {"table_id": 1}, _TABLE_RIGHT),
    ("get_table_profile", {"table_id": 1}, _TABLE_RIGHT),
    ("list_profile_constraints", {"table_id": 1}, _TABLE_RIGHT),
    (
        "decide_profile_constraint",
        {"table_id": 1, "kind": "not_null", "column": "id", "status": "accepted"},
        _TABLE_RIGHT,
    ),
    ("forget_profile_constraint", {"table_id": 1, "constraint_id": "c1"}, _TABLE_RIGHT),
    ("export_profile_constraint", {"table_id": 1, "constraint_id": "c1"}, _TABLE_RIGHT),
    ("list_profile_checks", {"table_id": 1}, _TABLE_RIGHT),
    ("create_drift_check", {"table_id": 1}, _TABLE_RIGHT),
    ("create_expectation_check", {"table_id": 1, "expectations_table_id": 2}, _TABLE_RIGHT),
    ("list_fake_kinds", {}, _TABLE_RIGHT),
    ("get_table_fakes", {"table_id": 1}, _TABLE_RIGHT),
    ("set_column_fake", {"table_id": 1, "column": "email", "fake": "email()"}, _TABLE_RIGHT),
    ("propose_fakes", {"table_id": 1}, _TABLE_RIGHT),
    ("list_synthetic_datasets", {}, _ENV_RIGHT),
    ("list_synthetic_profile_runs", {"env": "dev"}, _ENV_RIGHT),
    (
        "define_synthetic_dataset",
        {"dataset_id": "d", "seed": 1, "scale": 1, "tables": []},
        _ENV_RIGHT,
    ),
    ("generate_synthetic_dataset", {"dataset_id": "d"}, _ENV_RIGHT),
    ("get_synthetic_report", {"dataset_id": "d"}, _ENV_RIGHT),
    ("drop_synthetic_dataset", {"dataset_id": "d"}, _ENV_RIGHT),
    ("get_environment_detail", {"env": "dev"}, _ENV_RIGHT),
    ("set_environment_data", {"env": "dev", "dataMode": "inherit"}, _DATA_RIGHT),
    (
        "set_source_binding",
        {"env": "dev", "sourceId": "pg", "binding": "inherited"},
        _DATA_RIGHT,
    ),
    ("get_environment_synthetic_plan", {"env": "dev"}, _ENV_RIGHT),
    ("generate_environment_model", {"env": "dev"}, _DATA_RIGHT),
    ("export_model_config", {}, "user_management in acme required"),
]


def test_every_spec_has_a_refusal_case_and_a_function():
    assert {name for name, _, _ in _REFUSALS} == model_tool_specs.NAMES
    for name in model_tool_specs.NAMES:
        assert callable(getattr(model_tools, name))


@pytest.mark.parametrize("name,args,refusal", _REFUSALS, ids=[r[0] for r in _REFUSALS])
async def test_a_role_without_the_right_is_refused_by_name(app_state, name, args, refusal):
    # No database is bound: a refusal that read anything first would fail on it instead.
    with pytest.raises(ApiError) as caught:
        await model_tool_specs.dispatch(app_state, "viewer", name, args, _request("viewer"))
    assert caught.value.status_code == 403
    assert refusal in caught.value.detail


async def test_dispatch_requires_the_verified_request(app_state):
    with pytest.raises(ValueError, match="requires a verified request context"):
        await model_tool_specs.dispatch(app_state, "steward", "list_profilers", {}, None)


async def test_chat_routes_the_new_tools_and_lists_them_for_the_model(app_state):
    listed = {t["name"] for t in chat._TOOLS}
    assert model_tool_specs.NAMES <= listed
    assert model_tool_specs.SYSTEM_SECTION in chat._SYSTEM
    with pytest.raises(ApiError, match="source_registration"):
        await chat._execute_tool(
            app_state, "viewer", "run_profiler", {"source_id": "p"}, request=_request("viewer")
        )


def test_the_mcp_server_registers_every_new_tool(monkeypatch, app_state):
    import asyncio

    from provisa.api.mcp.server import build_mcp_server

    monkeypatch.setenv("PROVISA_MCP_ROLE", "steward")
    mcp = build_mcp_server(app_state)
    names = {t.name for t in asyncio.run(mcp.list_tools())}
    assert model_tool_specs.NAMES <= names


def test_no_tool_text_names_the_fake_library():
    for spec in model_tool_specs.SPECS:
        assert "faker" not in spec["description"].lower()
    assert "faker" not in model_tool_specs.SYSTEM_SECTION.lower()


# -- profiler -----------------------------------------------------------------------------------


async def test_find_table_id_reads_the_registration(monkeypatch, app_state):
    _use_db(
        monkeypatch,
        app_state,
        {
            "registered_tables": [
                {
                    "id": 7,
                    "source_id": "pg",
                    "domain_id": "sales",
                    "table_name": "orders",
                    "alias": None,
                    "profiler_source_id": "prof",
                }
            ]
        },
    )
    found = await model_tools.find_table_id(
        app_state, "steward", _request("steward"), "sales", "orders"
    )
    assert found == [
        {
            "id": 7,
            "sourceId": "pg",
            "domain": "sales",
            "table": "orders",
            "alias": None,
            "profilerSourceId": "prof",
        }
    ]


async def test_list_profilers_shows_schedules_and_members(monkeypatch, app_state):
    _use_db(monkeypatch, app_state, {})
    settings = SimpleNamespace(
        cron="0 2 * * *",
        sample_above_cells=1000,
        low_cardinality_max=20,
        drift_window=8,
        drift_season="weekly",
    )
    with (
        patch(
            "provisa.profiler.source.profiler_sources",
            new=AsyncMock(return_value=[{"id": "prof", "settings": settings}]),
        ),
        patch(
            "provisa.profiler.source.members",
            new=AsyncMock(return_value=[{"table_name": "orders", "id": 7}]),
        ),
    ):
        out = await model_tools.list_profilers(app_state, "steward", _request("steward"))
    assert out == [
        {
            "id": "prof",
            "cron": "0 2 * * *",
            "sampleAboveCells": 1000,
            "lowCardinalityMax": 20,
            "driftWindow": 8,
            "driftSeason": "weekly",
            "members": ["orders"],
        }
    ]


async def test_run_profiler_names_a_source_that_is_no_profiler(app_state):
    with patch(
        "provisa.profiler.source.run_source",
        new=AsyncMock(side_effect=ValueError("'pg' is not a profiler source")),
    ):
        with pytest.raises(ApiError) as caught:
            await model_tools.run_profiler(app_state, "steward", _request("steward"), "pg")
    assert caught.value.status_code == 404
    assert caught.value.detail == "'pg' is not a profiler source"


_MEMBER = {"registered_tables": [{"table_name": "orders", "profiler_source_id": "prof"}]}


def _profile_db(monkeypatch, app_state, runs: list[dict], **kinds: list[dict]) -> None:
    by_table = dict(_MEMBER)
    by_table["orders_7_profile_runs"] = runs
    for kind, rows in kinds.items():
        by_table[f"orders_7_profile_{kind}"] = rows
    _use_db(monkeypatch, app_state, by_table)


def _run(run_id: str, status: str = "succeeded") -> dict:
    return {"run_id": run_id, "run_time": "2026-10-06T00:00:00", "status": status}


def _rules() -> list[ColumnRule]:
    return [
        ColumnRule("id", "id", frozenset({"steward"}), frozenset(), False, False),
        ColumnRule("email", "email", frozenset({"org_admin"}), frozenset(), False, False),
    ]


async def test_run_table_profile_refuses_a_table_outside_any_profiler(monkeypatch, app_state):
    _use_db(
        monkeypatch,
        app_state,
        {"registered_tables": [{"table_name": "orders", "profiler_source_id": None}]},
    )
    with pytest.raises(ApiError) as caught:
        await model_tools.run_table_profile(app_state, "steward", _request("steward"), 7)
    assert caught.value.detail == "Table 'orders' has not joined a profiler"


async def test_list_profile_runs_reads_the_history(monkeypatch, app_state):
    _profile_db(monkeypatch, app_state, [_run("r2"), _run("r1", "failed")])
    runs = await model_tools.list_profile_runs(app_state, "steward", _request("steward"), 7)
    assert [r["run_id"] for r in runs] == ["r2", "r1"]


async def test_get_table_profile_is_the_viewers_safe_view_of_the_latest_run(monkeypatch, app_state):
    _profile_db(
        monkeypatch,
        app_state,
        [_run("r2")],
        columns=[
            {"run_id": "r2", "column_name": "id", "null_count": 0},
            {"run_id": "r2", "column_name": "email", "null_count": 3},
        ],
    )
    with patch("provisa.profiler.governance.column_rules", new=AsyncMock(return_value=_rules())):
        out = await model_tools.get_table_profile(
            app_state, "steward", _request("steward"), 7, kinds=["columns"]
        )
    # email is visible to org_admin only: the steward's view leaves it out.
    assert out == {
        "runId": "r2",
        "columns": [{"run_id": "r2", "column_name": "id", "null_count": 0}],
    }


async def test_get_table_profile_without_a_succeeded_run_says_so(monkeypatch, app_state):
    _profile_db(monkeypatch, app_state, [_run("r1", "failed")])
    with pytest.raises(ValueError, match="no succeeded profile run; run run_table_profile first"):
        await model_tools.get_table_profile(app_state, "steward", _request("steward"), 7)


async def test_get_table_profile_names_an_unknown_kind(app_state):
    with pytest.raises(ValueError, match=r"unknown profile result kinds \['nope'\]"):
        await model_tools.get_table_profile(
            app_state, "steward", _request("steward"), 7, kinds=["nope"]
        )


def _proposal(kind: str, column: str, definition: dict) -> dict:
    return {
        "run_id": "r2",
        "constraint": kind,
        "column_name": column,
        "other_column": None,
        "involved_columns": json.dumps([column]),
        "value_bearing": kind in ("value_set", "range"),
        "definition": json.dumps(definition),
        "evidence": f"{column} held in every row",
        "share": 1.0,
        "sampled": False,
        "status": "proposed",
    }


async def test_list_profile_constraints_shows_proposals_and_decisions(monkeypatch, app_state):
    _profile_db(monkeypatch, app_state, [_run("r2")], constraints=[_proposal("not_null", "id", {})])
    with (
        patch("provisa.profiler.governance.column_rules", new=AsyncMock(return_value=_rules())),
        patch("provisa.profiler.constraints.decisions", new=AsyncMock(return_value=[])),
        patch("provisa.profiler.export.checkers_scanning", new=AsyncMock(return_value=[])),
    ):
        out = await model_tools.list_profile_constraints(
            app_state, "steward", _request("steward"), 7
        )
    assert out["runId"] == "r2"
    assert out["proposals"] == [
        {
            "constraint": "not_null",
            "column": "id",
            "otherColumn": None,
            "definition": {},
            "evidence": "id held in every row",
            "share": 1.0,
            "sampled": False,
            "status": "proposed",
        }
    ]
    assert out["decisions"] == [] and out["checkers"] == []


async def test_decide_profile_constraint_records_the_runs_evidence(monkeypatch, app_state):
    _profile_db(monkeypatch, app_state, [_run("r2")], constraints=[_proposal("not_null", "id", {})])
    decide = AsyncMock(return_value="c-1")
    with (
        patch("provisa.profiler.governance.column_rules", new=AsyncMock(return_value=_rules())),
        patch("provisa.profiler.constraints.decide", new=decide),
        patch(
            "provisa.api.admin.profiler_checks_router._physical_names",
            return_value={"id": "id"},
        ),
    ):
        out = await model_tools.decide_profile_constraint(
            app_state, "steward", _request("steward"), 7, "not_null", "id", "accepted"
        )
    assert out == {"id": "c-1"}
    kwargs = decide.call_args.kwargs
    assert kwargs["status"] == "accepted" and kwargs["run_id"] == "r2"
    assert kwargs["evidence"] == "id held in every row" and kwargs["share"] == 1.0


async def test_decide_profile_constraint_refuses_what_the_run_does_not_propose(
    monkeypatch, app_state
):
    _profile_db(monkeypatch, app_state, [_run("r2")], constraints=[])
    with patch("provisa.profiler.governance.column_rules", new=AsyncMock(return_value=_rules())):
        with pytest.raises(ValueError, match="run r2 proposes no unique constraint on 'id'"):
            await model_tools.decide_profile_constraint(
                app_state, "steward", _request("steward"), 7, "unique", "id", "accepted"
            )


async def test_decide_profile_constraint_takes_only_accept_or_dismiss(app_state):
    with pytest.raises(ValueError, match="status must be 'accepted' or 'dismissed'"):
        await model_tools.decide_profile_constraint(
            app_state, "steward", _request("steward"), 7, "unique", "id", "maybe"
        )


async def test_forget_profile_constraint_names_a_missing_decision(monkeypatch, app_state):
    _use_db(monkeypatch, app_state, dict(_MEMBER))
    with patch("provisa.profiler.constraints.forget", new=AsyncMock(return_value=False)):
        with pytest.raises(ApiError) as caught:
            await model_tools.forget_profile_constraint(
                app_state, "steward", _request("steward"), 7, "c9"
            )
    assert caught.value.detail == "No decision c9 on table 7"


async def test_export_profile_constraint_refuses_one_not_accepted(monkeypatch, app_state):
    _use_db(monkeypatch, app_state, dict(_MEMBER))
    with patch("provisa.profiler.constraints.accepted_constraints", new=AsyncMock(return_value=[])):
        with pytest.raises(ApiError) as caught:
            await model_tools.export_profile_constraint(
                app_state, "steward", _request("steward"), 7, "c9"
            )
    assert caught.value.detail == "Table 'orders' has no accepted constraint c9"


async def test_list_profile_checks_reports_the_drift_table_and_checkers(monkeypatch, app_state):
    by_table = dict(_MEMBER)
    _use_db(monkeypatch, app_state, by_table)
    with (
        patch(
            "provisa.api.admin.profiler_checks_router._registered_drift_ids",
            new=AsyncMock(return_value=set()),
        ),
        patch("provisa.profiler.export.checkers_scanning", new=AsyncMock(return_value=[])),
        # No table is published for the org admin here, so none is of the expectations shape.
        patch("provisa.api.admin.profiler_checks_router._published", return_value=None),
    ):
        out = await model_tools.list_profile_checks(app_state, "steward", _request("steward"), 7)
    assert out == {"driftTableRegistered": False, "checkers": [], "expectationTables": []}


async def test_create_drift_check_needs_the_drift_table_registered(monkeypatch, app_state):
    _use_db(monkeypatch, app_state, dict(_MEMBER))
    with patch(
        "provisa.api.admin.profiler_checks_router._registered_drift_ids",
        new=AsyncMock(return_value=set()),
    ):
        with pytest.raises(ApiError) as caught:
            await model_tools.create_drift_check(app_state, "steward", _request("steward"), 7)
    assert "is not registered: register it on the profiler first" in caught.value.detail


async def test_create_expectation_check_names_a_table_it_cannot_read(app_state):
    with patch("provisa.api.admin.profiler_checks_router._published", return_value=None):
        with pytest.raises(ApiError) as caught:
            await model_tools.create_expectation_check(
                app_state, "steward", _request("steward"), 7, 99
            )
    assert caught.value.detail == "Table 99 is not a registered table the org admin reads"


# -- a table read and saved as the editor does it ----------------------------------------------


def _column(name: str, data_type: str, **kw: Any) -> admin_types.TableColumnType:
    base: dict[str, Any] = {
        "id": 1,
        "column_name": name,
        "visible_to": ["steward"],
        "writable_by": [],
        "unmasked_to": [],
        "mask_type": None,
        "mask_pattern": None,
        "mask_replace": None,
        "mask_value": None,
        "mask_precision": None,
        "alias": None,
        "computed_sql_alias": name,
        "computed_gql_alias": name,
        "description": None,
        "data_type": data_type,
    }
    base.update(kw)
    return admin_types.TableColumnType(**base)


def _table(**kw: Any) -> admin_types.RegisteredTableType:
    base: dict[str, Any] = {
        "id": 7,
        "source_id": "pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "orders",
        "write_ops": [],
        "alias": None,
        "description": "Orders",
        "cache_ttl": None,
        "replicate": None,
        "load_protected": True,
        "off_peak_window": "01:00-05:00",
        "off_peak_tz": "UTC",
        "gql_naming_convention": None,
        "watermark_column": None,
        "columns": [
            _column("id", "integer", is_primary_key=True),
            _column("email", "varchar", is_pii=True),
            _column("created", "bigint", epoch_unit="ms"),
        ],
        "profiler_source_id": "prof",
        "stored_product_id": "p1",
    }
    base.update(kw)
    return admin_types.RegisteredTableType(**base)


def _saved() -> tuple[AsyncMock, list]:
    captured: list = []

    async def update_table(self, info, table_input):
        captured.append((info, table_input))
        return admin_types.MutationResult(success=True, message="Table 'orders' updated (id=7)")

    return update_table, captured


async def test_get_table_fakes_lists_each_columns_fake(monkeypatch, app_state):
    table = _table(columns=[_column("email", "varchar", fake="email()", is_pii=True)])
    with patch("provisa.api.mcp.table_edit.read_table", new=AsyncMock(return_value=table)):
        out = await model_tools.get_table_fakes(app_state, "steward", _request("steward"), 7)
    assert out["columns"] == [
        {
            "column": "email",
            "dataType": "varchar",
            "fake": "email()",
            "fakeStable": False,
            "syntheticRule": None,
            "isPii": True,
        }
    ]


async def test_set_column_fake_is_checked_then_saved_with_the_whole_table(monkeypatch, app_state):
    _use_db(monkeypatch, app_state, {})
    update_table, captured = _saved()
    request = _request("steward")
    with (
        patch("provisa.api.mcp.table_edit.read_table", new=AsyncMock(return_value=_table())),
        patch(
            "provisa.api.admin.db_queries.fetch_tables",
            new=AsyncMock(return_value=[]),
        ),
        patch("provisa.api.admin.db_queries.fetch_relationships", new=AsyncMock(return_value=[])),
        patch("provisa.api.admin.schema_mutation.Mutation.update_table", new=update_table),
    ):
        out = await model_tools.set_column_fake(
            app_state, "steward", request, 7, "email", fake="email()", stable=True
        )
    assert out["fake"] == "email()" and out["fakeStable"] is True
    info, saved = captured[0]
    assert info.context["request"] is request
    by_name = {c.name: c for c in saved.columns}
    assert by_name["email"].fake == "email()" and by_name["email"].fake_stable is True
    # Everything else is saved back as it was, including what the editor holds elsewhere.
    assert by_name["id"].fake is None and by_name["created"].epoch_unit == "ms"
    assert saved.mv_primary_key == ["id"]
    assert (saved.load_protected, saved.off_peak_window, saved.product_id) == (
        True,
        "01:00-05:00",
        "p1",
    )
    assert saved.profiler_source_id == "prof"


async def test_set_column_fake_passes_the_check_refusal_back_verbatim(monkeypatch, app_state):
    _use_db(monkeypatch, app_state, {})
    update_table, captured = _saved()
    with (
        patch("provisa.api.mcp.table_edit.read_table", new=AsyncMock(return_value=_table())),
        patch("provisa.api.admin.db_queries.fetch_tables", new=AsyncMock(return_value=[])),
        patch("provisa.api.admin.db_queries.fetch_relationships", new=AsyncMock(return_value=[])),
        patch("provisa.api.admin.schema_mutation.Mutation.update_table", new=update_table),
    ):
        with pytest.raises(ApiError) as caught:
            await model_tools.set_column_fake(
                app_state, "steward", _request("steward"), 7, "email", fake="bool(2)"
            )
    assert caught.value.status_code == 422
    assert caught.value.code == "schema.fake_refused"
    assert captured == []  # refused before the save


async def test_set_column_fake_clears_with_an_empty_string(monkeypatch, app_state):
    _use_db(monkeypatch, app_state, {})
    update_table, captured = _saved()
    table = _table(columns=[_column("email", "varchar", fake="email()")])
    with (
        patch("provisa.api.mcp.table_edit.read_table", new=AsyncMock(return_value=table)),
        patch("provisa.api.admin.db_queries.fetch_tables", new=AsyncMock(return_value=[])),
        patch("provisa.api.admin.db_queries.fetch_relationships", new=AsyncMock(return_value=[])),
        patch("provisa.api.admin.schema_mutation.Mutation.update_table", new=update_table),
    ):
        await model_tools.set_column_fake(
            app_state, "steward", _request("steward"), 7, "email", fake=""
        )
    assert captured[0][1].columns[0].fake is None


async def test_set_column_fake_names_a_missing_column(app_state):
    with patch("provisa.api.mcp.table_edit.read_table", new=AsyncMock(return_value=_table())):
        with pytest.raises(ValueError, match="table 'orders' has no column 'nope'"):
            await model_tools.set_column_fake(
                app_state, "steward", _request("steward"), 7, "nope", fake="email()"
            )


async def test_set_table_profiler_joins_and_leaves(app_state):
    update_table, captured = _saved()
    with (
        patch(
            "provisa.api.mcp.table_edit.read_table",
            new=AsyncMock(return_value=_table(profiler_source_id=None)),
        ),
        patch("provisa.api.admin.schema_mutation.Mutation.update_table", new=update_table),
    ):
        await model_tools.set_table_profiler(app_state, "steward", _request("steward"), 7, "prof")
        await model_tools.set_table_profiler(app_state, "steward", _request("steward"), 7, None)
    assert [c[1].profiler_source_id for c in captured] == ["prof", None]


async def test_set_table_profiler_passes_the_save_refusal_back_verbatim(app_state):
    async def refused(self, info, table_input):
        return admin_types.MutationResult(
            success=False,
            message="a table whose rows need a required filter cannot join a profiler",
        )

    with (
        patch("provisa.api.mcp.table_edit.read_table", new=AsyncMock(return_value=_table())),
        patch("provisa.api.admin.schema_mutation.Mutation.update_table", new=refused),
    ):
        with pytest.raises(ValueError, match="^a table whose rows need a required filter"):
            await model_tools.set_table_profiler(
                app_state, "steward", _request("steward"), 7, "prof"
            )


async def test_list_fake_kinds_is_compact_and_never_names_the_library(app_state):
    out = await model_tools.list_fake_kinds(app_state, "steward", _request("steward"))
    names = {k["name"] for k in out["kinds"]}
    assert {"categories", "bool"} <= names
    assert any(m["name"] == "email" for m in out["methods"])
    assert "faker" not in json.dumps(out).lower()


@pytest.fixture
def acme(monkeypatch):
    """The request is bound to org acme (no org runtime is built in a unit test)."""
    monkeypatch.setattr("provisa.core.request_context.require_current_org", lambda: "acme")


async def test_propose_fakes_refuses_an_unprofiled_table(monkeypatch, app_state, acme):
    _use_db(monkeypatch, app_state, {})
    with (
        patch(
            "provisa.api.admin.db_queries.fetch_tables",
            new=AsyncMock(return_value=[{"id": 7, "table_name": "orders", "columns": []}]),
        ),
        patch("provisa.fakes.propose.latest_facts", new=AsyncMock(return_value=None)),
        patch("provisa.profiler.run.column_tags", new=AsyncMock(return_value={})),
    ):
        with pytest.raises(ApiError) as caught:
            await model_tools.propose_fakes(app_state, "steward", _request("steward"), 7)
    assert "has no succeeded profile run in this environment" in caught.value.detail


async def test_propose_fakes_returns_proposals_and_saves_nothing(monkeypatch, app_state, acme):
    _use_db(monkeypatch, app_state, {})
    proposed = SimpleNamespace(
        run_id="r2", columns=[{"column": "email", "fake": "email()"}], unmatched_pii=[]
    )
    with (
        patch(
            "provisa.api.admin.db_queries.fetch_tables",
            new=AsyncMock(return_value=[{"id": 7, "table_name": "orders", "columns": []}]),
        ),
        patch("provisa.fakes.propose.latest_facts", new=AsyncMock(return_value=("r2", object()))),
        patch("provisa.profiler.run.column_tags", new=AsyncMock(return_value={})),
        patch("provisa.fakes.propose.propose", return_value=proposed),
        patch("provisa.api.admin.schema_mutation.Mutation.update_table") as save,
    ):
        out = await model_tools.propose_fakes(app_state, "steward", _request("steward"), 7)
    assert out == {
        "runId": "r2",
        "columns": [{"column": "email", "fake": "email()"}],
        "unmatchedPii": [],
    }
    save.assert_not_called()


# -- synthetic datasets -------------------------------------------------------------------------


async def test_list_synthetic_datasets(monkeypatch, app_state):
    _use_db(monkeypatch, app_state, {})
    from provisa.synthetic.datasets import DatasetRow, DatasetTableRow

    # The real row type, so fields a dataset gains later take their defaults here too.
    row = DatasetRow(
        id="d1",
        seed=7,
        scale=0.5,
        status="generated",
        store_schema="synthetic_d1",
        error=None,
        generated_at=None,
        tables=(DatasetTableRow(table_id=7, profile_env="prod", run_id="r2", scale=None),),
    )
    with patch("provisa.synthetic.datasets.list_datasets", new=AsyncMock(return_value=[row])):
        out = await model_tools.list_synthetic_datasets(app_state, "steward", _request("steward"))
    assert out[0]["id"] == "d1" and out[0]["tables"][0]["runId"] == "r2"


async def test_define_synthetic_dataset_refuses_a_bad_name_verbatim(app_state):
    with pytest.raises(ApiError) as caught:
        await model_tools.define_synthetic_dataset(
            app_state, "steward", _request("steward"), "Bad Name!", 1, 1.0, []
        )
    assert caught.value.code == "synthetic.refused"


async def test_define_synthetic_dataset_refuses_production_verbatim(monkeypatch, app_state):
    _use_db(monkeypatch, app_state, {})
    with pytest.raises(ApiError) as caught:
        await model_tools.define_synthetic_dataset(
            app_state, "steward", _request("steward"), "d1", 1, 1.0, []
        )
    assert caught.value.detail == (
        "the production environment cannot hold a synthetic dataset; select another environment"
    )


async def test_define_and_generate_synthetic_dataset(monkeypatch, app_state):
    _use_db(monkeypatch, app_state, {})
    define = AsyncMock()
    # A synthetic dataset lives in a non-production environment.
    monkeypatch.setattr("provisa.synthetic.run._env", lambda: "dev")
    with (
        patch("provisa.synthetic.run.check_closure_of", new=AsyncMock()),
        patch("provisa.synthetic.datasets.define", new=define),
        patch("provisa.synthetic.run.generate", new=AsyncMock()) as generate,
    ):
        defined = await model_tools.define_synthetic_dataset(
            app_state,
            "steward",
            _request("steward"),
            "d1",
            7,
            0.5,
            [{"tableId": 7, "profileEnv": "prod", "runId": "r2"}],
        )
        generated = await model_tools.generate_synthetic_dataset(
            app_state, "steward", _request("steward"), "d1"
        )
    assert defined["id"] == "d1" and define.call_args.kwargs["seed"] == 7
    assert generated == {"id": "d1", "status": "generated"}
    generate.assert_awaited_once()


async def test_get_synthetic_report(monkeypatch, app_state):
    _use_db(
        monkeypatch,
        app_state,
        {
            "synthetic_report": [
                {
                    "table_name": "orders",
                    "column_name": "amount",
                    "measure": "mean",
                    "source_value": 10.0,
                    "synthetic_value": 10.4,
                    "delta": 0.4,
                    "note": None,
                }
            ]
        },
    )
    out = await model_tools.get_synthetic_report(app_state, "steward", _request("steward"), "d1")
    assert out == [
        {
            "table": "orders",
            "column": "amount",
            "measure": "mean",
            "source": 10.0,
            "synthetic": 10.4,
            "delta": 0.4,
            "note": None,
        }
    ]


async def test_drop_synthetic_dataset_names_an_unknown_one(app_state):
    with patch(
        "provisa.synthetic.run.drop", new=AsyncMock(side_effect=LookupError("no dataset 'd9'"))
    ):
        with pytest.raises(ApiError) as caught:
            await model_tools.drop_synthetic_dataset(
                app_state, "steward", _request("steward"), "d9"
            )
    assert caught.value.status_code == 404 and caught.value.detail == "no dataset 'd9'"


async def test_list_synthetic_profile_runs(monkeypatch, app_state):
    _use_db(monkeypatch, app_state, {})
    runs = [{"tableId": 7, "tableName": "orders", "runs": []}]
    with (
        patch("provisa.api.admin.db_queries.fetch_tables", new=AsyncMock(return_value=[])),
        patch("provisa.synthetic.run.profile_runs_in", new=AsyncMock(return_value=runs)),
    ):
        out = await model_tools.list_synthetic_profile_runs(
            app_state, "steward", _request("steward"), "prod"
        )
    assert out == runs


# -- config ------------------------------------------------------------------------------------


async def test_export_model_config_returns_the_org_yaml(monkeypatch, app_state):
    _admin = _Pool(_Conn({"orgs": [{"id": "acme"}], "user_org_memberships": [{"org_id": "acme"}]}))
    monkeypatch.setattr(app_state, "admin_db", _admin, raising=False)
    with (
        patch("provisa.api.admin.orgs_router._org_model_db", new=AsyncMock()),
        patch(
            "provisa.api.admin.config_export.build_live_config_yaml",
            new=AsyncMock(return_value="tables: []\n"),
        ),
    ):
        out = await model_tools.export_model_config(app_state, "org_admin", _request("org_admin"))
    assert out == {"filename": "acme-config.yaml", "yaml": "tables: []\n"}


async def test_one_mcp_tool_call_is_one_model_change():
    """REQ-1524: a write tool's model writes are owned by the call's change scope, as an HTTP
    request's are (the MCP server is its own app, outside ModelChangeMiddleware)."""
    from provisa.api.mcp.server import _within_request
    from provisa.core import model_change

    seen = []

    async def body():
        current = model_change._SCOPE.get()
        seen.append((current.label, current.closed))
        return "done"

    assert await _within_request(body, "MCP set_column_fake") == "done"
    assert seen == [("MCP set_column_fake", False)]
    assert model_change._SCOPE.get() is None
