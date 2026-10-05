# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7a2c19-9b36-4f51-a8d0-6c3e1f5b2a97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an audit row records about how a statement was governed (provisa/audit/provenance.py):
taken from the statement's own governance, never the values of a person's session variables."""

from __future__ import annotations

from types import SimpleNamespace

import sqlglot

from provisa.audit.provenance import data_age, enforced_for_request, enforced_summary
from provisa.compiler.rls import RLSContext
from provisa.security.masking import MaskingRule, MaskType
from tests.write_governance import write_governance

_ORDERS = {"sales.orders": (1, ["id", "region", "email"])}


def _gov(**kw):
    gov = write_governance(_ORDERS, **kw)
    gov.masking_rules = {
        (1, "email"): (MaskingRule(mask_type=MaskType.regex, pattern=".", replace="*"), "varchar")
    }
    gov.limit_ceiling = 100
    gov.table_ceilings = {1: 50}
    gov.visible_columns = {1: None}  # None: every column of the table is visible
    return gov


def test_a_read_records_its_filters_masks_and_row_caps():
    gov = _gov(rls={1: "region = current_setting('provisa.user_region')", 2: "x = 1"})
    summary = enforced_summary(gov, [1], sqlglot.parse_one("SELECT id FROM sales.orders"))
    assert summary == {
        "row_filters": {
            "1": {
                "filter": "region = current_setting('provisa.user_region')",
                "session_variables": ["user_region"],  # the name, never the value
            }
        },
        "masks": {"1": {"email": "regex"}},
        "visible_columns": {"1": ["email", "id", "region"]},
        "row_cap": 100,
        "table_caps": {"1": 50},
        "sample": None,
    }  # table 2's filter is not the statement's: it read only table 1


def test_a_write_records_its_table_its_columns_and_whether_a_filter_governed_it():
    insert = sqlglot.parse_one("INSERT INTO sales.orders (id, region) VALUES (1, 'east')")
    summary = enforced_summary(_gov(rls={1: "region = 'east'"}), [1], insert)
    assert summary["write"] == {"table": 1, "columns": ["id", "region"], "row_filter": True}
    assert "row_cap" not in summary  # a write has no row cap

    delete = sqlglot.parse_one("DELETE FROM sales.orders WHERE id = 1")
    assert enforced_summary(_gov(), [1], delete)["write"] == {
        "table": 1,
        "columns": ["email", "id", "region"],  # whole rows
        "row_filter": False,
    }


def test_a_graphql_request_records_the_governance_its_role_had_when_it_ran():
    from tests.unit.test_mutation_sql import _build

    _schema, ctx = _build()
    tables = [
        {
            "id": 1,
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                {"column_name": c, "visible_to": ["admin"]} for c in ("id", "amount", "region")
            ],
        }
    ]
    state = SimpleNamespace(
        rls_contexts={"admin": RLSContext(rules={1: "region = 'us'"})},
        masking_rules={},
        tables=tables,
        relationships=[],
        roles={"admin": {"id": "admin", "capabilities": ["full_results"], "domain_access": ["*"]}},
    )
    summary = enforced_for_request(state, "admin", ctx)
    # the role's rules change after the request: the row still records what applied to it
    state.rls_contexts["admin"] = RLSContext(rules={1: "region = 'eu'"})
    recorded = summary((1,))
    assert recorded["row_filters"] == {"1": {"filter": "region = 'us'", "session_variables": []}}
    assert recorded["row_cap"] is None  # full_results


def test_data_age_is_the_cache_entry_age_or_the_as_of_time_or_nothing_for_a_live_read():
    assert data_age(SimpleNamespace(cache_as_of=None), SimpleNamespace(age_seconds=42)) == {
        "cache_age_seconds": 42
    }
    assert data_age(SimpleNamespace(cache_as_of="2026-01-01T00:00:00Z"), None) == {
        "as_of": "2026-01-01T00:00:00Z"
    }
    assert data_age(SimpleNamespace(cache_as_of=None), None) is None


# --- the model commit: named only when proven ------------------------------------------------------


def _audit_record(stamp: int | None, enforced: dict):
    from datetime import datetime, timezone

    from provisa.audit.writer import AuditRecord
    from provisa.encryption import NullEncryption

    return AuditRecord(
        record_db=object(),
        tenant_id="acme",
        user_id="alice",
        role_id="analyst",
        query_text="SELECT 1",
        table_ids=(1,),
        source="http",
        status_code=200,
        duration_ms=1,
        logged_at=datetime.now(timezone.utc),
        trace_id=None,
        encryption=NullEncryption(),
        meter_pool=object(),
        meter_org="acme",
        model_stamp=stamp,
        model_env="prod",
        enforced=enforced,
        route_reason=None,
        sources=(),
        data_age=None,
        region="default",
    )


def test_a_row_names_the_commit_only_when_the_stamp_is_proven_and_then_drops_visible_columns():
    import asyncio

    from provisa.audit.writer import AuditWriter

    asked: list[int] = []

    async def deployed_commit(_pool, _org, _env, stamp):
        asked.append(stamp)
        return "abc123" if stamp == 5 else None  # the environment stands at a commit made at 5

    writer = AuditWriter(deployed_commit=deployed_commit)
    enforced = {"row_filters": {}, "visible_columns": {"1": ["id"]}}

    async def _rows():
        proven = _audit_record(5, enforced)
        unproven = _audit_record(6, enforced)
        rows = [
            proven.row(await writer._model_commit(proven)),
            unproven.row(await writer._model_commit(unproven)),
            proven.row(await writer._model_commit(proven)),
        ]
        no_stamp = _audit_record(None, enforced)
        rows.append(no_stamp.row(await writer._model_commit(no_stamp)))
        return rows

    rows = asyncio.run(_rows())
    assert [r["model_commit"] for r in rows] == ["abc123", None, "abc123", None]
    assert "visible_columns" not in rows[0]["enforced"]  # the commit holds them
    assert rows[1]["enforced"]["visible_columns"] == {"1": ["id"]}  # no commit: kept
    assert asked == [5, 6]  # a proven stamp is not asked again; no stamp is never asked


def _write_through_harness(monkeypatch, tmp_path, stamps: list[int]):
    from provisa.core import env_repo

    monkeypatch.setenv("PROVISA_REPO_DIR", str(tmp_path))
    positions: list[dict] = []
    seq = iter(stamps)

    async def _stamp(_conn, _schema):
        return next(seq)

    async def _model(_conn, _schema):
        return {}

    async def _set_position(_db, _org, _env, **values):
        positions.append(values)

    async def _nothing(*_a, **_k):
        return None

    monkeypatch.setattr("provisa.core.env_project.model_stamp", _stamp)
    monkeypatch.setattr(env_repo, "project", _model)
    monkeypatch.setattr(env_repo, "dump", lambda _m: {"sales/domain.yaml": "id: sales\n"})
    monkeypatch.setattr("provisa.core.env_store.set_position", _set_position)
    monkeypatch.setattr("provisa.core.env_store.set_drifted", _nothing)
    monkeypatch.setattr("provisa.core.env_ci.announce", _nothing)
    return env_repo, positions


def test_a_commit_records_its_stamp_only_when_the_model_did_not_move_while_it_was_read(
    monkeypatch, tmp_path
):
    import asyncio

    env_repo, positions = _write_through_harness(monkeypatch, tmp_path / "a", [7, 7])
    sha = asyncio.run(env_repo.write_through(None, None, "acme", "prod", "org_acme", "one", None))
    assert positions == [{"deployed_sha": sha, "deployed_stamp": 7, "redo_sha": None}]

    env_repo, positions = _write_through_harness(monkeypatch, tmp_path / "b", [7, 8])
    sha = asyncio.run(env_repo.write_through(None, None, "acme", "prod", "org_acme", "one", None))
    # a change landed while the tree was read: the commit is recorded without a stamp
    assert positions == [{"deployed_sha": sha, "deployed_stamp": None, "redo_sha": None}]
