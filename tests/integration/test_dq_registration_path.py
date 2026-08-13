# Copyright (c) 2026 Kenneth Stott
# Canary: 9d70b1c4-6a2f-4e83-b5d1-0c8ea34f7b26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: registering a checker table through the ADMIN path (REQ-1443).

The UI contract builder registers through ``registerTable``/``updateTable``, not through YAML, so the
contract-driven derivation has to happen against the control plane rather than against a
ProvisaConfig. These tests drive :func:`apply_dq_registration` over a real control-plane store —
sources and registered_tables as the mutation sees them — and then persist and read the table back,
because a derivation that did not survive the round trip would leave a table whose declared shape and
actual rows disagree just as surely as no derivation at all.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.api.admin._dq_registration import apply_dq_registration
from provisa.core.database import Database
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_refs,
    glossary_terms,
    registered_tables,
    roles,
    sources,
    table_columns,
)
from provisa.dq.results import DQ_PROMOTIONS, DQ_WATERMARK_COLUMN, RESULT_FIELDS

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_TABLES = [
    sources,
    registered_tables,
    table_columns,
    roles,
    glossary_terms,
    glossary_term_refs,
    glossary_term_edges,
    glossary_term_experts,
]

CONTRACT = """
dataset: provisa/sales/orders
columns:
  - name: customer
    checks:
      - missing:
"""


@asynccontextmanager
async def _conn(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'dq.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: registered_tables.metadata.create_all(s, tables=_TABLES))
    try:
        async with Database(engine, name="dq").acquire() as conn:
            yield conn
    finally:
        await engine.dispose()


_MAPPING = {
    "host": "127.0.0.1",
    "port": 5439,
    "database": "provisa",
    "user": "checker",
    "password": "secret",
}


async def _seed_estate(conn, checker_type: str = "soda") -> None:
    """A checker source, a plain source, and the one governed table the contract may observe."""
    await conn.execute_core(insert(sources).values(id="dq", type=checker_type, mapping=_MAPPING))
    await conn.execute_core(insert(sources).values(id="warehouse", type="postgresql"))
    await table_repo.upsert(
        conn,
        Table(
            source_id="warehouse",
            domain_id="default",
            schema="sales",
            table="orders",
            columns=[Column(name="id", data_type="bigint", visible_to=["analyst"])],
        ),
    )


def _results_table(contract: str | None = CONTRACT) -> Table:
    return Table(
        source_id="dq",
        domain_id="default",
        schema="quality",
        table="orders_scans",
        dq_contract=contract,
        columns=[Column(name="placeholder", data_type="varchar", visible_to=["analyst"])],
    )


async def test_the_admin_path_derives_the_same_registration_the_loader_does(tmp_path):
    """One helper, two surfaces: a table registered through the UI must come out identical to the
    same table written in YAML, or the two surfaces disagree about what a checker table IS."""
    async with _conn(tmp_path) as conn:
        await _seed_estate(conn)
        model = _results_table()
        await apply_dq_registration(conn, model)

    assert tuple(c.name for c in model.columns) == RESULT_FIELDS
    assert all(c.visible_to == ["analyst"] for c in model.columns)
    assert model.watermark_column == DQ_WATERMARK_COLUMN
    assert [p["target_column"] for p in model.promotions] == [
        p["target_column"] for p in DQ_PROMOTIONS
    ]


async def test_the_derived_table_and_its_contract_survive_the_round_trip(tmp_path):
    """The scan runner reads the contract back off the registered table, so persistence is part of
    the registration rather than an afterthought."""
    async with _conn(tmp_path) as conn:
        await _seed_estate(conn)
        model = _results_table()
        await apply_dq_registration(conn, model)
        await table_repo.upsert(conn, model)

        from provisa.api.admin.db_queries import fetch_tables

        rows = await fetch_tables(conn)

    stored = next(r for r in rows if r["table_name"] == "orders_scans")
    assert stored["dq_contract"] == CONTRACT
    assert tuple(c["column_name"] for c in stored["columns"]) == RESULT_FIELDS


async def test_a_checker_table_registered_without_a_contract_is_rejected(tmp_path):
    async with _conn(tmp_path) as conn:
        await _seed_estate(conn)
        with pytest.raises(ValueError, match="must carry a dq_contract"):
            await apply_dq_registration(conn, _results_table(contract=None))


async def test_a_contract_on_a_non_checker_source_is_rejected(tmp_path):
    """The same whole-estate rule the YAML loader applies — its rows would come from wherever that
    source's loader fetched them, which the results schema does not describe."""
    async with _conn(tmp_path) as conn:
        await _seed_estate(conn)
        model = _results_table()
        model.source_id = "warehouse"
        with pytest.raises(ValueError, match="only valid on a data-quality checker source"):
            await apply_dq_registration(conn, model)


async def test_a_contract_naming_an_ungoverned_table_is_rejected(tmp_path):
    """A checker may only observe what Provisa governs (REQ-967) — and on this path "governed" means
    what the control plane actually has registered, not what a config file listed."""
    async with _conn(tmp_path) as conn:
        await conn.execute_core(insert(sources).values(id="dq", type="soda"))
        with pytest.raises(ValueError, match="resolves to no governed table"):
            await apply_dq_registration(conn, _results_table())


async def test_a_contract_pointed_at_its_own_results_table_is_rejected(tmp_path):
    """Matching is by NAME here: the results table's own registered row is a different object than
    the model being registered, so identity would miss the self-reference entirely."""
    async with _conn(tmp_path) as conn:
        await _seed_estate(conn)
        registered = _results_table()
        await apply_dq_registration(conn, registered)
        await table_repo.upsert(conn, registered)

        model = _results_table(contract="dataset: provisa/quality/orders_scans\n")
        with pytest.raises(ValueError, match="resolves to the results table itself"):
            await apply_dq_registration(conn, model)


async def test_a_plain_table_registers_untouched(tmp_path):
    """Every non-checker registration goes through the same call, so it must be a no-op on one."""
    async with _conn(tmp_path) as conn:
        await _seed_estate(conn)
        plain = Table(
            source_id="warehouse",
            domain_id="default",
            schema="sales",
            table="returns",
            columns=[Column(name="id", data_type="bigint", visible_to=["analyst"])],
        )
        await apply_dq_registration(conn, plain)

    assert [c.name for c in plain.columns] == ["id"]
    assert plain.watermark_column is None
    assert plain.promotions == []


# ── The dry run (REQ-1443 clause 7) ──


async def test_a_dry_run_reports_the_outcomes_and_lands_nothing(tmp_path, monkeypatch):
    """The scan is real; the landing is not. What the panel needs back is the per-check outcomes AND
    the table the dataset actually resolved to, since a contract aimed at the wrong table still scans
    cleanly and still lands nothing but passing rows."""
    from provisa.api.admin._dq_resolvers import dry_run_contract

    seen: dict = {}

    async def _fake_run_contract(**kwargs):
        seen.update(kwargs)
        return [
            {
                "scan_id": kwargs["scan_id"],
                "scan_time": kwargs["scan_time"],
                "checker": "soda",
                "checker_version": "4.0.0",
                "dataset": "provisa/sales/orders",
                "target_table": kwargs["target_table"],
                "column_name": "customer",
                "check_name": "missing",
                "check_type": "missing",
                "check_definition": "missing:",
                "outcome": "fail",
                "metric_value": 3.0,
                "threshold": "0",
                "rows_tested": 100,
                "failed_rows": 3,
                "diagnostics": {"missing_count": 3},
            }
        ]

    monkeypatch.setattr("provisa.dq.runner.run_contract", _fake_run_contract)

    async with _conn(tmp_path) as conn:
        await _seed_estate(conn)
        result = await dry_run_contract(conn, source_id="dq", contract_text=CONTRACT)

        # Nothing landed: the results table does not even exist, and no row was written anywhere.
        rows = (await conn.execute_core(select(registered_tables.c.table_name))).fetchall()

    assert [r._mapping["table_name"] for r in rows] == ["orders"]
    assert result["success"] is True
    assert result["checker_version"] == "4.0.0"
    assert result["checks"] == [
        {
            "column_name": "customer",
            "check_type": "missing",
            "outcome": "fail",
            "rows_tested": 100,
            "failed_rows": 3,
            "value": 3.0,
            "diagnostics": '{"missing_count": 3}',
        }
    ]
    assert seen["target_table"] == "sales.orders"
    assert seen["data_source_name"] == "provisa"
    assert seen["connection"] == _MAPPING


async def test_a_dry_run_against_an_ungoverned_dataset_reports_where_it_resolved(tmp_path):
    """The whole point of the dry run: the contract parses, the checker would happily scan, and the
    panel still has to say that the dataset names nothing Provisa governs."""
    from provisa.api.admin._dq_resolvers import dry_run_contract

    async with _conn(tmp_path) as conn:
        await conn.execute_core(insert(sources).values(id="dq", type="soda", mapping=_MAPPING))
        result = await dry_run_contract(conn, source_id="dq", contract_text=CONTRACT)

    assert result["success"] is False
    assert "resolves to no governed table" in result["message"]


async def test_a_dry_run_against_a_non_checker_source_is_refused(tmp_path):
    from provisa.api.admin._dq_resolvers import dry_run_contract

    async with _conn(tmp_path) as conn:
        await _seed_estate(conn)
        result = await dry_run_contract(conn, source_id="warehouse", contract_text=CONTRACT)

    assert result["success"] is False
    assert "not a data-quality checker" in result["message"]


async def test_a_dry_run_against_an_unknown_source_is_refused(tmp_path):
    from provisa.api.admin._dq_resolvers import dry_run_contract

    async with _conn(tmp_path) as conn:
        await _seed_estate(conn)
        result = await dry_run_contract(conn, source_id="nope", contract_text=CONTRACT)

    assert result["success"] is False
    assert "no source 'nope'" in result["message"]


async def test_a_dry_run_reports_a_checker_failure_rather_than_raising(tmp_path, monkeypatch):
    """A wedged or missing checker is an answer the panel shows, not a GraphQL error it swallows."""
    from provisa.api.admin._dq_resolvers import dry_run_contract
    from provisa.dq.runner import CheckerError

    async def _boom(**_kwargs):
        raise CheckerError("checker 'soda' exited 1: No module named soda_core")

    monkeypatch.setattr("provisa.dq.runner.run_contract", _boom)

    async with _conn(tmp_path) as conn:
        await _seed_estate(conn)
        result = await dry_run_contract(conn, source_id="dq", contract_text=CONTRACT)

    assert result["success"] is False
    assert "No module named soda_core" in result["message"]
