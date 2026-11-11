# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2180 (amended 2026-11-11, AN OFFER'S DATA IS FULL, A SAMPLE VIEW, OR SYNTHETIC), REQ-1494:
a synthetic offer's role reads every column of every dataset of its product through the column's
fake -- a role-wide fake mask, computed by the serving engine at the read -- and never a real
record, while a full offer's role reads the real rows. The masks are loaded from a real control
plane by the model build's loader, the read governed as each role and run on DuckDB with the fake
functions registered, as the engine runs it."""

# Requirements: REQ-2180, REQ-1494

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest
import sqlglot
from sqlalchemy import insert

from provisa.api import app as app_module
from provisa.api import app_loaders
from provisa.compiler import stage2
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.core.models import DataProduct, DataProductOffer
from provisa.core.repositories import data_product as product_repo
from provisa.core.repositories import data_product_offer as offers
from provisa.core.schema_org import (
    data_product_tables,
    domains,
    registered_tables,
    sources,
    table_columns,
)
from provisa.fakes import digest as digest_mod
from provisa.fakes.duckdb_functions import register
from provisa.security.masking import MaskType

_KEY = b"k" * 32
#: orders (1), a dataset of product c360: column, type, declared fake.
_COLUMNS = (
    ("id", "integer", "hash()"),
    ("email", "varchar", "email()"),
    ("region", "varchar", "categories((north, south))"),
    ("total", "double", "uniform(min=1000, max=2000)"),
)
#: customers (2), a table of no product: read real by every role.
_ROWS = [(i, f"user{i}@real.example", "east" if i % 2 else "west", float(i)) for i in range(1, 51)]


@pytest.fixture
def engine(monkeypatch):
    """The serving engine: DuckDB with the fake functions, keyed by the platform key."""
    monkeypatch.setattr(digest_mod, "_key", _KEY)
    con = duckdb.connect()
    register(con)
    con.execute("CREATE SCHEMA sales")
    for table in ("orders", "customers"):
        con.execute(
            f"CREATE TABLE sales.{table} (id INTEGER, email VARCHAR, region VARCHAR, total DOUBLE)"
        )
        con.executemany(f"INSERT INTO sales.{table} VALUES (?, ?, ?, ?)", _ROWS)
    return con


async def _model(control_plane, *, synthetic_role: str | None) -> None:
    """Product c360 in sales holding orders (1); customers (2) in no product. Its default offer
    (viewer, the full data) and, with ``synthetic_role``, a synthetic offer to that role."""
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await conn.execute_core(insert(domains).values(id="sales"))
            await conn.execute_core(insert(sources).values(id="crm", type="postgres"))
            await conn.execute_core(insert(sources).values(id="prof", type="data_profiler"))
            for tid, name in ((1, "orders"), (2, "customers")):
                await conn.execute_core(
                    insert(registered_tables).values(
                        id=tid,
                        source_id="crm",
                        domain_id="sales",
                        schema_name="sales",
                        table_name=name,
                        profiler_source_id="prof",
                    )
                )
                for column, _dtype, fake in _COLUMNS:
                    await conn.execute_core(
                        insert(table_columns).values(
                            table_id=tid, column_name=column, fake=fake if tid == 1 else None
                        )
                    )
            await product_repo.upsert(
                conn,
                DataProduct(
                    id="c360", domain_id="sales", name="C360", owner_role="owner", created_by="o1"
                ),
            )
            await conn.execute_core(
                insert(data_product_tables).values(product_id="c360", table_id=1)
            )
            if synthetic_role is not None:
                await offers.save(
                    conn,
                    "c360",
                    DataProductOffer(name="syn", role_id=synthetic_role, data_level="synthetic"),
                    offer_id=None,
                    by="o1",
                )
            await app_loaders._load_masking_rules(
                conn,
                {
                    tid: [ColumnMetadata(c, dtype, True) for c, dtype, _ in _COLUMNS]
                    for tid in (1, 2)
                },
                [{"id": "viewer"}, {"id": "synth"}],
            )
    finally:
        await db.close()


@pytest.fixture
def state(monkeypatch):
    held = SimpleNamespace(
        masking_rules={},
        meta_roles={},
        # prod: no data mode, so no column is faked for every role (REQ-1942)
        _active_runtime=lambda: SimpleNamespace(data_mode=None, env="prod", data_refusal=None),
    )
    monkeypatch.setattr(app_module, "state", held)
    return held


def _read(engine, state, role_id: str, sql: str) -> tuple[str, list[tuple]]:
    """``sql`` governed as ``role_id`` reads it (its published masks, as the governance build
    flattens them), and what the engine returns for it."""
    gov = GovernanceContext(role_id=role_id)
    gov.table_map = {"sales.orders": 1, "sales.customers": 2}
    gov.all_columns = {t: [(c, dtype) for c, dtype, _ in _COLUMNS] for t in (1, 2)}
    gov.visible_columns = {1: None, 2: None}
    for (table_id, rid), cols in state.masking_rules.items():
        if rid == role_id:
            for column, entry in cols.items():
                gov.masking_rules[(table_id, column)] = entry
    stage2._bind_fakes(gov)
    governed = apply_governance(sql, gov)
    return governed, engine.execute(
        sqlglot.transpile(governed, read="postgres", write="duckdb")[0]
    ).fetchall()


_ORDERS = "SELECT id, email, region, total FROM sales.orders ORDER BY id"


@pytest.mark.asyncio
async def test_a_synthetic_offer_s_role_reads_every_column_through_its_fake(
    control_plane, engine, state
):
    await _model(control_plane, synthetic_role="synth")
    rules = state.masking_rules[(1, "synth")]
    assert {c: rule.mask_type for c, (rule, _dtype) in rules.items()} == {
        c: MaskType.fake for c, _dtype, _fake in _COLUMNS
    }
    assert {c: rule.fake for c, (rule, _dtype) in rules.items()} == {
        c: fake for c, _dtype, fake in _COLUMNS
    }
    governed, rows = _read(engine, state, "synth", _ORDERS)
    # Computed by the serving engine at the read: the statement carries the keyed digest call,
    # and the engine evaluates it.
    assert "provisa_digest(" in governed.lower()
    assert len(rows) == len(_ROWS)
    for column in range(4):
        real = {r[column] for r in _ROWS}
        shown = {r[column] for r in rows}
        assert not shown & real, f"column {_COLUMNS[column][0]} shows a real value"
    assert {r[2] for r in rows} <= {"north", "south"}
    assert all(1000 <= r[3] < 2000 for r in rows)


@pytest.mark.asyncio
async def test_a_full_offer_s_role_reads_the_real_rows(control_plane, engine, state):
    await _model(control_plane, synthetic_role="synth")
    assert (1, "viewer") not in state.masking_rules
    governed, rows = _read(engine, state, "viewer", _ORDERS)
    assert "provisa_digest(" not in governed.lower()
    assert rows == _ROWS


@pytest.mark.asyncio
async def test_a_table_of_no_synthetic_offer_is_read_real_by_its_role(control_plane, engine, state):
    await _model(control_plane, synthetic_role="synth")
    assert (2, "synth") not in state.masking_rules
    _governed, rows = _read(engine, state, "synth", _ORDERS.replace("orders", "customers"))
    assert rows == _ROWS


@pytest.mark.asyncio
async def test_without_a_synthetic_offer_no_role_reads_a_fake(control_plane, engine, state):
    await _model(control_plane, synthetic_role=None)
    assert state.masking_rules == {}
    _governed, rows = _read(engine, state, "synth", _ORDERS)
    assert rows == _ROWS


@pytest.mark.asyncio
async def test_a_column_whose_fake_was_removed_reads_completely_masked(control_plane, state):
    db = control_plane.org()
    try:
        await _model(control_plane, synthetic_role="synth")
        async with db.acquire() as conn:
            await conn.execute_core(
                table_columns.update()
                .values(fake=None)
                .where(table_columns.c.table_id == 1, table_columns.c.column_name == "email")
            )
            await app_loaders._load_masking_rules(
                conn,
                {1: [ColumnMetadata(c, dtype, True) for c, dtype, _ in _COLUMNS]},
                [{"id": "viewer"}, {"id": "synth"}],
            )
    finally:
        await db.close()
    rule, _dtype = state.masking_rules[(1, "synth")]["email"]
    assert (rule.mask_type, rule.value) == (MaskType.constant, "****")
    assert "email" not in state.masking_rules.get((1, "viewer"), {})
