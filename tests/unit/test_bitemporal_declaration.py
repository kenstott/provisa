# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2109: a table or a view declares its business key, valid time and recorded time, all three
or none, each naming a column it has; a view declaring them is materialized. The declaration is
accepted by the config, saved through the admin API and read back from the control plane."""

from __future__ import annotations

import asyncio

import pytest

from provisa.core.models import Column, Table

pytestmark = pytest.mark.unit

_THREE = {"business_key": ["trade_id"], "valid_time": "trade_date", "recorded_time": "updated_at"}
_COLUMNS = ("trade_id", "quantity", "trade_date", "updated_at")


def _columns() -> list[Column]:
    return [Column(name=n, data_type="varchar", visible_to=["*"]) for n in _COLUMNS]


def _table(**declared) -> Table:
    return Table(
        source_id="pg",
        domain_id="d",
        schema="public",
        table="trades",
        columns=_columns(),
        **declared,
    )


def _view(materialize: bool = True, **declared) -> Table:
    return Table(
        source_id="__derived__",
        domain_id="d",
        schema="views",
        table="trade_view",
        view_sql="SELECT trade_id, quantity, trade_date, updated_at FROM trades",
        materialize=materialize,
        columns=_columns(),
        **declared,
    )


@pytest.mark.parametrize("build", [_table, _view])
def test_all_three_declare_it(build):
    declared = build(**_THREE)
    assert declared.is_bitemporal
    assert (declared.business_key, declared.valid_time, declared.recorded_time) == (
        ["trade_id"],
        "trade_date",
        "updated_at",
    )


@pytest.mark.parametrize("build", [_table, _view])
def test_none_declares_nothing(build):
    assert not build().is_bitemporal


@pytest.mark.parametrize("build", [_table, _view])
@pytest.mark.parametrize(
    ("left_out", "missing"),
    [
        (("business_key",), "business_key"),
        (("valid_time",), "valid_time"),
        (("recorded_time",), "recorded_time"),
        (("valid_time", "recorded_time"), "valid_time, recorded_time"),
    ],
)
def test_some_but_not_all_is_refused_naming_what_is_missing(build, left_out, missing):
    given = {k: v for k, v in _THREE.items() if k not in left_out}
    with pytest.raises(ValueError, match=f"declared together; missing {missing} \\["):
        build(**given)


@pytest.mark.parametrize("build", [_table, _view])
def test_each_names_a_column_the_table_has(build):
    with pytest.raises(ValueError, match=r"columns the table does not have: \['booked_on'\]"):
        build(**{**_THREE, "valid_time": "booked_on"})


def test_a_view_declaring_them_is_materialized():
    with pytest.raises(ValueError, match="view 'trade_view': .* is materialized"):
        _view(materialize=False, **_THREE)


def test_a_table_declaring_them_needs_no_materialization():
    assert _table(**_THREE).materialize is False


def _config(table_entry: dict | None = None, view_entry: dict | None = None) -> dict:
    cols = [{"name": n, "data_type": "varchar", "visible_to": ["analyst"]} for n in _COLUMNS]
    raw: dict = {
        "sources": [{"id": "pg", "type": "postgresql", "host": "h", "database": "db"}],
        "domains": [{"id": "d"}],
        "roles": [
            {
                "id": "analyst",
                "description": "reads trades",
                "capabilities": [],
                "domain_access": ["d"],
            }
        ],
        "tables": [],
        "views": [],
    }
    if table_entry is not None:
        raw["tables"].append(
            {
                "source_id": "pg",
                "domain_id": "d",
                "schema": "public",
                "table": "trades",
                "columns": cols,
                **table_entry,
            }
        )
    if view_entry is not None:
        raw["views"].append(
            {
                "id": "trade_view",
                "domain_id": "d",
                "sql": "SELECT trade_id, quantity, trade_date, updated_at FROM trades",
                "materialize": True,
                "columns": cols,
                **view_entry,
            }
        )
    return raw


def test_the_config_accepts_the_three_on_a_table():
    from provisa.core.config_loader import parse_config_dict, views_as_tables

    config = parse_config_dict(views_as_tables(_config(table_entry=_THREE)))
    [table] = [t for t in config.tables if t.table_name == "trades"]
    assert (table.business_key, table.valid_time, table.recorded_time) == (
        ["trade_id"],
        "trade_date",
        "updated_at",
    )


def test_the_config_accepts_the_three_on_a_view():
    from provisa.core.config_loader import parse_config_dict, views_as_tables

    config = parse_config_dict(views_as_tables(_config(view_entry=_THREE)))
    [view] = [t for t in config.tables if t.table_name == "view_trade_view"]
    assert (view.business_key, view.valid_time, view.recorded_time) == (
        ["trade_id"],
        "trade_date",
        "updated_at",
    )
    # REQ-2064: a view keeping its versions answers each version's times beside its columns.
    assert [c.name for c in view.columns][-3:] == ["source_from", "source_to", "recorded_at"]


@pytest.mark.parametrize("old", ["bitemporal_mode", "bitemporal_key", "bitemporal_source_time"])
def test_the_config_refuses_the_old_view_keys(old):
    from provisa.core.config_loader import views_as_tables

    with pytest.raises(ValueError, match=f"unknown keys \\['{old}'\\]"):
        views_as_tables(_config(view_entry={old: "x"}))


def test_a_declared_view_keeps_its_versions_keyed_on_its_business_key():
    from provisa.mv.bitemporal import MODE_DELTA, declared_spec

    spec = declared_spec(["trade_id"], "updated_at", list(_COLUMNS))
    assert (spec.key, spec.mode, spec.source_column, spec.columns) == (
        ("trade_id",),
        MODE_DELTA,
        "updated_at",
        _COLUMNS,
    )


@pytest.fixture
def tenant_db(control_plane):
    from sqlalchemy import insert

    from provisa.core.schema_org import domains, sources

    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        c.execute(insert(domains).values(id="d", description=""))
        c.execute(insert(sources).values(id="pg", type="postgresql"))
    return control_plane.org()


def test_the_admin_api_saves_and_reads_back_the_three(tenant_db):
    """The admin input's three are the model's, stored on the table's row and read back as the
    admin reads a table's row."""
    from provisa.api.admin._live_mappers import table_model_from_input
    from provisa.api.admin.types import ColumnInput, TableInput
    from provisa.core.repositories import table as table_repo

    inp = TableInput(
        source_id="pg",
        domain_id="d",
        schema_name="public",
        table_name="trades",
        columns=[ColumnInput(name=n, visible_to=["*"]) for n in _COLUMNS],
        business_key=["trade_id"],
        valid_time="trade_date",
        recorded_time="updated_at",
    )
    model = table_model_from_input(inp, _columns(), None)
    assert (model.business_key, model.valid_time, model.recorded_time) == (
        ["trade_id"],
        "trade_date",
        "updated_at",
    )

    async def _go() -> dict | None:
        async with tenant_db.acquire() as conn:
            table_id = await table_repo.upsert(conn, model)
            assert table_id is not None
            return await table_repo.get(conn, table_id)

    row = asyncio.run(_go())
    assert row is not None
    assert (row["business_key"], row["valid_time"], row["recorded_time"]) == (
        ["trade_id"],
        "trade_date",
        "updated_at",
    )
