# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo's bitemporal views keep their history from its bundle build to every sandbox (REQ-1162,
REQ-2006): the build writes their append logs beside the seed, the template adopts them into its
store, and a sandbox is given the template's."""

# Requirements: REQ-1162, REQ-2006

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import duckdb
import pytest

from provisa.api import demo_replicas
from provisa.core.config_loader import parse_config_dict, views_as_tables
from provisa.core.environments import org_schema
from provisa.federation.materialize_broker import copy_schema
from provisa.federation.replica_address import MVS_SUFFIX

# In the order read back: by when each version was recorded, then by customer.
_HISTORY = [
    ("h1", "Berlin", "2026-01-01 00:00:00"),
    ("h2", "Paris", "2026-01-01 00:00:00"),
    ("h1", "Hamburg", "2026-02-01 00:00:00"),
]


def _view(view_id: str, **declared) -> dict:
    return {
        "id": view_id,
        "domain_id": "vault",
        "sql": "SELECT 1 AS customer_hk",
        "columns": [{"name": "customer_hk", "data_type": "varchar", "visible_to": []}],
        **declared,
    }


def _config() -> dict:
    return views_as_tables(
        {
            "domains": [{"id": "vault"}],
            "roles": [],
            "sources": [],
            "views": [
                _view(
                    "sat_customer",
                    materialize=True,
                    bitemporal_mode="delta",
                    bitemporal_key=["customer_hk"],
                ),
                _view("mart", materialize=True),
                _view("live"),
            ],
        }
    )


def test_a_config_view_declares_how_it_keeps_its_versions():
    [sat] = [t for t in _config()["tables"] if t["table"] == "view_sat_customer"]
    assert (sat["mv_bitemporal_mode"], sat["mv_bitemporal_key"]) == ("delta", ["customer_hk"])


def test_the_append_logs_are_those_of_the_bitemporal_materialized_views():
    config = parse_config_dict(_config())
    assert demo_replicas.bitemporal_view_tables(config) == ["mv_view_sat_customer"]


def _store(path: Path, schema: str, tables: dict[str, list[tuple]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    try:
        con.execute(f'CREATE SCHEMA "{schema}"')
        for name, rows in tables.items():
            con.execute(
                f'CREATE TABLE "{schema}"."{name}" '
                "(customer_hk VARCHAR, city VARCHAR, sys_recorded_at TIMESTAMP)"
            )
            con.executemany(f'INSERT INTO "{schema}"."{name}" VALUES (?, ?, ?)', rows)
    finally:
        con.close()


def _rows(path: Path, schema: str, table: str) -> list[tuple]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        return [
            (hk, city, str(at))
            for hk, city, at in con.execute(
                f'SELECT * FROM "{schema}"."{table}" ORDER BY sys_recorded_at, customer_hk'
            ).fetchall()
        ]
    finally:
        con.close()


def _tables(path: Path, schema: str) -> list[str]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        return [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = ? ORDER BY 1", [schema]
            ).fetchall()
        ]
    finally:
        con.close()


def test_a_copy_names_the_tables_it_takes(tmp_path):
    src, dst = tmp_path / "a.duckdb", tmp_path / "b.duckdb"
    _store(src, "s", {"kept": _HISTORY, "left": _HISTORY})
    assert copy_schema(str(src), "s", str(dst), "t", ["kept", "absent"]) == ["kept"]
    assert _tables(dst, "t") == ["kept"]


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    for org in ("default", "sbxtpldv", "sbx0a1b"):
        (tmp_path / "data" / "orgs" / org).mkdir(parents=True)
    return tmp_path / "data" / "orgs"


def _views(org: str) -> str:
    return org_schema(org, None, MVS_SUFFIX)


def test_the_history_reaches_the_template_and_each_sandbox(data_dir, tmp_path):
    demo = tmp_path / "demo"
    _store(
        data_dir / "default" / "materialize.duckdb",
        _views("default"),
        {"mv_view_sat_customer": _HISTORY, "mv_view_mart": [("h1", "x", "2026-03-01 00:00:00")]},
    )
    demo_replicas.export_views("default", demo, ["mv_view_sat_customer"])
    record = json.loads((demo / "seeded" / "views.json").read_text())
    assert record["tables"] == ["mv_view_sat_customer"]

    assert demo_replicas.adopt_views("sbxtpldv", demo)
    template = data_dir / "sbxtpldv" / "materialize.duckdb"
    assert _rows(template, _views("sbxtpldv"), "mv_view_sat_customer") == _HISTORY

    asyncio.run(
        demo_replicas.copy_views_to_sandbox("sbxtpldv", "sbx0a1b", ["mv_view_sat_customer"])
    )
    sandbox = data_dir / "sbx0a1b" / "materialize.duckdb"
    assert _rows(sandbox, _views("sbx0a1b"), "mv_view_sat_customer") == _HISTORY
    assert _tables(sandbox, _views("sbx0a1b")) == ["mv_view_sat_customer"]


def test_a_template_already_holding_the_history_is_left_as_it_is(data_dir, tmp_path):
    demo = tmp_path / "demo"
    _store(data_dir / "default" / "materialize.duckdb", _views("default"), {"mv_view_s": _HISTORY})
    demo_replicas.export_views("default", demo, ["mv_view_s"])
    assert demo_replicas.adopt_views("sbxtpldv", demo)
    assert not demo_replicas.adopt_views("sbxtpldv", demo)


@pytest.mark.usefixtures("data_dir")
def test_a_demo_with_no_views_history_adopts_nothing(tmp_path):
    assert not demo_replicas.adopt_views("sbxtpldv", tmp_path / "demo")


def test_a_bitemporal_view_the_setup_never_built_fails_the_build(data_dir, tmp_path):
    _store(data_dir / "default" / "materialize.duckdb", _views("default"), {"mv_view_s": _HISTORY})
    with pytest.raises(RuntimeError, match="mv_view_absent"):
        demo_replicas.export_views("default", tmp_path / "demo", ["mv_view_s", "mv_view_absent"])
