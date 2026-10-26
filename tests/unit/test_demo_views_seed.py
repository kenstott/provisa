# Copyright (c) 2026 Kenneth Stott
# Canary: a96d70ca-e41b-47a8-a21f-4ce7711dbaf6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo's bitemporal views keep their history from its bundle build to every sandbox (REQ-1162,
REQ-2006): the build writes their append logs beside the seed, the template adopts them into its
store, and a sandbox is given the template's. The records of the refreshes that built that history
(REQ-862) travel with it, each landed as the organisation's that now holds it."""

# Requirements: REQ-1162, REQ-2006, REQ-862

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
                    business_key=["customer_hk"],
                    valid_time="effective_at",
                    recorded_time="loaded_at",
                    sql="SELECT 1 AS customer_hk, now() AS effective_at, now() AS loaded_at",
                    columns=[
                        {"name": n, "data_type": t, "visible_to": []}
                        for n, t in (
                            ("customer_hk", "varchar"),
                            ("effective_at", "timestamp"),
                            ("loaded_at", "timestamp"),
                        )
                    ],
                ),
                _view("mart", materialize=True),
                _view("live"),
            ],
        }
    )


def test_a_config_view_declares_how_it_keeps_its_versions():
    [sat] = [t for t in _config()["tables"] if t["table"] == "view_sat_customer"]
    assert (sat["business_key"], sat["valid_time"], sat["recorded_time"]) == (
        ["customer_hk"],
        "effective_at",
        "loaded_at",
    )


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
    # Where this process lands its trace records, the refresh records among them (REQ-862).
    monkeypatch.setenv("PROVISA_TELEMETRY_DIR", str(tmp_path / "telemetry"))
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
        demo_replicas.copy_views_to_sandbox("sbxtpldv", "sbx0a1b", ["mv_view_sat_customer"], demo)
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


def _refresh(mv_id: str, start_s: int, name: str | None = None) -> dict:
    from provisa.observability.ops_schema import MV_REFRESH_LINEAGE_SPAN, trace_row

    row = trace_row(
        trace_id=f"{start_s:032x}",
        span_id=f"{start_s:016x}",
        parent_span_id=None,
        name=name or MV_REFRESH_LINEAGE_SPAN,
        kind=1,
        attributes={"mv.id": mv_id, "lineage.definition_version": "sha256:d"},
        resource_attributes={"service.name": "provisa"},
        scope_name="provisa",
        start_ns=start_s * 1_000_000_000,
        end_ns=start_s * 1_000_000_000 + 1,
        status_code=0,
        status_message=None,
    )
    return {**row, "tenant_id": "default"}


def _landed_refreshes(org: str) -> list[tuple[str, str]]:
    from provisa.observability import native_traces

    return [
        (r["trace_id"], r["tenant_id"])
        for r in native_traces.refresh_records(
            native_traces.traces_dir(), ["view-view_sat_customer"]
        )
        if r["tenant_id"] == org
    ]


def test_the_refreshes_that_built_the_history_travel_with_it(data_dir, tmp_path):
    from provisa.observability import native_traces

    demo = tmp_path / "demo"
    _store(
        data_dir / "default" / "materialize.duckdb",
        _views("default"),
        {"mv_view_sat_customer": _HISTORY},
    )
    # The build's own refreshes: the view's two, another view's and a span that is no refresh.
    native_traces.write_batch(
        native_traces.traces_dir(),
        [
            _refresh("view-view_sat_customer", 1_790_000_000),
            _refresh("view-view_sat_customer", 1_790_000_005),
            _refresh("view-view_mart", 1_790_000_003),
            _refresh("view-view_sat_customer", 1_790_000_004, name="mv.refresh"),
        ],
    )
    demo_replicas.export_views("default", demo, ["mv_view_sat_customer"])
    assert json.loads((demo / "seeded" / "views.json").read_text())["refreshes"] == 2

    built = [(f"{s:032x}", "default") for s in (1_790_000_000, 1_790_000_005)]
    assert _landed_refreshes("default") == built
    demo_replicas.adopt_views("sbxtpldv", demo)
    asyncio.run(
        demo_replicas.copy_views_to_sandbox("sbxtpldv", "sbx0a1b", ["mv_view_sat_customer"], demo)
    )
    # The same records, in order, each the organisation's that now holds the history.
    for org in ("sbxtpldv", "sbx0a1b"):
        assert _landed_refreshes(org) == [(trace, org) for trace, _ in built]


def test_a_bundle_carrying_no_refresh_records_lands_none(data_dir, tmp_path):
    demo = tmp_path / "demo"
    _store(
        data_dir / "default" / "materialize.duckdb",
        _views("default"),
        {"mv_view_sat_customer": _HISTORY},
    )
    demo_replicas.export_views("default", demo, ["mv_view_sat_customer"])
    assert not (demo / "seeded" / "refresh_lineage.parquet").exists()
    assert demo_replicas.land_refreshes(demo, "sbx0a1b") == 0


def test_a_regions_demo_s_append_logs_are_each_its_home_region_s():
    """REQ-2068: a bitemporal view naming a region is kept there; one naming none in every region
    (demo_region_seed.kept_here)."""
    from types import SimpleNamespace

    from provisa.core.models import DERIVED_SOURCE_ID

    def view(name, region, bitemporal=True):
        return SimpleNamespace(
            source_id=DERIVED_SOURCE_ID,
            materialize=True,
            is_bitemporal=bitemporal,
            table_name=name,
            region=region,
        )

    config = SimpleNamespace(
        tables=[
            view("view_eod_totals", "warehouse"),
            view("view_everywhere", None),
            view("view_plain", "ledger", bitemporal=False),
        ]
    )
    assert demo_replicas.bitemporal_view_homes(config, ["ledger", "warehouse"]) == {
        "ledger": ["mv_view_everywhere"],
        "warehouse": ["mv_view_eod_totals", "mv_view_everywhere"],
    }
