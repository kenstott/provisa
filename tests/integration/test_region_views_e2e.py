# Copyright (c) 2026 Kenneth Stott
# Canary: 8ebb7346-3e77-4aa8-9d84-e358459c4b84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Integration: views and regions (REQ-1921, A VIEW MAY NAME A REGION; REQ-1922).

Two real DuckDB-engine nodes of one org, one in ``eu`` and one in ``us`` (the stack of
``test_region_read_in_place_e2e``), each building into its own Postgres views store. The orders
table carries a row rule on the region attribute for the organisation's administrator.

* A view naming no region is built by each region, governed as that region's administrator: each
  region's copy holds only that region's rows.
* A view naming ``eu`` is built only by ``eu``; a read through ``us`` is served from ``eu``'s copy,
  and ``us``'s record names ``eu`` as the region that answered."""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import httpx
import psycopg
import pytest
from psycopg import sql
import yaml

from tests.integration.test_region_read_in_place_e2e import _ROLE, _config, _server, _Stack

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")


def _view(name: str, region: str | None) -> dict:
    v = {"visible_to": [_ROLE]}
    entry = {
        "source_id": "__derived__",
        "domain_id": "sales",
        "schema": "views",
        "table": name,
        "view_sql": 'SELECT "id", "region" FROM "orders"',
        "materialize": True,
        "columns": [
            {"name": "id", "data_type": "integer", **v},
            {"name": "region", "data_type": "varchar", **v},
        ],
    }
    if region is not None:
        entry["region"] = region
    return entry


def _copy(stack: _Stack, region: str, view: str) -> list[tuple[int, str]] | None:
    """The rows of ``view``'s copy in ``region``'s views store, None while there is none."""
    with psycopg.connect(stack.url(stack.engine_port, f"{region}_store")) as conn:
        found = conn.execute(
            "SELECT table_schema FROM information_schema.tables "
            "WHERE table_schema LIKE '%%_mv_cache' AND table_name = %s",
            (f"mv_{view}",),
        ).fetchone()
        if found is None:
            return None
        rows = conn.execute(
            sql.SQL('SELECT "id", "region" FROM {}.{} ORDER BY "id"').format(
                sql.Identifier(found[0]), sql.Identifier(f"mv_{view}")
            )
        )
        return [(int(r[0]), r[1]) for r in rows]


def _until_copy(stack: _Stack, region: str, view: str) -> list[tuple[int, str]]:
    deadline, last = time.monotonic() + 120, None
    while time.monotonic() < deadline:
        last = _copy(stack, region, view)
        if last:
            return last
        time.sleep(1)
    with psycopg.connect(stack.url(stack.engine_port, f"{region}_store")) as conn:
        present = conn.execute(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables "
            "WHERE table_name LIKE 'mv%%' OR table_schema LIKE '%%mv%%'"
        ).fetchall()
        builds = []
        for (qualified,) in present:
            schema, name = qualified.split(".", 1)
            if name == "mv_build_state":
                builds = conn.execute(
                    sql.SQL("SELECT * FROM {}.mv_build_state").format(sql.Identifier(schema))
                ).fetchall()
    raise AssertionError(
        f"{region} never built its copy of {view}: {last}; its store holds {sorted(present)}; "
        f"builds {builds}"
    )


def _read_view(srv, view: str) -> list[tuple[int, str]]:
    deadline, last = time.monotonic() + 90, None
    while time.monotonic() < deadline:
        response = httpx.post(
            f"{srv.base_url}/data/sql",
            json={"sql": f'SELECT "id", "region" FROM "{view}" ORDER BY "id"', "role": _ROLE},
            headers={"X-Provisa-Role": _ROLE},
            timeout=srv.request_timeout + 10,
        )
        last = (response.status_code, response.text)
        if response.status_code == 200 and response.json()["data"]["sql"]:
            return [(int(r["id"]), r["region"]) for r in response.json()["data"]["sql"]]
        time.sleep(1)
    raise AssertionError(f"{srv.base_url} never answered {view}: {last}")


def _last_audit_region(stack: _Stack, region: str, view: str) -> str:
    """The region ``region``'s record names for its latest statement over ``view`` — waited for:
    the audit writer lands its rows in batches, after the answer."""
    deadline = time.monotonic() + 60
    while True:
        with psycopg.connect(stack.url(stack.engine_port, f"{region}_store")) as conn:
            schemas = [
                r[0]
                for r in conn.execute(
                    "SELECT table_schema FROM information_schema.tables "
                    "WHERE table_name = 'query_audit_log'"
                )
            ]
            assert len(schemas) == 1, schemas
            row = conn.execute(
                sql.SQL("SELECT region FROM {}.query_audit_log ORDER BY id DESC LIMIT 1").format(
                    sql.Identifier(schemas[0])
                )
            ).fetchone()
        if row is not None:
            return row[0]
        assert time.monotonic() < deadline, f"{region} recorded no statement over {view}"
        time.sleep(1)


@pytest.fixture
def stack():
    s = _Stack()
    s.start()
    try:
        yield s
    finally:
        s.stop()


def test_each_region_builds_its_own_copy_and_a_view_naming_eu_is_read_from_eu(stack):
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        (work / "orders").mkdir()
        files = work / "orders" / "orders.csv"
        files.write_text("id,amount,region\n1,10,eu\n2,20,us\n3,30,us\n4,40,eu\n")
        config = _config(stack, files, work, "csv")
        config["tables"] += [_view("every_orders", None), _view("eu_orders", "eu")]
        # The administrator refreshes the views now rather than the test waiting for their
        # schedule: table_registration is the right refreshMv asks for.
        config["roles"][0]["capabilities"].append("table_registration")
        path = work / "config.yaml"
        path.write_text(yaml.safe_dump(config))
        eu, us = _server(stack, "eu", path), _server(stack, "us", path)
        try:
            eu.start()
            us.start()
            _assertions(stack, eu, us)
        except AssertionError as failed:
            logs = "\n".join(
                f"--- {name} stderr (views, event loop, errors) ---\n{_relevant(srv)}"
                for name, srv in (("eu", eu), ("us", us))
            )
            raise AssertionError(f"{failed}\n{logs}") from failed
        finally:
            eu.stop_process()
            us.stop_process()


def _relevant(srv) -> str:
    """The lines of ``srv``'s log about its views and its event loop, and its other errors."""
    keep = (
        "event",
        "boot",
        "every_orders",
        "eu_orders",
        "view",
        "MV",
        "mv_",
        "ERROR",
        "Error",
        "refresh",
        "claim",
        "skip",
    )
    lines = [
        line
        for line in srv.dump_stderr_debug().splitlines()
        if any(k in line for k in keep) and "reclamation" not in line
    ]
    return "\n".join(lines[-80:])


def _refresh(srv, view: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": f'mutation {{ refreshMv(mvId: "view-{view}") {{ success message }} }}'},
        headers={"X-Provisa-Role": _ROLE},
        timeout=120,
    )
    assert response.status_code == 200, response.text
    return response.json()["data"]["refreshMv"]


def _assertions(stack: _Stack, eu, us) -> None:
    for srv in (eu, us):
        refreshed = _refresh(srv, "every_orders")
        assert refreshed["success"], (srv.base_url, refreshed)
    assert _refresh(eu, "eu_orders")["success"]
    refused = _refresh(us, "eu_orders")
    assert not refused["success"] and "names region 'eu'" in refused["message"], refused
    # A view naming no region: each region's copy holds what its administrator may see.
    assert _until_copy(stack, "eu", "every_orders") == [(1, "eu"), (4, "eu")]
    assert _until_copy(stack, "us", "every_orders") == [(2, "us"), (3, "us")]
    # A view naming eu: built by eu alone, and read through us from eu's copy.
    assert _until_copy(stack, "eu", "eu_orders") == [(1, "eu"), (4, "eu")]
    assert _read_view(us, "eu_orders") == [(1, "eu"), (4, "eu")]
    assert _copy(stack, "us", "eu_orders") is None
    assert _last_audit_region(stack, "us", "eu_orders") == "eu"
