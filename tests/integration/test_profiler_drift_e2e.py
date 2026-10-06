# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2a9c41-d83e-4b17-a5c0-9e7b1d4f2a68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Drift across runs, duplicates and constraints on a real server (REQ-1934).

One table is profiled five times. The first runs set the window; the fifth run's amounts move far
from the window's and it holds repeated rows, so its comparison and drift measures show it: the
amount distribution drifting against the pooled window, the duplicate rows counted, a declared key
held by more than one row. Constraints the profile proposes are accepted through the admin surface
and checked by the next run; exporting one with no checker scanning the table is refused by name.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

import pytest
import sqlalchemy as sa

from tests.helpers import PROFILER_RUN_DEFAULTS
from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_READERS = ["org_admin", "analyst"]
_WINDOW = 3


def _col(name: str, data_type: str, **extra) -> dict:
    return {"name": name, "data_type": data_type, "visible_to": _READERS, **extra}


@pytest.fixture(scope="module")
def server():
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    base = _config(pg_host, pg_port, "unused")
    events = {
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema": "public",
        "table": "events",
        "profiler_source_id": "profiler",
        "watermark_column": "placed",
        "columns": [
            # Declared the key, not enforced by the store: the profile counts its repeats.
            _col("id", "integer", is_primary_key=True),
            _col("region", "varchar"),
            _col("amount", "integer"),
            # Published as placed_on: a constraint names both, the published and the physical.
            _col("placed", "date", alias="placed_on"),
            _col("shipped", "date"),
        ],
    }
    boot = WorkerBoot(
        1,
        pg_host=pg_host,
        pg_port=pg_port,
        extra_config={"tables": [base["tables"][0], events]},
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot._extra_config["sources"] = _config(pg_host, pg_port, boot.database)["sources"] + [
        {
            "id": "profiler",
            "type": "data_profiler",
            "mapping": {"cron": "0 3 * * *", **PROFILER_RUN_DEFAULTS, "drift_window": _WINDOW},
        }
    ]
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    "CREATE TABLE public.events (id integer, region varchar, amount integer, "
                    "placed date, shipped date)"
                )
            )
            conn.execute(
                sa.text(
                    "INSERT INTO public.events SELECT g, "
                    "CASE WHEN g % 2 = 0 THEN 'east' ELSE 'west' END, g % 50, "
                    "DATE '2026-01-01' + g, DATE '2026-01-01' + g + g % 3 "
                    "FROM generate_series(1, 400) g"
                )
            )
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot, engine
    finally:
        boot.cleanup()


def _call(boot, method: str, path: str, body: dict | None = None) -> tuple[int, Any]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": "org_admin"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()}


def _member_id(boot) -> int:
    status, catalog = _call(boot, "GET", "/admin/profilers/profiler/catalog")
    assert status == 200, catalog
    return next(m["memberId"] for m in catalog if m["member"] == "events")


def _run(boot, member: int) -> dict:
    status, run = _call(boot, "POST", f"/admin/tables/{member}/profile-runs")
    assert status == 200, run
    status, results = _call(boot, "GET", f"/admin/tables/{member}/profile-runs/{run['runId']}")
    assert status == 200, results
    return results


def _drift(results: dict, scope: str, measure: str, column: str | None = None) -> dict:
    return next(
        r
        for r in results["drift"]
        if r["scope"] == scope and r["measure"] == measure and r["column_name"] == column
    )


@pytest.fixture(scope="module")
def runs(server) -> dict:
    boot, engine = server
    member = _member_id(boot)
    out = []
    with engine.connect() as conn:
        for i in range(4):
            if i:
                # A few new rows each time, from the same distribution.
                conn.execute(
                    sa.text(
                        "INSERT INTO public.events SELECT g, "
                        "CASE WHEN g % 2 = 0 THEN 'east' ELSE 'west' END, g % 50, "
                        "DATE '2026-01-01' + g, DATE '2026-01-01' + g + g % 3 "
                        f"FROM generate_series({400 + 10 * i - 9}, {400 + 10 * i}) g"
                    )
                )
            out.append(_run(boot, member))
        # The fifth run: the amounts move far from the window's, and 25 rows repeat others.
        conn.execute(sa.text("UPDATE public.events SET amount = amount * 10 + 1000"))
        conn.execute(
            sa.text("INSERT INTO public.events SELECT * FROM public.events WHERE id <= 25")
        )
        out.append(_run(boot, member))
    return {"boot": boot, "engine": engine, "member": member, "runs": out}


def test_each_run_is_compared_with_the_one_before(runs):
    first, second = runs["runs"][0], runs["runs"][1]
    assert _drift(first, "run", "previous_run")["detail"] == "no previous run"
    assert second["runs"][0]["previous_run_id"] == first["runs"][0]["run_id"]
    count = _drift(second, "table", "row_count")
    assert (count["previous"], count["current"], count["change"]) == (400, 410, 10)


def test_no_drift_is_measured_before_the_window_is_full(runs):
    third = runs["runs"][2]
    assert third["runs"][0]["window_runs"] == 2
    count = _drift(third, "table", "row_count")
    assert count["baseline"] is None and count["drifting"] is None


def test_a_shifted_distribution_and_repeated_rows_drift_against_the_window(runs):
    fifth = runs["runs"][4]
    run = fifth["runs"][0]
    assert run["window_runs"] == _WINDOW
    # 25 rows repeat another row in every column; the declared key holds 25 ids twice.
    assert run["duplicate_rows"] == 25 and run["key_duplicates"] == 25
    dist = _drift(fifth, "column", "distribution", "amount")
    assert dist["ks"] > 0.9 and dist["drifting"] is True
    mean = _drift(fifth, "column", "mean", "amount")
    assert mean["baseline"] is not None and mean["drifting"] is True
    dups = _drift(fifth, "table", "duplicate_share")
    assert dups["previous"] == 0 and dups["current"] > 0 and dups["drifting"] is True
    # Freshness is measured from the declared watermark, and region did not drift.
    assert _drift(fifth, "table", "freshness_seconds")["current"] is not None
    assert _drift(fifth, "column", "category_shares", "region")["drifting"] is False
    # The measure's history across the window, the run last.
    status, history = _call(
        runs["boot"],
        "GET",
        f"/admin/tables/{runs['member']}/profile-runs/{run['run_id']}/history"
        "?scope=column&measure=mean&column=amount",
    )
    assert status == 200, history
    assert len(history["points"]) == _WINDOW + 1 and history["points"][-1]["current"] is True


def test_proposed_constraints_are_accepted_and_checked_by_the_next_run(runs):
    boot, member, engine = runs["boot"], runs["member"], runs["engine"]
    latest = runs["runs"][4]
    proposals = {(p["constraint"], p["column_name"]): p for p in latest["constraints"]}
    ordering = proposals[("ordering", "placed_on")]
    assert ordering["other_column"] == "shipped"
    for p in (ordering, proposals[("value_set", "region")]):
        status, body = _call(
            boot,
            "POST",
            f"/admin/tables/{member}/profile-constraints",
            {
                "kind": p["constraint"],
                "column": p["column_name"],
                "otherColumn": p["other_column"],
                "definition": json.loads(p["definition"]),
                "evidence": p["evidence"],
                "share": p["share"],
                "sampled": p["sampled"],
                "status": "accepted",
                "runId": p["run_id"],
            },
        )
        assert status == 200, body
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO public.events VALUES "
                "(9001, 'north', 1, DATE '2026-06-02', DATE '2026-06-01')"
            )
        )
    checks = {c["constraint"]: c for c in _run(boot, member)["constraint_checks"]}
    assert checks["ordering"]["violations"] == 1
    assert checks["value_set"]["violations"] == 1 and checks["value_set"]["pass_share"] < 1

    status, decided = _call(boot, "GET", f"/admin/tables/{member}/profile-constraints")
    assert status == 200, decided
    by_kind = {d["kind"]: d for d in decided["decisions"]}
    assert (by_kind["ordering"]["column_name"], by_kind["ordering"]["physical_column"]) == (
        "placed_on",
        "placed",
    )
    assert by_kind["ordering"]["physical_other_column"] == "shipped"
    cid = by_kind["value_set"]["id"]
    status, refused = _call(
        boot, "POST", f"/admin/tables/{member}/profile-constraints/{cid}/export", {}
    )
    assert (
        status == 409 and "no Soda or Great Expectations checker source scans" in refused["error"]
    )
    status, refused = _call(boot, "POST", f"/admin/tables/{member}/profile-checks/drift", {})
    assert status == 409 and "is not registered" in refused["error"]
