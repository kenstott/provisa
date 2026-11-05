# Copyright (c) 2026 Kenneth Stott
# Canary: 763d929d-fa80-4dbe-bba5-370f8c07f642
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Inventory and remove what Provisa's tests created in the Snowflake account (REQ-254).

Run only from .github/workflows/snowflake-cleanup.yml, with the account's CI credentials:

* ``list`` (read-only): every warehouse, database (except Snowflake's own SNOWFLAKE and
  SNOWFLAKE_SAMPLE_DATA), schema, table, view, stage, task, pipe, stream, dynamic table,
  materialized view, alert and resource monitor the CI role can see, with each warehouse's size,
  state and auto-suspend, written to a JSON file and printed. Tasks, dynamic tables, materialized
  views, pipes and alerts run (and bill) on their own, so they are listed first.
* ``delete``: drops exactly the objects named in a JSON list taken from a ``list`` run --
  ``[{"kind": "database", "name": "X"}, {"kind": "warehouse", "name": "Y"}, ...]`` -- and nothing
  else. Each drop is reported; a drop Snowflake refuses fails the run naming the object.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import snowflake.connector

_OWN = {"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"}
# Objects that run on their own schedule or continuously, billing without a query.
_SELF_RUNNING = ("TASKS", "DYNAMIC TABLES", "MATERIALIZED VIEWS", "PIPES", "ALERTS")
_IN_SCHEMA = ("TABLES", "VIEWS", "STAGES", "STREAMS", *_SELF_RUNNING)
_DROP = {
    "database": "DROP DATABASE IF EXISTS {}",
    "warehouse": "DROP WAREHOUSE IF EXISTS {}",
    "resource_monitor": "DROP RESOURCE MONITOR IF EXISTS {}",
    "schema": "DROP SCHEMA IF EXISTS {}",
    "task": "DROP TASK IF EXISTS {}",
    "dynamic_table": "DROP DYNAMIC TABLE IF EXISTS {}",
    "materialized_view": "DROP MATERIALIZED VIEW IF EXISTS {}",
    "pipe": "DROP PIPE IF EXISTS {}",
    "alert": "DROP ALERT IF EXISTS {}",
}


def _connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )


def _rows(cur, sql: str) -> list[dict]:
    cur.execute(sql)
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def inventory(cur) -> dict:
    warehouses = [
        {k: w.get(k) for k in ("name", "size", "state", "auto_suspend", "auto_resume", "owner")}
        for w in _rows(cur, "SHOW WAREHOUSES")
    ]
    monitors = [m["name"] for m in _rows(cur, "SHOW RESOURCE MONITORS")]
    databases = []
    for d in _rows(cur, "SHOW DATABASES"):
        if d["name"] in _OWN or d.get("kind") in ("APPLICATION", "IMPORTED DATABASE"):
            continue
        db = {
            "name": d["name"],
            "owner": d.get("owner"),
            "created_on": str(d.get("created_on")),
            "schemas": [],
        }
        for s in _rows(cur, f"SHOW SCHEMAS IN DATABASE {_q(d['name'])}"):
            if s["name"] == "INFORMATION_SCHEMA":
                continue
            where = f"{_q(d['name'])}.{_q(s['name'])}"
            schema = {"name": s["name"]}
            for kind in _IN_SCHEMA:
                schema[kind.lower().replace(" ", "_")] = [
                    o["name"] for o in _rows(cur, f"SHOW {kind} IN SCHEMA {where}")
                ]
            db["schemas"].append(schema)
        databases.append(db)
    return {"warehouses": warehouses, "resource_monitors": monitors, "databases": databases}


def summary(inv: dict) -> list[str]:
    lines = ["Warehouses:"]
    lines += [
        f"  {w['name']}: size {w['size']}, {w['state']}, auto_suspend {w['auto_suspend']}s, owner {w['owner']}"
        for w in inv["warehouses"]
    ]
    lines.append(f"Resource monitors: {', '.join(inv['resource_monitors']) or 'none'}")
    lines.append("Databases:")
    for d in inv["databases"]:
        lines.append(f"  {d['name']} (owner {d['owner']}, created {d['created_on']})")
        for s in d["schemas"]:
            counts = ", ".join(f"{k} {len(v)}" for k, v in s.items() if k != "name" and v)
            running = [
                f"{k}: {', '.join(s[k])}"
                for k in (x.lower().replace(" ", "_") for x in _SELF_RUNNING)
                if s[k]
            ]
            lines.append(f"    {s['name']}: {counts or 'empty'}")
            lines += [f"      SELF-RUNNING {r}" for r in running]
    return lines


def delete(cur, items: list[dict]) -> int:
    failed = 0
    for item in items:
        kind, name = item["kind"], item["name"]
        if kind not in _DROP:
            print(f"refused: unknown kind {kind!r} for {name}")
            failed += 1
            continue
        ident = ".".join(_q(p) for p in name.split("."))
        try:
            cur.execute(_DROP[kind].format(ident))
            print(f"dropped {kind} {name}")
        except snowflake.connector.errors.ProgrammingError as exc:
            print(f"FAILED {kind} {name}: {exc}")
            failed += 1
    return 1 if failed else 0


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["list", "delete"])
    p.add_argument("--out")
    p.add_argument("--items")
    a = p.parse_args(argv)
    with _connect() as conn, conn.cursor() as cur:
        if a.mode == "list":
            inv = inventory(cur)
            print("\n".join(summary(inv)))
            with open(a.out, "w") as fh:
                json.dump(inv, fh, indent=2, default=str)
            return 0
        return delete(cur, json.loads(a.items))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
