# Copyright (c) 2026 Kenneth Stott
# Canary: 13aa66e9-a220-4cac-9aa9-0de3649dbaaf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
"""The banking demo's setup client (setup.yaml; scripts/run_demo_setup.py runs it against the
server the setup is run on): thirty business days of end-of-day reporting, one day at a time
(REQ-2107).

For each business day, in order, it moves the postings the bank's core systems deliver that day
(db/init.sql's feed.postings) into the ledger, as those systems would, then builds the end-of-day
totals sealed at that day's close (refreshMv with the day as its window, as org_admin). Each
day's figures are so recorded at its own close, and a restatement delivered on a later day -- the
reversed payment, the late trade -- is recorded at the close of the day it arrived. After each
build the day's figures are read back as recorded at its close; a day with none stops the setup.

Environment: PROVISA_URL (the server, from run_demo_setup.py); the ledger's address as config.yaml
reads it (BANKING_PG_HOST, BANKING_PG_PORT, BANKING_PG_NS). It writes to the ledger as the
database's owner, the role db/init.sql ran as: Provisa itself reads the ledger read only.
"""

from __future__ import annotations

import os
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

import httpx
import psycopg

URL = os.environ["PROVISA_URL"]
ADMIN = {"X-Provisa-Role": "org_admin"}
MV_ID = "view-view_eod_totals"
#: The bank's close, as config.yaml's calendar "bank" declares it.
CLOSE = time(18, 0)
ZONE = ZoneInfo("Europe/London")
_COLUMNS = (
    "row_id, transaction_id, account_id, business_date, branch, product, currency, kind, amount, "
    "counterparty, status, updated_at"
)


def _ledger() -> psycopg.Connection:
    # The ledger where config.yaml reads it: its variables, else its own defaults (a local start
    # sets none; the bundle build sets the port it ran the store on).
    return psycopg.connect(
        host=os.environ.get("BANKING_PG_HOST", "localhost"),
        port=int(os.environ.get("BANKING_PG_PORT", "27032")),
        dbname=f"ledger{os.environ.get('BANKING_PG_NS', '')}",
        user="postgres",
        password="postgres",
        # Each day's postings are committed before its build reads them: without autocommit the
        # first statement opens a transaction and each day's block is only a savepoint inside it.
        autocommit=True,
    )


def close_of(day: date) -> str:
    """The day's close in UTC, as an as-recorded time: what its sealed build is stamped with."""
    local = datetime.combine(day, CLOSE, tzinfo=ZONE)
    return local.astimezone(timezone.utc).replace(tzinfo=None).isoformat()


def _admin(client: httpx.Client, query: str) -> dict:
    response = client.post("/admin/graphql", json={"query": query})
    if response.status_code != 200:
        raise SystemExit(f"{query}: HTTP {response.status_code}: {response.text}")
    body = response.json()
    if body.get("errors"):
        raise SystemExit(f"{query}: {body['errors']}")
    return body["data"]


def _figures_at_close(client: httpx.Client, day: date) -> int:
    """How many of the day's end-of-day totals the view answers as recorded at its close."""
    response = client.post(
        "/data/sql",
        json={
            "sql": 'SELECT COUNT(*) AS n FROM "banking"."view_eod_totals" '
            f"WHERE business_date = DATE '{day.isoformat()}'"
        },
        headers={"X-Provisa-As-Recorded": close_of(day)},
    )
    if response.status_code != 200:
        raise SystemExit(f"{day}: HTTP {response.status_code}: {response.text}")
    [row] = response.json()["data"]["sql"]
    return int(row["n"])


def _why_none(client: httpx.Client, day: date) -> str:
    """What the view holds when a day has no figure at its close: its build state, and when each
    of the day's figures was recorded."""
    views = _admin(client, "{ mvList { id status rowCount lastError } }")["mvList"]
    [view] = [v for v in views if v["id"] == MV_ID]
    response = client.post(
        "/data/sql",
        json={
            "sql": 'SELECT e.recorded_at, COUNT(*) AS n FROM "banking"."view_eod_totals" AS e '
            f"WHERE e.business_date = DATE '{day.isoformat()}' GROUP BY e.recorded_at"
        },
    )
    if response.status_code != 200:
        return f"view {view}; its figures unread: HTTP {response.status_code}: {response.text}"
    return f"view {view}; the day's figures by recorded time: {response.json()['data']['sql']}"


def main() -> None:
    with _ledger() as conn, httpx.Client(base_url=URL, headers=ADMIN, timeout=600) as client:
        days = [
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT arrives_on FROM feed.postings ORDER BY arrives_on"
            ).fetchall()
        ]
        for day in days:
            with conn.transaction():
                conn.execute(
                    f"INSERT INTO public.transactions ({_COLUMNS}) "
                    f"SELECT {_COLUMNS} FROM feed.postings WHERE arrives_on = %s ORDER BY row_id",
                    (day,),
                )
            built = _admin(
                client,
                f'mutation {{ refreshMv(mvId: "{MV_ID}", window: "{day.isoformat()}") '
                "{ success message } }",
            )["refreshMv"]
            if not built["success"]:
                raise SystemExit(f"{day}: the end-of-day build failed: {built['message']}")
            if _figures_at_close(client, day) == 0:
                raise SystemExit(
                    f"{day}: no end-of-day figure is recorded at its close {close_of(day)} "
                    f"({built['message']}); {_why_none(client, day)}"
                )
            print(f"banking: {day} closed", flush=True)


if __name__ == "__main__":
    main()
