# Copyright (c) 2026 Kenneth Stott
# Canary: 4fc00b56-59bb-44d6-a598-812760053ae1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a replica in BigQuery is filled as Arrow through one pending Storage Write stream
into a build table and replaced in one statement, the replica's own table kept. Live project;
one throwaway dataset, deleted afterwards."""

from __future__ import annotations

import datetime
import os
from decimal import Decimal

import pyarrow as pa
import pytest

pytest.importorskip("google.cloud.bigquery", reason="google-cloud-bigquery required")

from provisa.federation.replica_address import replica_schema  # noqa: E402
from provisa.federation.replica_target import build_table_name  # noqa: E402
from provisa.federation.replica_target_warehouse import BigQueryStoreTarget  # noqa: E402

_ENV = ("GOOGLE_CLOUD_PROJECT", "GOOGLE_APPLICATION_CREDENTIALS")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
    # A live cloud warehouse: the warehouse lane only, where its credentials load (a missing one
    # fails that lane's credential check). Never a skip in the default lanes.
    pytest.mark.requires_warehouse,
]

_DATASET = replica_schema(f"swaptest{os.getpid()}")
_TABLE = "src__public__orders"
_COLUMNS = [
    ("id", "integer"),
    ("name", "text"),
    ("amount", "numeric"),
    ("placed", "timestamp"),
    ("day", "date"),
    ("at", "time"),
    ("ok", "boolean"),
    ("doc", "json"),
]


def _rows(n: int, tag: str) -> list[dict]:
    return [
        {
            "id": i,
            "name": f"{tag}{i}",
            "amount": Decimal("1.5") + i,
            "placed": datetime.datetime(2026, 1, 2, 3, 4, 5),
            "day": datetime.date(2026, 1, 2),
            "at": "03:04:05",
            "ok": i % 2 == 0,
            "doc": {"k": [i, tag]},
        }
        for i in range(n)
    ]


@pytest.fixture
def client():
    from google.cloud import bigquery

    project = os.environ["GOOGLE_CLOUD_PROJECT"]
    found = bigquery.Client(project=project, location=os.environ.get("BIGQUERY_LOCATION", "US"))
    try:
        yield found
    finally:
        found.delete_dataset(f"{project}.{_DATASET}", delete_contents=True, not_found_ok=True)
        found.close()


def _target(client) -> BigQueryStoreTarget:
    return BigQueryStoreTarget(
        client,
        project=client.project,
        dataset=_DATASET,
        table=_TABLE,
        columns=_COLUMNS,
        pk_columns=["id"],
    )


async def _build(client, rows: list[dict]) -> None:
    target = _target(client)
    await target.begin()
    for start in range(0, len(rows), 2):
        part = rows[start : start + 2]
        await target.write(pa.RecordBatch.from_pylist([{"id": r["id"]} for r in part]), part)
    await target.swap()


async def test_bigquery_replaces_the_replica_in_one_statement_and_keeps_its_table(client):
    replica = f"`{client.project}.{_DATASET}.{_TABLE}`"

    await _build(client, _rows(5, "a"))
    got = [
        tuple(r.values())
        for r in client.query(
            f"SELECT id, name, amount, placed, day, `at`, ok, JSON_VALUE(doc, '$.k[1]') "
            f"FROM {replica} ORDER BY 1"
        ).result()
    ]
    assert len(got) == 5
    assert got[1] == (
        1,
        "a1",
        Decimal("2.5"),
        datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        datetime.date(2026, 1, 2),
        datetime.time(3, 4, 5),
        False,
        "a",
    )
    created = client.get_table(f"{client.project}.{_DATASET}.{_TABLE}").created

    await _build(client, _rows(3, "b"))
    names = [r[0] for r in client.query(f"SELECT name FROM {replica} ORDER BY 1").result()]
    assert names == ["b0", "b1", "b2"]
    # The replica's own table stood through the rebuild, its key still on it.
    table = client.get_table(f"{client.project}.{_DATASET}.{_TABLE}")
    assert table.created == created
    assert table.table_constraints.primary_key.columns == ["id"]

    # An abandoned build leaves the replica as it was and no build table behind.
    target = _target(client)
    await target.begin()
    await target.write(pa.RecordBatch.from_pylist([{"id": 9}]), _rows(1, "c"))
    await target.abort()
    assert len(list(client.query(f"SELECT 1 FROM {replica}").result())) == 3
    tables = {t.table_id for t in client.list_tables(f"{client.project}.{_DATASET}")}
    assert tables == {_TABLE}
    assert build_table_name(_TABLE) not in tables
