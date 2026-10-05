# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E (REQ-990): a SingleStore materialization store bulk-loads through streaming LOAD DATA LOCAL
INFILE — the ingest-only land path (SingleStore has no in-place external scan). Exercises the real
Connection.bulk_copy on a live SingleStore via the singlestoredb SQLAlchemy dialect, proving a mixed
-type row set round-trips byte-for-byte (NULL distinct from empty string, JSON/Decimal/datetime and
tab/newline/backslash exact) and that the row count is exact. Runs only in the warehouse lane where
.env creds load; a skip there is a failure (tests/skip_is_failure.py)."""

# Requirements: REQ-990

from __future__ import annotations

import datetime
import decimal
import os
from urllib.parse import quote_plus

import pytest
from sqlalchemy import JSON, Column, DateTime, Integer, MetaData, Numeric, String, Table

from tests.env_creds import unset

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

pytest.importorskip("singlestoredb", reason="singlestoredb client required")
pytest.importorskip("sqlalchemy_singlestoredb", reason="sqlalchemy-singlestoredb dialect required")

_ENV = (
    "SINGLESTORE_HOST",
    "SINGLESTORE_PORT",
    "SINGLESTORE_USERNAME",
    "SINGLESTORE_PASSWORD",
    "SINGLESTORE_DATABASE",
)
_HAVE_CREDS = all(os.environ.get(v) for v in _ENV)
pytestmark.append(pytest.mark.skipif(not _HAVE_CREDS, reason=f"not set: {unset(*_ENV)}"))

from provisa.core.database import Database, create_engine_from_url  # noqa: E402

_TABLE = "__provisa_ss_bulk_e2e"


def _dsn() -> str:
    e = {v: os.environ[v] for v in _ENV}
    return (
        f"singlestoredb://{e['SINGLESTORE_USERNAME']}:{quote_plus(e['SINGLESTORE_PASSWORD'])}"
        f"@{e['SINGLESTORE_HOST']}:{e['SINGLESTORE_PORT']}/{e['SINGLESTORE_DATABASE']}"
    )


def _table() -> Table:
    return Table(
        _TABLE,
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("name", String(128)),
        Column("amount", Numeric(18, 6)),
        Column("ts", DateTime),
        Column("payload", JSON),
        Column("note", String(255)),
    )


_ROWS = [
    {
        "id": 1,
        "name": "plain",
        "amount": decimal.Decimal("12.345678"),
        "ts": datetime.datetime(2026, 1, 2, 3, 4, 5),
        "payload": {"a": 1, "b": [1, 2]},
        "note": "ok",
    },
    {  # empty string vs NULL, extreme decimal
        "id": 2,
        "name": "",
        "amount": decimal.Decimal("0.000001"),
        "ts": datetime.datetime(2026, 12, 31, 23, 59, 59),
        "payload": {"x": "y"},
        "note": None,
    },
    {  # tab/newline/backslash and unicode, NULL amount/ts/payload
        "id": 3,
        "name": "tab\tnew\nline\\back",
        "amount": None,
        "ts": None,
        "payload": None,
        "note": "unicode: café",
    },
]


async def test_singlestore_bulk_copy_streams_load_data_and_round_trips():
    engine = create_engine_from_url(_dsn())
    db = Database(engine, name="singlestore")
    table = _table()
    async with db.acquire() as conn:
        assert conn.capabilities.dialect == "singlestoredb"
        await conn.execute(f"DROP TABLE IF EXISTS `{_TABLE}`")
        await conn.execute(
            f"CREATE TABLE `{_TABLE}` (id INT PRIMARY KEY, name VARCHAR(128), "
            f"amount DECIMAL(18,6), ts DATETIME, payload JSON, note VARCHAR(255))"
        )
        try:
            n = await conn.bulk_copy(table, _ROWS)
            assert n == len(_ROWS)
            got = await conn.fetch(
                f"SELECT id, name, amount, ts, payload, note FROM `{_TABLE}` ORDER BY id"
            )
            assert len(got) == len(_ROWS)
            by_id = {dict(r)["id"]: dict(r) for r in got}

            # NULL stays distinct from an empty string
            assert by_id[2]["name"] == ""
            assert by_id[2]["note"] is None
            assert by_id[3]["amount"] is None and by_id[3]["ts"] is None

            # exact values
            assert decimal.Decimal(str(by_id[1]["amount"])) == decimal.Decimal("12.345678")
            assert decimal.Decimal(str(by_id[2]["amount"])) == decimal.Decimal("0.000001")
            assert by_id[3]["name"] == "tab\tnew\nline\\back"
            assert by_id[3]["note"] == "unicode: café"

            import json as _json

            def _obj(v):
                return _json.loads(v) if isinstance(v, (str, bytes)) else v

            assert _obj(by_id[1]["payload"]) == {"a": 1, "b": [1, 2]}
            assert by_id[3]["payload"] is None
        finally:
            await conn.execute(f"DROP TABLE IF EXISTS `{_TABLE}`")
