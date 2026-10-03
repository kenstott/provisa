# Copyright (c) 2026 Kenneth Stott
# Canary: 1c7e5a93-8b24-4f60-a2d9-6e3f0b8c4d17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A masked column keeps its name on the SQL surfaces, on a real server: a reader whose
``email`` is masked receives a column named ``email`` holding the mask, over SQL over HTTP and
pgwire, where it used to receive a nameless ``?column?``."""

from __future__ import annotations

import pytest

from tests.integration.test_view_lowering_e2e import _post, server  # noqa: F401

pytestmark = [pytest.mark.integration]

_SQL = "SELECT id, region, email FROM sales.orders ORDER BY id"


def test_over_sql_http(server):  # noqa: F811
    status, body = _post(server, "east_reader", "/data/sql", {"sql": _SQL})
    assert status == 200, body
    assert body["columns"] == ["id", "region", "email"]
    assert body["data"]["sql"] == [
        {"id": 1, "region": "east", "email": "***"},
        {"id": 3, "region": "east", "email": "***"},
    ]


def test_over_pgwire(server):  # noqa: F811
    import psycopg

    with psycopg.connect(
        host="127.0.0.1",
        port=server.ports["pgwire"],
        user="east_reader",
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    ) as conn:
        cur = conn.execute(_SQL)
        assert [d.name for d in cur.description] == ["id", "region", "email"]
        assert cur.fetchall() == [(1, "east", "***"), (3, "east", "***")]
