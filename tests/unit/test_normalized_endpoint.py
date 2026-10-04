# Copyright (c) 2026 Kenneth Stott
# Canary: cec0c6bc-22ba-48b1-920d-201b3c1f3c4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-049 — endpoint wiring for normalized output (per-table CTAS → manifest)."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from provisa.api.data.endpoint import _handle_normalized
from provisa.compiler.normalize import NormalizeError, NormalizedTable
from provisa.compiler.sql_gen import CompiledQuery


def _ntable(name, path, sql):
    return NormalizedTable(
        table_name=name,
        path=path,
        compiled=CompiledQuery(
            sql=sql,
            params=[],
            root_field=name,
            columns=[],
            sources=set(),
        ),
    )


def _pipeline(handles):
    """The one pipeline stood in: each plan records how it was asked for, and executing it answers
    the next forced delivery's handle."""
    asked: list[dict] = []
    it = iter(handles)

    async def _govern(sql, role_id, **kwargs):
        asked.append({"sql": sql, "role_id": role_id, **kwargs})
        return SimpleNamespace(sql=sql)

    async def _execute(plan, state):
        return SimpleNamespace(redirect=next(it))

    return asked, _govern, _execute


def _handle(url, rows):
    return {"sink": "s3", "redirect_url": url, "row_count": rows, "expires_in": 60}


@pytest.mark.asyncio
async def test_normalized_returns_manifest_of_tables():
    ntables = [
        _ntable("orders", ("orders",), "SELECT DISTINCT id FROM orders"),
        _ntable("customers", ("orders", "customer"), "SELECT DISTINCT id FROM customers"),
    ]
    asked, govern, execute = _pipeline([_handle("https://x/a", 10), _handle("https://x/b", 3)])
    with (
        patch("provisa.compiler.normalize.compile_normalized", return_value=ntables),
        patch("provisa.pgwire._pipeline._govern_and_route_compiled", new=govern),
        patch("provisa.pgwire._pipeline._execute_plan", new=execute),
    ):
        resp = await _handle_normalized(
            document=MagicMock(),
            ctx=MagicMock(),
            state=MagicMock(),
            variables=None,
            role_id="admin",
        )

    body = json.loads(bytes(resp.body))
    rows = body["normalized"]
    assert [r["table"] for r in rows] == ["orders", "customers"]
    assert rows[0]["path"] == ["orders"]
    assert rows[1]["path"] == ["orders", "customer"]
    assert [r["rowCount"] for r in rows] == [10, 3]
    assert [r["url"] for r in rows] == ["https://x/a", "https://x/b"]


@pytest.mark.asyncio
async def test_each_table_is_a_governed_read_landed_by_a_forced_delivery():
    """Every entity is read through the one pipeline, as the acting role, with its compiled
    statement (the prepare and governance stages are the pipeline's) and a forced parquet
    delivery: the materialize stage lands it."""
    ntables = [_ntable("orders", ("orders",), "SELECT DISTINCT id FROM orders")]
    asked, govern, execute = _pipeline([_handle("https://x/u", 1)])
    with (
        patch("provisa.compiler.normalize.compile_normalized", return_value=ntables),
        patch("provisa.pgwire._pipeline._govern_and_route_compiled", new=govern),
        patch("provisa.pgwire._pipeline._execute_plan", new=execute),
    ):
        await _handle_normalized(
            document=MagicMock(),
            ctx=MagicMock(),
            state=MagicMock(),
            variables=None,
            role_id="admin",
        )
    assert len(asked) == 1
    (call,) = asked
    assert call["sql"] == "SELECT DISTINCT id FROM orders" and call["role_id"] == "admin"
    assert call["compiled"] is ntables[0].compiled
    assert call["deliver"] is not None and call["deliver"].output_format == "parquet"


@pytest.mark.asyncio
async def test_non_normalizable_query_returns_400():
    with (
        patch(
            "provisa.compiler.normalize.compile_normalized",
            side_effect=NormalizeError("relationship 'x' joins on a computed expression"),
        ),
    ):
        with pytest.raises(HTTPException) as ei:
            await _handle_normalized(
                document=MagicMock(),
                ctx=MagicMock(),
                state=MagicMock(),
                variables=None,
                role_id="admin",
            )
    assert ei.value.status_code == 400
