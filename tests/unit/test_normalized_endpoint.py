# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-049 — endpoint wiring for normalized output (per-table CTAS → manifest)."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

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


def _state():
    st = MagicMock()
    st.mv_registry.get_fresh.return_value = []
    st.trino_conn = MagicMock()
    return st


@pytest.mark.asyncio
async def test_normalized_returns_manifest_of_tables():
    ntables = [
        _ntable("orders", ("orders",), "SELECT DISTINCT id FROM orders"),
        _ntable("customers", ("orders", "customer"), "SELECT DISTINCT id FROM customers"),
    ]
    ctas_results = iter(
        [
            {"table_name": "r_a", "s3_prefix": "s3a://b/results/a", "row_count": 10},
            {"table_name": "r_b", "s3_prefix": "s3a://b/results/b", "row_count": 3},
        ]
    )
    with (
        patch("provisa.compiler.normalize.compile_normalized", return_value=ntables),
        patch("provisa.api.data.endpoint._prepare_compiled", new=AsyncMock()),
        patch(
            "provisa.api.data.endpoint.rewrite_semantic_to_trino_physical",
            side_effect=lambda s, _c: s,
        ),
        patch("provisa.transpiler.transpile.transpile_to_trino", side_effect=lambda s: s),
        patch(
            "provisa.executor.trino_write.execute_ctas_redirect",
            side_effect=lambda *_a, **_k: next(ctas_results),
        ),
        patch(
            "provisa.executor.trino_write.presign_ctas_result",
            new=AsyncMock(side_effect=lambda p, _c: f"https://x/{p[-1]}"),
        ),
        patch("provisa.executor.trino_write.schedule_s3_cleanup", new=AsyncMock()),
        patch("provisa.executor.redirect.RedirectConfig.from_env", return_value=MagicMock()),
    ):
        resp = await _handle_normalized(
            document=MagicMock(),
            ctx=MagicMock(),
            rls=MagicMock(),
            state=_state(),
            variables=None,
            role_id="admin",
            role={"id": "admin"},
        )

    body = json.loads(bytes(resp.body))
    rows = body["normalized"]
    assert [r["table"] for r in rows] == ["orders", "customers"]
    assert rows[0]["path"] == ["orders"]
    assert rows[1]["path"] == ["orders", "customer"]
    assert rows[0]["rowCount"] == 10
    assert rows[1]["rowCount"] == 3
    assert all(r["url"].startswith("https://x/") for r in rows)


@pytest.mark.asyncio
async def test_normalized_governs_each_table():
    ntables = [_ntable("orders", ("orders",), "SELECT DISTINCT id FROM orders")]
    with (
        patch("provisa.compiler.normalize.compile_normalized", return_value=ntables),
        patch("provisa.api.data.endpoint._prepare_compiled", new=AsyncMock()) as prep,
        patch(
            "provisa.api.data.endpoint.rewrite_semantic_to_trino_physical",
            side_effect=lambda s, _c: s,
        ),
        patch("provisa.transpiler.transpile.transpile_to_trino", side_effect=lambda s: s),
        patch(
            "provisa.executor.trino_write.execute_ctas_redirect",
            return_value={"table_name": "r", "s3_prefix": "s3a://b/x", "row_count": 1},
        ),
        patch(
            "provisa.executor.trino_write.presign_ctas_result",
            new=AsyncMock(return_value="https://x/u"),
        ),
        patch("provisa.executor.trino_write.schedule_s3_cleanup", new=AsyncMock()),
        patch("provisa.executor.redirect.RedirectConfig.from_env", return_value=MagicMock()),
    ):
        await _handle_normalized(
            document=MagicMock(),
            ctx=MagicMock(),
            rls=MagicMock(),
            state=_state(),
            variables=None,
            role_id="admin",
            role={"id": "admin"},
        )
    assert prep.await_count == 1  # governance applied to the one table


@pytest.mark.asyncio
async def test_non_normalizable_query_returns_400():
    with (
        patch(
            "provisa.compiler.normalize.compile_normalized",
            side_effect=NormalizeError("relationship 'x' joins on a computed expression"),
        ),
        patch("provisa.executor.redirect.RedirectConfig.from_env", return_value=MagicMock()),
    ):
        with pytest.raises(HTTPException) as ei:
            await _handle_normalized(
                document=MagicMock(),
                ctx=MagicMock(),
                rls=MagicMock(),
                state=_state(),
                variables=None,
                role_id="admin",
                role={"id": "admin"},
            )
    assert ei.value.status_code == 400
