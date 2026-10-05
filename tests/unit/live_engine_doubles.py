# Copyright (c) 2026 Kenneth Stott
# Canary: edcb489b-d68d-4841-885b-a5703b7992a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Doubles for the live engine's unit tests (REQ-286).

The engine reads every row through the governed pipeline (``provisa.live.governed``); these tests
stand that seam in: ``governed`` records each governed read -- its SQL and the key it ran as --
and answers with the rows the pipeline would return for that key.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Callable
from unittest.mock import AsyncMock, MagicMock, patch

from provisa.live.engine import LiveEngine, LiveSpec
from provisa.live.governed import GovernanceKey

ORG = "default"
KEY = GovernanceKey(ORG, "analyst", ())
OTHER_KEY = GovernanceKey(ORG, "analyst", (("region", "eu"),))
REF = '"sales"."events"'


def make_pool_with_conn(conn: Any) -> MagicMock:
    """A store double whose acquire() context manager yields *conn*."""
    pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    pool.acquire = MagicMock(return_value=cm)
    return pool


def make_engine(pool: Any = None, scheduler: Any = None, *, started: bool = False) -> LiveEngine:
    engine = LiveEngine(
        tenant_db=pool if pool is not None else MagicMock(),
        org_id=ORG,
        scheduler=scheduler if scheduler is not None else MagicMock(),
    )
    if started:
        engine._scheduler = engine._process_scheduler
    return engine


def spec(query_id: str = "q1", **kw: Any) -> LiveSpec:
    values: dict[str, Any] = {"table_id": 7, "watermark_column": "ts", "poll_interval": 5}
    values.update(kw)
    return LiveSpec(query_id=query_id, **values)


def sse_type(key: GovernanceKey = KEY) -> str:
    return f"sse:{key.digest}"


def sse_job_id(query_id: str = "q1", key: GovernanceKey = KEY) -> str:
    return f"live_{query_id}:org_{ORG}:{sse_type(key)}"


@contextmanager
def governed(
    answer: "list[dict] | Callable[[str, GovernanceKey], list[dict]] | Exception | None" = None,
):
    """Stand in the governed pipeline. ``answer`` is the rows every governed read returns, a
    function of (sql, key) to them, or an exception every read raises. Yields the reads made."""
    reads: list[tuple[str, GovernanceKey, list | None]] = []

    async def _rows(sql: str, key: GovernanceKey, params: list | None = None) -> list[dict]:
        reads.append((sql, key, params))
        if isinstance(answer, Exception):
            raise answer
        if callable(answer):
            return answer(sql, key)
        return [dict(r) for r in answer or []]

    def _output_key(role_id: str) -> GovernanceKey:
        return GovernanceKey(ORG, role_id, ())

    with (
        patch("provisa.live.governed.governed_rows", _rows),
        patch("provisa.live.governed.table_meta", lambda _tid: SimpleNamespace()),
        patch("provisa.live.governed.table_ref", lambda _meta: REF),
        patch("provisa.live.governed.output_key", _output_key),
    ):
        yield reads
