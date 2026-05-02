# Copyright (c) 2026 Kenneth Stott
# Canary: 4ecf03ff-c6d5-4527-8556-a887a9647739
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Per-request query stats accumulator (opt-in via X-Provisa-Stats header)."""
from __future__ import annotations

import time as _time
from contextvars import ContextVar
from dataclasses import dataclass, field as _field


@dataclass
class FieldStat:
    field: str
    source: str      # source_id, or "cache"
    strategy: str    # "direct:postgresql", "federated:sqlite", "api:openapi", "api:dataloader", "cache"
    elapsed_ms: float
    rows: int
    cache_hit: bool = False
    physical_sql: str | None = None  # final Trino SQL sent to federation engine


@dataclass
class QueryStats:
    entries: list[FieldStat] = _field(default_factory=list)
    mermaid: str | None = None
    wall_ms: float | None = None  # true end-to-end wall-clock set by caller
    _t0: float = _field(default_factory=_time.perf_counter)

    def record(self, *, field: str, source: str, strategy: str, elapsed_ms: float, rows: int, cache_hit: bool = False, physical_sql: str | None = None) -> None:
        self.entries.append(FieldStat(field=field, source=source, strategy=strategy, elapsed_ms=elapsed_ms, rows=rows, cache_hit=cache_hit, physical_sql=physical_sql))

    def to_dict(self) -> dict:
        if self.wall_ms is not None:
            total = self.wall_ms
        else:
            total = (_time.perf_counter() - self._t0) * 1000
        result: dict = {
            "total_elapsed_ms": round(total, 1),
            "sources": [
                {
                    "field": e.field,
                    "source": e.source,
                    "strategy": e.strategy,
                    "elapsed_ms": round(e.elapsed_ms, 1),
                    "rows": e.rows,
                    **({"cache_hit": True} if e.cache_hit else {}),
                    **({"physical_sql": e.physical_sql} if e.physical_sql else {}),
                }
                for e in self.entries
            ],
        }
        if self.mermaid:
            result["mermaid"] = self.mermaid
        return result


_ctx: ContextVar[QueryStats | None] = ContextVar("query_stats", default=None)


def begin() -> QueryStats:
    s = QueryStats()
    _ctx.set(s)
    return s


def current() -> QueryStats | None:
    return _ctx.get()


def record(**kwargs) -> None:
    s = _ctx.get()
    if s is not None:
        s.record(**kwargs)
