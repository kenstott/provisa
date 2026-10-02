# Copyright (c) 2026 Kenneth Stott
# Canary: 67c1a0d4-c086-40a5-80d2-2bb52fe5a6e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prometheus over its HTTP API, engine-independently (REQ-1689).

The Trino connector was the only reader of a Prometheus source. This module is the native reader,
with the connector's own table shape: a metric is a table; a row is one sample with ``timestamp``,
``value`` and one column per label. Register Table lists the metric names as tables and types a
metric's columns from its series' labels and its metadata type (``discover_schema``); a
``mapping.tables`` entry (the type's mapping DSL, REQ-251) names the metric, the labels kept as
columns and the range read. Synchronous httpx — callers run it in a thread.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from collections.abc import Iterator
from typing import Any

import httpx

from provisa.prometheus.source import discover_schema

DEFAULT_RANGE = "1h"
_RANGE_RE = re.compile(r"^(\d+)([smhd])$")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_MAX_POINTS = 11000  # Prometheus refuses a range query above this many points


@dataclass(frozen=True)
class PrometheusConnection:  # REQ-1689
    base_url: str
    token: str | None = None
    timeout: float = 60.0

    @classmethod
    def build(cls, url: str, *, token: str | None = None) -> "PrometheusConnection":
        return cls(base_url=url.rstrip("/"), token=token or None)

    def _client(self) -> httpx.Client:
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        return httpx.Client(base_url=self.base_url, headers=headers, timeout=self.timeout)


def range_seconds(spec: str) -> int:  # REQ-1689
    """``"15m"`` → 900. A range the mapping DSL cannot express is a config error."""
    m = _RANGE_RE.match(spec.strip())
    if not m:
        raise ValueError(f"Prometheus range {spec!r}: use <n>s|m|h|d (e.g. 1h)")
    return int(m.group(1)) * _UNIT_SECONDS[m.group(2)]


def _data(resp: httpx.Response) -> Any:
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != "success":
        raise ValueError(f"Prometheus API error: {body.get('errorType')}: {body.get('error')}")
    return body.get("data")


def list_metrics(conn: PrometheusConnection) -> list[str]:  # REQ-1689
    """Every metric name the server knows, sorted."""
    with conn._client() as c:
        return sorted(_data(c.get("/api/v1/label/__name__/values")) or [])


def metric_columns(conn: PrometheusConnection, metric: str) -> list[dict]:  # REQ-1689
    """``discover_schema`` over the metric's live labels and metadata type."""
    with conn._client() as c:
        series = _data(c.get("/api/v1/series", params={"match[]": metric})) or []
        meta = _data(c.get("/api/v1/metadata", params={"metric": metric})) or {}
    labels: set[str] = set()
    for s in series:
        labels.update(k for k in s if k != "__name__")
    entries = meta.get(metric) or []
    mtype = entries[0].get("type", "gauge") if entries else "gauge"
    return discover_schema({"labels": sorted(labels), "type": mtype}, metric)


def _table_entry(mapping: dict, table_name: str) -> dict | None:
    return next((t for t in mapping.get("tables", []) if t.get("name") == table_name), None)


def table_metric(mapping: dict, table_name: str) -> tuple[str, str, str]:  # REQ-1689
    """(metric, value column, range) for a table: the mapping DSL's entry when declared, else the
    connector convention — the table name IS the metric, ``value``, the default range."""
    entry = _table_entry(mapping, table_name)
    if entry is None:
        return table_name, "value", DEFAULT_RANGE
    return (
        entry.get("metric") or table_name,
        entry.get("value_column") or "value",
        entry.get("default_range") or DEFAULT_RANGE,
    )


def _range_params(mapping: dict, table_name: str) -> tuple[dict, str]:
    """The ``query_range`` parameters for the table's metric over its range, and its value
    column."""
    metric, value_column, spec = table_metric(mapping, table_name)
    seconds = range_seconds(spec)
    end = datetime.now(UTC).timestamp()
    start = end - seconds
    step = max(1, seconds // _MAX_POINTS + (1 if seconds % _MAX_POINTS else 0), 15)
    return {"query": metric, "start": start, "end": end, "step": step}, value_column


def _series_rows(series: dict, columns: list[str], value_column: str) -> Iterator[dict]:
    """One series' samples as rows of ``columns``: ``timestamp`` (UTC), the value column
    (float), and each other column from the series' labels."""
    labels = series.get("metric", {})
    for ts, val in series.get("values", []):
        row: dict = {}
        for col in columns:
            if col == "timestamp":
                row[col] = datetime.fromtimestamp(float(ts), tz=UTC)
            elif col == value_column:
                row[col] = float(val)
            else:
                row[col] = labels.get(col)
        yield row


def fetch_rows(
    conn: PrometheusConnection, mapping: dict, table_name: str, columns: list[str]
) -> list[dict]:  # REQ-1689
    """Every sample of the metric over the table's range as a row of ``columns``: ``timestamp``
    (UTC), the value column (float), and each other column from the sample's labels."""
    params, value_column = _range_params(mapping, table_name)
    with conn._client() as c:
        result = _data(c.get("/api/v1/query_range", params=params))
    rows: list[dict] = []
    for series in (result or {}).get("result", []):
        rows.extend(_series_rows(series, columns, value_column))
    return rows


def iter_rows_spooled(
    conn: PrometheusConnection, mapping: dict, table_name: str, columns: list[str], spooled: Any
) -> Iterator[dict]:  # REQ-1915
    """:func:`fetch_rows`, with the API's answer written to a spool file as it arrives and
    parsed from the file: first its status (an error answer is refused before any row is read),
    then one series at a time. A single series' samples are read whole."""
    from provisa.federation.replica_spool import json_items

    params, value_column = _range_params(mapping, table_name)
    with conn._client() as c:
        with spooled(lambda: c.stream("GET", "/api/v1/query_range", params=params)) as body:
            if next(json_items(body, "status"), None) != "success":
                kind = next(json_items(body, "errorType"), None)
                error = next(json_items(body, "error"), None)
                raise ValueError(f"Prometheus API error: {kind}: {error}")
            for series in json_items(body, "data.result.item"):
                yield from _series_rows(series, columns, value_column)
