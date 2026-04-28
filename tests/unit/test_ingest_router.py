# Copyright (c) 2026 Kenneth Stott
# Canary: 6d39bfc1-b6e0-406c-b2cd-4299211ce4ee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for ingest router helpers (path extraction, row building)."""

import pytest

from provisa.ingest.router import _extract_row


def test_extract_row_top_level():
    payload = {"level": "INFO", "msg": "started"}
    cols = [
        {"column_name": "level", "path": "level", "data_type": "text"},
        {"column_name": "msg", "path": "msg", "data_type": "text"},
    ]
    row = _extract_row(payload, cols)
    assert row == {"level": "INFO", "msg": "started"}


def test_extract_row_dot_path():
    payload = {"resource": {"service": {"name": "my-svc"}}, "severity": "WARN"}
    cols = [
        {"column_name": "service_name", "path": "resource.service.name"},
        {"column_name": "severity", "path": "severity"},
    ]
    row = _extract_row(payload, cols)
    assert row["service_name"] == "my-svc"
    assert row["severity"] == "WARN"


def test_extract_row_missing_path_yields_none():
    payload = {"a": 1}
    cols = [{"column_name": "b", "path": "b.c.d"}]
    row = _extract_row(payload, cols)
    assert row["b"] is None


def test_extract_row_fallback_to_name_when_no_path():
    payload = {"level": "DEBUG"}
    cols = [{"column_name": "level"}]
    row = _extract_row(payload, cols)
    assert row["level"] == "DEBUG"


def test_extract_row_skips_system_columns():
    payload = {"_updated_at": "2026-01-01", "body": "hello"}
    cols = [
        {"column_name": "_updated_at", "path": "_updated_at"},
        {"column_name": "body", "path": "body"},
    ]
    row = _extract_row(payload, cols)
    assert "_updated_at" not in row
    assert row["body"] == "hello"


def test_extract_row_empty_payload():
    cols = [{"column_name": "msg", "path": "msg"}]
    row = _extract_row({}, cols)
    assert row == {"msg": None}


def test_extract_row_empty_columns():
    row = _extract_row({"x": 1}, [])
    assert row == {}
