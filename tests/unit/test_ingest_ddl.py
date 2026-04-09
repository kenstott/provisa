# Copyright (c) 2026 Kenneth Stott
# Canary: 71ffb9b9-1319-4734-bd00-c1b73c7c954c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for ingest DDL generation (provisa/ingest/ddl.py)."""

import pytest

from provisa.ingest.ddl import generate_create_table, extract_value, _safe_type


# ---------------------------------------------------------------------------
# _safe_type
# ---------------------------------------------------------------------------


def test_safe_type_text():
    assert _safe_type("text") == "text"


def test_safe_type_case_insensitive():
    assert _safe_type("TIMESTAMPTZ") == "timestamptz"


def test_safe_type_unknown_falls_back():
    assert _safe_type("evil_type; DROP TABLE") == "text"


def test_safe_type_none_falls_back():
    assert _safe_type(None) == "text"


def test_safe_type_empty_falls_back():
    assert _safe_type("") == "text"


# ---------------------------------------------------------------------------
# generate_create_table
# ---------------------------------------------------------------------------


def test_basic_ddl_structure():
    cols = [{"column_name": "msg", "data_type": "text"}]
    ddl = generate_create_table("logs", cols)
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS logs")
    assert "id SERIAL PRIMARY KEY" in ddl
    assert "msg TEXT" in ddl
    assert "_received_at TIMESTAMPTZ" in ddl
    assert "_updated_at TIMESTAMPTZ" in ddl


def test_multiple_columns():
    cols = [
        {"column_name": "severity", "data_type": "text"},
        {"column_name": "ts", "data_type": "timestamptz"},
        {"column_name": "count", "data_type": "integer"},
    ]
    ddl = generate_create_table("events", cols)
    assert "severity TEXT" in ddl
    assert "ts TIMESTAMPTZ" in ddl
    assert "count INTEGER" in ddl


def test_unknown_type_becomes_text():
    cols = [{"column_name": "payload", "data_type": "BLOB"}]
    ddl = generate_create_table("raw", cols)
    assert "payload TEXT" in ddl


def test_system_columns_skipped():
    """Columns starting with _ must not be double-inserted."""
    cols = [
        {"column_name": "_updated_at", "data_type": "timestamptz"},
        {"column_name": "body", "data_type": "jsonb"},
    ]
    ddl = generate_create_table("test_tbl", cols)
    # Only one _updated_at (the injected one)
    assert ddl.count("_updated_at") == 1
    assert "body JSONB" in ddl


def test_empty_columns_still_valid():
    ddl = generate_create_table("empty_tbl", [])
    assert "id SERIAL PRIMARY KEY" in ddl
    assert "_received_at" in ddl


# ---------------------------------------------------------------------------
# extract_value
# ---------------------------------------------------------------------------


def test_extract_top_level():
    payload = {"severity": "ERROR", "body": "hello"}
    assert extract_value(payload, "severity") == "ERROR"


def test_extract_nested():
    payload = {"resource": {"service": {"name": "my-svc"}}}
    assert extract_value(payload, "resource.service.name") == "my-svc"


def test_extract_list_index():
    payload = {"items": ["a", "b", "c"]}
    assert extract_value(payload, "items.1") == "b"


def test_extract_missing_returns_none():
    payload = {"foo": "bar"}
    assert extract_value(payload, "foo.bar.baz") is None


def test_extract_none_path():
    payload = {"x": 1}
    assert extract_value(payload, None) is None


def test_extract_empty_path():
    payload = {"x": 1}
    assert extract_value(payload, "") is None


def test_extract_deeply_nested():
    payload = {
        "resourceLogs": [
            {"resource": {"attributes": [{"key": "host", "value": "srv1"}]}}
        ]
    }
    assert extract_value(payload, "resourceLogs.0.resource.attributes.0.key") == "host"
