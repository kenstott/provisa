# Copyright (c) 2025 Kenneth Stott
# Canary: 005f3b2c-8591-4e61-b5e7-8bb3d49c041f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for API source cache (Phase U)."""

import pytest

from provisa.api_source.cache import (
    _params_hash,
    generate_cache_table_ddl,
    resolve_ttl,
)
from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint


def _make_endpoint(**kwargs) -> ApiEndpoint:
    defaults = {
        "id": 1,
        "source_id": "test-api",
        "path": "/users",
        "method": "GET",
        "table_name": "users",
        "columns": [
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(name="name", type=ApiColumnType.string),
            ApiColumn(name="data", type=ApiColumnType.jsonb),
        ],
        "ttl": 300,
    }
    defaults.update(kwargs)
    return ApiEndpoint(**defaults)


# --- Cache key generation ---

def test_params_hash_deterministic():
    """Same inputs produce the same hash."""
    h1 = _params_hash(1, {"a": 1, "b": 2})
    h2 = _params_hash(1, {"b": 2, "a": 1})
    assert h1 == h2


def test_params_hash_different_endpoint():
    """Different endpoint IDs produce different hashes."""
    h1 = _params_hash(1, {"a": 1})
    h2 = _params_hash(2, {"a": 1})
    assert h1 != h2


def test_params_hash_different_params():
    """Different params produce different hashes."""
    h1 = _params_hash(1, {"a": 1})
    h2 = _params_hash(1, {"a": 2})
    assert h1 != h2


def test_params_hash_empty():
    """Empty params produce a valid hash."""
    h = _params_hash(1, {})
    assert isinstance(h, str)
    assert len(h) == 64  # SHA-256 hex


# --- TTL resolution ---

def test_ttl_endpoint_wins():
    """Endpoint TTL takes priority."""
    assert resolve_ttl(60, 120, 300) == 60


def test_ttl_source_fallback():
    """Source TTL used when endpoint TTL is None."""
    assert resolve_ttl(None, 120, 300) == 120


def test_ttl_global_fallback():
    """Global TTL used when both endpoint and source are None."""
    assert resolve_ttl(None, None, 300) == 300


def test_ttl_default():
    """Default 300s when all are None."""
    assert resolve_ttl(None, None, None) == 300


# --- Cache table DDL ---

def test_generate_cache_table_ddl():
    """DDL includes all columns with correct PG types."""
    endpoint = _make_endpoint()
    ddl = generate_cache_table_ddl(endpoint)

    assert "api_cache_users" in ddl
    assert "_cache_id SERIAL PRIMARY KEY" in ddl
    assert "_endpoint_id INTEGER NOT NULL" in ddl
    assert "_params_hash TEXT NOT NULL" in ddl
    assert "_cached_at TIMESTAMPTZ" in ddl
    assert "id BIGINT" in ddl
    assert "name TEXT" in ddl
    assert "data JSONB" in ddl


def test_generate_cache_table_ddl_boolean():
    """Boolean columns get BOOLEAN type."""
    endpoint = _make_endpoint(columns=[
        ApiColumn(name="active", type=ApiColumnType.boolean),
    ])
    ddl = generate_cache_table_ddl(endpoint)
    assert "active BOOLEAN" in ddl


def test_generate_cache_table_ddl_number():
    """Number columns get DOUBLE PRECISION type."""
    endpoint = _make_endpoint(columns=[
        ApiColumn(name="score", type=ApiColumnType.number),
    ])
    ddl = generate_cache_table_ddl(endpoint)
    assert "score DOUBLE PRECISION" in ddl
