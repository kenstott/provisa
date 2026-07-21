# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The materialization-store locality classifier (REQ-879).

Consistency across a multi-instance deployment follows the STORE, not the engine: a local-file store
(embedded DuckDB/SQLite) is instance-local — each instance keeps its own copy and they diverge behind
a load balancer; a remote/shared store (Postgres, object-store, warehouse) is one shared copy. The
classifier keys on the RESOLVED store DSN, so it reflects the Settings ``store_url`` override too.
"""

from __future__ import annotations

import pytest

from provisa.api.admin.schema_query import _is_instance_local_store


@pytest.mark.parametrize(
    "dsn",
    [
        "duckdb:///Users/x/.provisa/materialize.duckdb",
        "duckdb:///:memory:",
        "sqlite:///./store.db",
        "sqlite+aiosqlite:///tmp/store.db",  # driver suffix is stripped
        "DuckDB:///CASE.db",  # scheme is case-insensitive
    ],
)
def test_local_file_stores_are_instance_local(dsn: str) -> None:
    assert _is_instance_local_store(dsn) is True


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://host/db",
        "postgres://user:pw@shared-host:5432/prov",
        "postgresql+asyncpg://host/db",
        "s3://bucket/warehouse",  # object-store lakehouse
        "snowflake://acct/db",
        "bigquery://project/ds",
    ],
)
def test_remote_and_shared_stores_are_not_instance_local(dsn: str) -> None:
    assert _is_instance_local_store(dsn) is False


def test_unconfigured_store_is_not_instance_local() -> None:
    # No store yet (None) → not local (nothing to diverge); the UI shows no warning.
    assert _is_instance_local_store(None) is False
    assert _is_instance_local_store("") is False
