# Copyright (c) 2026 Kenneth Stott
# Canary: ab9f62c9-bf7c-4eb6-8bfc-7ce997c9985c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regression guard: physical-store/remote-service client libraries must stay behind
the federation, executor, or dedicated source-module abstractions — never imported
directly by callers. See REQ-012 audit plan (abstraction-leak sweep).

Two allowlists per tracked library:
  * OWNERS — the module(s) allowed to import the driver directly, because they *are*
    the abstraction boundary for that physical store.
  * ACCEPTED_OUT_OF_SCOPE — files that import a driver for a self-contained concern
    that is not "federated querying" (CDC signal consumption, subscription/live
    delivery, caching, ACL, health checks, Provisa's own served protocol) — there is
    no second implementation to abstract behind, so routing them through the source
    owner would be a fake abstraction. Each entry is commented with why.
"""

from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

PROVISA_ROOT = Path(__file__).resolve().parents[2] / "provisa"

# library import root -> files allowed to import it directly (the abstraction owners)
OWNERS: dict[str, set[str]] = {
    "duckdb": {
        "provisa/federation/duckdb_extensions.py",
        "provisa/federation/duckdb_backend.py",
        "provisa/federation/duckdb_runtime.py",
        "provisa/federation/backend.py",
        "provisa/executor/drivers/duckdb_driver.py",
    },
    "trino": {
        "provisa/federation/trino_lifecycle.py",
        "provisa/federation/backend.py",
        "provisa/executor/trino.py",
        "provisa/executor/trino_write.py",
    },
    "clickhouse_connect": {
        "provisa/federation/clickhouse_runtime.py",
        "provisa/executor/drivers/clickhouse.py",
    },
    "clickhouse_driver": {
        "provisa/federation/clickhouse_runtime.py",
    },
    "snowflake.connector": {
        "provisa/federation/snowflake_runtime.py",
        "provisa/executor/drivers/snowflake.py",
    },
    "snowflake.sqlalchemy": set(),
    "databricks.sql": set(),
    "google.cloud.bigquery": set(),
    "asyncpg": {
        "provisa/executor/drivers/postgresql.py",
        "provisa/federation/connector_postgres.py",
        # self-connects to Provisa's own pgwire endpoint for the LAND-via-SELECT
        # flow — not a leak into an external physical store.
        "provisa/federation/pgwire_replica.py",
    },
    "psycopg": {
        "provisa/federation/pg_runtime.py",
        "provisa/federation/pg_backend.py",
    },
    "psycopg2": {
        "provisa/federation/pg_runtime.py",
        "provisa/federation/pg_backend.py",
        "provisa/core/trino_system_catalogs.py",
    },
    "sqlite3": {
        "provisa/federation/connector_sqlite.py",
        # snapshots the control-plane file with sqlite3's own online-backup API before DuckDB
        # attaches it — reading Provisa's own store, not federating an external one.
        "provisa/federation/duckdb_runtime.py",
    },
    "aiosqlite": set(),
    "motor": set(),
    "pymongo": set(),
    "redis": {
        "provisa/cache/store.py",  # caching feature, not a federated source — see Context
        "provisa/api/admin/system_health.py",  # health-check ping, not a federated source
    },
    "grpc": {
        "provisa/grpc_remote/executor.py",
        "provisa/source_adapters/grpc_remote_adapter.py",
        "provisa/executor/function_dispatch.py",
        # dynamic gRPC reflection-based invocation/introspection for API-source
        # registration — a distinct concern from the structured pb2 execution path
        # above; both are legitimate owners of raw grpc usage, not duplicates.
        "provisa/api_source/caller.py",
        "provisa/api_source/introspect.py",
        # Provisa's own served gRPC protocol (not a remote-source client) — same
        # "listening" pattern as bolt/pgwire/flight/MCP — plus its internal client.
        "provisa/grpc/reflection.py",
        "provisa/grpc/server.py",
        "provisa/grpc/auth.py",  # auth interceptors for the served protocol, same owner family
        "provisa/security/mtls.py",  # builds the served protocol's mTLS server credentials
        "provisa/auth/approval_pb2_grpc.py",
        "provisa/auth/approval_hook.py",
    },
    "confluent_kafka": {
        "provisa/kafka/sink.py",  # delivery, not federated querying — see Context
        "provisa/kafka/sink_executor.py",  # re-executes + delivers to a sink, same role
        "provisa/kafka/change_events.py",  # change-event publisher, same delivery role
    },
    "aiokafka": {
        "provisa/kafka/source.py",  # kafka as federated source, per owner map
    },
    "kafka": set(),
    "aiomysql": {
        "provisa/federation/connector_mysql.py",
        "provisa/executor/drivers/mysql.py",
    },
    "aioodbc": {
        "provisa/federation/connector_mssql.py",
        "provisa/executor/drivers/sqlserver.py",
    },
}

# files that import a tracked driver for a concern outside the "federated querying"
# abstraction boundary entirely — accepted, not owners, still must be declared
ACCEPTED_OUT_OF_SCOPE: dict[str, str] = {
    "provisa/subscriptions/pg_provider.py": (
        "CDC-signal consumption (asyncpg) — self-contained, not federated querying"
    ),
    "provisa/subscriptions/kafka_provider.py": (
        "CDC-signal consumption (aiokafka) — self-contained, not federated querying"
    ),
    "provisa/subscriptions/mongo_provider.py": (
        "CDC-signal consumption (motor) — self-contained, not federated querying"
    ),
    "provisa/subscriptions/debezium_provider.py": (
        "CDC-signal consumption (aiokafka/confluent_kafka) — self-contained, not federated"
        " querying"
    ),
    "provisa/subscriptions/trino_polling_provider.py": (
        "CDC-signal consumption (trino) — self-contained, not federated querying"
    ),
    "provisa/live/outputs/kafka.py": (
        "subscription/live-output delivery (confluent_kafka) — self-contained, not federated"
        " querying"
    ),
    "provisa/core/redis_factory.py": "redis-as-cache connection factory, not a federated source",
    "provisa/core/org_provisioning.py": "redis ACL provisioning, not a federated source",
}


def _iter_py_files():
    for path in PROVISA_ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        rel = path.relative_to(PROVISA_ROOT.parent).as_posix()
        yield rel, path


def _is_type_checking_guard(test: ast.expr) -> bool:
    if isinstance(test, ast.Name):
        return test.id == "TYPE_CHECKING"
    if isinstance(test, ast.Attribute):
        return test.attr == "TYPE_CHECKING"
    return False


def _imported_top_level_libs(tree: ast.Module) -> set[str]:
    """Collect every imported library anywhere in the file (module-level, function-local,
    inside try/except — lazy-import is an established, deliberate pattern here), except
    inside ``if TYPE_CHECKING:`` blocks, which are type-only and never execute."""
    found: set[str] = set()

    def visit(node: ast.AST) -> None:
        if isinstance(node, ast.If) and _is_type_checking_guard(node.test):
            for child in node.orelse:
                visit(child)
            return
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                found.add(node.module)
        for child in ast.iter_child_nodes(node):
            visit(child)

    visit(tree)
    return found


def _matches_lib(imported: str, lib: str) -> bool:
    return imported == lib or imported.startswith(lib + ".")


def test_no_direct_driver_imports_outside_owners():
    violations = []
    for rel, path in _iter_py_files():
        if rel in ACCEPTED_OUT_OF_SCOPE:
            continue
        tree = ast.parse(path.read_text(), filename=rel)
        imported = _imported_top_level_libs(tree)
        for lib, owners in OWNERS.items():
            if rel in owners:
                continue
            if any(_matches_lib(name, lib) for name in imported):
                violations.append(f"{rel} imports {lib!r} directly (not an owner)")
    assert not violations, "Abstraction leak(s):\n" + "\n".join(violations)


def test_accepted_out_of_scope_files_still_exist():
    missing = [rel for rel in ACCEPTED_OUT_OF_SCOPE if not (PROVISA_ROOT.parent / rel).exists()]
    assert not missing, f"Accepted-out-of-scope entries reference missing files: {missing}"


def test_owner_files_still_exist():
    missing = []
    for owners in OWNERS.values():
        for rel in owners:
            if not (PROVISA_ROOT.parent / rel).exists():
                missing.append(rel)
    assert not missing, f"Owner allowlist references missing files: {missing}"


# ---------------------------------------------------------------------------
# Behavioral check: isolated_sync() yields an EngineSession, never a raw driver
# connection.
# ---------------------------------------------------------------------------


class _FakeRuntime:
    def __init__(self):
        self.connection = MagicMock(name="raw_driver_connection")

    def ensure_materialize_attached(self):
        return "materialize"


def test_native_backend_isolated_sync_yields_engine_session_not_raw_connection():
    from provisa.federation.native_backend import NativeEngineBackend
    from provisa.executor.session import EngineSession

    class _FakeNativeBackend(NativeEngineBackend):
        def __init__(self):  # avoids real engine wiring
            self._runtime = _FakeRuntime()

        def _runtime_for(self, state):
            return self._runtime

    backend = _FakeNativeBackend()
    with backend.isolated_sync(SimpleNamespace()) as session:
        assert isinstance(session, EngineSession)
        assert not isinstance(session, MagicMock)
