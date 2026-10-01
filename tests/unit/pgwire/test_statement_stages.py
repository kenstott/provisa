# Copyright (c) 2026 Kenneth Stott
# Canary: 8a5d2c71-4e9f-4b36-a1c8-3f7e0d6b9a24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pipeline's two stages and what a described statement's Execute is held to (REQ-589, amended
2026-10-01): a governed statement is routed only if the pipeline governed it under the live schema
generation; a described statement's rows are encoded to the described shape; per-statement lookups
read the in-memory registry; and the pgwire server installs no logging of its own."""

# Requirements: REQ-589, REQ-1865, REQ-1882

from __future__ import annotations

import asyncio
import datetime
import logging
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from provisa.executor.result import QueryResult as EngineResult


def _governed(stamp: str, generation=("boot", 3)):
    from provisa.pgwire._pipeline import _Governed

    return _Governed(
        sql="SELECT 1",
        role_id="r",
        role=None,
        ctx=None,
        gov_ctx=None,
        comment_params=None,
        parsed=None,
        metric_semantic_sql=None,
        table_ids=(),
        governed_semantic="SELECT 1",
        schema_generation=generation,
        stamp=stamp,
    )


def test_a_held_statement_is_current_only_under_the_generation_it_was_governed_in():
    from provisa.pgwire._pipeline import _mint_stamp, governed_statement_is_current

    state = SimpleNamespace(schema_boot_id="boot", schema_version=3)
    held = _governed(_mint_stamp())
    assert governed_statement_is_current(held, state)
    state.schema_version = 4  # a rebuild since the Describe
    assert not governed_statement_is_current(held, state)
    assert not governed_statement_is_current(
        _governed("forged"), SimpleNamespace(schema_boot_id="boot", schema_version=3)
    )


def test_route_governed_refuses_a_statement_the_pipeline_did_not_govern():
    from provisa.pgwire._pipeline import route_governed

    with patch("provisa.api.app.state", MagicMock()):
        with pytest.raises(PermissionError, match="ungoverned statement"):
            asyncio.run(route_governed(_governed("forged")))


def test_pk_bounds_resolve_from_the_in_memory_registry_without_a_control_plane_read():
    """REQ-1865/REQ-1882: this runs for every governed statement, so it must not await the
    registry view (a control-plane fetch) — the registered tables are already in memory."""
    from provisa.pgwire._pipeline import _resolve_pk_bounds

    engine = MagicMock()
    state = SimpleNamespace(
        federation_engine=engine,
        source_types={"neo": "neo4j", "pg": "postgresql"},
        tables=[
            {
                "id": 1,
                "source_id": "neo",
                "schema_name": "graph",
                "table_name": "customer_node",
                "alias": "Customer",
                "row_materialize": True,
                "columns": [
                    {
                        "column_name": "customer_id",
                        "data_type": "integer",
                        "is_primary_key": True,
                        "native_filter_type": None,
                    },
                    {
                        "column_name": "name",
                        "data_type": "varchar",
                        "is_primary_key": False,
                        "native_filter_type": None,
                    },
                ],
            },
            {
                "id": 2,
                "source_id": "pg",
                "schema_name": "public",
                "table_name": "orders",
                "row_materialize": False,
                "columns": [],
            },
        ],
    )

    async def _never(*_a, **_k):
        raise AssertionError("the per-statement path read the control plane")

    with (
        patch("provisa.federation.registry_view.registered_tables", _never),
        patch("provisa.federation.query_residency.row_materialized_tables_by_name", _never),
        patch("provisa.federation.strategy.engine_attaches", lambda eng, source_type: False),
    ):
        bounds = asyncio.run(
            _resolve_pk_bounds("SELECT name FROM customer_node WHERE customer_id = $1", state, [42])
        )
        assert [(b.table_name, list(b.values)) for b in bounds] == [("customer_node", [(42,)])]
        # A statement touching no row_materialize table resolves nothing, still without a read.
        assert asyncio.run(_resolve_pk_bounds("SELECT * FROM orders WHERE id = 1", state)) == ()


def test_row_materialize_is_ignored_for_a_source_the_engine_attaches():
    from provisa.pgwire._pipeline import _row_materialize_tables_in_memory

    table = {
        "id": 1,
        "source_id": "mongo",
        "schema_name": "db",
        "table_name": "order_docs",
        "row_materialize": True,
        "columns": [],
    }
    state = SimpleNamespace(
        federation_engine=MagicMock(), source_types={"mongo": "mongodb"}, tables=[table]
    )
    with patch("provisa.federation.strategy.engine_attaches", lambda eng, source_type: True):
        assert _row_materialize_tables_in_memory(state) == {}


def test_a_described_statements_rows_are_encoded_to_the_described_shape():
    """Postgres averages an integer to numeric and DuckDB sums one to a wide integer; the client
    was told the described types, so the values are brought to them."""
    from buenavista.core import BVType

    from provisa.pgwire.server import ProvisaQueryResult

    result = EngineResult(
        rows=[(Decimal("2.5"), 7, datetime.date(2026, 1, 2), None)],
        column_names=["avg(x)", "count_star()", "d", "n"],
        column_types=["numeric", "HUGEINT", "date", "numeric"],
    )
    shape = [("avg", "DOUBLE PRECISION"), ("n", "BIGINT"), ("d", "TIMESTAMP"), ("z", "INT")]
    qr = ProvisaQueryResult(result, "SELECT 1", shape)
    assert [qr.column(i) for i in range(4)] == [
        ("avg", BVType.FLOAT),
        ("n", BVType.BIGINT),
        ("d", BVType.TIMESTAMP),
        ("z", BVType.INTEGER),
    ]
    (row,) = list(qr.rows())
    assert row == [2.5, 7, datetime.datetime(2026, 1, 2), None]
    assert isinstance(row[0], float)


def test_an_execution_that_contradicts_the_described_column_count_is_an_error():
    from provisa.pgwire.server import ProvisaQueryResult

    result = EngineResult(rows=[(1, 2)], column_names=["a", "b"], column_types=["int4", "int4"])
    with pytest.raises(RuntimeError, match=r"described with 1 result column.*returned 2"):
        ProvisaQueryResult(result, "SELECT 1", [("a", "INT")])


def test_starting_the_pgwire_server_installs_no_log_handler_and_changes_no_level():
    """An unconditional DEBUG file handler here made every protocol message of every connection
    format a record and take the handler's lock."""
    from provisa.pgwire.server import start_pgwire_server
    from tests.port_lease import lease_port

    loggers = [logging.getLogger("provisa.pgwire"), logging.getLogger("buenavista")]
    before = [(lg.level, list(lg.handlers)) for lg in loggers]
    server = start_pgwire_server("127.0.0.1", lease_port(), None)
    try:
        assert [(lg.level, list(lg.handlers)) for lg in loggers] == before
    finally:
        server.shutdown()
        server.server_close()
