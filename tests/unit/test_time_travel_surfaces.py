# Copyright (c) 2026 Kenneth Stott
# Canary: fd234cfe-5594-4552-8557-210684f6db97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-2109: as_of and as_recorded mean the same on every surface that takes them. Each surface
turns its own inputs into the one canonical value -- the HTTP headers (GraphQL, /data/sql), the
Flight ticket's options, the gRPC and MCP resolve arguments, a pgwire session's SET -- and the one
pipeline reads a versioned table under it: as_of filters valid time, as_recorded what was known.
The governed statement is run against real rows (DuckDB) so the answer is checked, not its text."""

# Requirements: REQ-2109, REQ-2110

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import duckdb
import pytest

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.federation.replica_address import ReplicaRoutes
from tests.role_state import RoleState

pytestmark = pytest.mark.asyncio

_COLUMNS = ["row_id", "payment_id", "business_date", "updated_at", "amount"]
_PAYMENTS = TableMeta(
    table_id=1,
    field_name="payments",
    type_name="Payments",
    source_id="pg",
    catalog_name="pg",
    schema_name="fin",
    table_name="payments",
    domain_id="fin",
)
_TYPES = {
    "row_id": "integer",
    "payment_id": "varchar",
    "business_date": "date",
    "updated_at": "timestamp",
    "amount": "integer",
}


def _state() -> RoleState:
    ctx = CompilationContext()
    ctx.tables = {"payments": _PAYMENTS}
    return RoleState(
        admin_db=None,
        contexts={"analyst": ctx},
        rls_contexts={"analyst": RLSContext.empty()},
        roles={"analyst": {"id": "analyst", "capabilities": [], "domain_access": ["*"]}},
        masking_rules={},
        tables=[
            {
                "id": 1,
                "source_id": "pg",
                "schema_name": "fin",
                "table_name": "payments",
                "domain_id": "fin",
                "view_sql": None,
                # a correction is a new row with the same payment_id: the key is not the PK
                "business_key": ["payment_id"],
                "valid_time": "business_date",
                "recorded_time": "updated_at",
                "columns": [
                    {
                        "column_name": c,
                        "data_type": _TYPES[c],
                        "visible_to": ["analyst"],
                        "is_primary_key": c == "row_id",
                    }
                    for c in _COLUMNS
                ],
            }
        ],
        source_types={"pg": "postgresql"},
        source_catalogs={},
        relationships=[],
        metrics={},
        federation_engine=None,
        view_sql_map=None,
        security_high=False,
        schema_boot_id="test-boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        routing_cache=CompiledQueryCache(),
        replica_routes=ReplicaRoutes(),
        model_stamp=1,
    )


class _Routed(Exception):
    def __init__(self, governed_semantic: str) -> None:
        self.governed_semantic = governed_semantic


async def _governed(monkeypatch, as_of: str | None) -> str:
    """The statement the one pipeline routes for the analyst's read, under ``as_of``."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", _state(), raising=False)

    async def _routing(_physical, governed_semantic, *_args, **_kwargs):
        raise _Routed(governed_semantic)

    sql = "SELECT payment_id, SUM(amount) AS total FROM fin.payments GROUP BY payment_id"
    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_routing)):
        with pytest.raises(_Routed) as routed:
            await _pipeline._govern_and_route(sql, "analyst", as_of=as_of)
    return routed.value.governed_semantic


def _http(as_of, as_recorded):
    from provisa.api.data.endpoint_helpers import request_time_travel

    return request_time_travel(as_of, as_recorded)


def _flight(as_of, as_recorded):
    from provisa.api.flight.server import _ticket_as_of

    options = {k: v for k, v in (("as_of", as_of), ("as_recorded", as_recorded)) if v}
    return _ticket_as_of(options)


def _resolve(as_of, as_recorded):
    # the gRPC and MCP resolve arguments are validated as resolve() validates them
    from provisa.mv.bitemporal import request_as_of

    return request_as_of(as_of, as_recorded)


def _pgwire(as_of, as_recorded):
    from provisa.pgwire.server import ProvisaSession

    session = ProvisaSession()
    if as_of:
        assert session._set_time_travel(f"SET provisa.as_of = '{as_of}'")
    if as_recorded:
        assert session._set_time_travel(f"SET provisa.as_recorded TO '{as_recorded}'")
    return session.time_travel()


_SURFACES = {"http": _http, "flight": _flight, "resolve": _resolve, "pgwire": _pgwire}

# A payment of 100 for 3 Oct, corrected to 120 on 5 Oct; one of 50 for 4 Oct, recorded on 4 Oct.
_ROWS = [
    (1, "p1", "2026-10-03", "2026-10-03 18:00:00", 100),
    (2, "p1", "2026-10-03", "2026-10-05 09:00:00", 120),
    (3, "p2", "2026-10-04", "2026-10-04 18:00:00", 50),
]


@pytest.fixture
def ledger():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA fin")
    con.execute(
        "CREATE TABLE fin.payments (row_id INT, payment_id TEXT, business_date DATE, "
        "updated_at TIMESTAMP, amount INT)"
    )
    con.executemany("INSERT INTO fin.payments VALUES (?, ?, ?, ?, ?)", _ROWS)
    yield con
    con.close()


@pytest.mark.parametrize(
    ("as_of", "as_recorded", "answer"),
    [
        (None, None, {"p1": 120, "p2": 50}),  # current: the latest version of each payment
        ("2026-10-03", None, {"p1": 120}),  # valid time up to 3 Oct, as known now
        ("2026-10-03", "2026-10-04T23:00:00", {"p1": 100}),  # as reported on 4 Oct
        (None, "2026-10-04T23:00:00", {"p1": 100, "p2": 50}),
    ],
)
@pytest.mark.parametrize("surface", sorted(_SURFACES))
async def test_every_surface_reads_a_versioned_table_with_the_same_meaning(
    monkeypatch, ledger, surface, as_of, as_recorded, answer
):
    travel = _SURFACES[surface](as_of, as_recorded)
    assert travel == _http(as_of, as_recorded)  # one canonical value, whichever surface
    governed = await _governed(monkeypatch, travel)
    assert dict(ledger.execute(governed).fetchall()) == answer
