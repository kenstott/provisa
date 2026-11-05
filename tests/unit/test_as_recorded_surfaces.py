# Copyright (c) 2026 Kenneth Stott
# Canary: 777c4b52-e390-4cf0-814d-2b00183e4b8f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""As recorded (REQ-1163) wherever as of is taken: a relation's arguments, the Flight ticket's
option, the gRPC resolve message; each value validated by name; and the audit row records both."""

# Requirements: REQ-1163

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone

import pytest

from provisa.audit.writer import AuditRecord
from provisa.encryption import NullEncryption
from provisa.mv.bitemporal import AsOf, request_as_of

pytestmark = pytest.mark.unit


def _lifted(sql: str) -> str | None:
    """The time a statement's one argument-bearing reference says (provisa.mv.time_travel)."""
    from provisa.mv.time_travel import lift

    _, travel = lift(sql, lambda _table: "bitemporal")
    return travel.at("p")


def test_a_relations_arguments_travel_as_one_value():
    key = _lifted(
        "SELECT * FROM fin.payments(as_of => '2026-09-30T12:00:00', "
        "as_recorded => '2026-10-02T00:00:00') p"
    )
    assert key == request_as_of("2026-09-30T12:00:00", "2026-10-02T00:00:00")
    assert AsOf.from_key(key).recorded_sql == "TIMESTAMP '2026-10-02 00:00:00.000000'"


def test_a_reference_without_arguments_is_no_time_travel():
    assert _lifted("SELECT * FROM fin.payments p") is None


@pytest.mark.parametrize(
    ("args", "named"),
    [
        ("as_of => 'yesterday'", "invalid as_of"),
        ("as_recorded => 'yesterday'", "invalid as_recorded"),
        ("as_recorded => '2026-10-01/2026-10-05'", "invalid as_recorded"),
    ],
)
def test_a_malformed_value_is_refused_naming_its_argument(args, named):
    with pytest.raises(ValueError, match=f"fin.payments: {named}"):
        _lifted(f"SELECT * FROM fin.payments({args}) p")


def test_a_flight_ticket_takes_as_recorded_as_a_relation_does():
    from provisa.api.flight.server import _ticket_as_of

    assert _ticket_as_of({"as_of": "2026-09-30T12:00:00", "as_recorded": "2026-10-02"}) == (
        request_as_of("2026-09-30T12:00:00", "2026-10-02")
    )
    assert _ticket_as_of({"as_recorded": "2026-10-02"}) == request_as_of(None, "2026-10-02")
    assert _ticket_as_of({}) is None


def test_a_flight_ticket_refuses_a_malformed_as_recorded_by_name():
    from provisa.api.flight.server import _ticket_as_of

    with pytest.raises(Exception, match="as_recorded"):
        _ticket_as_of({"as_recorded": "2026-10-01/2026-10-05"})


def _record(time_travel: str | None) -> AuditRecord:
    return AuditRecord(
        record_db=object(),
        tenant_id="default",
        user_id="alice",
        role_id="regulator",
        query_text="SELECT 1",
        table_ids=(),
        source="http",
        status_code=200,
        duration_ms=1,
        logged_at=datetime.now(timezone.utc),
        trace_id=None,
        encryption=NullEncryption(),
        model_stamp=1,
        model_env="prod",
        enforced={},
        route_reason=None,
        sources=(),
        data_age=None,
        region="default",
        time_travel=time_travel,
        tables=[],
    )


def test_the_audit_row_records_both_times():
    row = _record(request_as_of("2026-09-30T12:00:00", "2026-10-02T00:00:00")).row(None)
    assert row["as_of"] == "2026-09-30T12:00:00.000000"
    assert row["as_recorded"] == "2026-10-02T00:00:00.000000"


def test_the_audit_row_records_a_range_and_a_read_of_now():
    ranged = _record(request_as_of("2026-09-01/2026-10-01", None)).row(None)
    assert ranged["as_of"] == "2026-09-01T00:00:00.000000/2026-10-01T00:00:00.000000"
    assert ranged["as_recorded"] is None
    now = dataclasses.replace(_record(None)).row(None)
    assert (now["as_of"], now["as_recorded"]) == (None, None)
