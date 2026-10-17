# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""As recorded (REQ-1163) wherever as of is taken: each HTTP surface's header, the Flight ticket's
option, the gRPC resolve message; each value validated by name; and the audit row records both."""

# Requirements: REQ-1163

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone

import pytest

from provisa.api.data.endpoint_helpers import request_time_travel
from provisa.api.errors import ApiError
from provisa.audit.writer import AuditRecord
from provisa.encryption import NullEncryption
from provisa.mv.bitemporal import AsOf, request_as_of

pytestmark = pytest.mark.unit


def test_the_headers_travel_as_one_value():
    key = request_time_travel("2026-09-30T12:00:00", "2026-10-02T00:00:00")
    assert key == request_as_of("2026-09-30T12:00:00", "2026-10-02T00:00:00")
    assert AsOf.from_key(key).recorded_sql == "TIMESTAMP '2026-10-02 00:00:00.000000'"


def test_no_header_or_an_empty_one_is_no_time_travel():
    assert request_time_travel(None, None) is None
    assert request_time_travel("", "") is None


@pytest.mark.parametrize(
    ("as_of", "as_recorded", "code"),
    [
        ("yesterday", None, "data.invalid_as_of"),
        (None, "yesterday", "data.invalid_as_recorded"),
        (None, "2026-10-01/2026-10-05", "data.invalid_as_recorded"),
    ],
)
def test_a_malformed_value_is_refused_naming_its_header(as_of, as_recorded, code):
    with pytest.raises(ApiError) as refused:
        request_time_travel(as_of, as_recorded)
    assert refused.value.status_code == 400
    assert refused.value.code == code


def test_a_flight_ticket_takes_as_recorded_as_the_header_does():
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
