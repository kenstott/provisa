# Copyright (c) 2026 Kenneth Stott
# Canary: 8a4c1f07-2d9e-4b63-a17e-5c0b3d9f2e84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The "Load Management and Timeliness" source settings through the admin source input (REQ-1909):
each is carried by SourceInput, read back on SourceType, persisted by the source upsert, and
validated at write time with the config loader's rules."""

# Requirements: REQ-1909, REQ-1141, REQ-1148, REQ-860, REQ-826

from __future__ import annotations

import pytest
from pydantic import ValidationError

from provisa.api.admin._row_mappers import _source_from_row
from provisa.api.admin.schema_mutation import _validate_source_load_management
from provisa.api.admin.types import SourceInput
from provisa.core.models import Source, SourceType
from provisa.core.repositories.source import _source_values, source_from_row


def _input(**kw) -> SourceInput:
    return SourceInput(id="orders_pg", type="postgresql", **kw)


def test_valid_settings_pass():
    assert (
        _validate_source_load_management(
            _input(
                cache_ttl=60,
                change_signal="ttl_probe",
                sentinel_path="https://example.com/marker",
                freshness_gate=True,
                replicate=None,
                max_live_concurrency=3,
                load_protected=True,
                off_peak_window="01:00-05:00",
                off_peak_tz="UTC",
            )
        )
        is None
    )


@pytest.mark.parametrize(
    ("kw", "code"),
    [
        ({"max_live_concurrency": 0}, "schema.max_live_concurrency_invalid"),
        ({"cache_ttl": -2}, "schema.invalid_cache_ttl"),  # -1 is legal: never expires
        ({"change_signal": "sometimes"}, "schema.invalid_change_signal"),
        ({"sentinel_path": "gopher://host/marker"}, "schema.invalid_sentinel_path"),
        ({"freshness_gate": True, "change_signal": "ttl"}, "schema.invalid_freshness_gate"),
        ({"off_peak_window": "25:00-26:00"}, "schema.invalid_off_peak_window"),
        ({"load_protected": True}, "schema.load_protection_gate_required"),
    ],
)
def test_each_rule_refuses_with_its_code(kw, code):
    result = _validate_source_load_management(_input(**kw))
    assert result is not None and result.success is False
    assert result.code == code
    assert "orders_pg" in result.message or code == "schema.invalid_off_peak_window"


def test_model_rejects_a_zero_cap():
    with pytest.raises(ValidationError):
        Source(id="s", type=SourceType.postgresql, max_live_concurrency=0)


def test_every_panel_field_round_trips_through_the_row():
    src = Source(
        id="orders_pg",
        type=SourceType.postgresql,
        cache_enabled=False,
        cache_ttl=90,
        change_signal="probe",
        sentinel_path="file:///tmp/marker",
        freshness_gate=True,
        replicate=0,
        load_protected=True,
        off_peak_window="02:00-04:00",
        off_peak_tz="America/New_York",
        max_live_concurrency=4,
    )
    # REQ-1921: withdrawn is a stored column of its own, set by withdrawSource alone.
    row = {**_source_values(src), "allowed_domains": [], "binding": "own", "withdrawn": False}
    back = source_from_row(row)
    # The panel's fields are not connection details: they reach a caller with or without
    # source_registration (connection=False nulls only what locates or authenticates).
    for connection in (True, False):
        admin = _source_from_row(row, connection=connection)
        for field in _PANEL_FIELDS:
            assert getattr(admin, field) == getattr(src, field), (field, connection)
    for field in _PANEL_FIELDS:
        assert getattr(back, field) == getattr(src, field), field


_PANEL_FIELDS = (
    "cache_enabled",
    "cache_ttl",
    "change_signal",
    "sentinel_path",
    "freshness_gate",
    "replicate",
    "load_protected",
    "off_peak_window",
    "off_peak_tz",
    "max_live_concurrency",
)


# --- a sentinel path only where a source can have one (REQ-1148) ---


def test_a_sentinel_applies_where_the_operators_producer_can_mark_a_probed_source():
    from provisa.events.sentinel_probe import applies_to

    for kind in ("postgresql", "openapi", "files", "csv", "mongodb"):
        assert applies_to(kind) is True, kind
    # Not probed at all: rows arrive by push, or are written where they are read.
    for kind in ("kafka", "websocket", "ingest", "data_profiler"):
        assert applies_to(kind) is False, kind
    # A mailbox's data is produced inside the provider's service: nothing of the operator's
    # could drop a marker.
    for kind in ("microsoft_365", "google_workspace"):
        assert applies_to(kind) is False, kind


@pytest.mark.parametrize(
    ("kind", "signal"), [("microsoft_365", "ttl"), ("google_workspace", "ttl"), ("kafka", "kafka")]
)
def test_a_sentinel_path_on_a_kind_that_cannot_have_one_is_refused_by_name(kind, signal):
    refused = _validate_source_load_management(
        SourceInput(
            id="s", type=kind, change_signal=signal, sentinel_path="https://example.com/marker"
        )
    )
    assert refused is not None and refused.success is False
    assert refused.code == "schema.sentinel_path_not_applicable"
    assert refused.params == {"source": "s", "type": kind}
    # Without one the same source is not refused for it.
    assert (
        _validate_source_load_management(SourceInput(id="s", type=kind, change_signal=signal))
        is None
    )


async def test_the_form_is_told_whether_a_kind_can_have_a_sentinel(monkeypatch):
    import provisa.api.app as appmod
    from provisa.api.admin import source_kinds_router as kinds
    from provisa.federation.engine import build_engine

    monkeypatch.setattr(kinds, "require_capability_request", lambda request, capability: None)
    monkeypatch.setattr(appmod.state, "federation_engine", build_engine("duckdb"), raising=False)
    mail = await kinds.source_kind(None, "microsoft_365")
    assert (mail["cache_ttl_required"], mail["sentinel_path"]) == (True, False)
    database = await kinds.source_kind(None, "postgresql")
    assert (database["cache_ttl_required"], database["sentinel_path"]) == (False, True)


# --- a probing signal on a source whose kind cannot yet be checked for changes ---


@pytest.mark.parametrize("kind", ["microsoft_365", "google_workspace"])
@pytest.mark.parametrize("signal", ["probe", "ttl_probe"])
def test_a_probing_signal_on_a_mail_source_is_taken_because_its_kind_can_be_checked(
    kind, signal, monkeypatch
):
    """Both mail kinds have their check for changes wired (probes.LOADER_PROBE_TYPES): Gmail's
    history id, Graph's delta links. A kind without one is still refused by name."""
    from provisa.events import probes

    assert kind in probes.LOADER_PROBE_TYPES
    asked = SourceInput(id="mail", type=kind, change_signal=signal, cache_ttl=300)
    assert _validate_source_load_management(asked) is None
    assert (
        _validate_source_load_management(
            SourceInput(id="mail", type=kind, change_signal="ttl", cache_ttl=300)
        )
        is None
    )
    monkeypatch.setattr(probes, "LOADER_PROBE_TYPES", frozenset())
    refused = _validate_source_load_management(asked)
    assert refused is not None and refused.code == "schema.change_signal_not_honoured"
    assert refused.params == {"source": "mail", "type": kind, "signal": signal}


def test_a_probing_signal_on_another_kind_of_source_is_as_it_was():
    # Its tables carry their own probe query; the source's signal is what they inherit.
    assert (
        _validate_source_load_management(
            SourceInput(id="pg", type="postgresql", change_signal="ttl_probe", cache_ttl=60)
        )
        is None
    )


def test_a_native_signal_on_a_source_nothing_receives_is_refused_by_name():
    refused = _validate_source_load_management(
        SourceInput(id="shop_db", type="mysql", change_signal="native", cache_ttl=300)
    )
    assert refused is not None and refused.code == "schema.change_signal_no_receiver"
    assert refused.params == {"source": "shop_db", "type": "mysql", "signal": "native"}
    assert (
        _validate_source_load_management(
            SourceInput(id="orders", type="mongodb", change_signal="native", cache_ttl=300)
        )
        is None
    )
