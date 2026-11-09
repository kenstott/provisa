# Copyright (c) 2026 Kenneth Stott
# Canary: 23a9a380-a688-4b4a-9405-19e346ee3c43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2067: the provisioning demo's consumer (demo/named/provisioning/consumer.py). On a notice
addressed to it, a team pulls the dataset over Arrow Flight as its own role, as of the notice's
version time, and records the pull; a pull the server refuses, such as one outside the dataset's
window, is recorded naming why. Each team's governed view of the one dataset is the config's mask
list, read through the resolver the compiler uses."""

# Requirements: REQ-2067

from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pyarrow.flight as flight
import pytest
import yaml

from provisa.federation.scheduled_refresh import parse_off_peak_window, window_open
from provisa.security.masking import MaskType, resolve

_DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "provisioning"
_VERSION = "2026-11-09T01:00:00+00:00"


def _notice(consumer: str, notice_id: int = 1) -> SimpleNamespace:
    body = {
        "notice_id": notice_id,
        "dataset": "order_margins",
        "consumer": consumer,
        "version_time": _VERSION,
    }
    return SimpleNamespace(value=json.dumps(body).encode())


class _Broker:
    """aiokafka's consumer as consumer.py uses it: started, iterated, stopped."""

    messages: list[SimpleNamespace] = []
    instances: list[_Broker] = []

    def __init__(self, *topics: str, **options) -> None:
        self.topics, self.options, self.stopped = topics, options, False
        _Broker.instances.append(self)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        self.stopped = True

    async def __aiter__(self):
        for message in type(self).messages:
            yield message


class _Flight:
    """A Flight server: answers each ticket with ``batches``, or raises ``refusal``."""

    def __init__(self, batches=(3, 4), refusal: Exception | None = None) -> None:
        self.batches, self.refusal = batches, refusal
        self.tickets: list[dict] = []
        self.address = ""

    def __call__(self, address: str) -> "_Flight":
        self.address = address
        return self

    def do_get(self, ticket: flight.Ticket):
        self.tickets.append(json.loads(ticket.ticket))
        if self.refusal is not None:
            raise self.refusal
        return [SimpleNamespace(data=SimpleNamespace(num_rows=n)) for n in self.batches]


@pytest.fixture
def consumer(monkeypatch):
    monkeypatch.setenv("FEEDS_BOOTSTRAP", "feeds:9092")
    monkeypatch.setenv("PROVISA_FLIGHT", "grpc://provisa:8815")
    monkeypatch.setenv("CONSUMERS", "finance, data_science")
    monkeypatch.setenv("PULLS_DSN", "postgresql://warehouse/pulls")
    monkeypatch.setenv("TOPIC_SUFFIX", "_ns")
    spec = importlib.util.spec_from_file_location("provisioning_consumer", _DEMO / "consumer.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "AIOKafkaConsumer", _Broker)
    monkeypatch.setattr(_Broker, "instances", [])
    return module


def _recording(consumer, monkeypatch) -> list[tuple]:
    recorded: list[tuple] = []
    monkeypatch.setattr(
        consumer,
        "record",
        lambda notice, rows, _seconds, refused: recorded.append(
            (notice["consumer"], rows, refused)
        ),
    )
    return recorded


def test_a_pull_reads_the_dataset_as_the_role_as_of_the_version_time(consumer, monkeypatch):
    server = _Flight(batches=(3, 4, 5))
    monkeypatch.setattr(consumer.flight, "connect", server)
    rows, seconds = consumer.pull("order_margins", "data_science", _VERSION)
    assert server.address == "grpc://provisa:8815"
    assert server.tickets == [
        {
            "query": 'SELECT * FROM "provisioning"."view_order_margins"',
            "role": "data_science",
            "as_recorded": _VERSION,
        }
    ]
    assert rows == 12 and seconds >= 0


async def test_each_team_pulls_its_own_notice_and_other_messages_are_passed_over(
    consumer, monkeypatch
):
    server = _Flight()
    monkeypatch.setattr(consumer.flight, "connect", server)
    recorded = _recording(consumer, monkeypatch)
    monkeypatch.setattr(
        _Broker,
        "messages",
        [
            SimpleNamespace(value=None),  # a tombstone
            _notice("audit"),  # another team's
            _notice("finance", 2),
            _notice("data_science", 3),
            _notice("finance", 4),  # past the two asked for
        ],
    )
    await consumer.main(notices=2)
    assert [t["role"] for t in server.tickets] == ["finance", "data_science"]
    assert {t["as_recorded"] for t in server.tickets} == {_VERSION}
    assert recorded == [("finance", 7, None), ("data_science", 7, None)]
    [broker] = _Broker.instances
    assert broker.topics == ("dataset-ready_ns",) and broker.stopped
    assert broker.options["auto_offset_reset"] == "earliest"


async def test_a_refused_pull_is_recorded_naming_why_and_the_next_notice_is_still_read(
    consumer, monkeypatch
):
    refusal = flight.FlightServerError("order_margins is read 00:30-00:00 UTC\nserver detail")
    monkeypatch.setattr(consumer.flight, "connect", _Flight(refusal=refusal))
    recorded = _recording(consumer, monkeypatch)
    monkeypatch.setattr(_Broker, "messages", [_notice("finance", 2), _notice("data_science", 3)])
    await consumer.main(notices=2)
    assert recorded == [
        ("finance", None, "order_margins is read 00:30-00:00 UTC"),
        ("data_science", None, "order_margins is read 00:30-00:00 UTC"),
    ]


async def test_a_server_that_cannot_be_reached_is_not_a_refusal_and_stops_the_consumer(
    consumer, monkeypatch
):
    down = flight.FlightUnavailableError("connection refused")
    monkeypatch.setattr(consumer.flight, "connect", _Flight(refusal=down))
    recorded = _recording(consumer, monkeypatch)
    monkeypatch.setattr(_Broker, "messages", [_notice("finance", 2)])
    with pytest.raises(flight.FlightUnavailableError):
        await consumer.main(notices=1)
    assert recorded == []
    assert _Broker.instances[0].stopped


class _Warehouse:
    def __init__(self) -> None:
        self.dsn = ""
        self.statements: list[tuple[str, tuple]] = []

    def connect(self, dsn: str, autocommit: bool) -> "_Warehouse":
        assert autocommit
        self.dsn = dsn
        return self

    def __enter__(self) -> "_Warehouse":
        return self

    def __exit__(self, *_exc) -> None:
        return None

    def execute(self, sql: str, params: tuple) -> None:
        self.statements.append((sql, params))


def _body(notice_id: int = 1) -> dict:
    return json.loads(_notice("finance", notice_id).value) | {
        "notified_at": datetime(2026, 11, 9, 1, 0, 5, tzinfo=timezone.utc)
    }


def test_a_pull_is_recorded_with_its_rate(consumer, monkeypatch):
    warehouse = _Warehouse()
    monkeypatch.setattr(consumer.psycopg, "connect", warehouse.connect)
    consumer.record(_body(), 1000, 4.0, None)
    assert warehouse.dsn == "postgresql://warehouse/pulls"
    [(sql, params)] = warehouse.statements
    assert sql.startswith("INSERT INTO pulls")
    assert params[:4] == (1, "finance", "order_margins", _VERSION)
    assert params[5:7] == (1000, 4.0)
    assert params[7] == pytest.approx(250.0, rel=1e-14)
    assert params[8] is None


def test_a_refusal_is_recorded_with_no_rows_and_no_rate(consumer, monkeypatch):
    warehouse = _Warehouse()
    monkeypatch.setattr(consumer.psycopg, "connect", warehouse.connect)
    consumer.record(_body(), None, None, "outside the window")
    [(_, params)] = warehouse.statements
    assert params[5:] == (None, None, None, "outside the window")


_CONFIG = yaml.safe_load((_DEMO / "config.yaml").read_text())
_DATASET = next(v for v in _CONFIG["views"] if v["id"] == "order_margins")


@pytest.mark.parametrize("column", ["cost", "margin"])
def test_finance_reads_the_margins_and_data_science_reads_them_masked(column):
    masks = next(c for c in _DATASET["columns"] if c["name"] == column)["masks"]
    assert resolve(masks, ["finance", "developer"], "decimal") is None
    rule = resolve(masks, ["data_science", "developer"], "decimal")
    assert rule is not None and rule.mask_type == MaskType.constant


def test_data_science_alone_has_a_row_rule_on_the_dataset():
    rules = [r for r in _CONFIG["rls_rules"] if r["table_id"] == "view_order_margins"]
    assert [r["role_id"] for r in rules] == ["data_science"]


def test_the_dataset_is_versioned_for_as_of_pulls():
    assert _DATASET["materialize"] is True
    assert _DATASET["business_key"] == ["order_id"]
    assert (_DATASET["valid_time"], _DATASET["recorded_time"]) == ("ordered_at", "landed_at")


def _fires(cron: str) -> list[tuple[int, int]]:
    """The (hour, minute) of each firing of a day, for the minute and hour forms the demo uses."""
    minutes, hours, *_ = cron.split()
    span, _, step = minutes.partition("/")
    first, last = (0, 59) if span == "*" else map(int, span.split("-"))
    low, high = map(int, hours.split("-"))
    return [(h, m) for h in range(low, high + 1) for m in range(first, last + 1, int(step))]


def _trigger(trigger_id: str) -> dict:
    return next(t for t in _CONFIG["scheduled_triggers"] if t["id"] == trigger_id)


def test_each_team_is_told_in_turn_inside_the_dataset_s_window():
    window = parse_off_peak_window(_DATASET["off_peak_window"], _DATASET["off_peak_tz"])
    finance = _trigger("notify-finance")
    science = _trigger("notify-data-science")
    assert "'finance'" in finance["sql"] and "'data_science'" in science["sql"]
    first, then = _fires(finance["cron"]), _fires(science["cron"])
    assert then == [(hour, minute + 2) for hour, minute in first]
    midnight = datetime(2026, 11, 9, tzinfo=timezone.utc).timestamp()
    for hour, minute in first + then:
        assert window_open(window, midnight + hour * 3600 + minute * 60), (hour, minute)


def test_a_read_in_the_dataset_s_closed_half_hour_is_outside_its_window():
    window = parse_off_peak_window(_DATASET["off_peak_window"], _DATASET["off_peak_tz"])
    midnight = datetime(2026, 11, 9, tzinfo=timezone.utc).timestamp()
    assert not window_open(window, midnight + 15 * 60)
    assert window_open(window, midnight + 45 * 60)
