# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7e4c92-1a5d-4e38-8f60-3c9d2a7b5e14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The audit writer (provisa.audit.writer): the audit INSERT is off the request path.

REQ-074/REQ-1386 amended 2026-10-01 — a request thread enqueues its finished audit record and
returns; one writer thread per process inserts the queued records in batches. What must hold:

- the request does not wait for the INSERT;
- records land, batched, within the batch interval, each exactly once;
- nothing is lost quietly: a full queue makes the enqueue wait for the request's remaining
  deadline and then raise; a failed insert is logged at error and retried until it lands;
- shutdown writes what is queued;
- every worker process has its own writer and all of them land rows in the one log.
"""

# Requirements: REQ-074, REQ-1386, REQ-1454, REQ-1882

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import threading
import time
import uuid
from datetime import datetime, timezone

import pytest

from provisa.audit.writer import AuditQueueFull, AuditRecord, AuditWriter
from provisa.core import request_deadline
from provisa.encryption import NullEncryption

_DB = object()


def _record(
    n: int,
    *,
    db: object = _DB,
    meter_pool: object | None = None,
    hot_counts: object | None = None,
    route: str | None = None,
    status_code: int = 200,
) -> AuditRecord:
    return AuditRecord(
        tenant_db=db,
        tenant_id="default",
        user_id="alice",
        role_id="analyst",
        query_text=f"SELECT {n}",
        table_ids=(7,),
        source="pgwire",
        status_code=status_code,
        duration_ms=n,
        logged_at=datetime.now(timezone.utc),
        trace_id=None,
        encryption=NullEncryption(),
        meter_pool=meter_pool,
        meter_org="default",
        route=route,
        hot_counts=hot_counts,
        hot_scope="default:prod",
        model_stamp=1,
        model_env="prod",
        enforced={},
        route_reason=None,
        sources=(),
        data_age=None,
    )


class _Store:
    """Stands in for the tenant database: records each INSERT (one list of rows per call)."""

    def __init__(self) -> None:
        self.calls: list[tuple[object, list[dict]]] = []
        self.gate = threading.Event()
        self.gate.set()
        self.entered = threading.Event()
        self.fail_next = 0
        self.thread_names: list[str] = []
        self.deadlines: list[object] = []

    async def insert(self, db: object, rows: list[dict]) -> None:
        self.entered.set()
        self.thread_names.append(threading.current_thread().name)
        self.deadlines.append(request_deadline.current())
        assert self.gate.wait(10), "test never released the insert"
        if self.fail_next:
            self.fail_next -= 1
            raise ConnectionError("store unreachable")
        self.calls.append((db, rows))

    def rows(self) -> list[dict]:
        return [row for _db, batch in self.calls for row in batch]


@pytest.fixture
def store():
    return _Store()


@pytest.fixture
def writer(store):
    w = AuditWriter(
        capacity=1000, batch_size=100, interval_s=0.05, retry_s=0.05, insert=store.insert
    ).start()
    yield w
    store.gate.set()
    w.close(5.0)


def test_the_request_does_not_wait_for_the_insert(writer, store):
    store.gate.clear()  # the INSERT cannot complete
    started = time.monotonic()
    writer.enqueue(_record(1))
    returned_in = time.monotonic() - started
    assert store.entered.wait(2), "the writer never attempted the insert"
    assert returned_in < 0.02
    assert writer.pending() == 1  # accepted, not yet written
    assert store.calls == []
    store.gate.set()
    assert writer.flush(2.0)
    assert [r["duration_ms"] for r in store.rows()] == [1]


def test_the_insert_runs_on_the_writer_thread_with_no_request_context(writer, store):
    with request_deadline.within(5.0):
        writer.enqueue(_record(1))
    assert writer.flush(2.0)
    assert store.thread_names == ["provisa-bg:audit-writer"]
    assert store.deadlines == [None]  # the first caller's deadline did not follow it there


def test_records_land_in_a_batch_within_the_interval(writer, store):
    started = time.monotonic()
    for n in range(40):
        writer.enqueue(_record(n))
    assert writer.flush(2.0)
    elapsed = time.monotonic() - started
    assert [r["duration_ms"] for r in store.rows()] == list(range(40))  # each once, in order
    assert len(store.calls) <= 2  # one INSERT (two if the first record's window closed early)
    assert elapsed < 0.5


def test_a_batch_is_one_insert_per_tenant_database(writer, store):
    other = object()
    store.gate.clear()
    writer.enqueue(_record(0))
    assert store.entered.wait(2)  # the writer is busy: the next records gather behind it
    for n in range(1, 7):
        writer.enqueue(_record(n, db=other if n % 2 else _DB))
    store.gate.set()
    assert writer.flush(2.0)
    by_db = {id(db): [r["duration_ms"] for r in rows] for db, rows in store.calls[1:]}
    assert by_db == {id(other): [1, 3, 5], id(_DB): [2, 4, 6]}


def test_the_row_carries_the_statement_not_the_batch(writer, store):
    record = _record(3)
    writer.enqueue(record)
    assert writer.flush(2.0)
    (row,) = store.rows()
    assert row["logged_at"] == record.logged_at  # when the statement finished
    assert (row["tenant_id"], row["user_id"], row["role_id"]) == ("default", "alice", "analyst")
    assert (row["source"], row["status_code"], row["table_ids"]) == ("pgwire", 200, [7])
    assert row["query_text_enc"] == b"SELECT 3" and len(row["query_hash"]) == 64


def test_a_full_queue_makes_the_enqueue_wait_for_the_deadline_then_raise(store):
    writer = AuditWriter(
        capacity=2, batch_size=1, interval_s=0.01, retry_s=0.01, insert=store.insert
    ).start()
    try:
        store.gate.clear()
        writer.enqueue(_record(0))
        assert store.entered.wait(2)  # record 0 is in the writer's hands, stuck
        writer.enqueue(_record(1))
        writer.enqueue(_record(2))  # the queue (capacity 2) is now full
        started = time.monotonic()
        with request_deadline.within(0.3):
            with pytest.raises(AuditQueueFull, match="audit queue full"):
                writer.enqueue(_record(3))
        waited = time.monotonic() - started
        assert 0.25 <= waited < 1.0  # it waited for the request's budget, no longer
        assert writer.pending() == 3  # the refused record is not counted as accepted
        store.gate.set()
        assert writer.flush(2.0)
        # Nothing accepted was dropped to make room.
        assert [r["duration_ms"] for r in store.rows()] == [0, 1, 2]
    finally:
        store.gate.set()
        writer.close(5.0)


def test_a_waiting_enqueue_is_accepted_as_soon_as_there_is_room(store):
    writer = AuditWriter(
        capacity=1, batch_size=1, interval_s=0.01, retry_s=0.01, insert=store.insert
    ).start()
    try:
        store.gate.clear()
        writer.enqueue(_record(0))
        assert store.entered.wait(2)
        writer.enqueue(_record(1))  # queue full
        threading.Timer(0.1, store.gate.set).start()
        with request_deadline.within(5.0):
            writer.enqueue(_record(2))  # waits ~0.1 s, then lands
        assert writer.flush(2.0)
        assert [r["duration_ms"] for r in store.rows()] == [0, 1, 2]
    finally:
        store.gate.set()
        writer.close(5.0)


def test_a_failed_insert_is_logged_at_error_and_retried_until_it_lands(writer, store, caplog):
    store.fail_next = 2
    with caplog.at_level(logging.ERROR, logger="provisa.audit.writer"):
        writer.enqueue(_record(1))
        writer.enqueue(_record(2))
        assert writer.flush(3.0)
    assert sorted(r["duration_ms"] for r in store.rows()) == [1, 2]  # each landed exactly once
    failures = [r for r in caplog.records if "audit batch insert failed" in r.getMessage()]
    assert len(failures) == 2 and all(r.levelno == logging.ERROR for r in failures)


def test_flush_does_not_report_success_while_an_insert_keeps_failing(writer, store):
    store.fail_next = 10_000
    writer.enqueue(_record(1))
    assert writer.flush(0.3) is False
    assert writer.pending() == 1
    store.fail_next = 0
    assert writer.flush(2.0)


def test_a_failed_meter_is_retried_without_inserting_the_row_again(store):
    metered: list[str] = []
    fail = [1]

    async def meter(pool: object, org_id: str) -> None:
        if fail[0]:
            fail[0] -= 1
            raise ConnectionError("control plane unreachable")
        metered.append(org_id)

    async def no_commit(pool: object, org_id: str, env: str, stamp: int) -> None:
        return None  # no environment position: the row names no model commit

    writer = AuditWriter(
        interval_s=0.01, retry_s=0.01, insert=store.insert, meter=meter, deployed_commit=no_commit
    ).start()
    try:
        writer.enqueue(_record(1, meter_pool=object()))
        assert writer.flush(2.0)
        assert [r["duration_ms"] for r in store.rows()] == [1]
        assert metered == ["default"]
    finally:
        writer.close(5.0)


# -- Hot counts (REQ-826): counted where the audit row is written, after it has landed -------------


class _Counts:
    """Stands in for the count store: records each batch it is handed."""

    def __init__(self, *, fail: Exception | None = None) -> None:
        self.added: list[tuple[dict, int]] = []
        self.fail = fail

    def add(self, hits: dict, interval: int) -> None:
        if self.fail is not None:
            raise self.fail
        self.added.append((dict(hits), interval))


def _counting_writer(store, **kw) -> AuditWriter:
    return AuditWriter(
        capacity=1000, batch_size=100, interval_s=0.05, retry_s=0.05, insert=store.insert, **kw
    ).start()


def test_a_landed_batch_is_counted_once_per_table_after_its_rows_are_inserted(store, monkeypatch):
    monkeypatch.setattr("provisa.core.settings_registry.value", lambda key: 60)
    counts = _Counts()
    order: list[str] = []
    insert = store.insert

    async def _insert(db, rows):
        await insert(db, rows)
        order.append("insert")

    add = counts.add
    counts.add = lambda hits, interval: (order.append("count"), add(hits, interval))[1]
    w = AuditWriter(
        capacity=1000, batch_size=100, interval_s=0.05, retry_s=0.05, insert=_insert
    ).start()
    try:
        for n in range(3):
            w.enqueue(_record(n, hot_counts=counts, route="engine"))
        w.enqueue(_record(9, hot_counts=counts, route="cache"))  # reached no data
        w.enqueue(_record(10, hot_counts=counts, route=None, status_code=403))  # refused
        assert w.flush(5.0)
    finally:
        w.close(5.0)
    assert len(store.rows()) == 5  # every statement is audited
    assert sum(hits[("default:prod", 7)] for hits, _ in counts.added if hits) == 3
    assert {interval for _hits, interval in counts.added} == {60}
    assert order[0] == "insert", "a batch was counted before its audit rows had landed"


def test_a_batch_whose_insert_failed_is_not_counted_until_it_lands(store, monkeypatch):
    monkeypatch.setattr("provisa.core.settings_registry.value", lambda key: 60)
    counts = _Counts()
    store.fail_next = 2
    w = _counting_writer(store)
    try:
        w.enqueue(_record(1, hot_counts=counts, route="direct"))
        assert w.flush(5.0)
    finally:
        w.close(5.0)
    assert [hits for hits, _ in counts.added] == [{("default:prod", 7): 1}]


def test_a_count_store_failure_loses_no_audit_row_and_is_logged(store, monkeypatch, caplog):
    """REQ-826 / REQ-1920: the count is derived state; the audit row is the record."""
    from redis.exceptions import ConnectionError as RedisConnectionError

    monkeypatch.setattr("provisa.core.settings_registry.value", lambda key: 60)
    counts = _Counts(fail=RedisConnectionError("redis unreachable"))
    w = _counting_writer(store)
    try:
        with caplog.at_level("ERROR", logger="provisa.audit.writer"):
            w.enqueue(_record(1, hot_counts=counts, route="engine"))
            w.enqueue(_record(2, hot_counts=counts, route="engine"))
            assert w.flush(5.0), "the writer held the batch for a count it could not record"
    finally:
        w.close(5.0)
    assert len(store.rows()) == 2
    assert w.pending() == 0 and w.last_error is None
    assert any(
        "Hot count not recorded for a batch of" in r.getMessage() and r.exc_info
        for r in caplog.records
    )


def test_a_record_with_no_count_store_is_not_counted(store, monkeypatch):
    monkeypatch.setattr("provisa.core.settings_registry.value", lambda key: 60)
    w = _counting_writer(store)
    try:
        w.enqueue(_record(1, route="engine"))
        assert w.flush(5.0)
    finally:
        w.close(5.0)
    assert len(store.rows()) == 1


def test_shutdown_writes_what_is_queued(store):
    writer = AuditWriter(batch_size=10, interval_s=5.0, retry_s=0.05, insert=store.insert).start()
    store.gate.clear()
    writer.enqueue(_record(0))
    assert store.entered.wait(10)  # the writer is mid-insert; 25 more queue behind it
    for n in range(1, 26):
        writer.enqueue(_record(n))
    threading.Timer(0.05, store.gate.set).start()
    writer.close(5.0)
    assert [r["duration_ms"] for r in store.rows()] == list(range(26))
    with pytest.raises(RuntimeError, match="not running"):
        writer.enqueue(_record(99))  # a writer that was shut down accepts nothing


def test_a_request_never_starts_the_writer():
    """The writer is started at application startup. An enqueue with no writer running raises —
    it does not start one from the request's thread."""
    writer = AuditWriter(insert=_Store().insert)
    before = {t.name for t in threading.enumerate()}
    with pytest.raises(RuntimeError, match="not running"):
        writer.enqueue(_record(1))
    assert {t.name for t in threading.enumerate()} == before
    assert writer.pending() == 0


def test_the_process_writer_must_be_started_before_it_accepts_a_record():
    from provisa.audit import writer as writer_mod

    assert writer_mod.shutdown_audit_writer(1.0) == 0
    try:
        assert not writer_mod.audit_writer_running()
        with pytest.raises(RuntimeError, match="application startup"):
            writer_mod.audit_writer()
    finally:
        started = writer_mod.start_audit_writer()
    assert writer_mod.start_audit_writer() is started  # idempotent
    assert writer_mod.audit_writer() is started


def test_shutdown_gives_up_on_a_store_that_is_gone_within_its_budget(store, caplog):
    """A writer being shut down retries a failing insert only for the shutdown budget, then
    reports the unwritten count and stops: its thread ends, nothing keeps retrying."""
    writer = AuditWriter(interval_s=0.01, retry_s=0.05, insert=store.insert).start()
    store.fail_next = 10_000_000  # the database is gone for good
    for n in range(3):
        writer.enqueue(_record(n))
    time.sleep(0.1)
    started = time.monotonic()
    with caplog.at_level(logging.ERROR, logger="provisa.audit.writer"):
        unwritten = writer.close(0.5)
    assert unwritten == 3
    assert time.monotonic() - started < 3.0
    assert any("3 record(s) unwritten" in r.getMessage() for r in caplog.records)
    assert writer._thread is not None and writer._thread.done()  # noqa: SLF001 - the thread ended
    attempts = len(store.thread_names)
    time.sleep(0.3)
    assert len(store.thread_names) == attempts  # and no insert is attempted after it


def test_the_application_lifespan_starts_and_stops_the_writer():
    import inspect

    import provisa.api.app as app_mod

    source = inspect.getsource(app_mod.lifespan)
    assert source.index("start_audit_writer()") < source.index("yield")
    assert source.index("yield") < source.index("shutdown_audit_writer")


def test_shutdown_reports_records_it_could_not_write(store, caplog):
    writer = AuditWriter(interval_s=0.01, retry_s=0.01, insert=store.insert).start()
    store.fail_next = 10_000
    writer.enqueue(_record(1))
    time.sleep(0.1)
    with caplog.at_level(logging.ERROR, logger="provisa.audit.writer"):
        writer.close(5.0)
    assert any("1 record(s) unwritten" in r.getMessage() for r in caplog.records)


# ------------------------------------------------------------------ against a real store


def _tenant_db(path: str):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_org import query_audit_log

    engine = create_engine_from_url(f"sqlite:///{path}")
    query_audit_log.create(engine, checkfirst=True)
    return Database(engine, "audit-test")


def _logged(path: str) -> list[tuple]:
    import sqlite3

    con = sqlite3.connect(path)
    try:
        return con.execute(
            "SELECT tenant_id, user_id, role_id, table_ids, source, status_code FROM "
            "query_audit_log ORDER BY id"
        ).fetchall()
    finally:
        con.close()


def test_write_audit_lands_one_row_in_the_tenant_database(tmp_path, monkeypatch):
    """The whole seam: write_audit returns, the writer inserts through the real Database."""
    from types import SimpleNamespace

    from provisa.audit.pipeline import PendingAudit, write_audit
    from provisa.audit.writer import audit_writer_status, flush_audit

    path = str(tmp_path / "tenant.db")
    from provisa.federation.replica_hot import HotCounts

    org = f"audit-seam-{uuid.uuid4().hex}"
    counts = HotCounts(None)
    state = SimpleNamespace(
        tenant_db=_tenant_db(path), org_id=org, admin_db=None, hot_counts=counts
    )
    monkeypatch.setattr("provisa.encryption.runtime.encryption_service", NullEncryption)
    monkeypatch.setattr("provisa.core.settings_registry.value", lambda key: 60)
    pending = PendingAudit("alice", "graphql", "analyst", "{ orders { id } }", [7, 9], 0.0, 1, {})
    asyncio.run(write_audit(pending, 200, state, route="engine"))
    assert flush_audit(5.0), audit_writer_status()
    assert _logged(path) == [(org, "alice", "analyst", "[7, 9]", "graphql", 200)]
    # REQ-826: and each table the statement read is counted once, in this org environment.
    assert counts.counts(f"{org}:prod", [7, 9, 11], 60) == {7: 1.0, 9: 1.0, 11: 0.0}


def _worker(path: str, worker: int, count: int) -> None:
    """One worker process: its own writer, its own records, a clean shutdown."""
    db = _tenant_db(path)
    writer = AuditWriter(interval_s=0.01, retry_s=0.05).start()
    for n in range(count):
        writer.enqueue(
            AuditRecord(
                tenant_db=db,
                tenant_id="default",
                user_id=f"worker-{worker}",
                role_id="analyst",
                query_text=f"SELECT {n}",
                table_ids=(),
                source="pgwire",
                status_code=200,
                duration_ms=n,
                logged_at=datetime.now(timezone.utc),
                trace_id=None,
                encryption=NullEncryption(),
                model_stamp=1,
                model_env="prod",
                enforced={},
                route_reason=None,
                sources=(),
                data_age=None,
            )
        )
    writer.close(20.0)


def test_every_worker_process_lands_its_own_records_in_the_one_log(tmp_path):
    path = str(tmp_path / "tenant.db")
    _tenant_db(path)  # the log exists before the workers start
    ctx = multiprocessing.get_context("spawn")
    workers = [ctx.Process(target=_worker, args=(path, w, 60)) for w in range(3)]
    for proc in workers:
        proc.start()
    for proc in workers:
        proc.join(60)
        assert proc.exitcode == 0
    users = [row[1] for row in _logged(path)]
    assert {u: users.count(u) for u in set(users)} == {
        "worker-0": 60,
        "worker-1": 60,
        "worker-2": 60,
    }


def test_a_writer_whose_thread_was_stopped_from_outside_accepts_nothing(store):
    """Background shutdown (provisa.core.connection_loop.shutdown_background) cancels every
    long-lived thread, the writer's included. A writer whose thread has ended must refuse a
    record — accepting it would queue it for a thread that will never read it."""
    writer = AuditWriter(interval_s=0.01, retry_s=0.01, insert=store.insert).start()
    writer.enqueue(_record(1))
    assert writer.flush(2.0)
    assert writer._thread is not None  # noqa: SLF001
    writer._thread.cancel()  # noqa: SLF001 - what shutdown_background does to it
    assert writer._thread.join(5.0)  # noqa: SLF001
    assert not writer.running()
    with pytest.raises(RuntimeError, match="not running"):
        writer.enqueue(_record(2))
    assert writer.pending() == 0
    assert "stopped" in writer.status()


def test_start_replaces_a_process_writer_whose_thread_has_ended():
    from provisa.audit import writer as writer_mod
    from provisa.core.connection_loop import shutdown_background

    first = writer_mod.start_audit_writer()
    shutdown_background(timeout=5.0)  # stops every long-lived thread in the process
    assert not first.running()
    second = writer_mod.start_audit_writer()
    assert second is not first and second.running()
    assert writer_mod.audit_writer() is second
