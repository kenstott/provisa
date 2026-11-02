# Copyright (c) 2026 Kenneth Stott
# Canary: 61d29b66-3ab8-434f-a255-09f51d08e89c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data node reports what its statements did; the control node records it (REQ-1916).

A data node's audit writer (:class:`RemoteAuditWriter`, started in place of the writer that
inserts rows) turns each finished statement's record into its facts -- who ran what, over which
transport, how it was answered, how many rows it returned, what the row limit did, the model stamp
it was governed under -- and hands them to the control node, which writes them as the rows of the
audit log (:func:`record_reported`). Counters cross the socket, never rows.
"""

# Requirements: REQ-1916, REQ-074

from __future__ import annotations

import logging
import queue
import threading
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.audit.writer import AuditRecord

log = logging.getLogger(__name__)

#: The record's own facts, carried as they are.
_FACTS = (
    "tenant_id",
    "user_id",
    "role_id",
    "query_text",
    "source",
    "status_code",
    "duration_ms",
    "logged_at",
    "trace_id",
    "route",
    "row_count",
    "model_stamp",
    "model_env",
    "route_reason",
    "sources",
    "data_age",
    "region",
    "time_travel",
    "meter_org",
    "hot_scope",
)

_BATCH = 200
_INTERVAL_S = 0.25
_RETRY_S = 2.0


def facts_of(record: "AuditRecord") -> dict[str, Any]:
    """``record``'s facts, with what it resolves lazily resolved now (on the reporter's thread)."""
    facts = {name: getattr(record, name) for name in _FACTS}
    facts["table_ids"] = tuple(
        record.table_ids() if callable(record.table_ids) else record.table_ids
    )
    facts["enforced"] = record.enforced() if callable(record.enforced) else record.enforced
    facts["used_roles"] = None if record.used_roles is None else tuple(record.used_roles())
    return facts


async def record_reported(reported: list[dict[str, Any]]) -> None:
    """Write the reported statements' audit rows on this control node: each in its own org and
    environment, through this node's writer, as a statement run here is written."""
    from provisa.api.app import ensure_serving_runtime, state
    from provisa.audit.writer import AuditRecord, audit_writer
    from provisa.core.environments import PROD
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )
    from provisa.encryption.runtime import encryption_service

    for facts in reported:
        facts = dict(facts)
        org_id = facts["tenant_id"]
        env = None if facts["model_env"] == PROD else facts["model_env"]
        await ensure_serving_runtime(org_id, env)
        org_token, env_token = set_current_org(org_id), set_current_env(env)
        try:
            used = facts.pop("used_roles")
            audit_writer().enqueue(
                AuditRecord(
                    record_db=state.record_db,
                    encryption=encryption_service(),
                    tables=state.tables,
                    meter_pool=state.admin_db,
                    hot_counts=state.hot_counts,
                    usage_db=state.model_db,
                    used_roles=None if used is None else (lambda used=used: used),
                    **facts,
                )
            )
        finally:
            reset_current_env(env_token)
            reset_current_org(org_token)


class RemoteAuditWriter:
    """A data node's audit writer: the interface of ``audit.writer.AuditWriter``, with the facts
    of each record sent to the control node instead of inserted here."""

    def __init__(self) -> None:
        self._queue: queue.Queue[AuditRecord] = queue.Queue()
        self._held: list[dict[str, Any]] = []
        self._progress = threading.Condition()
        self._accepted = 0
        self._settled = 0
        self._stopping = threading.Event()
        self._thread: threading.Thread | None = None
        self._closed = False
        self.last_error: str | None = None

    def enqueue(self, record: "AuditRecord") -> None:
        if not self.running():
            raise RuntimeError("audit reporter is not running: it is started at startup")
        with self._progress:
            self._accepted += 1
        self._queue.put(record)

    def flush(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self._progress:
            target = self._accepted
            while self._settled < target:
                left = deadline - time.monotonic()
                if left <= 0:
                    return False
                self._progress.wait(left)
        return True

    def pending(self) -> int:
        with self._progress:
            return self._accepted - self._settled

    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive() and not self._closed

    def status(self) -> str:
        pending = self.pending()
        if not pending:
            return "audit reporter: nothing pending"
        why = self.last_error or "the control node has not been sent them yet"
        return f"audit reporter: {pending} record(s) unreported — {why}"

    def start(self) -> "RemoteAuditWriter":
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="audit-reporter", daemon=True)
            self._thread.start()
        return self

    def _take(self) -> list[dict[str, Any]]:
        batch = list(self._held)
        until = time.monotonic() + _INTERVAL_S
        while len(batch) < _BATCH:
            left = until - time.monotonic()
            if left <= 0:
                break
            try:
                batch.append(facts_of(self._queue.get(timeout=left)))
            except queue.Empty:
                break
        return batch

    def _run(self) -> None:
        from provisa.planes.facade import report

        while not (self._stopping.is_set() and self._queue.empty() and not self._held):
            batch = self._take()
            if not batch:
                continue
            try:
                report(batch)
            except Exception as exc:  # noqa: BLE001 - held and sent again; the status names why
                self._held = batch
                self.last_error = str(exc)
                log.error("audit facts not reported to the control node: %s", exc)
                if self._stopping.wait(_RETRY_S):
                    return
                continue
            self._held = []
            with self._progress:
                self._settled += len(batch)
                self._progress.notify_all()

    def close(self, timeout: float) -> int:
        self._closed = True
        self._stopping.set()
        if self._thread is not None:
            self._thread.join(timeout)
        return self.pending()
