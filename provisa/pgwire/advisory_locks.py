# Copyright (c) 2026 Kenneth Stott
# Canary: 2a6fd0c3-84b1-4e9a-9f37-1d5c7be6a028
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's advisory locks on pgwire connections (REQ-2167, CLIENT-CALLED SERVER FUNCTIONS
ANSWER LIKE POSTGRESQL), as src/backend/utils/adt/lockfuncs.c answers them: exclusive or shared,
held by a session or until its transaction ends, re-entrant, and each key's locks conflicting only
with another session's. A database is an org here: an org's keys are its own.

The table lives in this server process, as PostgreSQL's lives in one server's shared memory: two
Provisa processes do not exclude each other. A wait ends with a lock, the request's deadline or a
cancel; there is no deadlock detector, so a deadlock ends with the deadline."""

# Requirements: REQ-2167

from __future__ import annotations

import threading
from collections import Counter
from typing import Hashable

from provisa.core import request_deadline

#: How often a wait looks for a cancel.
_POLL_SECONDS = 0.05

EXCLUSIVE = "ExclusiveLock"
SHARED = "ShareLock"

_SESSION = "session"
_TRANSACTION = "transaction"


class AdvisoryLocks:
    """The server's advisory locks: per key, per holder, how many times at each mode and level."""

    def __init__(self) -> None:
        self._changed = threading.Condition()
        self._held: dict[Hashable, dict[object, Counter[tuple[str, str]]]] = {}

    def _conflicts(self, key: Hashable, mode: str, holder: object) -> bool:
        for other, counts in self._held.get(key, {}).items():
            if other is holder:
                continue
            if mode == EXCLUSIVE or any(m == EXCLUSIVE for m, _ in counts):
                return True
        return False

    def _grant(self, key: Hashable, mode: str, level: str, holder: object) -> None:
        self._held.setdefault(key, {}).setdefault(holder, Counter())[(mode, level)] += 1

    def try_acquire(self, key: Hashable, mode: str, holder: object, *, transaction: bool) -> bool:
        with self._changed:
            if self._conflicts(key, mode, holder):
                return False
            self._grant(key, mode, _TRANSACTION if transaction else _SESSION, holder)
            return True

    def acquire(self, key: Hashable, mode: str, holder: object, *, transaction: bool) -> None:
        """Wait for the lock, until the request's deadline or a cancel says otherwise."""
        woken = threading.Event()
        with request_deadline.cancel_on_deadline(woken.set), self._changed:
            while self._conflicts(key, mode, holder):
                if woken.is_set():
                    break
                self._changed.wait(_POLL_SECONDS)
            else:
                self._grant(key, mode, _TRANSACTION if transaction else _SESSION, holder)
                return
        request_deadline.check()

    def _drop(self, key: Hashable, holder: object, entry: tuple[str, str]) -> None:
        counts = self._held[key][holder]
        counts[entry] -= 1
        if counts[entry] <= 0:
            del counts[entry]
        if not counts:
            del self._held[key][holder]
        if not self._held[key]:
            del self._held[key]

    def release(self, key: Hashable, mode: str, holder: object) -> bool:
        """Release one session-level hold; False where ``holder`` has none."""
        entry = (mode, _SESSION)
        with self._changed:
            if self._held.get(key, {}).get(holder, Counter())[entry] <= 0:
                return False
            self._drop(key, holder, entry)
            self._changed.notify_all()
            return True

    def _release_level(self, holder: object, levels: tuple[str, ...]) -> None:
        with self._changed:
            for key in list(self._held):
                counts = self._held[key].get(holder)
                if counts is None:
                    continue
                for entry in [e for e in counts if e[1] in levels]:
                    del counts[entry]
                if not counts:
                    del self._held[key][holder]
                if not self._held[key]:
                    del self._held[key]
            self._changed.notify_all()

    def end_transaction(self, holder: object) -> None:
        """A transaction ended: its locks go, the session's stay."""
        self._release_level(holder, (_TRANSACTION,))

    def release_all(self, holder: object) -> None:
        """pg_advisory_unlock_all: the session-level locks."""
        self._release_level(holder, (_SESSION,))

    def end_session(self, holder: object) -> None:
        self._release_level(holder, (_SESSION, _TRANSACTION))


#: The server process's locks.
LOCKS = AdvisoryLocks()
