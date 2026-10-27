# Copyright (c) 2026 Kenneth Stott
# Canary: 60aaba8c-b5d9-4ef9-9b95-a3c3fb540659
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Flight SQL sessions: the options a client sets (REQ-2175), per session.

Flight names a session by the ``arrow_flight_session_id`` cookie the server sets; a client that
keeps cookies presents it on every call. The server sets it on an action that presents none, and
holds nothing for it until the session sets an option. A session is bound to the caller who
began it -- their identity and org -- and a cookie presented by any other caller is refused. The
store is held in this worker, bounded, and a session ends when its client closes it
(CloseSession, sent as the connection closes) or when newer sessions push it out. A session the
store no longer holds is served the defaults, as a reconnecting pgwire client is.
"""

# Requirements: REQ-2175

from __future__ import annotations

import secrets
import threading
from collections import OrderedDict
from dataclasses import dataclass, field

#: The cookie Flight names a session by.
COOKIE = "arrow_flight_session_id"

#: The sessions one worker holds at once; the oldest is forgotten beyond it.
SESSION_LIMIT = 4096


def new_session_id() -> str:
    """A session id no caller can guess."""
    return secrets.token_urlsafe(24)


#: Who began a session: their identity and org.
Owner = tuple[str | None, str | None]


class ForeignSession(PermissionError):
    """A session cookie presented by a caller other than the one who began the session."""

    def __init__(self) -> None:
        super().__init__("the session was begun by another caller")


@dataclass
class _Session:
    owner: Owner
    options: dict[str, str] = field(default_factory=dict)


class SessionStore:
    """The sessions of one worker, oldest first, at most ``limit``."""

    def __init__(self, limit: int = SESSION_LIMIT) -> None:
        self._limit = limit
        self._sessions: OrderedDict[str, _Session] = OrderedDict()
        self._lock = threading.Lock()

    def __len__(self) -> int:
        return len(self._sessions)

    def ids(self) -> list[str]:
        return list(self._sessions)

    def _held(self, session_id: str | None, owner: Owner) -> _Session | None:
        session = self._sessions.get(session_id) if session_id else None
        if session is None:
            return None
        if session.owner != owner:
            raise ForeignSession()
        self._sessions.move_to_end(session_id)  # type: ignore[arg-type]  # held: not None
        return session

    def options(self, session_id: str | None, owner: Owner) -> dict[str, str] | None:
        """The options of ``session_id``, or None when the store holds no such session."""
        with self._lock:
            session = self._held(session_id, owner)
            return None if session is None else dict(session.options)

    def update(self, session_id: str, owner: Owner, options: dict[str, str]) -> None:
        """Set ``options`` on ``session_id``, beginning it as ``owner``'s when the store holds no
        such session."""
        with self._lock:
            session = self._held(session_id, owner)
            if session is None:
                session = self._sessions[session_id] = _Session(owner)
                while len(self._sessions) > self._limit:
                    self._sessions.popitem(last=False)
            session.options.update(options)

    def close(self, session_id: str | None, owner: Owner) -> bool:
        """End ``session_id``; whether the store held it."""
        with self._lock:
            if self._held(session_id, owner) is None:
                return False
            del self._sessions[session_id]  # type: ignore[arg-type]  # held: not None
            return True
