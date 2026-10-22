# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DIAGNOSTIC ONLY (flight-relay-diag branch; never merged): who closes which descriptor.

tests/unit/test_flight_relay.py::test_a_large_stream_arrives_whole_through_the_relay failed under
-n 4 (gw3): its Flight server's listener refused a connection moments after the server bound it.
The suspected cause is a descriptor closed twice in that worker: something from an earlier test
closes an fd number it no longer owns, now gRPC's listener. This records, per process, every
Python-level close of a descriptor -- ``os.close``, ``socket.close`` (``_real_close``) and an
unclosed socket finalized by the garbage collector (its ResourceWarning) -- with the running test,
the thread and a short stack, so the failing test can name the closer of its listener's fd.
"""

from __future__ import annotations

import collections
import os
import socket
import threading
import time
import traceback
import warnings

#: (when, fd, how, test, thread, stack) of the latest closes in this process.
CLOSES: collections.deque = collections.deque(maxlen=20000)
current_test = ["(collection)"]


def _record(fd: int, how: str) -> None:
    CLOSES.append(
        (
            time.monotonic(),
            fd,
            how,
            current_test[0],
            threading.current_thread().name,
            "".join(traceback.format_stack(limit=12)[:-2]),
        )
    )


def install() -> None:
    real_os_close = os.close

    def _os_close(fd: int) -> None:
        _record(fd, "os.close")
        real_os_close(fd)

    os.close = _os_close

    real_socket_close = socket.socket._real_close  # noqa: SLF001

    def _real_close(self, _ss=socket._socket.socket):  # noqa: SLF001
        try:
            fd = self.fileno()
        except OSError:
            fd = -1
        _record(fd, "socket.close")
        real_socket_close(self, _ss)

    socket.socket._real_close = _real_close  # noqa: SLF001

    # A socket nobody closed is closed by its finalizer, in C, which neither hook above sees: its
    # ResourceWarning names its fd.
    warnings.simplefilter("always", ResourceWarning)
    real_show = warnings.showwarning

    def _show(message, category, filename, lineno, file=None, line=None):
        if issubclass(category, ResourceWarning):
            text = str(message)
            if "fd=" in text:
                fd = int(text.split("fd=", 1)[1].split(",", 1)[0].split(">", 1)[0])
                _record(fd, f"finalized ({text[:160]})")
        real_show(message, category, filename, lineno, file, line)

    warnings.showwarning = _show


def closes_of(fd: int, since: float) -> str:
    found = [c for c in list(CLOSES) if c[1] == fd and c[0] >= since]
    if not found:
        return f"no Python-level close of fd {fd} recorded since the server started"
    return "\n\n".join(
        f"+{when - since:.3f}s {how} fd={fd} during {test} on thread {thread}:\n{stack}"
        for when, _fd, how, test, thread, stack in found
    )


def listener_fd(port: int) -> int | None:
    """The fd of this process's socket listening on 127.0.0.1:``port``."""
    import psutil

    for conn in psutil.Process().net_connections(kind="tcp"):
        if conn.status == psutil.CONN_LISTEN and conn.laddr and conn.laddr.port == port:
            return conn.fd
    return None
