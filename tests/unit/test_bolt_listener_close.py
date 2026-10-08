# Copyright (c) 2026 Kenneth Stott
# Canary: e8898e24-3d61-4932-84f6-2c4b77b94b7a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Closing the Bolt listener ends its accept thread, whoever else listens on the port (REQ-1900).

Every worker binds the Bolt port (SO_REUSEPORT). close() used to close the listening socket and
return: a thread blocked in accept() is not woken by that, so it stayed there on a descriptor the
process was free to hand to its next socket. The Flight relay's close, which woke its thread with
a connection to the port instead, hung whenever the kernel gave that connection to another
listener. Here the thread reads a closed flag between bounded waits and close() joins it."""

from __future__ import annotations

import socket
import threading

from provisa.bolt.server import BoltListener
from tests.port_lease import lease_port


def _accepts(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=2).close()
    except OSError:
        return False
    return True


def test_close_returns_with_its_accept_thread_ended_and_its_socket_closed_after():
    listener = BoltListener("127.0.0.1", lease_port(), None)
    assert _accepts(listener.port)
    listener.close()
    assert not listener._thread.is_alive()  # noqa: SLF001 - the property under test
    assert listener._sock.fileno() == -1  # noqa: SLF001 - closed only after the thread ended
    assert not _accepts(listener.port)


def test_a_listener_closes_while_another_shares_its_port():
    port = lease_port()
    other = BoltListener("127.0.0.1", port, None)  # another worker's: stays open throughout
    try:
        for _ in range(8):
            listener = BoltListener("127.0.0.1", port, None)
            closing = threading.Thread(target=listener.close, daemon=True)
            closing.start()
            closing.join(timeout=10)
            assert not closing.is_alive(), (
                "close() did not return while another listener held the port"
            )
            assert not listener._thread.is_alive()  # noqa: SLF001 - the property under test
        assert _accepts(port)  # the listener that stayed still accepts
    finally:
        other.close()
