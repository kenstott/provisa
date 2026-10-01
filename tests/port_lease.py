# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0c1a77-3b9e-4f26-9a41-c2e8b7f6d913
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Host ports for test stacks and test servers, leased rather than guessed.

The allocator this replaces bound port 0, read the number, CLOSED the socket and handed the
number on — to ``docker compose up`` minutes later, or to a server started a few lines below.
Between the close and the real bind the port belonged to nobody, and the kernel was free to
hand the same number to any other ``bind(0)`` or outbound connection on the machine. A parallel
pytest session doing the same thing was the usual taker (``listen tcp 0.0.0.0:58891: bind:
address already in use`` at stack start-up).

Two properties close that window:

* Ports come from a range BELOW the kernel's ephemeral range, so no ``bind(0)`` and no outbound
  connection can ever be assigned one. :func:`_assert_outside_ephemeral_range` checks the claim
  against the running kernel instead of assuming it.
* The range is cut into blocks and a process owns a block by holding ``flock`` on its file, so
  two test processes never draw from the same ports. The kernel drops the lock when the process
  dies, however it dies, so there is no stale lease to reason about.

What is left is a process that binds one of these numbers EXPLICITLY. One already listening is
seen by the bind probe and skipped; the range is chosen to contain no port this repository
configures anywhere.

The numbers must be known at conftest import — dozens of test modules read ``PG_PORT`` and
friends into module-level constants during collection, and Kafka must be told its advertised
host port before it starts — which is why Docker is not left to pick them at ``up``.
"""

from __future__ import annotations

import fcntl
import os
import socket
import subprocess
import sys
import threading

# No docker-compose file, config, script or chart in this repository names a port in this span
# (nearest neighbours: Atlas 21000 below, 25632 above).
LEASE_FIRST = 21100
LEASE_LAST = 25599
BLOCK_SIZE = 50

# NOT under /tmp, for the same reason as the stack slots in tests/itest_stack.py.
_LOCK_DIR = os.path.join(os.path.expanduser("~"), ".provisa", "test-port-leases")


def os_ephemeral_range() -> tuple[int, int]:
    """The port range the running kernel assigns ``bind(0)`` and outbound connections from."""
    if sys.platform == "darwin":
        first, last = (
            int(
                subprocess.run(
                    ["sysctl", "-n", f"net.inet.ip.portrange.{key}"],
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout
            )
            for key in ("first", "last")
        )
        return first, last
    if sys.platform.startswith("linux"):
        with open("/proc/sys/net/ipv4/ip_local_port_range", encoding="ascii") as fh:
            first, last = fh.read().split()
        return int(first), int(last)
    raise RuntimeError(f"no ephemeral-port-range probe for platform {sys.platform!r}")


def _assert_outside_ephemeral_range(first: int, last: int) -> None:
    eph_first, eph_last = os_ephemeral_range()
    if first <= eph_last and eph_first <= last:
        raise RuntimeError(
            f"test port lease range {first}-{last} overlaps the kernel ephemeral range "
            f"{eph_first}-{eph_last}; a bind(0) could take a leased port"
        )


def _bindable(port: int) -> bool:
    """True when nothing holds ``port`` on the wildcard address — what a Docker publish binds."""
    with socket.socket() as s:
        try:
            s.bind(("", port))
        except OSError:
            return False
    return True


class PortLease:
    """The ports one process owns: whole blocks of ``first..last`` held by ``flock``."""

    def __init__(self, first: int, last: int, block_size: int, lock_dir: str) -> None:
        _assert_outside_ephemeral_range(first, last)
        self._first = first
        self._last = last
        self._block_size = block_size
        self._lock_dir = lock_dir
        self._mutex = threading.Lock()
        self._reset()

    def _reset(self) -> None:
        self._pid = os.getpid()
        self._fds: list[int] = []
        self._ports: list[int] = []
        self._pinned: set[int] = set()
        self._cursor = 0

    def _lease_block(self) -> None:
        os.makedirs(self._lock_dir, exist_ok=True)
        for start in range(self._first, self._last - self._block_size + 2, self._block_size):
            if start in self._ports:
                continue
            fd = os.open(
                os.path.join(self._lock_dir, f"block-{start}"), os.O_RDWR | os.O_CREAT, 0o644
            )
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(fd)
                continue
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()}\n".encode())
            self._fds.append(fd)
            self._cursor = len(self._ports)
            self._ports.extend(range(start, start + self._block_size))
            return
        raise RuntimeError(
            f"every test port block in {self._first}-{self._last} is leased by a live process "
            f"(see {self._lock_dir})"
        )

    def _next(self) -> int:
        if os.getpid() != self._pid:
            # A forked child shares the parent's lock (same open file description) and would
            # hand out the parent's ports. Closing the inherited descriptors leaves the parent's
            # lock in place; the child then leases blocks of its own.
            for fd in self._fds:
                os.close(fd)
            self._reset()
        while True:
            for _ in range(len(self._ports)):
                port = self._ports[self._cursor % len(self._ports)]
                self._cursor += 1
                if port not in self._pinned and _bindable(port):
                    return port
            self._lease_block()

    def pinned(self, n: int) -> list[int]:
        """``n`` distinct ports that are never issued again by this process.

        For ports published by a container that may start much later or not at all (a heavy
        engine's port is exported at import and bound only if its test runs) — a port that is
        still unbound must not look available to the next caller.
        """
        with self._mutex:
            ports = []
            for _ in range(n):
                port = self._next()
                self._pinned.add(port)
                ports.append(port)
            return ports

    def transient(self) -> int:
        """One port for a server the caller binds right away; reissued once it is free again."""
        with self._mutex:
            return self._next()


_lease = PortLease(LEASE_FIRST, LEASE_LAST, BLOCK_SIZE, _LOCK_DIR)


def lease_ports(n: int) -> list[int]:
    return _lease.pinned(n)


def lease_port() -> int:
    return _lease.transient()
