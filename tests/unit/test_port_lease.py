# Copyright (c) 2026 Kenneth Stott
# Canary: 8e4f2b19-6c0d-4a73-b5e8-1f9d3a7c2e64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The test-stack port allocator cannot lose a port between choosing it and binding it.

The defect: ports were found with bind(0), released, and bound by ``docker compose up`` much
later; another process took one in between and the stack failed with "address already in use".
"""

from __future__ import annotations

import fcntl
import os
import socket
import subprocess
import sys
import textwrap

import pytest

from tests import port_lease
from tests.port_lease import PortLease, os_ephemeral_range

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# A private span for these tests, inside the production range's own guarantee (below the
# ephemeral range) but outside the production range, so a lease here never competes with the
# session's real one.
_FIRST = 25700
_BLOCK = 8


@pytest.fixture(autouse=True)
def _span_is_exclusive():
    """One process at a time on the fixed span above — xdist workers and parallel sessions
    would otherwise see each other's squatter sockets and fail the exact-port assertions."""
    os.makedirs(port_lease._LOCK_DIR, exist_ok=True)
    fd = os.open(os.path.join(port_lease._LOCK_DIR, "unit-test-span"), os.O_RDWR | os.O_CREAT)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def _lease(tmp_path, blocks: int = 2) -> PortLease:
    return PortLease(_FIRST, _FIRST + blocks * _BLOCK - 1, _BLOCK, str(tmp_path))


def _hold_in_subprocess(tmp_path, n: int) -> tuple[subprocess.Popen, list[int]]:
    """Another PROCESS leasing ``n`` pinned ports from the same range, alive until stdin closes."""
    code = textwrap.dedent(
        f"""
        import sys
        from tests.port_lease import PortLease
        lease = PortLease({_FIRST}, {_FIRST + 2 * _BLOCK - 1}, {_BLOCK}, {str(tmp_path)!r})
        print(" ".join(map(str, lease.pinned({n}))), flush=True)
        sys.stdin.read()
        """
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        cwd=_REPO_ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    return proc, [int(p) for p in proc.stdout.readline().split()]


def test_production_range_is_unreachable_by_bind_zero():
    """No bind(0) or outbound connection can be assigned a leased port — the old race's taker."""
    eph_first, eph_last = os_ephemeral_range()
    assert port_lease.LEASE_LAST < eph_first or port_lease.LEASE_FIRST > eph_last
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        taken = s.getsockname()[1]
    assert not port_lease.LEASE_FIRST <= taken <= port_lease.LEASE_LAST


def test_range_overlapping_the_ephemeral_range_is_refused(tmp_path):
    eph_first, _ = os_ephemeral_range()
    with pytest.raises(RuntimeError, match="overlaps the kernel ephemeral range"):
        PortLease(eph_first - 10, eph_first + 10, _BLOCK, str(tmp_path))


def test_port_held_by_another_process_is_never_issued(tmp_path):
    """The port the allocator would pick next is occupied: it is skipped, not handed out."""
    with socket.socket() as squatter:
        squatter.bind(("", _FIRST))
        squatter.listen()
        ports = _lease(tmp_path).pinned(_BLOCK - 1)
    assert _FIRST not in ports
    assert ports == list(range(_FIRST + 1, _FIRST + _BLOCK))


def test_two_processes_never_share_a_port(tmp_path):
    """Ports another live process leased stay its own, though it has bound none of them."""
    proc, theirs = _hold_in_subprocess(tmp_path, _BLOCK)
    try:
        assert theirs == list(range(_FIRST, _FIRST + _BLOCK))
        ours = _lease(tmp_path).pinned(_BLOCK)
        assert not set(ours) & set(theirs)
    finally:
        proc.communicate("")


def test_dead_process_releases_its_block(tmp_path):
    proc, theirs = _hold_in_subprocess(tmp_path, _BLOCK)
    proc.communicate("")
    assert _lease(tmp_path).pinned(_BLOCK) == theirs


def test_exhausted_range_raises(tmp_path):
    proc, _ = _hold_in_subprocess(tmp_path, 2 * _BLOCK)
    try:
        with pytest.raises(RuntimeError, match="every test port block"):
            _lease(tmp_path).transient()
    finally:
        proc.communicate("")


def test_pinned_port_is_not_reissued_while_unbound(tmp_path):
    """A heavy engine's port is exported at import and bound only if its test runs."""
    lease = _lease(tmp_path, blocks=1)
    pinned = lease.pinned(_BLOCK - 2)
    transient = [lease.transient() for _ in range(3 * _BLOCK)]
    assert not set(transient) & set(pinned)
    assert set(transient) == set(range(_FIRST, _FIRST + _BLOCK)) - set(pinned)


def test_transient_port_in_use_is_skipped_until_released(tmp_path):
    lease = _lease(tmp_path, blocks=1)
    first = lease.transient()
    with socket.socket() as server:
        server.bind(("127.0.0.1", first))
        server.listen()
        assert first not in [lease.transient() for _ in range(2 * _BLOCK)]
    assert first in [lease.transient() for _ in range(_BLOCK)]


def test_session_stack_ports_are_leased(monkeypatch):
    """The env the compose files interpolate at `up` carries leased ports, all distinct."""
    from tests.conftest import _ITEST_PORT_ENV, _allocate_itest_ports

    env = {k: v for k, v in os.environ.items() if k not in ("PROVISA_URL", "PROVISA_BASE_URL")}
    monkeypatch.setattr(os, "environ", env)
    _allocate_itest_ports()
    ports = [int(env[name]) for name in _ITEST_PORT_ENV]
    ports.append(int(env["PROVISA_URL"].rsplit(":", 1)[1]))
    assert len(set(ports)) == len(ports)
    assert all(port_lease.LEASE_FIRST <= p <= port_lease.LEASE_LAST for p in ports)


def test_itest_compose_env_keeps_its_own_ports_when_the_e2e_stack_assigns_the_same_names():
    """An e2e session provisions two stacks. tests/e2e/conftest.py leases its own ports and exports
    them under the SAME env names tests/conftest.py used (PG_PORT, TRINO_PORT, ...), because the
    in-process app and the e2e clients read those names. The itest stack is brought up later, at
    collection finish, and compose interpolates ``${PG_PORT}`` from the env it is given — so that
    env must still carry the itest stack's own ports, or the itest postgres binds the e2e stack's
    port and the e2e stack cannot start ("Bind for 0.0.0.0:<port> failed: port is already
    allocated").

    Run in a child process: importing the two conftests assigns ports and env at import time."""
    script = textwrap.dedent(
        """
        import json, os
        import tests.conftest as root
        itest = {name: os.environ[name] for name in root._ITEST_PORT_ENV}
        import tests.e2e.conftest as e2e
        print(json.dumps({
            "itest": itest,
            "e2e": {name: os.environ[name] for name in e2e._PORT_ENV},
            "compose": {name: root._itest_compose_env()[name] for name in root._ITEST_PORT_ENV},
            "other": root._itest_compose_env().get("PROVISA_ITEST_PROJECT"),
            "project": os.environ.get("PROVISA_ITEST_PROJECT"),
        }))
        """
    )
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("PYTEST_NO_DOCKER", "PROVISA_E2E_EXTERNAL_STACK")
    }
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
        cwd=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    import json

    seen = json.loads(proc.stdout.strip().splitlines()[-1])
    itest, e2e, compose = seen["itest"], seen["e2e"], seen["compose"]

    # The e2e stack took the shared names for itself ...
    assert all(e2e[name] != itest[name] for name in e2e)
    # ... on ports no itest service publishes ...
    assert not set(e2e.values()) & set(itest.values())
    assert len(set(e2e.values())) == len(e2e) and len(set(itest.values())) == len(itest)
    # ... and the env the itest compose commands run with still names the itest stack's own.
    assert compose == itest
    # Everything else in that env is the live environment.
    assert seen["other"] == seen["project"]


def test_ports_issued_before_any_is_bound_are_distinct_across_the_end_of_a_block(tmp_path):
    """A server leases its HTTP, Flight and Airport ports, then binds them. Issued near the end
    of a block, the second and third must not wrap to the first port of the same block — that
    port is issued but not yet bound, so the bind probe passes it (the Airport fixture got one
    number for its HTTP and its Airport port, and its server exited at the second bind)."""
    lease = _lease(tmp_path, blocks=2)
    for _ in range(_BLOCK - 1):
        lease.transient()  # earlier servers, long since stopped
    server_ports = [lease.transient() for _ in range(3)]
    assert len(set(server_ports)) == 3, server_ports
    assert server_ports[1:] == [_FIRST + _BLOCK, _FIRST + _BLOCK + 1]  # the next block's


def test_ports_are_reissued_only_once_no_further_block_can_be_leased(tmp_path):
    lease = _lease(tmp_path, blocks=1)
    first_pass = [lease.transient() for _ in range(_BLOCK)]
    assert first_pass == list(range(_FIRST, _FIRST + _BLOCK))
    assert lease.transient() == _FIRST  # every block taken: start over at the first free port
