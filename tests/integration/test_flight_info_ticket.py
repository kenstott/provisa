# Copyright (c) 2026 Kenneth Stott
# Canary: 4a1d9e63-2b70-4c85-9f16-8d0e3b5a7c29
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A ticket FlightInfo hands out is redeemed by whichever worker takes the DoGet.

Every worker of a launch accepts on the one advertised Flight port (REQ-1900), so FlightInfo's
endpoint carries its ticket and NO location — "redeem where you asked". That holds only if a
ticket says what to return and nothing about who issued it. Which worker the kernel hands a
connection on the shared port to is the kernel's choice (and on some systems always the same
one), so this test does not leave it to chance: it takes a ticket from one worker and redeems it
on EACH worker's own Flight server, the loopback port its relay forwards to."""

from __future__ import annotations

import os
import subprocess

import pyarrow.flight as fl
import pytest

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_WORKERS = 4
_DESCRIPTOR = ("sales", "orders")


def _loopback_listening_ports(pid: int) -> set[int]:
    out = subprocess.run(
        ["lsof", "-nP", "-a", "-p", str(pid), "-iTCP", "-sTCP:LISTEN", "-Fn"],
        capture_output=True,
        text=True,
    ).stdout
    return {
        int(line.rsplit(":", 1)[1])
        for line in out.splitlines()
        if line.startswith("n127.0.0.1:") or line.startswith("nlocalhost:")
    }


def _own_flight_port(pid: int, advertised: set[int]) -> int:
    """The loopback port of this worker's own Flight server: the one of its loopback listeners
    that answers GetFlightInfo for the table."""
    answering = []
    for port in sorted(_loopback_listening_ports(pid) - advertised):
        client = fl.connect(f"grpc://127.0.0.1:{port}")
        try:
            client.get_flight_info(
                fl.FlightDescriptor.for_path(*_DESCRIPTOR), fl.FlightCallOptions(timeout=5)
            )
            answering.append(port)
        except fl.FlightError:
            pass  # another protocol's listener, or the airport Flight service: not this server
        finally:
            client.close()
    assert len(answering) == 1, f"worker {pid}: Flight servers found on {answering}"
    return answering[0]


def test_a_ticket_from_flight_info_is_redeemed_by_every_worker():
    boot = WorkerBoot(
        _WORKERS,
        pg_host=os.environ.get("PG_HOST", "localhost"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
    )
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        workers = sorted(set(boot.worker_pids()))
        assert len(workers) == _WORKERS
        advertised = set(boot.ports.values())
        own = {pid: _own_flight_port(pid, advertised) for pid in workers}
        assert len(set(own.values())) == _WORKERS  # four servers, one per worker

        # The advertised port answers FlightInfo with one endpoint: its ticket, and no location.
        front = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
        try:
            info = front.get_flight_info(fl.FlightDescriptor.for_path(*_DESCRIPTOR))
            assert len(info.endpoints) == 1
            assert list(info.endpoints[0].locations) == []
            via_advertised = front.do_get(info.endpoints[0].ticket).read_all()
        finally:
            front.close()
        assert via_advertised.num_rows > 0

        # A ticket issued by ONE worker, redeemed on EVERY worker's own server.
        issuer_pid = workers[0]
        issuer = fl.connect(f"grpc://127.0.0.1:{own[issuer_pid]}")
        try:
            issued = issuer.get_flight_info(fl.FlightDescriptor.for_path(*_DESCRIPTOR))
            ticket = issued.endpoints[0].ticket
        finally:
            issuer.close()
        for pid in workers:
            client = fl.connect(f"grpc://127.0.0.1:{own[pid]}")
            try:
                redeemed = client.do_get(ticket).read_all()
            finally:
                client.close()
            assert redeemed.equals(via_advertised), f"worker {pid} redeemed something else"
    finally:
        boot.cleanup()
