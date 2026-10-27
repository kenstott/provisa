# Copyright (c) 2026 Kenneth Stott
# Canary: 0cc6514d-77f7-4411-9488-46d478fc34ee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source on a remote host (REQ-2098, REQ-2100): the front end every built image runs, with
the Python host program as its worker, paired with and dialled by Provisa over mutual TLS, serving
the same calls as a local worker to each Provisa connection."""

# Requirements: REQ-2098, REQ-2100

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time

import pyarrow as pa
import pytest

from provisa.codehost import front
from provisa.codesource import remote
from provisa.codesource.arrow_rows import schema_text
from provisa.codesource.worker import WorkerFailed
from provisa.python.source import HOST

pytestmark = pytest.mark.unit

BOUNDS = {
    "memory_mb": 256,
    "threads": 2,
    "spill_cap_mb": 1024,
}  # the host's DuckDB, given at pairing

FRONT = front.__file__
_CODE = textwrap.dedent(
    '''
    def orders(n: int) -> list[dict]:
        """Orders.

        @table
        """
        return [{"n": i} for i in range(n)]
    '''
)


ORDERS = schema_text(pa.schema([("n", pa.int64())]))


@pytest.fixture
def started(tmp_path):
    """A remote host on a free port, its state in a directory of its own; its pairing code."""
    code = tmp_path / "code"
    code.mkdir()
    (code / "shop.py").write_text(_CODE)
    port = 0
    names = tmp_path / "names.json"
    names.write_text("{}")
    worker = [sys.executable, "-I", str(HOST), "{fd}", str(code), "shop"]
    process = subprocess.Popen(  # noqa: S603 -- the test's own interpreter and Provisa's front end
        [
            sys.executable,
            FRONT,
            str(port),
            str(tmp_path / "state"),
            str(tmp_path / "spill"),
            str(names),
            *worker,
        ],
        env={**os.environ, "PROVISA_CODE_HASH": "h1"},
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stderr is not None
    pairing = None
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        line = process.stderr.readline()
        if line.startswith("pairing code: "):
            pairing = line.removeprefix("pairing code: ").strip()
        if line.startswith("remote host listening on "):
            # Bound by the host itself (port 0): no other test can take it first.
            port = int(line.rsplit(" ", 1)[1])
            break
    assert pairing is not None, "the host printed no pairing code"
    yield port, pairing
    process.terminate()
    process.wait(timeout=10)


def _worker(port: int, pin: str, identity: remote.Identity, **kw) -> remote.RemoteWorker:
    return remote.RemoteWorker(
        "shop-src",
        kw.pop("content_hash", "h1"),
        address="127.0.0.1",
        port=port,
        pin=pin,
        identity=identity,
        **kw,
    )


def test_a_paired_host_serves_the_codes_tables_to_each_provisa_connection(started):
    port, code = started
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", port, code, identity, BOUNDS).pin
    first = _worker(port, pin, identity, exports=["orders"])
    second = _worker(port, pin, identity, exports=["orders"])
    first.start()
    second.start()  # each Provisa process keeps a connection of its own
    try:
        assert first.exports == ["orders"]
        assert [
            row for batch in first.rows("orders", "table", [3], schema=ORDERS) for row in batch
        ] == [
            {"n": 0},
            {"n": 1},
            {"n": 2},
        ]
        assert second.ping()
    finally:
        first.stop()
        second.stop()


def test_a_pairing_code_is_used_once_and_another_secret_is_refused(started):
    port, code = started
    fingerprint, _secret = remote.parse_pairing_code(code)
    with pytest.raises(remote.PairingRefused, match="not this host's pairing code"):
        remote.pair("127.0.0.1", port, f"{fingerprint}.wrong", remote.new_identity(), BOUNDS)
    remote.pair("127.0.0.1", port, code, remote.new_identity(), BOUNDS)
    # Paired: the host now takes only its Provisa's certificate.
    with pytest.raises(remote.PairingRefused):
        remote.pair("127.0.0.1", port, code, remote.new_identity(), BOUNDS)


def test_a_provisa_the_host_is_not_paired_to_is_refused(started):
    port, code = started
    pin = remote.pair("127.0.0.1", port, code, remote.new_identity(), BOUNDS).pin
    stranger = _worker(port, pin, remote.new_identity())
    with pytest.raises(WorkerFailed):
        stranger.start(timeout=5)


def test_a_host_running_other_code_or_offering_other_exports_is_refused_by_name(started):
    port, code = started
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", port, code, identity, BOUNDS).pin
    with pytest.raises(WorkerFailed) as other_code:
        _worker(port, pin, identity, content_hash="h2").start()
    assert str(other_code.value) == (
        f"source 'shop-src': remote host 127.0.0.1:{port}: its host runs code h1, "
        "not the h2 the source registered"
    )
    with pytest.raises(WorkerFailed) as other_exports:
        _worker(port, pin, identity, exports=["orders", "refunds"]).start()
    assert "offers the exports ['orders'], not the ['orders', 'refunds']" in str(
        other_exports.value
    )


def test_a_host_with_another_certificate_than_the_pin_is_refused(started):
    port, code = started
    identity = remote.new_identity()
    remote.pair("127.0.0.1", port, code, identity, BOUNDS)
    with pytest.raises(WorkerFailed, match="presented certificate .*, not the paired 0+"):
        _worker(port, "0" * 64, identity).start()


def test_a_host_that_is_not_there_is_named():
    from tests.port_lease import lease_port

    # A leased port: below the ephemeral range, so no bind(0) of another test is given it.
    port = lease_port()
    with pytest.raises(WorkerFailed, match=f"remote host 127.0.0.1:{port} could not be reached"):
        _worker(port, "0" * 64, remote.new_identity()).start(timeout=2)


def test_a_pairing_code_that_is_not_one_is_refused_by_name():
    with pytest.raises(remote.PairingRefused, match="the line the remote host printed"):
        remote.parse_pairing_code("hello")


def _ipc_text(table: pa.Table) -> str:
    """An Arrow table as the base64 IPC stream a residual's arguments are carried in."""
    import base64

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as stream:
        stream.write_table(table)
    return base64.b64encode(sink.getvalue().to_pybytes()).decode()


def test_a_read_carrying_its_statement_is_answered_with_it_applied_on_the_host(started):
    port, code = started
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", port, code, identity, BOUNDS).pin
    worker = _worker(port, pin, identity)
    worker.start()
    try:
        residual = {
            "sql": "SELECT n FROM shop.orders WHERE n >= 3 AND _nf_n = 6 ORDER BY n DESC",
            "dialect": "duckdb",
            "table": "orders",
            "schema": ORDERS,
            "arguments": _ipc_text(pa.table({"n": pa.array([6], pa.int64())})),
        }
        answered = [
            pa.ipc.open_stream(pa.py_buffer(ipc)).read_all()
            for ipc in worker.batches(
                "orders", "table", [6], ["n"], schema=ORDERS, residual=residual
            )
        ]
        assert pa.concat_tables(answered).column("n").to_pylist() == [5, 4, 3]
        # A statement the host cannot run fails the call, named, and the host serves on.
        with pytest.raises(Exception, match="nope"):
            list(
                worker.batches(
                    "orders",
                    "table",
                    [1],
                    ["n"],
                    schema=ORDERS,
                    residual={**residual, "sql": "SELECT nope FROM orders"},
                )
            )
        assert worker.ping()
    finally:
        worker.stop()
