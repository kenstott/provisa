# Copyright (c) 2026 Kenneth Stott
# Canary: 8446374e-555b-4f82-b510-6e5d9bad1d58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's rows streamed end to end (REQ-2100), through a built image's front end with the
Python host as its worker: each concurrent call has a connection of its own, so a slow reader
holds back its own function and no other call; a statement answered before the function's last
row stops it; Arrow the code returns or yields is sent as it is; a numeric's digits are
decimal(38, 9), and a value that does not fit is refused by name."""

# Requirements: REQ-2100

from __future__ import annotations

import base64
import os
import subprocess
import sys
import textwrap
import threading
import time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from provisa.codehost import front
from provisa.codesource import remote
from provisa.codesource.arrow_rows import IR_KEY, schema_text
from provisa.codesource.worker import _QUEUED as WINDOW
from provisa.codesource.worker import CONNECTIONS
from provisa.python.host import BATCH
from provisa.python.source import HOST

pytestmark = pytest.mark.unit

BOUNDS = {
    "memory_mb": 256,
    "threads": 2,
    "spill_cap_mb": 1024,
}  # the host's DuckDB, given at pairing

_CODE = textwrap.dedent(
    '''
    import time
    from pathlib import Path

    import pyarrow as pa

    PULLED = Path(__file__).parent / "pulled"
    CLOSED = Path(__file__).parent / "closed"


    def many(n: int):
        """Rows, each thousand pulled counted in a file beside the code; closed, said so.

        @table
        """
        try:
            for i in range(n):
                if i % 1000 == 0:
                    with PULLED.open("a") as f:
                        f.write("x")
                yield {"n": i}
        finally:
            CLOSED.write_text("closed")


    def arrow_table(n: int):
        """Arrow the code made, its column narrower than declared and one more beside it.

        @table
        """
        return pa.table({"extra": pa.array(["-"] * n), "n": pa.array(range(n), pa.int32())})


    def arrow_batches(n: int):
        """Arrow batches yielded between plain rows.

        @table
        """
        yield {"n": -1}
        for start in range(0, n, 2):
            yield pa.record_batch([pa.array(range(start, min(start + 2, n)), pa.int64())], ["n"])


    def amounts(digits: str):
        """A numeric column's digits.

        @table
        """
        return [{"amount": "1.25"}, {"amount": digits}]


    def slow(ms: int):
        """A row after a wait.

        @table
        """
        time.sleep(ms / 1000)
        return [{"n": ms}]
    '''
)

N = schema_text(pa.schema([("n", pa.int64())]))
AMOUNT = schema_text(pa.schema([pa.field("amount", pa.utf8(), metadata={IR_KEY: b"numeric"})]))


@pytest.fixture
def worker(tmp_path):
    """A paired, started remote worker against a front end serving the code."""
    code = tmp_path / "code"
    code.mkdir()
    (code / "shop.py").write_text(_CODE)
    names = tmp_path / "names.json"
    names.write_text("{}")
    host = [sys.executable, "-I", str(HOST), "{fd}", str(code), "shop"]
    process = subprocess.Popen(  # noqa: S603 -- the test's own interpreter and Provisa's front end
        [
            sys.executable,
            front.__file__,
            "0",
            str(tmp_path / "state"),
            str(tmp_path / "spill"),
            str(names),
            *host,
        ],
        env={**os.environ, "PROVISA_CODE_HASH": "h1"},
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stderr is not None
    pairing, port = None, 0
    until = time.monotonic() + 20
    while time.monotonic() < until:
        line = process.stderr.readline()
        if line.startswith("pairing code: "):
            pairing = line.removeprefix("pairing code: ").strip()
        if line.startswith("remote host listening on "):
            port = int(line.rsplit(" ", 1)[1])
            break
    assert pairing is not None and port, "the host did not start"
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", port, pairing, identity, BOUNDS)
    started = remote.RemoteWorker(
        "shop-src", "h1", address="127.0.0.1", port=port, pin=pin, identity=identity
    )
    started.start()
    yield started, code / "pulled"
    started.stop()
    process.terminate()
    process.wait(timeout=10)


def _pulled(path: Path) -> int:
    """How many rows the function has been pulled for, to the thousand."""
    return len(path.read_text()) * 1000 if path.exists() else 0


def _ipc_text(table: pa.Table) -> str:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as stream:
        stream.write_table(table)
    return base64.b64encode(sink.getvalue().to_pybytes()).decode()


def _answer(worker, export: str, args: list, names: list, schema: str, sql: str) -> pa.Table:
    residual = {
        "sql": sql,
        "dialect": "duckdb",
        "table": export,
        "schema": schema,
        "arguments": _ipc_text(pa.table({names[0]: pa.array([args[0]])})),
    }
    return pa.Table.from_batches(
        [
            b
            for ipc in worker.batches(
                export, "table", args, names, schema=schema, residual=residual
            )
            for b in pa.ipc.open_stream(pa.py_buffer(ipc))
        ]
    )


def test_a_slow_reader_holds_back_its_own_function_and_no_other_call(worker):
    worker, pulled = worker
    held = worker.batches("many", "table", [1_000_000], ["n"], schema=N)
    next(held)  # one batch taken, then the reader stops reading
    time.sleep(1.0)
    # The credit window: the host sends WINDOW frames ahead of the reader, one more for the frame
    # it took, and holds the batch it has pulled until a credit comes -- whatever the sockets
    # between them buffer. Each frame is a batch.
    bound = (WINDOW + 1 + 1) * BATCH
    stalled = _pulled(pulled)
    assert stalled <= bound
    # Another call meanwhile runs to its end on a connection of its own.
    rows = [row for batch in worker.rows("many", "table", [3000], ["n"], schema=N) for row in batch]
    assert [r["n"] for r in rows] == list(range(3000))
    time.sleep(0.5)
    assert _pulled(pulled) - 3000 == stalled  # the held call was pulled no further meanwhile
    held.close()  # closing it cancels the call
    assert worker.ping()


def test_the_function_stops_once_the_statement_is_answered(worker):
    worker, pulled = worker
    answered = _answer(worker, "many", [1_000_000], ["n"], N, "SELECT n FROM many LIMIT 5")
    assert answered.num_rows == 5
    closed = pulled.with_name("closed")
    until = time.monotonic() + 10
    while not closed.exists():
        assert time.monotonic() < until, "the function was never stopped"
        time.sleep(0.05)
    # Stopped: closed by the host before its last row, and pulled no further.
    assert _pulled(pulled) < 1_000_000
    stopped = _pulled(pulled)
    time.sleep(0.5)
    assert _pulled(pulled) == stopped
    assert worker.ping()


def test_arrow_the_code_returns_or_yields_is_sent_as_it_is(worker):
    worker, _ = worker
    table = [r for b in worker.rows("arrow_table", "table", [3], ["n"], schema=N) for r in b]
    assert table == [{"n": 0}, {"n": 1}, {"n": 2}]
    yielded = [r for b in worker.rows("arrow_batches", "table", [5], ["n"], schema=N) for r in b]
    assert [r["n"] for r in yielded] == [-1, 0, 1, 2, 3, 4]


def test_a_numeric_is_decimal_38_9_and_one_that_does_not_fit_is_refused_by_name(worker):
    worker, _ = worker
    total = _answer(
        worker, "amounts", ["7.5"], ["digits"], AMOUNT, "SELECT sum(amount) AS s FROM amounts"
    )
    assert total.schema.field("s").type == pa.decimal128(38, 9)
    assert total.column("s").to_pylist() == [Decimal("8.75")]
    with pytest.raises(Exception, match=r"does not fit decimal\(38, 9\).*never rounded"):
        _answer(
            worker,
            "amounts",
            ["1.0123456789"],
            ["digits"],
            AMOUNT,
            "SELECT amount FROM amounts",
        )
    assert worker.ping()


def test_every_connection_serves_a_call_at_once(worker):
    worker, _ = worker
    done: list[float] = []

    def call() -> None:
        list(worker.rows("slow", "table", [600], ["ms"], schema=N))
        done.append(time.monotonic())

    started = time.monotonic()
    threads = [threading.Thread(target=call) for _ in range(CONNECTIONS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert len(done) == CONNECTIONS
    # All at once, not one after another.
    assert max(done) - started < 600 * CONNECTIONS / 1000 / 2
