# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1f9c4a-2d7e-4a3b-9c11-8e5d2f0a7b64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: streaming reads are EMPIRICALLY memory-bounded; materializing reads are not (REQ-1220).

The capability flags and cursor plumbing only CLAIM boundedness. This test PROVES it: a large result
(millions of rows, ~1 GiB materialized) is queried inside a subprocess whose address space is capped
(``RLIMIT_AS``) just above the process baseline. A genuinely streaming read drains under the cap; a
read that buffers the whole result busts the cap with ``MemoryError``. Asserting BOTH directions locks
in exactly which surfaces stream and which still buffer — a surface that silently starts materializing
a streaming path fails loudly here.

Linux-only: ``RLIMIT_AS`` on macOS is not honored for the large arena maps pyarrow/libpq make, so the
cap would not discriminate. On darwin the test skips with a reason (never a silent pass).
"""

from __future__ import annotations

import multiprocessing as mp
import os
import queue
import sys
import tempfile
import traceback
from typing import Any

import pytest

pytestmark = [pytest.mark.integration]

# The linux-mem Docker lane provisions a real PostgreSQL and passes its DSN via
# PROVISA_MEM_TEST_DSN (Apple Silicon can neither run pgserver's bundled binaries under
# amd64 emulation nor install a linux/arm64 pgserver wheel — there is none). With an
# external DSN the embedded pgserver is not needed, so only require it otherwise.
_EXTERNAL_DSN = os.environ.get("PROVISA_MEM_TEST_DSN")
pgserver = None if _EXTERNAL_DSN else pytest.importorskip("pgserver")
pytest.importorskip("psycopg2")
pytest.importorskip("adbc_driver_postgresql")

_ROWS = 5_000_000  # ~1 GiB once materialized as Python tuples (3 cols: bigint, bigint, char(40))
_HEADROOM = 400 * 1024 * 1024  # AS cap = process baseline + this; one stream batch fits, 1 GiB does not
_SQL = "SELECT id, amount, label FROM big ORDER BY id"

# A busted RLIMIT_AS is signaled to the parent through the worker's EXIT CODE, never through the result
# queue: mp.Queue.put lazily starts a feeder thread on first use, and once the cap is exhausted
# _start_new_thread fails ("can't start new thread"), so the ('mem',) sentinel could never be delivered
# via the queue — the parent would just time out on an empty queue. os._exit(code) is a raw syscall that
# needs neither a thread nor an allocation, so it always gets the signal out (REQ-1220 control tests).
_MEM_EXIT_CODE = 42


def _is_cap_bust(exc: BaseException) -> bool:
    """True if ``exc`` (or anything in its cause/context chain) is an RLIMIT_AS bust. The bust surfaces
    in several shapes: a Python MemoryError; a libpq/driver "out of memory"; asyncpg raising
    ConnectionDoesNotExistError with a MemoryError as its __cause__ when the fetch allocation dies
    mid-operation; or "can't start new thread" once the exhausted address space denies any further
    thread-stack allocation. All are the same cap bust the materializing controls are asserting."""
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, MemoryError):
            return True
        msg = str(cur).lower()
        if "out of memory" in msg or "memory allocation" in msg or "can't start new thread" in msg:
            return True
        cur = cur.__cause__ or cur.__context__
    return False


class _DirectPools:
    """A real ``SourcePool`` fronting a single asyncpg-backed PostgreSQL driver at id ``src`` — the
    airport DIRECT variant's ``source_pools``. ``warm()`` builds the pool from the raw DSN (pgserver
    uses a unix-socket URI, so the host/port ``add()`` path does not apply) and registers the driver so
    ``has``/``supports_stream``/``open_stream`` behave exactly as in production."""

    def __init__(self, dsn: str, loop: Any) -> None:
        self._dsn = dsn
        self._loop = loop
        self._pool: Any = None  # provisa.executor.pool.SourcePool, built in warm()

    async def warm(self) -> None:
        import asyncpg

        from provisa.executor.drivers.postgresql import PostgreSQLDriver
        from provisa.executor.pool import SourcePool

        drv = PostgreSQLDriver()
        drv._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=2)
        sp = SourcePool()
        sp._drivers["src"] = drv
        sp._dialects["src"] = "postgresql"
        self._pool = sp

    def has(self, source_id: str) -> bool:
        return self._pool.has(source_id)

    def supports_stream(self, source_id: str) -> bool:
        return self._pool.supports_stream(source_id)

    async def open_stream(self, source_id: str, sql: str, params: list | None = None):
        return await self._pool.open_stream(source_id, sql, params)


def _vm_bytes() -> int:
    """Current virtual-memory size of this process (Linux ``/proc/self/statm`` field 0, in pages).

    ``/proc/self/statm`` doesn't exist on macOS. The cap-bust callers of this function
    (``_cap_address_space``) only ever run under ``@linux_only`` tests, but ``_trace()`` is also called
    from the cross-platform peak-RSS tests (REQ-1220, run on darwin too) — so this must degrade instead
    of raising there. ``ru_maxrss`` is a peak, not current VM size, but it's diagnostic-only on non-Linux.
    """
    if sys.platform.startswith("linux"):
        with open("/proc/self/statm") as fh:
            pages = int(fh.read().split()[0])
        import resource

        return pages * resource.getpagesize()
    return _peak_rss_bytes()


def _cap_address_space() -> None:
    """Cap RLIMIT_AS at the current baseline + headroom. Called AFTER imports/connection so the cap is
    relative to real library VM — a batched read stays under it, a full materialization busts it."""
    import resource

    limit = _vm_bytes() + _HEADROOM
    _trace(f"cap baseline={_vm_bytes()} limit={limit}")
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))


# ---- worker address-space trace ---------------------------------------------
# A capped worker can die in ways that carry no diagnosis: SIGSEGV when the exhausted address space
# denies a stack expansion, or a bare non-zero exit. The parent then knows only an exit code. So the
# worker appends address-space checkpoints to a plain file the parent reads after the join — the last
# line written says how much VM the read had consumed and where it was, which distinguishes "the read
# buffered the result" from "a post-cap library load / arena reservation ate the headroom".

_TRACE_FD: int | None = None


def _trace_open(path: str) -> None:
    global _TRACE_FD
    _TRACE_FD = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)


def _trace(tag: str) -> None:
    if _TRACE_FD is None:
        return
    os.write(_TRACE_FD, f"{tag} vm={_vm_bytes() // (1024 * 1024)}MiB\n".encode())


def _count_rows(rows: Any, tag: str) -> int:
    """Count a row iterator WITHOUT holding it, tracing address space every million rows."""
    n = 0
    for _ in rows:
        n += 1
        if n % 1_000_000 == 0:
            _trace(f"{tag} rows={n}")
    return n


def _count_batch_rows(batches: Any, tag: str) -> int:
    """Count an Arrow record-batch iterator WITHOUT holding the batches, tracing every 16 batches."""
    n = 0
    for i, batch in enumerate(batches):
        n += batch.num_rows
        if i % 16 == 0:
            _trace(f"{tag} batch={i} rows={n}")
    return n


def _peak_rss_bytes() -> int:
    """Peak resident-set size of this process so far, normalized to BYTES. ``ru_maxrss`` is KiB on Linux
    and bytes on macOS — normalize so the reported metric is cross-platform comparable (REQ-1220)."""
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def _mem_worker(dsn: str, variant: str, q: "mp.Queue", cap: bool, trace_path: str) -> None:
    """Run one read VARIANT; report ('ok', n, peak_rss_bytes) | ('mem',) | ('err', repr).

    Streaming variants count rows WITHOUT accumulating (peak = one batch). Materializing variants build
    the full Python/Arrow result in memory. With ``cap`` the address space is bounded (RLIMIT_AS,
    Linux-only) and a materializing read busts it; without ``cap`` every variant runs to completion and
    the reported peak RSS is the metric that proves streaming stays bounded (cross-platform, REQ-1220)."""
    try:
        _trace_open(trace_path)
        _trace(f"worker-start variant={variant} cap={cap}")
        # The allocator/thread knobs this worker runs under (MALLOC_ARENA_MAX, ARROW_DEFAULT_MEMORY_POOL,
        # OPENBLAS/OMP thread caps) are exported by _run_variant in the PARENT and inherited at exec —
        # see the comment there. They cannot be set here: glibc has already read MALLOC_ARENA_MAX by the
        # time any worker code runs.
        import asyncio

        # DIRECT route (REQ-1190): the single-reachable-source driver's server-side cursor, exercised
        # through the real PostgreSQLDriver.open_stream / _PgDirectStream. A streaming DIRECT scan is
        # bounded; the materializing control (driver.execute → full QueryResult) busts the cap.
        if variant == "airport_typed_stream":
            # Defect 5: the airport transport's OWN adapter, over a real server-side cursor. The DIRECT
            # streaming terminal (execute_native_stream) is driven on a background loop exactly as the
            # airport worker thread drives it, then reshaped by the airport's _typed_batches_from_rows
            # into typed Arrow RecordBatches. Counting num_rows WITHOUT holding the batches proves the
            # adapter (islice, batch_rows at a time) streams — it does NOT fetchall the ~1 GiB result
            # into Provisa RAM. Busts the cap only if the airport path silently materializes.
            import threading

            from provisa.api.airport.query import _direct_typed_schema, _typed_batches_from_rows
            from provisa.federation.runtime import EngineRuntime

            loop = asyncio.new_event_loop()
            ready = threading.Event()

            def _run_loop() -> None:
                asyncio.set_event_loop(loop)
                ready.set()
                loop.run_forever()

            t = threading.Thread(target=_run_loop, daemon=True)
            t.start()
            ready.wait()

            pools = _DirectPools(dsn, loop)
            asyncio.run_coroutine_threadsafe(pools.warm(), loop).result()
            if cap:
                _cap_address_space()  # cap AFTER the pool/arena is warm, like the rt variants
            rt = EngineRuntime.__new__(EngineRuntime)  # only execute_native_stream is exercised
            stream = rt.execute_native_stream(pools, "src", _SQL, [], loop=loop)
            typed = _direct_typed_schema(stream.column_names, stream.column_types)
            n = sum(b.num_rows for b in _typed_batches_from_rows(stream, typed))
            loop.call_soon_threadsafe(loop.stop)
            q.put(("ok", n, _peak_rss_bytes()))
            return

        if variant.startswith("direct_"):
            import asyncpg

            from provisa.executor.drivers.postgresql import PostgreSQLDriver

            async def _direct() -> int:
                drv = PostgreSQLDriver()
                drv._pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
                if cap:
                    _cap_address_space()  # cap AFTER the pool/arena is warm, like the rt variants
                if variant == "direct_stream":
                    ds = await drv.open_stream(_SQL)
                    n = 0
                    while True:
                        chunk = await ds.fetch(1000)
                        if not chunk:
                            break
                        n += len(chunk)
                    await ds.close()
                    return n
                res = await drv.execute(_SQL)  # materializing control
                return len(res.rows)

            q.put(("ok", asyncio.run(_direct()), _peak_rss_bytes()))
            return

        from provisa.federation.pg_runtime import PgFederationRuntime
        from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime

        rt: Any = (
            SqlAlchemyFederationRuntime(url=dsn)
            if variant.startswith("sqla_")
            else PgFederationRuntime(engine_dsn=dsn)
        )

        if cap:
            _cap_address_space()

        _trace(f"runtime-ready variant={variant}")

        if variant == "pg_row_stream":
            res = rt.run_sync(_SQL)  # named server-side cursor
            _trace("portal-open")
            n = _count_rows(res.iter_rows(), "pg_row_stream")
        elif variant == "pg_arrow_stream":
            gen = rt.run_arrow_stream(_SQL)[1]  # ADBC record-batch reader; [0] is the schema
            _trace("adbc-reader-open")
            n = _count_batch_rows(gen, "pg_arrow_stream")
        elif variant == "pg_materialized":
            res = asyncio.run(rt.run(_SQL))  # fetchall → full Python list (control)
            n = len(res.rows)
        elif variant == "sqla_row_stream":
            res = rt.run_sync(_SQL)  # unnamed psycopg2 cursor: buffers client-side on execute()
            n = sum(1 for _ in res.iter_rows())
        else:
            raise ValueError(f"unknown variant {variant!r}")

        q.put(("ok", n, _peak_rss_bytes()))
    except BaseException as exc:
        # A cap bust is signaled by the exit code, NOT the queue — see _MEM_EXIT_CODE. os._exit needs no
        # thread/allocation, so it delivers the signal even from a fully address-space-exhausted process.
        if _is_cap_bust(exc):
            os._exit(_MEM_EXIT_CODE)
        # Genuine unexpected error: try to surface a precise message. If even this put fails (feeder
        # thread can't start), fall through to a non-zero exit so the parent reports the raw exitcode.
        try:
            q.put(("err", traceback.format_exc()))
        except BaseException:
            raise exc from None


# RLIMIT_AS discriminates the cap only on Linux; macOS ignores it for the large arena maps pyarrow/libpq
# make. The cap-based tests carry this marker; the measured-peak-RSS metric tests run everywhere.
# @linux_only stacks TWO markers: linux_mem (so scripts/test-all's default lanes deselect these on a
# macOS host, where the linux-mem container lane runs them instead — no phantom skip) and the
# skipif safety net (so a direct macOS run still skips rather than falsely passing an unenforced cap).
# On a Linux host the marker is not excluded and skipif passes, so the default/core lane runs them.
_linux_only_skip = pytest.mark.skipif(
    sys.platform != "linux", reason="RLIMIT_AS cap discriminates only on Linux; macOS ignores it"
)


def linux_only(fn):
    return pytest.mark.linux_mem(_linux_only_skip(fn))


@pytest.fixture(scope="module")
def big_pg():
    import psycopg2

    if _EXTERNAL_DSN:
        dsn = _EXTERNAL_DSN
    else:
        assert pgserver is not None  # importorskip guaranteed it when no external DSN
        base = tempfile.mkdtemp(prefix="provisa_stream_mem_")
        server = pgserver.get_server(base)
        dsn = server.get_uri()
    con = psycopg2.connect(dsn)
    con.autocommit = True
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS big")
    cur.execute(
        "CREATE TABLE big AS SELECT g::bigint AS id, (g * 2)::bigint AS amount, "
        f"repeat('x', 40) AS label FROM generate_series(1, {_ROWS}) g"
    )
    con.close()
    yield dsn


def _run_variant(dsn: str, variant: str, cap: bool = True) -> tuple:
    # Allocator knobs MUST be exported HERE, in the parent, before the spawn — a "spawn" child execs a
    # fresh interpreter and inherits this environment at exec, which is the only moment glibc reads
    # MALLOC_ARENA_MAX. Setting it inside the worker is far too late: glibc has already initialized
    # malloc, so every thread build_pg_engine starts claims its own 64 MiB arena and a plain
    # fetchmany(1000) reserves ~726 MiB of address space instead of ~86 MiB — a cap bust with nothing
    # to do with result buffering, and nondeterministic (arena count varies with thread scheduling).
    # ARROW_DEFAULT_MEMORY_POOL/thread caps ride along for the same reason: keep the capped address
    # space tracking ACTUAL usage, so the cap measures streaming and not allocator reservations.
    os.environ["MALLOC_ARENA_MAX"] = "2"
    os.environ["ARROW_DEFAULT_MEMORY_POOL"] = "system"
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["OMP_NUM_THREADS"] = "1"
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    trace = os.path.join(tempfile.mkdtemp(prefix="provisa_mem_trace_"), f"{variant}.trace")
    proc = ctx.Process(target=_mem_worker, args=(dsn, variant, q, cap, trace))
    proc.start()
    proc.join(timeout=300)
    if proc.is_alive():
        proc.terminate()
        pytest.fail(f"variant {variant!r} did not finish within 300s\n{_read_trace(trace)}")
    # A cap bust exits with _MEM_EXIT_CODE and puts nothing on the queue (the feeder thread can't start
    # under the exhausted address space) — read the outcome from the exit code, not the queue.
    if proc.exitcode == _MEM_EXIT_CODE:
        return ("mem",)
    try:
        return q.get(timeout=10)
    except queue.Empty:
        pytest.fail(
            f"variant {variant!r} produced no result (worker exitcode={proc.exitcode})\n"
            f"{_read_trace(trace)}"
        )


def _read_trace(path: str) -> str:
    """The worker's address-space checkpoints, for a failure message. Missing means the worker died
    before it opened the file — itself the diagnosis, so report that rather than an empty string."""
    if not os.path.exists(path):
        return "[worker address-space trace: file never created]"
    return "[worker address-space trace]\n" + open(path).read()


# Peak-RSS ceiling for a streaming read of ~1 GiB of rows: a bounded scan holds a handful of fetch
# batches (~tens of MiB) plus interpreter/library baseline — well under this. A materializing read of
# the same result peaks near/over 1 GiB. The gap is what proves boundedness cross-platform (REQ-1220).
_STREAM_PEAK_CEILING = 500 * 1024 * 1024
_MATERIALIZE_PEAK_FLOOR = 700 * 1024 * 1024


def _mib(n: int) -> int:
    return n // (1024 * 1024)


# ---- streaming surfaces drain under the cap (REQ-1220) ----------------------


@linux_only
def test_pg_row_stream_is_memory_bounded(big_pg):
    # Named server-side cursor: peak memory is one fetchmany batch, so 1 GiB of rows drains under a
    # baseline+400MiB cap.
    assert _run_variant(big_pg, "pg_row_stream")[:2] == ("ok", _ROWS)


@linux_only
def test_pg_arrow_stream_is_memory_bounded(big_pg):
    # ADBC record-batch reader: batches are size-hint-bounded, so the full result never materializes.
    assert _run_variant(big_pg, "pg_arrow_stream")[:2] == ("ok", _ROWS)


@linux_only
def test_pg_direct_stream_is_memory_bounded(big_pg):
    # REQ-1190: the DIRECT route (single reachable source) now opens a server-side cursor via
    # PostgreSQLDriver.open_stream / _PgDirectStream — a single-source passthrough scan is bounded
    # identically to ENGINE, no longer a full driver.execute materialization.
    assert _run_variant(big_pg, "direct_stream")[:2] == ("ok", _ROWS)


# ---- materializing surfaces bust the cap — the control (REQ-1220) -----------


@linux_only
def test_pg_materialized_run_busts_the_cap(big_pg):
    # PgFederationRuntime.run does fetchall into a full Python list — it MUST exceed the cap. This is
    # the control proving the cap is tight enough to discriminate.
    assert _run_variant(big_pg, "pg_materialized") == ("mem",)


@linux_only
def test_pg_direct_materialized_busts_the_cap(big_pg):
    # Control for REQ-1190: driver.execute (the pre-streaming DIRECT path) fetchalls into a full Python
    # list and MUST bust the cap — proving the streaming variant above is what keeps DIRECT bounded.
    assert _run_variant(big_pg, "direct_materialized") == ("mem",)


@linux_only
def test_airport_typed_stream_is_memory_bounded(big_pg):
    # Defect 5 (supersedes REQ-1218): the airport transport reshapes the DIRECT server-side cursor with
    # _typed_batches_from_rows into typed Arrow RecordBatches, batch_rows at a time — so a governed
    # airport scan of ~1 GiB streams instead of a fetchall into Provisa RAM. Bounded under the cap.
    assert _run_variant(big_pg, "airport_typed_stream")[:2] == ("ok", _ROWS)


@linux_only
def test_sqla_over_pg_row_stream_is_memory_bounded(big_pg):
    # REQ-1222: SqlAlchemyFederationRuntime.run_sync now uses the stream_results execution option, so
    # over psycopg2 it opens a server-side cursor and stays bounded — no longer buffers client-side.
    assert _run_variant(big_pg, "sqla_row_stream")[:2] == ("ok", _ROWS)


# ---- measured peak-RSS metric: bounded vs materialized (cross-platform, REQ-1220) ----
# The RLIMIT tests above discriminate only on Linux. These run everywhere: they execute the read to
# completion WITHOUT a cap and assert on the measured peak RSS, so the boundedness guarantee is proven
# by a number (logged for every run), not just by a pass/fail cap that darwin cannot honor.


@pytest.mark.parametrize(
    "variant", ["pg_row_stream", "pg_arrow_stream", "direct_stream", "airport_typed_stream"]
)
def test_streaming_peak_rss_is_bounded(big_pg, variant, capsys):
    result = _run_variant(big_pg, variant, cap=False)
    if result[0] == "err":
        pytest.fail(f"variant {variant!r} raised:\n{result[1]}")
    status, n, peak = result
    with capsys.disabled():
        print(f"\n[mem] {variant}: rows={n} peak_rss={_mib(peak)} MiB (ceiling {_mib(_STREAM_PEAK_CEILING)})")
    assert (status, n) == ("ok", _ROWS)
    assert peak < _STREAM_PEAK_CEILING, f"{variant} peak {_mib(peak)} MiB exceeded stream ceiling"


def test_materialized_peak_rss_is_large(big_pg, capsys):
    # The materializing control, measured rather than cap-busted: its peak RSS is far above the stream
    # ceiling, quantifying the gap that boundedness closes.
    result = _run_variant(big_pg, "direct_materialized", cap=False)
    if result[0] == "err":
        pytest.fail(f"direct_materialized raised:\n{result[1]}")
    status, n, peak = result
    with capsys.disabled():
        print(f"\n[mem] direct_materialized: rows={n} peak_rss={_mib(peak)} MiB (floor {_mib(_MATERIALIZE_PEAK_FLOOR)})")
    assert (status, n) == ("ok", _ROWS)
    assert peak > _MATERIALIZE_PEAK_FLOOR, f"materialized peak {_mib(peak)} MiB unexpectedly small"
