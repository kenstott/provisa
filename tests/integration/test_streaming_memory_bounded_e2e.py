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
import sys
import tempfile
from typing import Any

import pytest

pytestmark = [pytest.mark.integration]

pgserver = pytest.importorskip("pgserver")
pytest.importorskip("psycopg2")
pytest.importorskip("adbc_driver_postgresql")

_ROWS = 5_000_000  # ~1 GiB once materialized as Python tuples (3 cols: bigint, bigint, char(40))
_HEADROOM = 400 * 1024 * 1024  # AS cap = process baseline + this; one stream batch fits, 1 GiB does not
_SQL = "SELECT id, amount, label FROM big ORDER BY id"


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
    """Current virtual-memory size of this process (Linux ``/proc/self/statm`` field 0, in pages)."""
    with open("/proc/self/statm") as fh:
        pages = int(fh.read().split()[0])
    import resource

    return pages * resource.getpagesize()


def _cap_address_space() -> None:
    """Cap RLIMIT_AS at the current baseline + headroom. Called AFTER imports/connection so the cap is
    relative to real library VM — a batched read stays under it, a full materialization busts it."""
    import resource

    limit = _vm_bytes() + _HEADROOM
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))


def _peak_rss_bytes() -> int:
    """Peak resident-set size of this process so far, normalized to BYTES. ``ru_maxrss`` is KiB on Linux
    and bytes on macOS — normalize so the reported metric is cross-platform comparable (REQ-1220)."""
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def _mem_worker(dsn: str, variant: str, q: "mp.Queue", cap: bool = True) -> None:
    """Run one read VARIANT; report ('ok', n, peak_rss_bytes) | ('mem',) | ('err', repr).

    Streaming variants count rows WITHOUT accumulating (peak = one batch). Materializing variants build
    the full Python/Arrow result in memory. With ``cap`` the address space is bounded (RLIMIT_AS,
    Linux-only) and a materializing read busts it; without ``cap`` every variant runs to completion and
    the reported peak RSS is the metric that proves streaming stays bounded (cross-platform, REQ-1220)."""
    try:
        import os

        # pyarrow's default (jemalloc/mimalloc) pool and OpenBLAS reserve large VIRTUAL arenas up front
        # that would bust RLIMIT_AS regardless of whether the read streams. Force plain malloc + single
        # thread so the address-space cap tracks ACTUAL usage — otherwise the arrow path aborts on the
        # arena reservation, a false negative unrelated to result buffering (REQ-1220).
        os.environ["ARROW_DEFAULT_MEMORY_POOL"] = "system"
        os.environ["OPENBLAS_NUM_THREADS"] = "1"
        os.environ["OMP_NUM_THREADS"] = "1"
        os.environ["MALLOC_ARENA_MAX"] = "2"

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

        if variant == "pg_row_stream":
            res = rt.run_sync(_SQL)  # named server-side cursor
            n = sum(1 for _ in res.iter_rows())
        elif variant == "pg_arrow_stream":
            _schema, gen = rt.run_arrow_stream(_SQL)  # ADBC record-batch reader
            n = sum(b.num_rows for b in gen)
        elif variant == "pg_materialized":
            res = asyncio.run(rt.run(_SQL))  # fetchall → full Python list (control)
            n = len(res.rows)
        elif variant == "sqla_row_stream":
            res = rt.run_sync(_SQL)  # unnamed psycopg2 cursor: buffers client-side on execute()
            n = sum(1 for _ in res.iter_rows())
        else:
            raise ValueError(f"unknown variant {variant!r}")

        q.put(("ok", n, _peak_rss_bytes()))
    except MemoryError:
        q.put(("mem",))
    except Exception as exc:  # surfaced to the parent for a precise assertion message
        # libpq raises its own out-of-memory (a driver DatabaseError) when the AS cap denies the
        # fetchall allocation, rather than a Python MemoryError — still a cap bust, not a bug.
        if "out of memory" in str(exc).lower() or "memory allocation" in str(exc).lower():
            q.put(("mem",))
        else:
            q.put(("err", repr(exc)))


# RLIMIT_AS discriminates the cap only on Linux; macOS ignores it for the large arena maps pyarrow/libpq
# make. The cap-based tests carry this marker; the measured-peak-RSS metric tests run everywhere.
linux_only = pytest.mark.skipif(
    sys.platform != "linux", reason="RLIMIT_AS cap discriminates only on Linux; macOS ignores it"
)


@pytest.fixture(scope="module")
def big_pg():
    import psycopg2

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
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    proc = ctx.Process(target=_mem_worker, args=(dsn, variant, q, cap))
    proc.start()
    proc.join(timeout=300)
    if proc.is_alive():
        proc.terminate()
        pytest.fail(f"variant {variant!r} did not finish within 300s")
    return q.get(timeout=10)


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
    status, n, peak = _run_variant(big_pg, variant, cap=False)
    with capsys.disabled():
        print(f"\n[mem] {variant}: rows={n} peak_rss={_mib(peak)} MiB (ceiling {_mib(_STREAM_PEAK_CEILING)})")
    assert (status, n) == ("ok", _ROWS)
    assert peak < _STREAM_PEAK_CEILING, f"{variant} peak {_mib(peak)} MiB exceeded stream ceiling"


def test_materialized_peak_rss_is_large(big_pg, capsys):
    # The materializing control, measured rather than cap-busted: its peak RSS is far above the stream
    # ceiling, quantifying the gap that boundedness closes.
    status, n, peak = _run_variant(big_pg, "direct_materialized", cap=False)
    with capsys.disabled():
        print(f"\n[mem] direct_materialized: rows={n} peak_rss={_mib(peak)} MiB (floor {_mib(_MATERIALIZE_PEAK_FLOOR)})")
    assert (status, n) == ("ok", _ROWS)
    assert peak > _MATERIALIZE_PEAK_FLOOR, f"materialized peak {_mib(peak)} MiB unexpectedly small"
