# Copyright (c) 2026 Kenneth Stott
# Canary: 3da1b457-c8a1-47c7-bd9a-752053494671
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Benchmark: the product's Python DataRow encode path against the Rust encoder, end to end from
Arrow record batches to DataRow bytes, over a large mixed-type result.

The dataset is generated once as Arrow (numpy-vectorized, seeded) in 65,536-row batches -- the
batch size DuckDB's Arrow reader streams (duckdb_runtime._ARROW_STREAM_BATCH_ROWS) -- and both
encoders read the same batches. Output is counted, never kept, so a run holds the Arrow input once
plus one batch of Python rows (the product's own peak). A run aborts if the system's free memory
drops under 25%.

    PYTHONPATH=.:vendor/buenavista \
        ~/.provisa-agent/heavy.sh .venv/bin/python tests/unit/perf_bench/pgwire_encoder_bench.py --rows 5000000
"""

from __future__ import annotations

import argparse
import cProfile
import io
import json
import pstats
import re
import subprocess
import time
from collections.abc import Callable, Iterator
from typing import Any

import numpy as np
import pyarrow as pa

from provisa._encoders import pgwire as pgwire_encoder
from tests.unit.pgwire_encoder.python_ref import (
    python_encode,
    python_encode_count,
    query_result,
    bvtype_names,
)
from tests.unit.pgwire_encoder.test_differential import _assert_same as assert_same

BATCH_ROWS = 65_536
MIN_FREE_PCT = 25

COLUMNS: list[tuple[str, str]] = [
    ("id", "BIGINT"),
    ("qty", "INTEGER"),
    ("price", "DOUBLE"),
    ("ratio", "FLOAT"),
    ("amount", "DECIMAL(18,2)"),
    ("name", "VARCHAR"),
    ("created", "TIMESTAMP WITH TIME ZONE"),
    ("day", "DATE"),
    ("active", "BOOLEAN"),
    ("payload", "JSON"),
    ("note", "VARCHAR"),  # 30% NULL
    ("parent_id", "BIGINT"),  # 20% NULL
]

_WORDS = np.array(
    [
        "alpha",
        "beta",
        "gamma",
        "delta",
        "épsilon",
        "zeta",
        "eta",
        "théta",
        "iota",
        "kappa",
        "日本",
        "lambda",
    ]
)


def free_pct() -> int:
    out = subprocess.run(
        ["memory_pressure", "-Q"], capture_output=True, text=True, check=True
    ).stdout
    found = re.search(r"free percentage: (\d+)%", out)
    assert found is not None, out
    return int(found.group(1))


def guard() -> None:
    pct = free_pct()
    if pct < MIN_FREE_PCT:
        raise SystemExit(f"free memory {pct}% is under {MIN_FREE_PCT}%: stopping")


def make_batch(rng: np.random.Generator, n: int, start: int) -> pa.RecordBatch:
    def nulls(p: float) -> np.ndarray:
        return rng.random(n) < p

    words = _WORDS[rng.integers(0, len(_WORDS), n)]
    suffix = rng.integers(0, 1_000_000, n).astype(str)
    name = np.char.add(np.char.add(words, "-"), suffix)
    payload = np.char.add(
        np.char.add('{"k": ', rng.integers(0, 10_000, n).astype(str)), ', "tag": "x"}'
    )
    created_us = rng.integers(946_684_800_000_000, 1_893_456_000_000_000, n)  # 2000..2030
    arrays = [
        pa.array(np.arange(start, start + n, dtype=np.int64)),
        pa.array(rng.integers(-(2**31), 2**31 - 1, n, dtype=np.int32)),
        pa.array(rng.normal(1000.0, 250.0, n)),
        pa.array(rng.random(n, dtype=np.float32)),
        pa.Array.from_buffers(
            pa.decimal128(18, 2),
            n,
            [None, pa.py_buffer(_decimal128_le(rng.integers(-(10**15), 10**15, n)))],
        ),
        pa.array(name.tolist(), pa.string()),
        pa.array(created_us, pa.timestamp("us", tz="UTC")),
        pa.array(rng.integers(0, 20_000, n, dtype=np.int32), pa.date32()),
        pa.array(rng.random(n) < 0.5),
        pa.array(payload.tolist(), pa.string()),
        pa.array(np.char.add("note ", suffix).tolist(), pa.string(), mask=nulls(0.3)),
        pa.array(rng.integers(0, 10**9, n), pa.int64(), mask=nulls(0.2)),
    ]
    return pa.RecordBatch.from_arrays(arrays, names=[c for c, _ in COLUMNS])


def _decimal128_le(unscaled: np.ndarray) -> bytes:
    """Little-endian 16-byte two's complement of each int64 unscaled value."""
    out = np.empty((len(unscaled), 2), dtype=np.int64)
    out[:, 0] = unscaled
    out[:, 1] = np.where(unscaled < 0, -1, 0)
    return out.tobytes()


def dataset(rows: int, seed: int = 483) -> list[pa.RecordBatch]:
    rng = np.random.default_rng(seed)
    batches = []
    for start in range(0, rows, BATCH_ROWS):
        batches.append(make_batch(rng, min(BATCH_ROWS, rows - start), start))
        if len(batches) % 20 == 0:
            guard()
    return batches


def guarded(batches: list[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    for i, b in enumerate(batches):
        if i % 16 == 0:
            guard()
        yield b


def timed(fn: Callable[[], tuple[int, int]], repeat: int) -> dict[str, Any]:
    runs = []
    for _ in range(repeat):
        w0, c0 = time.perf_counter(), time.process_time()
        rows, nbytes = fn()
        runs.append((time.perf_counter() - w0, time.process_time() - c0, rows, nbytes))
    best = min(runs, key=lambda r: r[0])
    return {
        "wall": best[0],
        "cpu": best[1],
        "rows": best[2],
        "bytes": best[3],
        "walls": [r[0] for r in runs],
    }


def run_python(
    schema: pa.Schema, batches: list[pa.RecordBatch], types: list[str], formats: list[int]
) -> tuple[int, int]:
    return python_encode_count(schema, guarded(batches), types, formats)


def run_rust(
    batches: list[pa.RecordBatch], bvtypes: list[str], formats: list[int]
) -> tuple[int, int]:
    total = [0]

    def write(b: bytes) -> None:
        total[0] += len(b)

    rows = pgwire_encoder.encode_stream(guarded(batches), bvtypes, write, formats)
    return rows, total[0]


def run_arrow_to_rows(
    schema: pa.Schema, batches: list[pa.RecordBatch], types: list[str]
) -> tuple[int, int]:
    """The Arrow -> Python row tuples step alone (stream_rows_from_arrow + ProvisaQueryResult.rows)."""
    qr = query_result(schema, guarded(batches), types, None)
    n = 0
    for _ in qr.rows():
        n += 1
    return n, 0


def profile_python(
    schema: pa.Schema, batches: list[pa.RecordBatch], types: list[str], formats: list[int], top: int
) -> str:
    prof = cProfile.Profile()
    prof.enable()
    python_encode_count(schema, iter(batches), types, formats)
    prof.disable()
    buf = io.StringIO()
    pstats.Stats(prof, stream=buf).sort_stats("tottime").print_stats(top)
    return buf.getvalue()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--py-repeat", type=int, default=3)
    ap.add_argument("--rs-repeat", type=int, default=5)
    ap.add_argument("--profile-rows", type=int, default=500_000)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    guard()
    t0 = time.perf_counter()
    batches = dataset(args.rows)
    schema = batches[0].schema
    types = [t for _, t in COLUMNS]
    arrow_bytes = sum(b.nbytes for b in batches)
    print(
        f"dataset: {args.rows:,} rows, {len(batches)} batches, {arrow_bytes / 1e6:.1f} MB Arrow, "
        f"built in {time.perf_counter() - t0:.1f}s, free memory {free_pct()}%",
        flush=True,
    )

    bvtypes = bvtype_names(query_result(schema, [], types, None))
    results: dict[str, Any] = {
        "rows": args.rows,
        "arrow_mb": arrow_bytes / 1e6,
        "bvtypes": bvtypes,
        "formats": {},
    }
    for label, fmt in (("text", 0), ("binary", 1)):
        formats = [fmt] * len(types)
        # the first two batches agree before anything is timed: every cell's bytes, a float
        # cell's value within the differential's tolerance
        py, _, _ = python_encode(schema, batches[:2], types, formats)
        rs = b"".join(pgwire_encoder.encode_batch(b, bvtypes, formats) for b in batches[:2])
        assert_same(py, rs, bvtypes, formats)
        del py, rs
        print(f"[{label}] first 2 batches agree; timing...", flush=True)
        rust = timed(lambda: run_rust(batches, bvtypes, formats), args.rs_repeat)
        print(
            f"[{label}] rust   best {rust['wall']:.3f}s wall {rust['cpu']:.3f}s cpu  runs {['%.3f' % w for w in rust['walls']]}",
            flush=True,
        )
        python = timed(lambda: run_python(schema, batches, types, formats), args.py_repeat)
        print(
            f"[{label}] python best {python['wall']:.3f}s wall {python['cpu']:.3f}s cpu  runs {['%.3f' % w for w in python['walls']]}",
            flush=True,
        )
        # float text may differ in length (PostgreSQL's digits, not repr's): rows must agree
        assert python["rows"] == rust["rows"] == args.rows
        results["formats"][label] = {"python": python, "rust": rust}

    to_rows = timed(lambda: run_arrow_to_rows(schema, batches, types), 1)
    results["arrow_to_rows"] = to_rows
    print(f"arrow->rows alone: {to_rows['wall']:.3f}s wall", flush=True)

    nprof = max(1, args.profile_rows // BATCH_ROWS)
    for label, fmt in (("text", 0), ("binary", 1)):
        prof = profile_python(schema, batches[:nprof], types, [fmt] * len(types), 25)
        results[f"profile_{label}"] = prof
        print(f"--- cProfile python {label}, {nprof * BATCH_ROWS:,} rows ---\n{prof}", flush=True)

    for label, r in results["formats"].items():
        py, rs = r["python"], r["rust"]
        print(
            f"{label}: {py['rows']:,} rows, {py['bytes'] / 1e6:.1f} MB out | python {py['wall']:.2f}s "
            f"({py['rows'] / py['wall'] / 1e6:.2f} Mrow/s, {py['bytes'] / py['wall'] / 1e6:.1f} MB/s) | rust {rs['wall']:.3f}s "
            f"({rs['rows'] / rs['wall'] / 1e6:.2f} Mrow/s, {rs['bytes'] / rs['wall'] / 1e6:.1f} MB/s) | speedup {py['wall'] / rs['wall']:.1f}x"
        )
    if args.out:
        with open(args.out, "w") as f:
            json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()
