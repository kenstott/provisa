# Copyright (c) 2026 Kenneth Stott
# Canary: 8c3e1f72-0a4d-4b96-b5e8-27d9c6a0f143
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Benchmark: the product's Python gRPC encode path against the Rust encoder, end to end from
Arrow record batches to serialized response messages, over a large mixed-type result.

Three shapes, all from the same Arrow batches:

- ``query``: Query{Type}, one {Type} message per row (the server-streaming RPC's messages).
- ``batch``: Query{Type}Batch, {Type}Batch messages of 5,000 rows (_GRPC_BATCH_ROWS).
- ``nested``: the same rows with a many-to-one relation as a struct column (one nesting level),
  through the product's nested-relationship encoder (_dict_row_to_message).

The dataset is generated once (numpy, seeded) in 65,536-row batches, the batch size DuckDB's
Arrow reader streams. Output is counted, never kept. A run aborts if the system's free memory
drops under 25%.

    PYTHONPATH=.:experiments/grpc_encoder_rs/build:experiments/grpc_encoder_rs \\
        ~/.provisa-agent/heavy.sh .venv/bin/python experiments/grpc_encoder_rs/bench.py --rows 2000000
"""

from __future__ import annotations

import argparse
import cProfile
import io
import pstats
import re
import subprocess
import time
from collections.abc import Callable
from typing import Any

import numpy as np
import pyarrow as pa

from python_ref import Service, python_encode, row_messages, rust_encode, rust_encoder
from provisa.federation.runtime_support import stream_rows_from_arrow

BATCH_ROWS = 65_536
GRPC_BATCH_ROWS = 5_000
MIN_FREE_PCT = 25

_WORDS = np.array(["alpha", "beta", "gamma", "épsilon", "zeta", "théta", "iota", "日本", "lambda"])


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


def _decimal128_le(unscaled: np.ndarray) -> bytes:
    out = np.empty((len(unscaled), 2), dtype=np.int64)
    out[:, 0] = unscaled
    out[:, 1] = np.where(unscaled < 0, -1, 0)
    return out.tobytes()


def make_batch(rng: np.random.Generator, n: int, start: int, nested: bool) -> pa.RecordBatch:
    def nulls(p: float) -> np.ndarray:
        return rng.random(n) < p

    words = _WORDS[rng.integers(0, len(_WORDS), n)]
    suffix = rng.integers(0, 1_000_000, n).astype(str)
    created = rng.integers(946_684_800_000_000, 1_893_456_000_000_000, n)  # 2000..2030
    payload = np.char.add(np.char.add('{"k": ', suffix), ', "tag": "x"}')
    columns: dict[str, pa.Array] = {
        "id": pa.array(np.arange(start, start + n, dtype=np.int64)),
        "qty": pa.array(rng.integers(-(2**31), 2**31 - 1, n, dtype=np.int32)),
        "price": pa.array(rng.normal(1000.0, 250.0, n)),
        "ratio": pa.array(rng.random(n, dtype=np.float32)),
        "amount": pa.Array.from_buffers(
            pa.decimal128(18, 2),
            n,
            [None, pa.py_buffer(_decimal128_le(rng.integers(-(10**15), 10**15, n)))],
        ),
        "name": pa.array(np.char.add(np.char.add(words, "-"), suffix).tolist(), pa.string()),
        "created": pa.array(created, pa.timestamp("us")),
        "created_tz": pa.array(created + 3_600_000_000, pa.timestamp("us", tz="UTC")),
        "day": pa.array(rng.integers(0, 20_000, n, dtype=np.int32), pa.date32()),
        "active": pa.array(rng.random(n) < 0.5),
        "payload": pa.array(payload.tolist(), pa.string()),
        "secret": pa.array(np.char.add("s-", suffix).tolist(), pa.string(), mask=nulls(0.3)),
        "customer_id": pa.array(rng.integers(0, 10**9, n), pa.int64(), mask=nulls(0.2)),
    }
    if nested:
        cid = rng.integers(0, 10**9, n)
        columns["customers"] = pa.StructArray.from_arrays(
            [
                pa.array(cid, pa.int64()),
                pa.array(np.char.add("cust-", cid.astype(str)).tolist(), pa.string()),
                pa.array(
                    rng.integers(946_684_800_000_000, 1_700_000_000_000_000, n),
                    pa.timestamp("us", tz="UTC"),
                ),
            ],
            names=["id", "name", "joined"],
            mask=pa.array(nulls(0.1)),
        )
    return pa.RecordBatch.from_pydict(columns)


def dataset(rows: int, nested: bool, seed: int = 495) -> list[pa.RecordBatch]:
    rng = np.random.default_rng(seed)
    batches = []
    for start in range(0, rows, BATCH_ROWS):
        batches.append(make_batch(rng, min(BATCH_ROWS, rows - start), start, nested))
        if len(batches) % 10 == 0:
            guard()
    return batches


# ---- the timed paths: each returns (messages, bytes) -------------------------------------------


def py_run(
    svc: Service, batches: list[pa.RecordBatch], mode: str, batch_rows: int
) -> tuple[int, int]:
    count = nbytes = 0
    if not batch_rows:
        for m in row_messages(svc.wire, "Orders", batches[0].schema, batches, mode):
            b = m.SerializeToString()
            count += 1
            nbytes += len(b)
        return count, nbytes
    for b in python_encode(
        svc.wire, "Orders", batches[0].schema, batches, mode=mode, batch_rows=batch_rows
    ):
        count += 1
        nbytes += len(b)
    return count, nbytes


def rs_run(
    svc: Service, batches: list[pa.RecordBatch], mode: str, batch_rows: int
) -> tuple[int, int]:
    """One bytes object per message, as a gRPC Python handler yields them."""
    enc = rust_encoder(svc, "Orders", batches[0].schema, mode=mode, batch_rows=batch_rows)
    count = nbytes = 0
    for b in batches:
        msgs = enc.encode(b)
        count += len(msgs)
        nbytes += sum(map(len, msgs))
    msgs = enc.finish()
    return count + len(msgs), nbytes + sum(map(len, msgs))


def rs_framed_run(
    svc: Service, batches: list[pa.RecordBatch], mode: str, batch_rows: int
) -> tuple[int, int]:
    """One buffer of gRPC length-prefixed frames per Arrow batch; counts message bytes only."""
    enc = rust_encoder(svc, "Orders", batches[0].schema, mode=mode, batch_rows=batch_rows)
    frames = 0
    for b in batches:
        frames += len(enc.encode_framed(b))
    frames += len(enc.finish_framed())
    return -1, frames


def best(
    fn: Callable[[], tuple[int, int]], runs: int
) -> tuple[float, float, list[float], tuple[int, int]]:
    walls, cpus, out = [], [], (0, 0)
    for _ in range(runs):
        guard()
        w0, c0 = time.perf_counter(), time.process_time()
        out = fn()
        walls.append(time.perf_counter() - w0)
        cpus.append(time.process_time() - c0)
    k = int(np.argmin(walls))
    return walls[k], cpus[k], walls, out


def profile(fn: Callable[[], Any], top: int = 18) -> str:
    pr = cProfile.Profile()
    pr.enable()
    fn()
    pr.disable()
    s = io.StringIO()
    pstats.Stats(pr, stream=s).sort_stats("tottime").print_stats(top)
    return s.getvalue()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--py-runs", type=int, default=3)
    ap.add_argument("--rs-runs", type=int, default=5)
    ap.add_argument("--profile-rows", type=int, default=200_000)
    args = ap.parse_args()

    svc = Service()
    print(f"free memory {free_pct()}%")
    for shape, mode, batch_rows, nested in (
        ("query", "row", 0, False),
        ("batch", "row", GRPC_BATCH_ROWS, False),
        ("nested", "node", 0, True),
    ):
        batches = dataset(args.rows, nested)
        arrow_mb = sum(b.nbytes for b in batches) / 1e6
        rows = sum(b.num_rows for b in batches)
        # the same messages before timing (the first two batches)
        head = batches[:2]
        same = python_encode(
            svc.wire, "Orders", head[0].schema, head, mode=mode, batch_rows=batch_rows
        )
        assert same == rust_encode(
            svc, "Orders", head[0].schema, head, mode=mode, batch_rows=batch_rows
        )
        print(
            f"\n== {shape}: {rows:,} rows, {len(batches)} Arrow batches, {arrow_mb:.1f} MB of Arrow, free {free_pct()}%"
        )

        # where the Python time goes: Arrow -> rows alone, then rows -> messages
        w0 = time.perf_counter()
        stream = stream_rows_from_arrow(batches[0].schema, iter(batches))
        n = sum(len(b) for b in stream.batches())
        to_rows = time.perf_counter() - w0
        print(f"python Arrow -> row tuples alone: {to_rows:.2f} s ({n:,} rows)")

        pw, pc, pall, (pm, pb) = best(lambda: py_run(svc, batches, mode, batch_rows), args.py_runs)
        rw, rc, rall, (rm, rb) = best(lambda: rs_run(svc, batches, mode, batch_rows), args.rs_runs)
        fw, fc, fall, (_, fb) = best(
            lambda: rs_framed_run(svc, batches, mode, batch_rows), args.rs_runs
        )
        assert (pm, pb) == (rm, rb), f"python {(pm, pb)} != rust {(rm, rb)}"
        assert fb == rb + 5 * rm, "framed size"
        mb = pb / 1e6
        print(f"messages {pm:,}, message bytes {mb:.1f} MB")
        for label, w, c, alls in (
            ("python", pw, pc, pall),
            ("rust list", rw, rc, rall),
            ("rust framed", fw, fc, fall),
        ):
            print(
                f"{label:12s} wall {w:8.3f} s cpu {c:8.3f} s  {rows / w / 1e6:7.3f} Mrow/s  {mb / w:8.1f} MB/s"
                f"  speedup {pw / w:6.1f}x  runs {', '.join(f'{x:.3f}' for x in alls)}"
            )
        if args.profile_rows:
            sample = batches[: max(1, args.profile_rows // BATCH_ROWS)]
            print(f"-- cProfile of the Python path over {sum(b.num_rows for b in sample):,} rows")
            print(profile(lambda: py_run(svc, sample, mode, batch_rows)))


if __name__ == "__main__":
    main()
