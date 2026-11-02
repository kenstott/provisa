# Copyright (c) 2026 Kenneth Stott
# Canary: 9e7ae22f-960a-4baf-8845-e3021c9a4c1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Benchmark: the product's GraphQL JSON path against the Rust encoder, end to end from Arrow record
batches to the response body, over a large mixed-type result with one level of nesting.

The result is an orders query selecting each order's items (a one-to-many relationship): one flat
row per item, the order's columns repeated on each, as the compiler's LEFT JOIN returns them.
``serialize_rows`` nests them (``_serialize_with_one_to_many``) and orjson writes the body. The
dataset is generated once as Arrow (numpy-vectorized, seeded) in 65,536-row batches, the size
DuckDB's Arrow reader streams. The GraphQL path buffers the whole result and its answer, so peak
memory is the Python path's: a run aborts if the system's free memory drops under 25%.

    PYTHONPATH=.:experiments/graphql_encoder_rs/build:experiments/graphql_encoder_rs \\
        ~/.provisa-agent/heavy.sh .venv/bin/python experiments/graphql_encoder_rs/bench.py --rows 2000000
"""

from __future__ import annotations

import argparse
import cProfile
import gc
import hashlib
import io
import json
import pstats
import re
import resource
import subprocess
import time
from collections.abc import Callable
from typing import Any

import numpy as np
import pyarrow as pa

import graphql_encoder_rs
import python_ref as ref
from provisa.compiler.sql_types import ColumnRef

BATCH_ROWS = 65_536
MIN_FREE_PCT = 25
ROOT = "orders"


def C(field, nested_in=None, cardinality=None):
    return ColumnRef(None, field, field, nested_in, cardinality)


O2M = "one-to-many"
#: The selection plan: the order's columns at the root, the item's under ``items``.
COLUMNS: list[ColumnRef] = [
    C("id"),
    C("customer"),
    C("total"),
    C("ratio"),
    C("amount"),
    C("name"),
    C("created"),
    C("day"),
    C("active"),
    C("payload"),
    C("note"),
    C("item_id", "items", O2M),
    C("sku", "items", O2M),
    C("qty", "items", O2M),
    C("price", "items", O2M),
    C("shipped", "items", O2M),
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


def rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6  # bytes on macOS


def _decimal128_le(unscaled: np.ndarray) -> bytes:
    out = np.empty((len(unscaled), 2), dtype=np.int64)
    out[:, 0] = unscaled
    out[:, 1] = np.where(unscaled < 0, -1, 0)
    return out.tobytes()


def _dec(t: pa.DataType, unscaled: np.ndarray, mask=None) -> pa.Array:
    a = pa.Array.from_buffers(t, len(unscaled), [None, pa.py_buffer(_decimal128_le(unscaled))])
    if mask is None:
        return a
    return pa.compute.if_else(pa.array(mask), pa.scalar(None, t), a)


def dataset(rows: int, seed: int = 494) -> tuple[pa.Schema, list[pa.RecordBatch], int]:
    """``rows`` item rows over orders of 1 to 7 items each."""
    rng = np.random.default_rng(seed)
    sizes = rng.integers(1, 8, rows)  # more than enough orders
    order_of_row = np.repeat(np.arange(len(sizes)), sizes)[:rows]
    orders = int(order_of_row[-1]) + 1
    o = order_of_row

    def per_order(v: np.ndarray) -> np.ndarray:
        return v[o]

    words = _WORDS[rng.integers(0, len(_WORDS), orders)]
    suffix = rng.integers(0, 1_000_000, orders).astype(str)
    name = per_order(np.char.add(np.char.add(words, "-"), suffix))
    payload = per_order(
        np.char.add(
            np.char.add('{"k": ', rng.integers(0, 10_000, orders).astype(str)),
            ', "tag": "x", "w": [1.5, 2]}',
        )
    )
    note_null = per_order(rng.random(orders) < 0.3)
    note = per_order(np.char.add("note ", suffix))
    cols: dict[str, pa.Array] = {
        "id": pa.array(o.astype(np.int64)),
        "customer": pa.array(per_order(rng.integers(0, 100_000, orders, dtype=np.int32))),
        "total": pa.array(per_order(rng.normal(1000.0, 250.0, orders))),
        "ratio": pa.array(per_order(rng.random(orders, dtype=np.float32))),
        "amount": _dec(pa.decimal128(18, 2), per_order(rng.integers(-(10**12), 10**12, orders))),
        "name": pa.array(name.tolist(), pa.string()),
        "created": pa.array(
            per_order(rng.integers(946_684_800_000_000, 1_893_456_000_000_000, orders)),
            pa.timestamp("us", tz="UTC"),
        ),
        "day": pa.array(per_order(rng.integers(0, 20_000, orders, dtype=np.int32)), pa.date32()),
        "active": pa.array(per_order(rng.random(orders) < 0.5)),
        "payload": pa.array(payload.tolist(), pa.string()),
        "note": pa.array(note.tolist(), pa.string(), mask=note_null),
        "item_id": pa.array(np.arange(rows, dtype=np.int64)),
        "sku": pa.array(
            np.char.add("SKU-", rng.integers(0, 50_000, rows).astype(str)).tolist(), pa.string()
        ),
        "qty": pa.array(rng.integers(1, 20, rows, dtype=np.int32)),
        "price": _dec(pa.decimal128(10, 2), rng.integers(1, 10**7, rows)),
        "shipped": pa.array(
            rng.integers(0, 20_000, rows, dtype=np.int32), pa.date32(), mask=rng.random(rows) < 0.2
        ),
    }
    table = pa.table(cols)
    batches = table.to_batches(max_chunksize=BATCH_ROWS)
    return table.schema, batches, orders


def timed(fn: Callable[[], bytes], repeat: int) -> dict[str, Any]:
    runs, digest, nbytes = [], None, 0
    for _ in range(repeat):
        gc.collect()
        guard()
        w0, c0 = time.perf_counter(), time.process_time()
        body = fn()
        runs.append((time.perf_counter() - w0, time.process_time() - c0))
        d = hashlib.sha256(body).hexdigest()
        assert digest in (None, d)
        digest, nbytes = d, len(body)
        del body
    best = min(runs)
    return {
        "wall": best[0],
        "cpu": best[1],
        "walls": [r[0] for r in runs],
        "sha256": digest,
        "bytes": nbytes,
    }


def python_stages(schema, batches) -> dict[str, float]:
    """The Python path split into its three stages, one run."""
    gc.collect()
    t0 = time.perf_counter()
    rows = ref.arrow_rows(schema, batches)
    t1 = time.perf_counter()
    shaped = ref.shape(rows, COLUMNS, ROOT, None)
    t2 = time.perf_counter()
    body = ref.render(ROOT, shaped)
    t3 = time.perf_counter()
    del rows, shaped, body
    return {"arrow_to_rows": t1 - t0, "serialize_rows": t2 - t1, "orjson_render": t3 - t2}


def profile_python(schema, batches, top: int) -> str:
    prof = cProfile.Profile()
    prof.enable()
    ref.encode(schema, batches, COLUMNS, ROOT)
    prof.disable()
    buf = io.StringIO()
    pstats.Stats(prof, stream=buf).sort_stats("tottime").print_stats(top)
    return buf.getvalue()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--py-repeat", type=int, default=3)
    ap.add_argument("--rs-repeat", type=int, default=5)
    ap.add_argument("--profile-rows", type=int, default=262_144)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    guard()
    t0 = time.perf_counter()
    schema, batches, orders = dataset(args.rows)
    arrow_mb = sum(b.nbytes for b in batches) / 1e6
    print(
        f"dataset: {args.rows:,} item rows, {orders:,} orders, {len(batches)} batches, "
        f"{arrow_mb:.1f} MB Arrow, built in {time.perf_counter() - t0:.1f}s, "
        f"free memory {free_pct()}%, peak rss {rss_mb():.0f} MB",
        flush=True,
    )
    plan = ref.plan_tuples(COLUMNS)

    head = batches[:2]
    assert ref.encode(schema, head, COLUMNS, ROOT) == graphql_encoder_rs.encode_response(
        head, plan, ROOT
    )
    print("first 2 batches byte-identical; timing...", flush=True)

    rust = timed(lambda: graphql_encoder_rs.encode_response(batches, plan, ROOT), args.rs_repeat)
    rust["peak_rss_mb"] = rss_mb()
    print(
        f"rust   best {rust['wall']:.3f}s wall {rust['cpu']:.3f}s cpu runs "
        f"{['%.3f' % w for w in rust['walls']]} peak rss {rust['peak_rss_mb']:.0f} MB "
        f"free {free_pct()}%",
        flush=True,
    )
    python = timed(lambda: ref.encode(schema, batches, COLUMNS, ROOT), args.py_repeat)
    python["peak_rss_mb"] = rss_mb()
    print(
        f"python best {python['wall']:.3f}s wall {python['cpu']:.3f}s cpu runs "
        f"{['%.3f' % w for w in python['walls']]} peak rss {python['peak_rss_mb']:.0f} MB "
        f"free {free_pct()}%",
        flush=True,
    )
    assert python["sha256"] == rust["sha256"] and python["bytes"] == rust["bytes"], "bodies differ"
    print(f"full bodies identical: sha256 {rust['sha256']}", flush=True)

    stages = python_stages(schema, batches)
    print("python stages: " + ", ".join(f"{k} {v:.2f}s" for k, v in stages.items()), flush=True)

    nprof = max(1, args.profile_rows // BATCH_ROWS)
    prof = profile_python(schema, batches[:nprof], 30)
    print(
        f"--- cProfile python, {sum(b.num_rows for b in batches[:nprof]):,} rows ---\n{prof}",
        flush=True,
    )

    mb = rust["bytes"] / 1e6
    py, rs = python, rust
    print(
        f"SUMMARY {args.rows:,} rows ({orders:,} orders), {mb:.1f} MB body | python {py['wall']:.2f}s "
        f"({args.rows / py['wall'] / 1e6:.3f} Mrow/s, {mb / py['wall']:.1f} MB/s) | rust {rs['wall']:.3f}s "
        f"({args.rows / rs['wall'] / 1e6:.2f} Mrow/s, {mb / rs['wall']:.1f} MB/s) | "
        f"speedup {py['wall'] / rs['wall']:.1f}x"
    )
    if args.out:
        with open(args.out, "w") as f:
            json.dump(
                {
                    "rows": args.rows,
                    "orders": orders,
                    "arrow_mb": arrow_mb,
                    "python": python,
                    "rust": rust,
                    "python_stages": stages,
                    "profile": prof,
                },
                f,
                indent=2,
            )


if __name__ == "__main__":
    main()
