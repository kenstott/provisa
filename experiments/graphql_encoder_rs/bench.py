# Copyright (c) 2026 Kenneth Stott
# Canary: 9e7ae22f-960a-4baf-8845-e3021c9a4c1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Benchmark: each surface's Python path (python_ref, the exact-values contract) against its Rust
encoder, end to end from Arrow record batches to the response body, on J494's dataset.

The result is an orders query selecting each order's items (a one-to-many relationship): one flat
row per item, the order's columns repeated on each, as the compiler's LEFT JOIN returns them, for
GraphQL, REST and JSON:API (which sideloads the items as included resources). OData reads the same
data as its compiler writes an expansion: one row per order, its items one JSON value (the
correlated subquery's text). The dataset is generated once as Arrow (numpy-vectorized, seeded) in
65,536-row batches, the size DuckDB's Arrow reader streams. A surface runs in its own process; a
run aborts if the system's free memory drops under 25%.

    cd experiments/graphql_encoder_rs && PYTHONPATH=build/ref:build:. \\
        ~/.provisa-agent/heavy.sh ../../.venv/bin/python bench.py --surface graphql --rows 2000000
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import re
import resource
import subprocess
import time
from collections.abc import Callable
from typing import Any

import numpy as np
import pyarrow as pa

import graphql_encoder_rs as rs
import python_ref as ref
import wiring
from provisa.compiler.sql_types import ColumnRef

BATCH_ROWS = 65_536
MIN_FREE_PCT = 25
ROOT = "orders"


def C(field, nested_in=None, cardinality=None, json=False):
    return ColumnRef(None, field, field, nested_in, cardinality, json=json)


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
    C("payload", json=True),
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


def odata_dataset(schema, batches, rows: int):
    """The same orders as OData reads them: one row per order (its first item row's columns), its
    items the JSON array the expansion's subquery writes."""
    import pyarrow.compute as pc

    table = pa.Table.from_batches(batches, schema)
    ids = table["id"].to_numpy()
    first = np.flatnonzero(np.r_[True, ids[1:] != ids[:-1]])
    bounds = np.r_[first, rows]
    item_id = table["item_id"].to_numpy()
    sku = table["sku"].to_pylist()
    qty = table["qty"].to_numpy()
    price = [str(p) for p in table["price"].to_pylist()]
    shipped = pc.strftime(table["shipped"].cast(pa.timestamp("s")), "%Y-%m-%d").to_pylist()
    texts = []
    for o in range(len(first)):
        parts = []
        for i in range(bounds[o], bounds[o + 1]):
            day = "null" if shipped[i] is None else f'"{shipped[i]}"'
            parts.append(
                f'{{"item_id": {item_id[i]}, "sku": "{sku[i]}", "qty": {qty[i]}, "price": {price[i]}, "shipped": {day}}}'
            )
        texts.append("[" + ", ".join(parts) + "]")
    root = table.take(pa.array(first)).select(
        [c.field_name for c in COLUMNS if c.nested_in is None]
    )
    root = root.append_column("items", pa.array(texts, pa.string()))
    return root.schema, root.to_batches(max_chunksize=BATCH_ROWS)


def surface_runs(surface: str, schema, batches, rows: int):
    """(python thunk, rust thunk, what is encoded) for a surface."""
    plan = ref.plan_tuples(COLUMNS)
    if surface == "graphql":
        return (
            lambda: ref.encode(schema, batches, COLUMNS, ROOT),
            lambda: rs.encode_response(batches, plan, ROOT),
            f"{rows:,} item rows",
        )
    if surface == "rest":
        return (
            lambda: ref.rest(schema, batches, COLUMNS, ROOT),
            lambda: rs.encode_rest(batches, plan),
            f"{rows:,} item rows",
        )
    if surface == "jsonapi":
        # Every order on one page (the router caps a page at 1,000 resources; this measures the
        # encoder's rate), its items sideloaded.
        kw = {
            "gql_to_physical": {"customer": "customer_id"},
            "rel_gql_to_physical": {"items": "items"},
            "rel_scalar_gql_to_physical": {"items": {"item_id": "item_id"}},
        }
        page = rows
        return (
            lambda: ref.jsonapi(
                schema,
                batches,
                COLUMNS,
                gql_table=ROOT,
                table_name="orders",
                domain_id="sales",
                page_number=1,
                page_size=page,
                probe_rows=1,
                total_count=None,
                base_keys=["id"],
                physical_include_names=["items"],
                include_keys={"items": ["item_id"]},
                extra={"include": "items"},
                warnings=None,
                **kw,
            ),
            lambda: rs.encode_jsonapi(
                batches,
                plan,
                resource_type="orders",
                page_size=page,
                page_number=1,
                probe=True,
                total=None,
                id_columns=["id"],
                scalar_names=kw["gql_to_physical"],
                rel_names=list(kw["rel_gql_to_physical"].items()),
                rel_scalar_names=kw["rel_scalar_gql_to_physical"],
                includes=["items"],
                include_keys={"items": ["item_id"]},
                base_path="/data/jsonapi/sales/orders",
                link_params=[("include", "items")],
            ),
            f"{rows:,} item rows, items included",
        )
    if surface == "odata":
        from provisa.api.odata.edm import edm_type
        from provisa.api.odata.expand import Plan, page_link
        from provisa.api.odata.model import EntitySet, Navigation, Property
        from provisa.api.odata.query import QueryOptions

        oschema, obatches = odata_dataset(schema, batches, rows)
        types = {"id": "bigint", "customer": "integer", "total": "double", "ratio": "real",
                 "amount": "numeric(18,2)", "name": "varchar", "created": "timestamptz",
                 "day": "date", "active": "boolean", "payload": "jsonb", "note": "varchar"}  # fmt: skip
        item_types = {
            "item_id": "bigint",
            "sku": "varchar",
            "qty": "integer",
            "price": "numeric(10,2)",
            "shipped": "date",
        }

        def ent(name, t, key):
            props = tuple(Property(n, edm_type(d), n, d) for n, d in t.items())
            return EntitySet(name, 1, name, key, props, (), key)

        orders, items = ent("Orders", types, ("id",)), ent("Items", item_types, ("item_id",))
        nav = Navigation("items", "Items", "Items", True, ())
        plans = (Plan(nav, items, ("id",), QueryOptions(), (), tuple(item_types), (), 1000, 1),)
        fields = tuple(types)
        names = list(oschema.names)
        n = sum(b.num_rows for b in obatches)
        root = "https://h.example/odata/v4"
        context = f"{root}/$metadata#Orders"
        return (
            lambda: ref.odata(
                oschema,
                obatches,
                names,
                entity=orders,
                fields=fields,
                plans=plans,
                context=context,
                count=None,
                page=n,
                root=root,
                items=(),
                options=QueryOptions(),
                warnings=None,
            ),
            lambda: rs.encode_odata(
                obatches,
                names,
                wiring.odata_entity(orders, fields, plans),
                context=context,
                count=None,
                page=n,
                next_link=page_link(f"{root}/Orders", (), QueryOptions(), n),
                root=root,
            ),
            f"{n:,} order rows, {rows:,} items expanded",
        )
    raise SystemExit(f"no surface {surface!r}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--surface", required=True, choices=["graphql", "rest", "jsonapi", "odata"])
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--py-repeat", type=int, default=3)
    ap.add_argument("--rs-repeat", type=int, default=5)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    guard()
    t0 = time.perf_counter()
    schema, batches, orders = dataset(args.rows)
    arrow_mb = sum(b.nbytes for b in batches) / 1e6
    py_fn, rs_fn, what = surface_runs(args.surface, schema, batches, args.rows)
    print(
        f"{args.surface}: {what}, {orders:,} orders, {arrow_mb:.1f} MB Arrow (flat), built in "
        f"{time.perf_counter() - t0:.1f}s, free memory {free_pct()}%, peak rss {rss_mb():.0f} MB",
        flush=True,
    )

    rust = timed(rs_fn, args.rs_repeat)
    rust["peak_rss_mb"] = rss_mb()
    print(
        f"rust   best {rust['wall']:.3f}s wall {rust['cpu']:.3f}s cpu runs "
        f"{['%.3f' % w for w in rust['walls']]} peak rss {rust['peak_rss_mb']:.0f} MB "
        f"free {free_pct()}%",
        flush=True,
    )
    python = timed(py_fn, args.py_repeat)
    python["peak_rss_mb"] = rss_mb()
    print(
        f"python best {python['wall']:.3f}s wall {python['cpu']:.3f}s cpu runs "
        f"{['%.3f' % w for w in python['walls']]} peak rss {python['peak_rss_mb']:.0f} MB "
        f"free {free_pct()}%",
        flush=True,
    )
    assert python["sha256"] == rust["sha256"] and python["bytes"] == rust["bytes"], "bodies differ"
    print(f"full bodies identical: sha256 {rust['sha256']}", flush=True)

    mb = rust["bytes"] / 1e6
    py, r = python, rust
    print(
        f"SUMMARY {args.surface} {what}, {mb:.1f} MB body | python {py['wall']:.2f}s "
        f"({args.rows / py['wall'] / 1e6:.3f} Mrow/s, {mb / py['wall']:.1f} MB/s) | rust {r['wall']:.3f}s "
        f"({args.rows / r['wall'] / 1e6:.2f} Mrow/s, {mb / r['wall']:.1f} MB/s) | "
        f"speedup {py['wall'] / r['wall']:.1f}x"
    )
    if args.out:
        with open(args.out, "w") as f:
            json.dump({"surface": args.surface, "what": what, "rows": args.rows, "orders": orders,
                       "arrow_mb": arrow_mb, "python": python, "rust": rust}, f, indent=2)  # fmt: skip


if __name__ == "__main__":
    main()
