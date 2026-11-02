# Copyright (c) 2026 Kenneth Stott
# Canary: 8ce2a64a-012a-4861-91e8-dcf16ebe7d10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where the Python GraphQL path serializes and deserializes, and what each costs, on the benchmark
dataset (bench.py). Every step that turns the engine's result into Python objects, or Python
objects into text and back, is timed on its own with the values and the call counts the product's
path gives it:

* the driver: DuckDB's DB-API rows (``stream_from_dbapi``, what ``run_sync`` returns for a result
  with no interval, date or timestamp column) against its Arrow reader;
* Arrow -> Python rows: ``to_pylist`` per column, ``_with_infinities``, the ``zip`` into tuples;
* inside ``serialize_rows``: ``_convert_value`` per column at the multiplicity
  ``_serialize_with_one_to_many`` calls it (every row for its root key, once more per new root for
  the object, every row for the element's values), the JSON column's ``json.loads``,
  ``_to_hashable``'s ``json.dumps(sort_keys=True)``, and the rest (tuples, dicts, hashing, the
  loop) as the remainder;
* orjson's render, with the calls it makes to ``_encode_unknown`` (``jsonable_encoder``) counted.

    PYTHONPATH=.:experiments/graphql_encoder_rs/build:experiments/graphql_encoder_rs \\
        ~/.provisa-agent/heavy.sh .venv/bin/python experiments/graphql_encoder_rs/breakdown.py
"""

from __future__ import annotations

import argparse
import gc
import json
import time
from collections.abc import Callable

import duckdb
import orjson
import pyarrow as pa

import bench
import python_ref as ref
from provisa.api import json_response
from provisa.core.ir_arrow import column_values
from provisa.executor.serialize import _convert_value, _to_hashable, serialize_rows
from provisa.federation.runtime_support import stream_from_dbapi


def clock(fn: Callable[[], object]) -> float:
    gc.collect()
    bench.guard()
    t = time.perf_counter()
    fn()
    return time.perf_counter() - t


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    schema, batches, orders = bench.dataset(args.rows)
    table = pa.Table.from_batches(batches)
    names = schema.names
    out: dict[str, float] = {}

    # --- the driver -------------------------------------------------------------------------
    con = duckdb.connect()
    con.register("t", table)

    def dbapi_rows() -> None:
        con.execute("SELECT * FROM t")  # a registered view is the connection's, not a cursor's
        for _ in stream_from_dbapi(con).iter_rows():
            pass

    def arrow_reader() -> None:
        con.execute("SELECT * FROM t")
        for _ in con.to_arrow_reader(bench.BATCH_ROWS):
            pass

    out["driver: DuckDB DB-API rows (fetchmany)"] = clock(dbapi_rows)
    out["driver: DuckDB Arrow reader (reference)"] = clock(arrow_reader)
    con.close()

    # --- Arrow -> Python rows ---------------------------------------------------------------
    total_rows = clock(lambda: ref.arrow_rows(schema, batches))
    out["arrow->rows: total (stream_rows_from_arrow, iter_rows)"] = total_rows
    pylist = 0.0
    for i, name in enumerate(names):
        t = clock(lambda: [b.column(i).to_pylist() for b in batches])
        cv = clock(lambda: [column_values(b.column(i)) for b in batches])
        out[f"arrow->rows: to_pylist {name}"] = t
        pylist += t
        if cv - t > 0.05:
            out[f"arrow->rows: _with_infinities {name}"] = cv - t
            pylist += cv - t
    out["arrow->rows: zip into tuples, batching (remainder)"] = total_rows - pylist

    # --- serialize_rows ---------------------------------------------------------------------
    rows = ref.arrow_rows(schema, batches)
    out["serialize_rows: total"] = clock(lambda: serialize_rows(rows, bench.COLUMNS, bench.ROOT))
    idx = {c.field_name: i for i, c in enumerate(bench.COLUMNS)}
    root_names = [c.field_name for c in bench.COLUMNS if c.nested_in is None]
    item_names = [c.field_name for c in bench.COLUMNS if c.nested_in is not None]
    new_root = [i == 0 or rows[i][0] != rows[i - 1][0] for i in range(len(rows))]
    root_rows = [r for r, n in zip(rows, new_root) if n]
    accounted = 0.0
    for name in root_names + item_names:
        j = idx[name]
        if name in root_names:
            # every row for the root key, each new root once more for its object
            t = clock(
                lambda: (
                    [_convert_value(r[j]) for r in rows],
                    [_convert_value(r[j]) for r in root_rows],
                )
            )
        else:
            t = clock(lambda: [_convert_value(r[j]) for r in rows])
        key = f"serialize_rows: _convert_value {name}"
        if name == "payload":
            key += " (json.loads + _recursive_json_convert)"
        out[key] = t
        accounted += t
    pj = idx["payload"]
    texts = [r[pj] for r in rows] + [r[pj] for r in root_rows]
    out["  of which json.loads alone (payload)"] = clock(lambda: [json.loads(s) for s in texts])
    parsed = [_convert_value(r[pj]) for r in rows]
    t = clock(lambda: [_to_hashable(v) for v in parsed])
    out["serialize_rows: _to_hashable json.dumps(sort_keys) payload"] = t
    accounted += t
    parsed.clear()
    texts.clear()
    others = [idx[n] for n in root_names + item_names if n != "payload"]
    t = clock(lambda: [_to_hashable(r[j]) for r in rows for j in others])
    out["serialize_rows: _to_hashable other columns"] = t
    accounted += t
    out["serialize_rows: tuples, dicts, hashing, loop (remainder)"] = (
        out["serialize_rows: total"] - accounted
    )

    # --- orjson -----------------------------------------------------------------------------
    shaped = ref.shape(rows, bench.COLUMNS, bench.ROOT, None)
    rows.clear()  # the shaped answer no longer needs them
    root_rows.clear()
    calls = [0]
    real = json_response._encode_unknown

    def counted(v):
        calls[0] += 1
        return real(v)

    body = {"data": {bench.ROOT: shaped}}
    out["orjson render (dumps)"] = clock(
        lambda: orjson.dumps(
            body, default=counted, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY
        )
    )
    out["_encode_unknown / jsonable_encoder calls"] = float(calls[0])

    for k, v in out.items():
        print(f"{k:72s} {v:10.2f}", flush=True)
    if args.out:
        with open(args.out, "w") as f:
            json.dump({"rows": args.rows, "orders": orders, "seconds": out}, f, indent=2)


if __name__ == "__main__":
    main()
