# Copyright (c) 2026 Kenneth Stott
# Canary: 4f2d9a61-8c37-4b0e-a5d2-e17c3b6f90a8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Spark connector's validator: a script's dry run and its resolvers' keys (REQ-2225).

The code host's front end runs this module under the code's own Python, the script loaded as the
host loads it (provisa/python/host.py ``load``), when Provisa asks it to ``validate`` a script
(provisa/codehost/front.py). The script's static check is the reader's document, which the front
end reads first; this is the two steps that need the cluster.

It reads one JSON request on stdin and writes one JSON report on stdout:

    {values,  -- the connector's setup values, its cluster among them
     calls: [{function, kind: "table"|"command", args, names, takes, context, schema, sample}],
     match: [{function, source_key, target_key}]}

DRY RUN. Each call runs its function once, as the host would run it, its table arguments read over
Arrow Flight as the caller (``TableRef.read``, a sample of each, which Provisa bounds), and takes
at most ``sample`` of its rows: a Spark DataFrame by ``limit``, other rows as they come. Its rows
are compared with ``schema``, the declared row type as Arrow: a declared column no row carries, or
a value that does not convert to its column's type, is a mismatch. A call with no ``schema``
returns a single value, which is reported, not compared.

KEYS. A two-table ``@query`` used to match rows (REQ-2249) carries the key of each side in every
row it returns: each of ``match`` is checked on its function's dry-run rows, each key a column of
every row and null in none.
"""

# Requirements: REQ-2225, REQ-2249
from __future__ import annotations

import asyncio
import base64
import importlib
import inspect
import itertools
import json
import os
import sys
from typing import Any

OK, MISMATCH, ERROR, EMPTY, NOT_RUN = "ok", "mismatch", "error", "empty", "not_run"
#: The module the host loads a connector's script as (provisa/python/host.py).
SCRIPT_MODULE = "connector_script"


def _failed(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _host() -> Any:  # provisa/python/host.py, beside which the front end runs this
    return importlib.import_module("host")


def _schema(text: str) -> Any:  # a pyarrow Schema
    import pyarrow as pa

    return pa.ipc.open_stream(pa.py_buffer(base64.b64decode(text))).schema


def _is_dataframe(value: Any) -> bool:
    """A Spark DataFrame (classic or Spark Connect): it is sampled on the cluster."""
    return callable(getattr(value, "limit", None)) and callable(getattr(value, "toArrow", None))


async def _taken(value: Any, sample: int) -> list:
    out: list = []
    async for row in value:
        if len(out) >= sample:
            break
        out.append(row)
    return out


def _sampled(result: Any, sample: int) -> Any:
    """At most ``sample`` of the rows ``result`` holds: a pyarrow Table for Arrow and a Spark
    DataFrame, else a list of the rows as the code made them."""
    import pyarrow as pa

    if _is_dataframe(result):
        return result.limit(sample).toArrow()
    if isinstance(result, pa.RecordBatchReader):
        batches, left = [], sample
        for batch in result:
            if left <= 0:
                break
            batches.append(batch.slice(0, left))
            left -= batch.num_rows
        return pa.Table.from_batches(batches, schema=result.schema)
    if isinstance(result, pa.RecordBatch):
        return pa.Table.from_batches([result]).slice(0, sample)
    if isinstance(result, pa.Table):
        return result.slice(0, sample)
    if hasattr(result, "__aiter__"):
        return asyncio.run(_taken(result, sample))
    if isinstance(result, (list, tuple)) and not hasattr(result, "_asdict"):
        return list(result[:sample])
    if hasattr(result, "__iter__") and not isinstance(result, (dict, str, bytes)):
        return list(itertools.islice(iter(result), sample))
    raise TypeError(f"a {type(result).__name__} is not a collection of rows")


def _arguments(host: Any, fn: Any, call: dict) -> tuple[list, dict]:
    """The function's arguments as the host makes them (provisa/python/host.py ``serve``)."""
    args = host.from_wire(call["args"])
    names = call["names"]
    if call["kind"] == "table" and call.get("takes") is not None:
        context = host.from_wire(call.get("context"))
        return host._inputs_and_context(fn, call["takes"], args, names, context), {}
    positional, keywords = host._call_arguments(args, names)
    if call["kind"] == "command":
        positional, keywords = host._with_table_refs(fn, positional, keywords)
    return positional, keywords


def _invoked(fn: Any, args: list, keywords: dict) -> Any:
    if inspect.iscoroutinefunction(fn):
        return asyncio.run(fn(*args, **keywords))
    return fn(*args, **keywords)


def _declared(schema: Any) -> list[dict]:
    return [{"name": f.name, "type": str(f.type)} for f in schema]


def _compare_arrow(table: Any, schema: Any) -> tuple[list[dict], list[str], list[dict]]:
    """The table's columns, each mismatch with the declared ``schema``, and its rows."""
    actual = [{"name": f.name, "type": str(f.type)} for f in table.schema]
    missing = [n for n in schema.names if n not in table.column_names]
    mismatches = [f"the declared column {n} is not among the rows' columns" for n in missing]
    if not missing:
        try:
            table = table.select(schema.names).cast(schema)
        except Exception as exc:  # noqa: BLE001 -- pyarrow's own words name the column
            mismatches.append(f"the rows do not convert to the declared types: {_failed(exc)}")
    return actual, mismatches, table.to_pylist()


def _type_name(values: list) -> str:
    seen = sorted({type(v).__name__ for v in values if v is not None})
    return " | ".join(seen) if seen else "null"


def _compare_rows(host: Any, rows: list, schema: Any) -> tuple[list[dict], list[str], list[dict]]:
    """The rows' columns (each the Python types its values have), each mismatch with the
    declared ``schema``, and the rows as plain dicts."""
    import pyarrow as pa

    plain = []
    for row in rows:
        made = host._plain(row)
        if not isinstance(made, dict):
            raise TypeError(f"a {type(row).__name__} is not a row")
        plain.append(made)
    names = list(dict.fromkeys(k for row in plain for k in row))
    actual = [{"name": n, "type": _type_name([r.get(n) for r in plain])} for n in names]
    mismatches = [
        f"the declared column {n} is not among the rows' columns"
        for n in schema.names
        if plain and n not in names
    ]
    fields = [schema.field(i) for i in range(len(schema))]
    try:
        pa.RecordBatch.from_pylist(
            [{f.name: host._for_field(r.get(f.name), f) for f in fields} for r in plain],
            schema=schema,
        )
    except Exception as exc:  # noqa: BLE001 -- pyarrow's own words name the column
        mismatches.append(f"the rows do not convert to the declared types: {_failed(exc)}")
    return actual, mismatches, plain


def dry_run(module: Any, call: dict) -> tuple[dict, list[dict] | None]:
    """One function's dry run, and its rows (None when it did not run to an end)."""
    host = _host()
    name, sample = call["function"], int(call["sample"])
    report: dict[str, Any] = {"function": name, "rows": 0}
    fn = getattr(module, name, None)
    if not inspect.isfunction(fn):
        return {**report, "status": ERROR, "error": f"the script has no function {name}"}, None
    try:
        result = _invoked(fn, *_arguments(host, fn, call))
        if call.get("schema") is None:
            value = host.to_wire(result)
            return {
                **report,
                "status": OK,
                "rows": 0 if result is None else 1,
                "actual": [{"name": "result", "type": type(result).__name__}],
                "value": value,
            }, None
        schema = _schema(call["schema"])
        report["declared"] = _declared(schema)
        taken = _sampled(result, sample)
        if isinstance(taken, list):
            actual, mismatches, rows = _compare_rows(host, taken, schema)
            compared = bool(taken)
        else:
            actual, mismatches, rows = _compare_arrow(taken, schema)
            compared = True
    except Exception as exc:  # noqa: BLE001 -- the script's own failure, reported by name
        return {**report, "status": ERROR, "error": _failed(exc)}, None
    status = MISMATCH if mismatches else (OK if compared else EMPTY)
    return {
        **report,
        "status": status,
        "rows": len(rows),
        "actual": actual,
        "mismatches": mismatches,
    }, rows


def keys(check: dict, rows: list[dict] | None) -> dict:
    """Whether the dry-run ``rows`` of a function used to match rows carry both keys."""
    source, target = check["source_key"], check["target_key"]
    report: dict[str, Any] = {
        "function": check["function"],
        "source_key": source,
        "target_key": target,
    }
    if rows is None:
        return {**report, "status": NOT_RUN, "error": "its dry run did not return rows"}
    if not rows:
        return {**report, "status": EMPTY, "rows": 0}
    problems = []
    for key in (source, target):
        absent = sum(1 for r in rows if key not in r)
        nulls = sum(1 for r in rows if key in r and r[key] is None)
        if absent:
            problems.append(f"{absent} of {len(rows)} rows carry no {key}")
        if nulls:
            problems.append(f"{nulls} of {len(rows)} rows have a null {key}")
    return {
        **report,
        "status": MISMATCH if problems else OK,
        "rows": len(rows),
        "problems": problems,
    }


def validate(module: Any, request: dict) -> dict:
    sys.modules["provisa_services"]._set_values(request["values"])
    ran: dict[str, list[dict] | None] = {}
    reports = []
    for call in request["calls"]:
        report, rows = dry_run(module, call)
        reports.append(report)
        ran[call["function"]] = rows
    matched = [keys(check, ran.get(check["function"])) for check in request["match"]]
    return {"dry_run": reports, "match": matched}


def main() -> int:
    """Run under the front end, after the host's ``load`` loaded the script."""
    request = json.loads(sys.stdin.read())
    # What the script prints goes to stderr; the report is stdout's alone.
    out = os.dup(1)
    os.dup2(2, 1)
    report = validate(sys.modules[SCRIPT_MODULE], request)
    with os.fdopen(out, "w") as stdout:
        json.dump(report, stdout)
    return 0
