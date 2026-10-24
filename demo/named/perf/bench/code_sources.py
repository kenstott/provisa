# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Code sources' throughput (REQ-2100): rows a second, recorded, with no floor.

    .venv/bin/python3 demo/named/perf/bench/code_sources.py --rows 10000000 --output-dir results

Two paths, each over the same Python generator of ``--rows`` rows:

- ``direct``: the function's rows through the local worker into the bounded DuckDB (``residual``),
  a statement applied and its answer read a batch at a time -- what a dev-mode source's direct
  read does;
- ``pgwire``: the same statement over pgwire, through a built image's front end (``front.py``,
  ``pgfront``), the way an engine that attaches the host reads it.

Each path's figure is written to ``<output-dir>/code_sources.json``: rows, seconds, rows a second,
the statement and the DuckDB bounds. Nothing is asserted about the numbers; a run is a record."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path
from typing import Any, LiteralString, cast

import pyarrow as pa

CODE = textwrap.dedent(
    '''
    from collections.abc import Iterator
    from dataclasses import dataclass


    @dataclass
    class Row:
        n: int
        s: str


    @dataclass
    class ManyInputs:
        n: int


    def many(inputs: ManyInputs) -> Iterator[Row]:
        """@table"""
        for i in range(inputs.n):
            yield Row(i, f"{(i * 7919) % 1000003:07d}-row")
    '''
)
STATEMENT = "SELECT n, s FROM many WHERE n % 2 = 0"
BOUNDS = {"memory_mb": 1024, "threads": 4, "spill_cap_mb": 10240}
EMPTY = {"filters": [], "order_by": [], "limit": None, "columns": [], "keys": None}


def _code(root: Path) -> Path:
    code = root / "code"
    code.mkdir(parents=True, exist_ok=True)
    (code / "bench.py").write_text(CODE)
    return code


def direct(rows: int, root: Path) -> dict[str, Any]:
    """The direct path's figure."""
    from provisa.codehost import residual
    from provisa.codesource import worker as w
    from provisa.codesource.arrow_rows import schema_text
    from provisa.python.source import HOST

    code = _code(root)
    command = w.HostCommand(
        argv=lambda address: [sys.executable, "-I", str(HOST), address, str(code), "bench"],
        env=w.private_environment(PYTHONDONTWRITEBYTECODE="1"),
        cwd=code,
    )
    worker = w.Worker("bench-src", "bench", command)
    worker.start()
    from provisa.core.org_files import data_dir

    # The process's one DuckDB spills where a dev-mode read's does.
    residual.configure(**BOUNDS, spill_dir=str(data_dir() / "code-spill"))
    schema = pa.schema([("n", pa.int64()), ("s", pa.utf8())])
    started = time.perf_counter()
    answered = 0
    try:
        ipcs = worker.batches(
            "many",
            "table",
            [rows],
            ["n"],
            schema=schema_text(schema),
            takes=("inputs",),
            context=EMPTY,
        )
        reader = pa.RecordBatchReader.from_batches(schema, residual.batches(w.pulled(ipcs)))
        for batch in residual.answer(STATEMENT, "duckdb", "many", reader):
            answered += batch.num_rows
    finally:
        worker.stop()
    return _figure("direct", rows, answered, time.perf_counter() - started)


def pgwire(rows: int, root: Path) -> dict[str, Any]:
    """The pgwire path's figure, through a front end started for the run."""
    import psycopg

    from provisa.codehost import front
    from provisa.codesource import remote
    from provisa.codesource.manifest import PYTHON_RULES, read_manifest
    from provisa.codesource.names import name_tables
    from provisa.python.source import HOST, READER

    code = _code(root)
    read = subprocess.run(
        [sys.executable, "-I", str(READER), str(code), "bench"],
        capture_output=True,
        text=True,
        check=True,
    )
    names = root / "names.json"
    names.write_text(json.dumps(name_tables(read_manifest(json.loads(read.stdout), PYTHON_RULES))))
    host = [sys.executable, "-I", str(HOST), "{fd}", str(code), "bench"]
    process = subprocess.Popen(  # noqa: S603 -- this interpreter and Provisa's front end
        [
            sys.executable,
            front.__file__,
            "0",
            str(root / "state"),
            str(root / "spill-front"),
            str(names),
            *host,
        ],
        env={**os.environ, "PROVISA_CODE_HASH": "bench"},
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stderr is not None
    pairing, port = None, 0
    until = time.monotonic() + 30
    while time.monotonic() < until:
        line = process.stderr.readline()
        if line.startswith("pairing code: "):
            pairing = line.removeprefix("pairing code: ").strip()
        if line.startswith("remote host listening on "):
            port = int(line.rsplit(" ", 1)[1])
            break
    if pairing is None or not port:
        raise RuntimeError("the front end did not start")
    try:
        identity = remote.new_identity()
        remote.pair("127.0.0.1", port, pairing, identity, BOUNDS)
        cert, key = root / "provisa.crt", root / "provisa.key"
        cert.write_text(identity.certificate)
        key.write_text(identity.key)
        key.chmod(0o600)
        dsn = (
            f"host=127.0.0.1 port={port} dbname=code user=provisa sslmode=require "
            f"sslcert={cert} sslkey={key}"
        )
        started = time.perf_counter()
        answered = 0
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            sql = f"{STATEMENT.replace('FROM many', 'FROM public.many')} AND _nf_n = {int(rows)}"
            cur.execute(cast(LiteralString, sql))
            while chunk := cur.fetchmany(65536):
                answered += len(chunk)
        return _figure("pgwire", rows, answered, time.perf_counter() - started)
    finally:
        process.terminate()
        process.wait(timeout=10)


def _figure(path: str, rows: int, answered: int, seconds: float) -> dict[str, Any]:
    return {
        "path": path,
        "rows": rows,
        "answered": answered,
        "seconds": round(seconds, 3),
        "rows_per_second": round(rows / seconds) if seconds else None,
        "statement": STATEMENT,
        "bounds": BOUNDS,
    }


def run(rows: int, output_dir: Path) -> list[dict[str, Any]]:
    """Both paths' figures, written to ``output_dir/code_sources.json``."""
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="provisa-bench-code-") as scratch:
        root = Path(scratch)
        figures = [direct(rows, root / "direct"), pgwire(rows, root / "pgwire")]
    (output_dir / "code_sources.json").write_text(json.dumps(figures, indent=2))
    return figures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--output-dir", default="results")
    args = parser.parse_args()
    for figure in run(args.rows, Path(args.output_dir)):
        print(json.dumps(figure))
    return 0


if __name__ == "__main__":
    sys.exit(main())
