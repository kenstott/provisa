# Copyright (c) 2026 Kenneth Stott
# Canary: 93c8ec00-e8ae-4e1d-9cd0-aa67e2f99a79
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's DuckDB under the operator's bounds (REQ-2100): its memory, threads and spill cap
given to a built image's front end with pairing and with every call, kept in its state directory
for a restart; a statement before any bounds, and a spill past the cap, refused by name."""

# Requirements: REQ-2100

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from provisa.codehost import front, residual
from provisa.codesource import remote
from provisa.codesource.arrow_rows import schema_text
from provisa.codesource.worker import _frame, _read_frame, duckdb_bounds
from provisa.core import settings_registry
from provisa.python.source import HOST

pytestmark = pytest.mark.unit

BOUNDS = {"memory_mb": 256, "threads": 2, "spill_cap_mb": 1024}
N = schema_text(pa.schema([("n", pa.int64())]))
_CODE = textwrap.dedent(
    '''
    def many(n: int):
        """Rows.

        @table
        """
        return [{"n": i} for i in range(n)]
    '''
)


def _start(tmp_path: Path) -> tuple[subprocess.Popen, int, str | None]:
    code = tmp_path / "code"
    code.mkdir(exist_ok=True)
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
    assert port, "the host did not start"
    return process, port, pairing


def _stop(process: subprocess.Popen) -> None:
    process.terminate()
    process.wait(timeout=10)


def test_a_pairing_that_gives_no_bounds_is_refused_and_the_host_stays_unpaired(tmp_path):
    process, port, pairing = _start(tmp_path)
    try:
        assert pairing is not None
        fingerprint, secret = remote.parse_pairing_code(pairing)
        identity = remote.new_identity()
        with remote._dial("127.0.0.1", port, fingerprint, None, 10) as conn:  # noqa: SLF001
            conn.sendall(
                _frame({"op": "pair", "secret": secret, "certificate": identity.certificate})
            )
            answer = _read_frame(conn)
        assert answer == {
            "op": "refused",
            "message": "the pairing gave no bounds for this host's DuckDB",
        }
        assert not (tmp_path / "state" / "provisa.crt").exists()
    finally:
        _stop(process)


def test_the_front_keeps_the_bounds_each_call_gives_and_starts_again_with_them(tmp_path):
    process, port, pairing = _start(tmp_path)
    held = tmp_path / "state" / front.Bounds.FILE
    try:
        assert pairing is not None
        identity = remote.new_identity()
        pin = remote.pair("127.0.0.1", port, pairing, identity, BOUNDS).pin
        assert json.loads(held.read_text()) == BOUNDS
        worker = remote.RemoteWorker(
            "shop-src", "h1", address="127.0.0.1", port=port, pin=pin, identity=identity
        )
        worker.start()
        try:
            rows = [r for b in worker.rows("many", "table", [3], ["n"], schema=N) for r in b]
            assert rows == [{"n": 0}, {"n": 1}, {"n": 2}]
        finally:
            worker.stop()
        # A call carries the operator's bounds as they are now: the front keeps them.
        assert json.loads(held.read_text()) == duckdb_bounds()
    finally:
        _stop(process)
    # Started again (a container restarted), it is paired and bounded before Provisa calls it.
    process, port, pairing = _start(tmp_path)
    try:
        assert pairing is None
        assert json.loads(held.read_text()) == duckdb_bounds()
    finally:
        _stop(process)


def test_kept_bounds_bound_the_duckdb_a_front_starts_with(tmp_path, monkeypatch):
    """The front's Bounds, made from what its state directory keeps, configure the DuckDB its
    statements run on -- before any pairing or call gives them."""
    monkeypatch.syspath_prepend(str(Path(front.__file__).parent))
    sys.modules.pop("residual", None)  # the image's own copy, beside front.py
    image_residual = importlib.import_module("residual")
    (tmp_path / front.Bounds.FILE).write_text(json.dumps(BOUNDS))
    try:
        front.Bounds(tmp_path, str(tmp_path / "spill"))
        settings = dict(
            image_residual._connection()  # noqa: SLF001
            .execute(
                "SELECT name, value FROM duckdb_settings() "
                "WHERE name IN ('memory_limit', 'threads', 'max_temp_directory_size')"
            )
            .fetchall()
        )
        assert settings == {
            "memory_limit": "256.0 MiB",
            "threads": "2",
            "max_temp_directory_size": "1.0 GiB",
        }
    finally:
        sys.modules.pop("residual", None)


def test_a_statement_before_any_bounds_is_refused_by_name(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(front.__file__).parent))
    sys.modules.pop("residual", None)
    unbounded = importlib.import_module("residual")
    try:
        with pytest.raises(unbounded.Unbounded, match="has no bounds yet"):
            unbounded.answer("SELECT 1 AS n FROM t", "duckdb", "t", pa.table({"n": [1]}))
    finally:
        sys.modules.pop("residual", None)


def test_a_spill_that_reaches_the_end_of_its_volume_is_refused_naming_the_cap():
    residual.configure(
        **duckdb_bounds(), spill_dir=str(Path(os.environ["PROVISA_DATA_DIR"]) / "code-spill")
    )
    full = duckdb.IOException(
        'IO Error: Could not write file "spill/x.tmp": No space left on device'
    )
    named = residual._named(full)  # noqa: SLF001
    assert isinstance(named, residual.SpillCapped)
    assert "codesource.duckdb_spill_cap_mb" in str(named)
    other = duckdb.IOException("IO Error: Permission denied")
    assert residual._named(other) is other  # noqa: SLF001


def test_a_spill_that_reaches_the_end_of_its_volume_as_the_answer_streams_is_refused_naming_the_cap():
    """DuckDB 2.0 streams the answer through Arrow's reader, which raises the engine's error as an
    OSError carrying only its message: the end of the spill volume, met there, is still refused
    naming the cap."""
    residual.configure(
        **duckdb_bounds(), spill_dir=str(Path(os.environ["PROVISA_DATA_DIR"]) / "code-spill")
    )
    rows = pa.table({"n": list(range(200_000))})
    answer = residual.answer(
        "SELECT CASE WHEN n = 150000 "
        "THEN error('IO Error: Could not write file \"spill/x.tmp\": No space left on device') "
        "ELSE n END AS n FROM t",
        "duckdb",
        "t",
        rows,
    )
    with pytest.raises(residual.SpillCapped, match="codesource.duckdb_spill_cap_mb"):
        for _ in answer:
            pass


_MEMORY_SETTINGS = ("codesource.duckdb_memory_mb", "codesource.duckdb_threads")


@pytest.mark.parametrize(
    "said",
    [
        "Out of Memory Error: failed to pin block of size 256.0 KiB (64.0 MiB/64.0 MiB used)",
        "Out of Memory Error: failed to allocate data of size 8.0 MiB (60.0 MiB/64.0 MiB used)",
    ],
)
def test_a_statement_past_the_memory_it_can_spill_from_is_refused_naming_both_settings(said):
    """Past the point DuckDB can sort by spilling, its failure names neither setting; the refusal
    names both, what each is now, and which way each has to move."""
    residual.configure(
        **duckdb_bounds(), spill_dir=str(Path(os.environ["PROVISA_DATA_DIR"]) / "code-spill")
    )
    named = residual._named(duckdb.OutOfMemoryException(said))  # noqa: SLF001
    assert isinstance(named, residual.MemoryCapped)
    bounds = duckdb_bounds()
    assert str(named) == (
        "the statement needs more memory than DuckDB can sort it in, even spilling to disk: "
        f"codesource.duckdb_memory_mb is {bounds['memory_mb']} MiB and "
        f"codesource.duckdb_threads is {bounds['threads']}. "
        "Raise codesource.duckdb_memory_mb, lower codesource.duckdb_threads, "
        "or narrow the statement"
    )


def test_running_out_of_the_spill_cap_is_still_refused_naming_the_cap_not_the_memory():
    residual.configure(
        **duckdb_bounds(), spill_dir=str(Path(os.environ["PROVISA_DATA_DIR"]) / "code-spill")
    )
    capped = duckdb.OutOfMemoryException(
        "Out of Memory Error: failed to offload data block: max_temp_directory_size reached"
    )
    named = residual._named(capped)  # noqa: SLF001
    assert isinstance(named, residual.SpillCapped)
    assert not any(setting in str(named) for setting in _MEMORY_SETTINGS)


def test_a_memory_failure_met_as_the_answer_streams_is_refused_naming_both_settings():
    """Arrow's reader raises the engine's failure as an OSError carrying only its message: met
    there, it is the same refusal."""
    residual.configure(
        **duckdb_bounds(), spill_dir=str(Path(os.environ["PROVISA_DATA_DIR"]) / "code-spill")
    )
    rows = pa.table({"n": list(range(200_000))})
    answer = residual.answer(
        "SELECT CASE WHEN n = 150000 "
        "THEN error('Out of Memory Error: failed to pin block of size 256.0 KiB') "
        "ELSE n END AS n FROM t",
        "duckdb",
        "t",
        rows,
    )
    with pytest.raises(residual.MemoryCapped) as refused:
        for _ in answer:
            pass
    assert all(setting in str(refused.value) for setting in _MEMORY_SETTINGS)


def _in_force(monkeypatch, memory_mb, threads, source="default"):
    """The two settings resolving to these values, whatever the deployment running the test."""
    resolved = {
        "codesource.duckdb_memory_mb": settings_registry.Resolved(memory_mb, source),
        "codesource.duckdb_threads": settings_registry.Resolved(threads, source),
    }
    real_resolve = settings_registry.resolve
    monkeypatch.setattr(
        settings_registry,
        "resolve",
        lambda key: resolved[key] if key in resolved else real_resolve(key),
    )


@pytest.mark.parametrize(
    ("memory_mb", "threads"),
    [(64, 4), (64, 1), (1024, 4), (1024, 64), (80, 5)],
)
def test_at_least_sixteen_mib_a_thread_is_accepted(monkeypatch, memory_mb, threads):
    _in_force(monkeypatch, memory_mb, threads)
    settings_registry.check_pairs()


@pytest.mark.parametrize(
    ("memory_mb", "threads"),
    [(64, 5), (64, 8), (1024, 65), (79, 5)],
)
def test_a_load_below_sixteen_mib_a_thread_stops_by_name(monkeypatch, memory_mb, threads):
    _in_force(monkeypatch, memory_mb, threads)
    with pytest.raises(settings_registry.SettingsConflict) as refused:
        settings_registry.check_pairs()
    assert (refused.value.field, refused.value.other) == _MEMORY_SETTINGS
    assert refused.value.reason == "memory_below_thread_minimum"
    assert (refused.value.value, refused.value.other_value) == (memory_mb, threads)


def test_a_save_below_sixteen_mib_a_thread_is_refused_on_the_setting_saved(monkeypatch):
    from provisa.api.admin import settings_guards

    _in_force(monkeypatch, 64, 4)
    with pytest.raises(settings_guards.Refused) as refused:
        settings_guards._pairs({"codesource.duckdb_threads": 8})  # noqa: SLF001
    assert (refused.value.field, refused.value.reason, refused.value.params) == (
        "codesource.duckdb_threads",
        "memory_below_thread_minimum",
        {"other": "codesource.duckdb_memory_mb"},
    )
    with pytest.raises(settings_guards.Refused) as refused:
        settings_guards._pairs({"codesource.duckdb_memory_mb": 63})  # noqa: SLF001
    assert (refused.value.field, refused.value.params) == (
        "codesource.duckdb_memory_mb",
        {"other": "codesource.duckdb_threads"},
    )
    # Saved together they are judged together, and exactly the minimum is allowed.
    settings_guards._pairs(  # noqa: SLF001
        {"codesource.duckdb_memory_mb": 128, "codesource.duckdb_threads": 8}
    )


_FAILS_MIDWAY = """
import sys, threading, time
sys.path.insert(0, sys.argv[1])
import pyarrow as pa
import residual
residual.configure(memory_mb=128, threads=4, spill_cap_mb=1024, spill_dir=sys.argv[2])
schema = pa.schema([("s", pa.utf8())])
pulled = [0]
def rows():
    for i in range(400):
        pulled[0] += 1
        time.sleep(0.01)
        yield pa.RecordBatch.from_arrays([pa.array(["x" if i == 3 else "1"] * 1000)], schema=schema)
reader = pa.RecordBatchReader.from_batches(schema, rows())
try:
    for _ in residual.answer("SELECT CAST(s AS INTEGER) AS n FROM t", "duckdb", "t", reader):
        pass
except Exception as failed:
    print("FAILED", type(failed).__name__, flush=True)
at_failure = pulled[0]
time.sleep(0.5)
print("PULLED AFTER", pulled[0] - at_failure, flush=True)
"""


def test_a_statement_that_fails_stops_reading_the_functions_rows_and_lets_the_host_exit(tmp_path):
    """DuckDB scans a reader through Arrow's read-ahead, which kept pulling the function's rows
    on Arrow's threads after the statement failed -- the function went on running, and a host
    exiting then hung (or, in a built image, crashed) with those threads still in Python."""
    spill = tmp_path / "spill"
    spill.mkdir()
    run = subprocess.run(  # noqa: S603 -- the test's own interpreter
        [
            sys.executable,
            "-I",
            "-c",
            _FAILS_MIDWAY,
            str(Path(front.__file__).parent),
            str(spill),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert run.returncode == 0, run.stdout + run.stderr
    assert "FAILED ConversionException" in run.stdout, run.stdout
    assert "PULLED AFTER 0" in run.stdout, run.stdout


_PULLED_WHERE = """
import sys, threading
sys.path.insert(0, sys.argv[1])
import pyarrow as pa
import residual
residual.configure(memory_mb=128, threads=1, spill_cap_mb=1024, spill_dir=sys.argv[2])
schema = pa.schema([("s", pa.utf8())])
foreign = set()
def rows(bad):
    for i in range(50):
        if threading.current_thread() is not threading.main_thread():
            foreign.add(threading.current_thread().name)
        yield pa.RecordBatch.from_arrays([pa.array(["x" if i == bad else "1"] * 1000)], schema=schema)
for bad in (-1, 20):
    reader = pa.RecordBatchReader.from_batches(schema, rows(bad))
    try:
        for _ in residual.answer("SELECT CAST(s AS INTEGER) AS n FROM t", "duckdb", "t", reader):
            pass
        print("ANSWERED", flush=True)
    except Exception as failed:
        print("FAILED", type(failed).__name__, flush=True)
print("FOREIGN", sorted(foreign), flush=True)
"""


def test_the_functions_rows_are_pulled_only_on_the_statements_own_thread(tmp_path):
    """A host whose DuckDB runs on one thread pulls the function's rows on the thread answering
    the statement, never on Arrow's read-ahead pool: a pool thread that comes back into Python
    after the statement has ended can do so as the interpreter finalizes, where CPython ends it
    mid-task and Arrow's pool, at exit, waits for it forever (the built image's spill-cap refusal
    printed, its process never ending)."""
    spill = tmp_path / "spill"
    spill.mkdir()
    run = subprocess.run(  # noqa: S603 -- the test's own interpreter
        [
            sys.executable,
            "-I",
            "-c",
            _PULLED_WHERE,
            str(Path(front.__file__).parent),
            str(spill),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert run.returncode == 0, run.stdout + run.stderr
    assert "ANSWERED\nFAILED ConversionException\n" in run.stdout, run.stdout
    assert "FOREIGN []" in run.stdout, run.stdout
