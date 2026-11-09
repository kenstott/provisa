# Copyright (c) 2026 Kenneth Stott
# Canary: 3d0c6f0e-6a3b-4f55-9a4e-8f2b7c1e5d90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An engine error met in a later batch is the statement's own error, every time (REQ-2194).

DuckDB 1.5.x let a pipeline error race its own interrupt: ``Executor::PushError`` sets the
connection's ``interrupted`` flag so sibling pipeline tasks stop, and the client was sometimes
answered with a sibling's ``INTERRUPT Error: Interrupted!`` (XX000 on pgwire) instead of the error
that stopped the statement -- up to one run in twenty for a raising macro, nothing having
cancelled anything. The statement is run many times so a race of that rate cannot pass."""

# Requirements: REQ-2194

from __future__ import annotations

import pytest

from provisa.core.pg_error import PostgresError
from provisa.executor.result import ArrowStreamResult
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.transpiler.transpile import transpile_to_duckdb

# A zero divisor on row 300,000: the transpiler's division guard (PostgreSQL's 22012), met as a
# later batch is read.
_LATE_ZERO_DIVISOR = transpile_to_duckdb(
    "SELECT 1/(g.i - 300000) AS v FROM generate_series(1, 400000) AS g(i)"
)
_RUNS = 5000


@pytest.fixture(scope="module")
def runtime():
    rt = DuckDBFederationRuntime()
    yield rt
    rt.close()


def test_the_late_error_is_the_statements_own_on_every_run(runtime: DuckDBFederationRuntime):
    answered: list[str] = []
    for _ in range(_RUNS):
        stream = runtime.run_sync(_LATE_ZERO_DIVISOR)
        assert isinstance(stream, ArrowStreamResult)
        try:
            for _ in stream.arrow_batches():
                pass
        except PostgresError as exc:
            answered.append(f"{exc.sqlstate} {exc}")
        except Exception as exc:  # allow-ble: any other answer is what the assertion reports
            answered.append(f"{type(exc).__name__} {exc}")
        else:
            answered.append("no error")
        finally:
            stream.close()
    wrong = [a for a in answered if not a.startswith("22012")]
    assert wrong == [], f"{len(wrong)} of {_RUNS} runs answered other than 22012: {wrong[:3]}"
