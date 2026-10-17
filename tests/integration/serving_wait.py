# Copyright (c) 2026 Kenneth Stott
# Canary: 5c85b653-b687-4043-ba39-551aea7cce94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wait, before a module's first read, until a server's tables on a slow source answer.

A source with a server of its own (AskAmerica's govdata mounting its schemas) can take longer
than one statement waits for it (``STATEMENT_START_WAIT_SECONDS``), and a TTL -1 replica lands on
its first read, longer than one request lasts. A read meanwhile answers 503
``data.source_starting`` or 504 while the replica is built; both go on without the reader. This
reads each table until it answers, and fails at once on any other answer.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

#: The answers that mean "not yet": the source's server is starting, or the read's request ran
#: out while the replica it needs was built.
_NOT_YET = {
    (503, "data.source_starting"),
    (504, "data.replica_building"),
    (504, "data.request_interrupted"),
}


#: How much of the server's own log an unexpected answer carries: the end, where the error that
#: answer reports was logged.
_LOG_TAIL_CHARS = 6000


def await_tables_serving(server: Any, role: str, tables: list[str], timeout: float) -> None:
    """Return once ``SELECT COUNT(*)`` on each of ``tables`` (as the test reads them) answers 200
    from the ``IsolatedServer`` ``server``, waiting at most ``timeout`` seconds in all. Any other
    answer fails at once with the end of the server's log, where its cause was logged."""
    base_url = server.base_url
    deadline = time.monotonic() + timeout
    for table in tables:
        sql = f"SELECT COUNT(*) AS n FROM {table}"
        while True:
            resp = httpx.post(
                f"{base_url}/data/sql",
                json={"sql": sql},
                headers={"X-Provisa-Role": role},
                timeout=max(deadline - time.monotonic(), 1.0),
            )
            if resp.status_code == 200:
                break
            code = resp.json().get("code")
            assert (resp.status_code, code) in _NOT_YET, (
                f"{sql}: HTTP {resp.status_code}: {resp.text}\nthe server's log, its end:\n"
                f"{server.dump_stderr_debug()[-_LOG_TAIL_CHARS:]}"
            )
            assert time.monotonic() < deadline, (
                f"{sql}: still not serving after {timeout:.0f}s: HTTP {resp.status_code}: {resp.text}"
            )
            time.sleep(5)
