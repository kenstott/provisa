# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The cloud node's engine-path probe: run by verify_engine (scripts/cloud-node-lib.sh) inside the
API container as ``python - <pin>``, with no account and no token.

A release deploy resets the control plane to zero accounts, so the probe cannot ask the public site
as a user. It runs the app's own engine resolution instead, in the container whose environment
the app runs on: the engine kind the app selects must be the pin; the boot shard is woken exactly as
a query wakes it (``engine_wake.ensure_shard_awake``), which records the ready coordinator pod's
address; the address is the one the app dials (``configured_engine_endpoint``); one statement runs
there as the app's engine principal for the bootstrap org. Trino gives every coordinator start a
random id that is also the suffix of each query id it issues (``trino_lifecycle.coordinator_start``),
so a query id ending in the id that coordinator reports proves the statement was answered by the
pinned engine at that address, not merely that something answered."""

from __future__ import annotations

import asyncio
import sys

#: Seconds the statement and the coordinator read may take. The wake has already waited out a cold
#: start, so this bounds a coordinator that is up but slow to answer its first statement.
_ANSWER_TIMEOUT = 240


def main(argv: list[str]) -> int:
    import trino

    from provisa.core import settings_registry
    from provisa.federation import engine_wake
    from provisa.federation.engine import configured_engine_endpoint, configured_engine_kind
    from provisa.federation.trino_lifecycle import ENGINE_USER, coordinator_start, engine_source

    (pin,) = argv
    kind = configured_engine_kind()
    if kind != pin:
        print(f"the app selects engine {kind!r}, not the pinned {pin!r}", file=sys.stderr)
        return 1
    shard = engine_wake.boot_shard()
    asyncio.run(engine_wake.ensure_shard_awake(shard))
    host, port = configured_engine_endpoint()
    kwargs = {
        "host": host,
        "port": port,
        "user": ENGINE_USER,
        "source": engine_source(settings_registry.value("control_plane.org_id")),
        "catalog": "system",
        "http_scheme": "http",
        "request_timeout": _ANSWER_TIMEOUT,
    }
    cursor = trino.dbapi.connect(**kwargs).cursor()
    cursor.execute("SELECT 1 AS ok")
    rows = cursor.fetchall()
    query_id = cursor.query_id
    identity = coordinator_start(kwargs, _ANSWER_TIMEOUT, fresh=True)
    coordinator_id = identity.rsplit("/", 1)[1]
    if rows != [[1]]:
        print(f"{identity} answered SELECT 1 AS ok with {rows!r}", file=sys.stderr)
        return 1
    if not query_id.endswith(f"_{coordinator_id}"):
        print(f"query {query_id} was not issued by the coordinator at {identity}", file=sys.stderr)
        return 1
    print(f"engine={kind} shard={shard} coordinator={identity} query={query_id} ok=1")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
