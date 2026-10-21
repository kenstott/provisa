# Copyright (c) 2026 Kenneth Stott
# Canary: c6d0fd6f-b97f-4310-a9f7-d75c27eca459
#
# This source code is licensed under the Business Source License 1.1
"""Inside the built image: a files source's connector server (the Calcite pgwire bundle, in the
image's own JVM) starts on a folder of one CSV and answers a SELECT of it. Run as
`docker run -i <image> python - < this`."""

from __future__ import annotations

import asyncio
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

from provisa.federation.pgwire_endpoint import sign_in_password
from provisa.federation.pgwire_replica import (
    PgwireServer,
    PortPair,
    build_model_json,
    land_via_select,
    schema_name,
)
from provisa.runtime_deps import BundleResolver, bundle_spec_for

START_SECONDS = 180

root = Path(tempfile.mkdtemp())
folder = root / "data"
folder.mkdir()
(folder / "pets.csv").write_text("id,name\n1,Rex\n2,Tom\n")
state = root / "state"
state.mkdir()

source = SimpleNamespace(
    id="smoke-files", type="files", mapping={}, path=str(folder), file_glob_tables=[]
)
spec = bundle_spec_for("files")
server = PgwireServer(
    bundle_dir=BundleResolver().resolve(spec),
    spec=spec,
    model=build_model_json(source, state_dir=state),
    ports=PortPair(pgwire_port=15433, calcite_child_host="127.0.0.1", calcite_child_port=15434),
)
server.start()
try:
    deadline = time.monotonic() + START_SECONDS
    while not server.health():
        if server.exit_code() is not None:
            raise SystemExit(f"connector server exited {server.exit_code()}:\n{server.log_tail()}")
        if time.monotonic() >= deadline:
            raise SystemExit(
                f"connector server not serving in {START_SECONDS}s:\n{server.log_tail()}"
            )
        time.sleep(1)
    rows = asyncio.run(
        land_via_select(
            server.ports, schema_name(source), "pets", password=sign_in_password(source)
        )
    )
    assert sorted(r["name"] for r in rows) == ["Rex", "Tom"], rows
    print(f"files connector served {len(rows)} rows from {spec.asset_filename}")
finally:
    server.stop()
