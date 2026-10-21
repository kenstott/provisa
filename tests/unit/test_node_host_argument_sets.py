# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Node host's serving of a table's argument sets (REQ-2099): one call carries every set, the
host runs the function once per set in order, sends each set's rows under the call's id and ends
the call once. Runs host-core.mjs on the machine's ``node``, its Arrow library a stand-in that
records the rows each frame would carry."""

# Requirements: REQ-2099

from __future__ import annotations

import json
import shutil
import subprocess
import textwrap

import pytest

from provisa.node.worker import HOST

pytestmark = pytest.mark.unit

CORE = HOST.with_name("host-core.mjs")

# The stand-in Arrow library: a schema, and nothing a test's sendRows stand-in needs beyond it.
_ARROW = "module.exports = { tableFromIPC: () => ({ schema: { fields: [] } }) };\n"

_SCRIPT = textwrap.dedent(
    """
    import { Connection } from %(core)s;
    const bundle = { squares: (n) => [{ n, square: n * n }] };
    const sent = [];
    const connection = new Connection({ bundle, services: {}, otel: null }, () => {}, () => {});
    connection.send = (message) => sent.push(message);
    connection.sendRows = (id, rows) => sent.push({ op: "rows", id, rows });
    await connection.serve({
      id: 7, export: "squares", kind: "table", schema: "c2NoZW1h",
      calls: [{ args: [2], names: ["n"] }, { args: [3], names: ["n"] }, { args: [4], names: ["n"] }],
    });
    process.stdout.write(JSON.stringify({ sent, running: connection.running.size }));
    """
)


def test_one_call_runs_the_function_per_argument_set_in_order(tmp_path):
    node = shutil.which("node")
    assert node is not None, "this test runs the host on the machine's node"
    (tmp_path / "lib" / "package").mkdir(parents=True)
    (tmp_path / "lib" / "package" / "Arrow.es2015.min.js").write_text(_ARROW)
    script = tmp_path / "serve.mjs"
    script.write_text(_SCRIPT % {"core": json.dumps(CORE.as_uri())})
    done = subprocess.run(  # noqa: S603 -- the machine's node and Provisa's host
        [node, str(script)],
        env={"PATH": "/usr/bin:/bin", "PROVISA_HOST_LIB": str(tmp_path / "lib")},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert done.returncode == 0, done.stderr
    out = json.loads(done.stdout)
    assert out["sent"] == [
        {"op": "rows", "id": 7, "rows": [{"n": 2, "square": 4}]},
        {"op": "rows", "id": 7, "rows": [{"n": 3, "square": 9}]},
        {"op": "rows", "id": 7, "rows": [{"n": 4, "square": 16}]},
    ]
    assert out["running"] == 0
