# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The UI e2e starts its demo sources with demo/sources/provision.py (provisa-ui/e2e/
demo-source-containers.ts). Each --env value reaches it whole: run 40119276840 joined the argv into
a shell line, and PROVISA_DEMO_SPARQL_JAVA_OPTIONS=-Xmx512m -Xms128m split at its space
("unrecognized arguments: -Xms128m"), failing both core jobs' global setup."""

from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
HELPER = REPO / "provisa-ui" / "e2e" / "demo-source-containers.ts"


def _argv(cmd: str, names: list[str]) -> list[str]:
    node = shutil.which("node")
    assert node, "node is on the path wherever the UI is built"
    script = (
        f"import {{ provisionArgv }} from {json.dumps(str(HELPER))};"
        f"console.log(JSON.stringify(provisionArgv({json.dumps(cmd)}, {json.dumps(names)})));"
    )
    out = subprocess.run(  # noqa: S603 -- node on the repo's own helper
        [node, "--experimental-strip-types", "--no-warnings", "--input-type=module", "-e", script],
        capture_output=True,
        text=True,
        check=True,
        cwd=REPO,
    ).stdout
    return json.loads(out)


@pytest.mark.parametrize("cmd", ["up", "down"])
def test_every_env_value_reaches_provision_py_whole(cmd, monkeypatch):
    argv = _argv(cmd, ["sparql", "redis"])
    assert argv[0].endswith("demo/sources/provision.py")
    spec = importlib.util.spec_from_file_location("provision", argv[0])
    provision = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(provision)
    seen: dict = {}
    monkeypatch.setattr(
        provision,
        "up",
        lambda names, prefix, engine, env, network: seen.update(names=names, env=env),
    )
    monkeypatch.setattr(
        provision, "down", lambda names, prefix, env: seen.update(names=names, env=env)
    )
    assert provision.main(argv[1:]) == 0
    assert seen["names"] == ["sparql", "redis"]
    assert seen["env"]["PROVISA_DEMO_SPARQL_JAVA_OPTIONS"] == "-Xmx512m -Xms128m"
