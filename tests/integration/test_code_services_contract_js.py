# Copyright (c) 2026 Kenneth Stott
# Canary: 263b0150-93fb-4571-a4c3-52bc59db874c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: Node's @provisa/services (REQ-2106) against the contract Python's module keeps
(tests/fixtures/code_services_contract.json), on each pinned runtime a Node source runs on --
Node, Bun and Deno, fetched as a worker's are -- with the services answered in-process."""

# Requirements: REQ-2106

from __future__ import annotations

import json
import subprocess

import pytest

from provisa.node.worker import HOST
from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver
from tests.unit.test_code_services_module_python import CONTRACT, TOKEN, _Services

pytestmark = [pytest.mark.integration]

SERVICES = HOST.with_name("services.mjs")

_RUNNER = """
const s = await import(process.argv[2]);
const [expression] = JSON.parse(process.argv[3]);
// An async function of the module, so an expression may await (eval code cannot).
const AsyncFunction = (async () => {}).constructor;
try {
  const value = await new AsyncFunction("s", `return (${expression});`)(s);
  console.log(JSON.stringify({ returns: value === undefined ? null : value }));
} catch (e) {
  console.log(JSON.stringify({ raises: { service: e.service, status: e.status, message: e.message } }));
}
"""


@pytest.fixture(scope="module")
def cache(tmp_path_factory):
    return tmp_path_factory.mktemp("runtimes")


@pytest.fixture(params=["node", "bun", "deno"])
def runtime(request, cache, tmp_path):
    executable = CodeRuntimeResolver(cache_root=cache).resolve(request.param)
    script = tmp_path / "run.mjs"
    script.write_text(_RUNNER)

    def run(expression: str, base: str) -> dict:
        argv = [str(executable)]
        if request.param == "deno":
            argv += ["run", "--allow-net", "--allow-env", "--allow-read"]
        out = subprocess.run(
            [*argv, str(script), SERVICES.as_uri(), json.dumps([expression])],
            env={"PROVISA_SERVICES": base, "PATH": "/usr/bin:/bin", "NO_COLOR": "1"},
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert out.returncode == 0, out.stderr
        return json.loads(out.stdout.strip().splitlines()[-1])

    return run


@pytest.mark.parametrize("case", CONTRACT["cases"], ids=[c["name"] for c in CONTRACT["cases"]])
def test_each_call_sends_what_the_contract_names_and_returns_what_it_says(runtime, case):
    served = _Services()
    try:
        served.answer = case["answer"]
        result = runtime(case["js"], served.base)
        if "raises" in case:
            assert result["raises"]["service"] == case["raises"]["service"]
            assert result["raises"]["status"] == case["raises"]["status"]
            assert case["raises"]["message"] in result["raises"]["message"]
        else:
            assert result == {"returns": case["returns"]}
        (sent,) = served.seen
        assert (sent["method"], sent["path"], sent["body"]) == (
            case["request"]["method"],
            case["request"]["path"],
            case["request"]["body"],
        )
        assert sent["token"] == TOKEN
    finally:
        served.stop()


def test_with_no_services_a_call_raises_naming_its_service(runtime):
    result = runtime("s.cache.get('rates')", "")
    assert result["raises"]["service"] == "cache" and result["raises"]["status"] == 0
