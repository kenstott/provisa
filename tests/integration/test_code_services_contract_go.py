# Copyright (c) 2026 Kenneth Stott
# Canary: 1a0f0ef8-ffa1-4bc4-84f0-dcd7636eb5a2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: Go's provisa_services (REQ-2106, REQ-2200) against the contract Python's and Node's
modules keep (tests/fixtures/code_services_contract.json), compiled by the pinned Go, with the
services answered in-process."""

# Requirements: REQ-2106, REQ-2200

from __future__ import annotations

import json
import math
import subprocess
from pathlib import Path
from typing import Any

import pytest

from provisa.golang.reader import go_environment, services_go_mod
from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver
from tests.unit.test_code_services_module_python import CONTRACT, TOKEN, _Services

pytestmark = [pytest.mark.integration]

_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
_EMPTY = {"status": 200, "type": "application/json", "body": {}}

_MAIN = """package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	ps "provisa_services"
)

var cases = map[string]func(context.Context) (any, error){
%(cases)s
	"bound": func(ctx context.Context) (any, error) {
		return nil, ps.Log(ps.BindCall(ctx, 7, %(traceparent)s), "x", "info", nil)
	},
	"bound, its trace read": func(ctx context.Context) (any, error) {
		return ps.Traceparent(ps.BindCall(ctx, 7, %(traceparent)s)), nil
	},
	"unbound": func(ctx context.Context) (any, error) {
		return nil, ps.Log(ctx, "x", "info", nil)
	},
	"no services": func(ctx context.Context) (any, error) {
		return ps.Cache.Get(ctx, "rates")
	},
}

func main() {
	value, err := cases[os.Args[1]](context.Background())
	out := map[string]any{"returns": value}
	if err != nil {
		var refused *ps.ServiceError
		if !errors.As(err, &refused) {
			panic(err)
		}
		out = map[string]any{"raises": map[string]any{
			"service": refused.Service, "status": refused.Status, "message": refused.Error(),
		}}
	}
	text, _ := json.Marshal(out)
	fmt.Println(string(text))
}
"""


def _same(found: Any, expected: Any) -> bool:
    """Equal as JSON values, a float within the suite's relative tolerance."""
    if isinstance(expected, dict):
        return (
            isinstance(found, dict)
            and found.keys() == expected.keys()
            and all(_same(found[k], expected[k]) for k in expected)
        )
    if isinstance(expected, list):
        return (
            isinstance(found, list)
            and len(found) == len(expected)
            and all(_same(a, b) for a, b in zip(found, expected, strict=True))
        )
    if isinstance(expected, float):
        return isinstance(found, (int, float)) and math.isclose(found, expected, rel_tol=1e-14)
    return found == expected


@pytest.fixture(scope="module")
def binary(tmp_path_factory) -> Path:
    """Every contract case, compiled into one program against the module a worker gets."""
    work = tmp_path_factory.mktemp("contract")
    go = CodeRuntimeResolver(cache_root=work / "runtimes").resolve("go")
    code = work / "code"
    code.mkdir()
    (code / "go.mod").write_text("module example.com/contract\n\ngo 1.23\n")
    bodies = "\n".join(
        f"\t{json.dumps(c['name'])}: func(ctx context.Context) (any, error) {{ {c['go']} }},"
        for c in CONTRACT["cases"]
    )
    (code / "main.go").write_text(
        _MAIN % {"cases": bodies, "traceparent": json.dumps(_TRACEPARENT)}
    )
    env = go_environment(go, gocache=work / "cache", gomodcache=work / "mod")
    env["GOFLAGS"] = f"-mod=readonly -modfile={services_go_mod(code, work)}"
    built = work / "contract-binary"
    subprocess.run(
        [str(go), "build", "-o", str(built), "."], cwd=code, env=env, check=True, timeout=240
    )
    return built


def _run(binary: Path, name: str, base: str) -> dict:
    out = subprocess.run(
        [str(binary), name],
        env={"PROVISA_SERVICES": base, "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout.strip().splitlines()[-1])


@pytest.fixture
def served():
    services = _Services()
    yield services
    services.stop()


@pytest.mark.parametrize("case", CONTRACT["cases"], ids=[c["name"] for c in CONTRACT["cases"]])
def test_each_call_sends_what_the_contract_names_and_returns_what_it_says(binary, served, case):
    served.answer = case["answer"]
    result = _run(binary, case["name"], served.base)
    if "raises" in case:
        assert result["raises"]["service"] == case["raises"]["service"]
        assert result["raises"]["status"] == case["raises"]["status"]
        assert case["raises"]["message"] in result["raises"]["message"]
    else:
        assert _same(result, {"returns": case["returns"]}), result
    (sent,) = served.seen
    assert (sent["method"], sent["path"]) == (case["request"]["method"], case["request"]["path"])
    assert _same(sent["body"], case["request"]["body"]), sent["body"]
    assert sent["token"] == TOKEN


def test_a_call_names_the_call_it_serves_and_its_trace(binary, served):
    served.answer = _EMPTY
    _run(binary, "bound", served.base)
    (sent,) = served.seen
    assert (sent["call"], sent["traceparent"]) == ("7", _TRACEPARENT)


def test_a_context_no_call_was_bound_to_names_none(binary, served):
    served.answer = _EMPTY
    _run(binary, "unbound", served.base)
    (sent,) = served.seen
    assert (sent["call"], sent["traceparent"]) == (None, None)


def test_the_code_reads_the_requests_trace_for_its_own_telemetry(binary, served):
    assert _run(binary, "bound, its trace read", served.base) == {"returns": _TRACEPARENT}


def test_with_no_services_a_call_raises_naming_its_service(binary):
    result = _run(binary, "no services", "")
    assert result["raises"]["service"] == "cache" and result["raises"]["status"] == 0
    assert "$PROVISA_SERVICES is not set" in result["raises"]["message"]
