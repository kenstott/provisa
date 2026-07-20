# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9a2c58-71d4-4e60-b8a3-5c9d0e2f7a14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-885: extensible functions — per-kind binding + dispatch, addressing/binding
decoupling, relation-argument kinds, and unknown-kind / missing-binding fail-loud."""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import provisa.executor.function_dispatch as fd
from provisa.executor.function_dispatch import dispatch_function
from provisa.otel_compat import MemoryUdfTraceSink


class _FakeResult:
    def __init__(self, cols, rows):
        self.column_names = cols
        self.rows = rows


class _FakePools:
    def __init__(self, connected=True, result=None):
        self._connected = connected
        self._result = result or _FakeResult(["id", "name"], [(1, "ada")])
        self.calls: list = []

    def has(self, _src_id):
        return self._connected

    async def execute(self, src_id, sql, params):
        self.calls.append((src_id, sql, params))
        return self._result


def _state(connected=True, pools=None, egress=None):
    return SimpleNamespace(
        roles={"ops": {"id": "ops", "capabilities": []}},
        source_pools=pools or _FakePools(connected=connected),
        udf_trace_sink=MemoryUdfTraceSink(),
        minted_sessions=[],
        # REQ-885: deny-by-default egress allow-list. Tests that reach external endpoints must
        # allow-list them explicitly; the loopback host is always permitted.
        udf_egress_allowlist=(
            ["a.example", "b.example", "svc", "svc:50051"] if egress is None else egress
        ),
    )


def _fn(**over):
    base = {
        "name": "createOrder",
        "source_id": "s1",
        "schema_name": "public",
        "function_name": "create_order",
        "kind": "mutation",
        "writable_by": ["ops"],
        "returns": "",
        "impl_kind": "source_procedure",
        "binding": {},
        "materialize": False,
        "arguments": [],
    }
    base.update(over)
    return base


# ---- source_procedure (existing path preserved) ----------------------------


@pytest.mark.asyncio
async def test_source_procedure_builds_sql():
    st = _state()
    rows = await dispatch_function(_fn(), {"a0": 7, "a1": "x"}, st, "ops")
    assert rows == [{"id": 1, "name": "ada"}]
    src, sql, params = st.source_pools.calls[0]
    assert src == "s1"
    assert sql == 'SELECT * FROM "public"."create_order"($1, $2)'
    assert params == [7, "x"]


# ---- script kind -----------------------------------------------------------


@pytest.mark.asyncio
async def test_script_kind_subprocess(monkeypatch):
    seen = {}

    async def fake_run(argv, payload):
        seen["argv"] = argv
        seen["payload"] = payload
        return b'[{"out": 42}]'

    monkeypatch.setattr(fd, "_run_subprocess", fake_run)
    fn = _fn(impl_kind="script", binding={"argv": ["/bin/transform", "--json"]})
    rows = await dispatch_function(fn, {"n": 3}, _state(), "ops")
    assert rows == [{"out": 42}]
    assert seen["argv"] == ["/bin/transform", "--json"]
    assert b"correlation_id" in seen["payload"]


@pytest.mark.asyncio
async def test_script_missing_argv_fails_loud():
    fn = _fn(impl_kind="script", binding={})
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(fn, {}, _state(), "ops")
    assert ei.value.status_code == 400


# ---- http kind + addressing/binding decoupling -----------------------------


@pytest.mark.asyncio
async def test_http_kind_and_binding_is_swappable(monkeypatch):
    seen = {}

    async def fake_http(method, url, payload, timeout):
        seen["method"] = method
        seen["url"] = url
        seen["payload"] = payload
        seen["timeout"] = timeout
        return [{"ok": 1}]

    monkeypatch.setattr(fd, "_http_call", fake_http)
    fn = _fn(impl_kind="http", binding={"url": "https://a.example/fn", "method": "post"})
    rows = await dispatch_function(fn, {"n": 5}, _state(), "ops")
    assert rows == [{"ok": 1}]
    assert seen["url"] == "https://a.example/fn"
    assert seen["method"] == "POST"

    # ADDRESSING (name) unchanged; BINDING swapped to a different location.
    fn["binding"]["url"] = "https://b.example/fn"
    await dispatch_function(fn, {"n": 5}, _state(), "ops")
    assert fn["name"] == "createOrder"
    assert seen["url"] == "https://b.example/fn"


@pytest.mark.asyncio
async def test_http_missing_url_fails_loud():
    fn = _fn(impl_kind="http", binding={"method": "POST"})
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(fn, {}, _state(), "ops")
    assert ei.value.status_code == 400


# ---- grpc kind -------------------------------------------------------------


@pytest.mark.asyncio
async def test_grpc_kind(monkeypatch):
    seen = {}

    async def fake_grpc(target, method, payload, **kw):
        seen["target"] = target
        seen["method"] = method
        seen["payload"] = payload
        seen["kw"] = kw
        return {"g": 1}

    monkeypatch.setattr(fd, "_grpc_call", fake_grpc)
    fn = _fn(
        impl_kind="grpc",
        binding={"target": "svc:50051", "method": "Pkg.Fn", "timeout_s": 12, "tls": True},
    )
    rows = await dispatch_function(fn, {"n": 1}, _state(), "ops")
    assert rows == [{"g": 1}]
    assert seen["target"] == "svc:50051"
    assert seen["method"] == "Pkg.Fn"
    assert seen["kw"] == {"timeout": 12.0, "tls": True}


# ---- grpc method-path normalization + JSON unary bridge --------------------


def test_grpc_method_path_forms():
    assert fd._grpc_method_path("/Pkg.Svc/Fn") == "/Pkg.Svc/Fn"
    assert fd._grpc_method_path("Pkg.Svc/Fn") == "/Pkg.Svc/Fn"
    assert fd._grpc_method_path("pkg.Svc.Fn") == "/pkg.Svc/Fn"
    with pytest.raises(HTTPException) as ei:
        fd._grpc_method_path("nodots")
    assert ei.value.status_code == 400


@pytest.mark.asyncio
async def test_grpc_bridge_serializes_json_and_decodes_response(monkeypatch):
    captured = {}

    class _FakeUnaryUnary:
        async def __call__(self, request, timeout=None):
            captured["request"] = request
            captured["timeout"] = timeout
            return b'[{"g": 7}]'

    class _FakeChannel:
        def unary_unary(self, path, request_serializer, response_deserializer):
            captured["path"] = path
            # proto-less bridge: serializers must be byte-identity
            assert request_serializer(b"x") == b"x"
            assert response_deserializer(b"y") == b"y"
            return _FakeUnaryUnary()

        async def close(self):
            captured["closed"] = True

    fake_grpc_mod = types.SimpleNamespace(
        aio=types.SimpleNamespace(insecure_channel=lambda t: _FakeChannel()),
    )
    monkeypatch.setitem(sys.modules, "grpc", fake_grpc_mod)
    out = await fd._grpc_call("svc:50051", "Pkg.Svc.Fn", {"args": {"n": 3}}, timeout=5.0)
    assert out == [{"g": 7}]
    assert captured["path"] == "/Pkg.Svc/Fn"
    assert captured["request"] == b'{"args": {"n": 3}}'
    assert captured["timeout"] == 5.0
    assert captured["closed"] is True


# ---- egress deny-by-default (REQ-885 security_constraint) -------------------


@pytest.mark.asyncio
async def test_http_egress_denied_when_host_not_allowlisted():
    fn = _fn(impl_kind="http", binding={"url": "https://evil.example/fn"})
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(fn, {}, _state(), "ops")
    assert ei.value.status_code == 403
    assert "egress" in ei.value.detail


@pytest.mark.asyncio
async def test_grpc_egress_denied_when_target_not_allowlisted():
    fn = _fn(impl_kind="grpc", binding={"target": "evil:50051", "method": "Pkg.Fn"})
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(fn, {}, _state(egress=[]), "ops")
    assert ei.value.status_code == 403


@pytest.mark.asyncio
async def test_loopback_egress_always_allowed(monkeypatch):
    async def fake_http(method, url, payload, timeout):
        return [{"ok": 1}]

    monkeypatch.setattr(fd, "_http_call", fake_http)
    fn = _fn(impl_kind="http", binding={"url": "http://localhost:8080/fn"})
    rows = await dispatch_function(fn, {}, _state(egress=[]), "ops")
    assert rows == [{"ok": 1}]


# ---- row-wise-external refusal (REQ-885 performance_constraint) -------------


@pytest.mark.asyncio
async def test_rowwise_external_refused_when_all_args_scalar():
    fn = _fn(
        impl_kind="http",
        binding={"url": "https://a.example/fn"},
        arguments=[{"name": "n", "type": "Int", "arg_kind": "column_value"}],
    )
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(fn, {"n": 1}, _state(), "ops")
    assert ei.value.status_code == 400
    assert "row-wise" in ei.value.detail


@pytest.mark.asyncio
async def test_setwise_external_allowed_with_relation_arg(monkeypatch):
    async def fake_http(method, url, payload, timeout):
        return [{"ok": 1}]

    monkeypatch.setattr(fd, "_http_call", fake_http)
    fn = _fn(
        impl_kind="http",
        binding={"url": "https://a.example/fn"},
        arguments=[
            {"name": "rs", "type": "String", "arg_kind": "result_set"},
            {"name": "n", "type": "Int", "arg_kind": "column_value"},
        ],
    )
    rows = await dispatch_function(fn, {"rs": "s3.public.items", "n": 1}, _state(), "ops")
    assert rows == [{"ok": 1}]


@pytest.mark.asyncio
async def test_grpc_missing_binding_fails_loud():
    fn = _fn(impl_kind="grpc", binding={"target": "svc:50051"})  # no method
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(fn, {}, _state(), "ops")
    assert ei.value.status_code == 400


# ---- python kind (in-process callable) -------------------------------------


@pytest.mark.asyncio
async def test_python_kind_in_process():
    mod = types.ModuleType("provisa_test_udf_mod")
    calls = {}

    def sample(payload, session):
        calls["payload"] = payload
        calls["corr"] = session.correlation_id
        return [{"py": payload["n"] * 2}]

    mod.sample = sample
    sys.modules["provisa_test_udf_mod"] = mod
    try:
        fn = _fn(impl_kind="python", binding={"callable": "provisa_test_udf_mod:sample"})
        rows = await dispatch_function(fn, {"n": 4}, _state(), "ops")
        assert rows == [{"py": 8}]
        assert calls["corr"]
    finally:
        del sys.modules["provisa_test_udf_mod"]


@pytest.mark.asyncio
async def test_python_bad_callable_spec_fails_loud():
    fn = _fn(impl_kind="python", binding={"callable": "nocolon"})
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(fn, {}, _state(), "ops")
    assert ei.value.status_code == 400


# ---- relation-argument kinds -----------------------------------------------


@pytest.mark.asyncio
async def test_relation_arg_kinds(monkeypatch):
    seen = {}

    async def fake_http(method, url, payload, timeout):
        seen["payload"] = payload
        return [{"ok": 1}]

    monkeypatch.setattr(fd, "_http_call", fake_http)
    st = _state()
    fn = _fn(
        impl_kind="http",
        binding={"url": "https://svc/fn"},
        arguments=[
            {"name": "tbl", "type": "String", "arg_kind": "table_ref"},
            {"name": "rs", "type": "String", "arg_kind": "result_set"},
            {"name": "n", "type": "Int", "arg_kind": "column_value"},
        ],
    )
    args = {"tbl": "s2.public.orders", "rs": "s3.public.items", "n": 9}
    await dispatch_function(fn, args, st, "ops")
    payload = seen["payload"]["args"]
    # table_ref = lazy: only the reference is passed, no materialization SELECT
    assert payload["tbl"] == {"kind": "table_ref", "ref": "s2.public.orders"}
    # result_set = eager: relation materialized to an Arrow-compatible batch
    assert payload["rs"]["kind"] == "result_set"
    assert payload["rs"]["rows"] == [{"id": 1, "name": "ada"}]
    # column_value = scalar row-wise
    assert payload["n"] == 9
    # exactly one SELECT was issued — for the result_set arg only (table_ref stayed lazy)
    assert st.source_pools.calls == [("s3", 'SELECT * FROM "public"."items"', [])]


@pytest.mark.asyncio
async def test_result_set_unqualified_ref_fails_loud():
    st = _state()
    fn = _fn(
        impl_kind="http",
        binding={"url": "https://svc/fn"},
        arguments=[{"name": "rs", "type": "String", "arg_kind": "result_set"}],
    )
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(fn, {"rs": "orders"}, st, "ops")
    assert ei.value.status_code == 400


# ---- unknown kind fail-loud ------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_impl_kind_fails_loud():
    fn = _fn(impl_kind="banana")
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(fn, {}, _state(), "ops")
    assert ei.value.status_code == 400
    assert "impl_kind" in ei.value.detail


# ---- REQ-1159: IR-typed dataset contract, validated both ends --------------


def _echo_http(rows):
    """A fake _http_call that echoes a fixed row set back as the command output."""

    async def _call(method, url, payload, timeout):
        return rows

    return _call


@pytest.mark.asyncio
async def test_result_set_input_contract_passes(monkeypatch):
    # Fake relation yields {"id": 1, "name": "ada"}; declared IR contract matches → no violation.
    monkeypatch.setattr(fd, "_http_call", _echo_http([{"ok": 1}]))
    fn = _fn(
        impl_kind="http",
        kind="query",
        binding={"url": "https://svc/fn"},
        arguments=[
            {
                "name": "rs",
                "type": "String",
                "arg_kind": "result_set",
                "columns": [{"name": "id", "type": "integer"}, {"name": "name", "type": "text"}],
            }
        ],
    )
    rows = await dispatch_function(fn, {"rs": "s3.public.items"}, _state(), "ops")
    assert rows == [{"ok": 1}]


@pytest.mark.asyncio
async def test_result_set_input_contract_violation_fails_loud(monkeypatch):
    # Relation column `id` is an int, but the contract declares it `text` → fail-loud 422.
    monkeypatch.setattr(fd, "_http_call", _echo_http([{"ok": 1}]))
    fn = _fn(
        impl_kind="http",
        kind="query",
        binding={"url": "https://svc/fn"},
        arguments=[
            {
                "name": "rs",
                "type": "String",
                "arg_kind": "result_set",
                "columns": [{"name": "id", "type": "text"}, {"name": "name", "type": "text"}],
            }
        ],
    )
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(fn, {"rs": "s3.public.items"}, _state(), "ops")
    assert ei.value.status_code == 422
    assert "input 'rs'" in ei.value.detail and "expected text" in ei.value.detail


@pytest.mark.asyncio
async def test_output_contract_passes(monkeypatch):
    monkeypatch.setattr(fd, "_http_call", _echo_http([{"score": 3}, {"score": 9}]))
    fn = _fn(
        impl_kind="http",
        kind="query",
        binding={"url": "https://svc/fn"},
        output_columns=[{"name": "score", "type": "integer"}],
    )
    rows = await dispatch_function(fn, {}, _state(), "ops")
    assert rows == [{"score": 3}, {"score": 9}]


@pytest.mark.asyncio
async def test_output_contract_violation_fails_loud(monkeypatch):
    # Service returns an off-contract value (string where integer declared) → fail-loud 422.
    monkeypatch.setattr(fd, "_http_call", _echo_http([{"score": "high"}]))
    fn = _fn(
        impl_kind="http",
        kind="query",
        binding={"url": "https://svc/fn"},
        output_columns=[{"name": "score", "type": "integer"}],
    )
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(fn, {}, _state(), "ops")
    assert ei.value.status_code == 422
    assert "output" in ei.value.detail and "expected integer" in ei.value.detail


@pytest.mark.asyncio
async def test_no_declared_contract_skips_validation(monkeypatch):
    # No columns / output_columns → no contract → rows pass through untouched (off-type included).
    monkeypatch.setattr(fd, "_http_call", _echo_http([{"anything": "goes", "n": 1}]))
    fn = _fn(
        impl_kind="http",
        kind="query",
        binding={"url": "https://svc/fn"},
        arguments=[{"name": "rs", "type": "String", "arg_kind": "result_set"}],
    )
    rows = await dispatch_function(fn, {"rs": "s3.public.items"}, _state(), "ops")
    assert rows == [{"anything": "goes", "n": 1}]
