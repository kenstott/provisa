# Copyright (c) 2026 Kenneth Stott
# Canary: 9b41d7e2-0c65-4a38-8f2d-6e1a3c9f4b57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-886: engine-emitted UDF invocation tracing is mandatory and non-bypassable.

Every dispatch kind emits a trace carrying all required fields; the correlation id is
stamped into the minted pgwire session so audit rows join to the invocation; and no kind
can skip the trace — it is emitted even when the executor raises."""

from __future__ import annotations

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
    def __init__(self, connected=True):
        self._connected = connected
        self._result = _FakeResult(["id", "name"], [(1, "ada"), (2, "grace")])

    def has(self, _src_id):
        return self._connected

    async def execute(self, _src_id, _sql, _params):
        return self._result


def _state(connected=True):
    return SimpleNamespace(
        roles={"ops": {"id": "ops", "capabilities": []}},
        source_pools=_FakePools(connected=connected),
        udf_trace_sink=MemoryUdfTraceSink(),
        minted_sessions=[],
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


_REQUIRED_FIELDS = (
    "udf_name",
    "transport",
    "identity",
    "input_refs",
    "output_cardinality",
    "output_bytes",
    "duration_ms",
    "status",
    "correlation_id",
)


def _kinds(monkeypatch):
    """Register mocked transports for every hosted kind; return {impl_kind: (fn, transport)}."""

    async def fake_http(_m, _u, _p, _t):
        return [{"ok": 1}, {"ok": 2}]

    async def fake_grpc(_target, _method, _payload):
        return [{"g": 1}, {"g": 2}]

    async def fake_run(_argv, _payload):
        return b'[{"s": 1}, {"s": 2}]'

    monkeypatch.setattr(fd, "_http_call", fake_http)
    monkeypatch.setattr(fd, "_grpc_call", fake_grpc)
    monkeypatch.setattr(fd, "_run_subprocess", fake_run)

    import sys
    import types

    mod = types.ModuleType("provisa_test_udf_trace_mod")
    mod.fn = lambda payload, session: [{"p": 1}, {"p": 2}]  # noqa: ARG005
    sys.modules["provisa_test_udf_trace_mod"] = mod

    return {
        "source_procedure": (_fn(), "sql"),
        "script": (_fn(impl_kind="script", binding={"argv": ["/bin/x"]}), "script"),
        "http": (_fn(impl_kind="http", binding={"url": "https://svc/fn"}), "http"),
        "grpc": (_fn(impl_kind="grpc", binding={"target": "svc:1", "method": "P.F"}), "grpc"),
        "python": (
            _fn(impl_kind="python", binding={"callable": "provisa_test_udf_trace_mod:fn"}),
            "python",
        ),
    }


@pytest.mark.asyncio
async def test_every_kind_emits_trace_with_all_fields(monkeypatch):
    for impl_kind, (fn, transport) in _kinds(monkeypatch).items():
        st = _state()
        await dispatch_function(fn, {"n": 1}, st, "ops")
        assert len(st.udf_trace_sink.records) == 1, impl_kind
        trace = st.udf_trace_sink.records[0]
        for fname in _REQUIRED_FIELDS:
            assert hasattr(trace, fname), (impl_kind, fname)
        assert trace.udf_name == "createOrder"
        assert trace.transport == transport
        assert trace.identity == "invoker"
        assert trace.output_cardinality == 2
        assert trace.output_bytes > 0
        assert trace.status == "ok"
        assert trace.correlation_id


@pytest.mark.asyncio
async def test_correlation_id_stamped_into_minted_session(monkeypatch):
    monkeypatch.setattr(fd, "_http_call", lambda *_a, **_k: _async([{"x": 1}]))
    st = _state()
    fn = _fn(impl_kind="http", binding={"url": "https://svc/fn"})
    await dispatch_function(fn, {"n": 1}, st, "ops")
    trace = st.udf_trace_sink.records[0]
    assert len(st.minted_sessions) == 1
    assert st.minted_sessions[0].correlation_id == trace.correlation_id


async def _async(value):
    return value


@pytest.mark.asyncio
async def test_identity_definer_when_materialize():
    st = _state()
    fn = _fn(materialize=True)  # DEFINER/admin, output-governed
    await dispatch_function(fn, {}, st, "ops")
    assert st.udf_trace_sink.records[0].identity == "definer"


@pytest.mark.asyncio
async def test_trace_emitted_even_on_error():
    st = _state(connected=False)  # source_procedure raises 503
    fn = _fn()
    with pytest.raises(HTTPException):
        await dispatch_function(fn, {}, st, "ops")
    assert len(st.udf_trace_sink.records) == 1
    assert st.udf_trace_sink.records[0].status == "error"


@pytest.mark.asyncio
async def test_tracing_not_bypassable_default_sink(monkeypatch):
    """Even with no sink on state, dispatch records to the default sink — never silent."""
    from provisa.otel_compat import default_udf_trace_sink

    before = len(default_udf_trace_sink().records)
    st = SimpleNamespace(
        roles={"ops": {"id": "ops", "capabilities": []}},
        source_pools=_FakePools(),
    )
    await dispatch_function(_fn(), {}, st, "ops")
    assert len(default_udf_trace_sink().records) == before + 1
