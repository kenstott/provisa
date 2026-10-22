# Copyright (c) 2026 Kenneth Stott
# Canary: 71fa6ac8-ee7a-4c46-8e1d-56226d4f8a2e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where the churnprod demo's at-risk list spends its time (REQ-2099).

The demo's database comes up as test_churnprod_demo brings it up; a real Provisa server (DuckDB
engine, its own control plane, debug trace detail) loads the demo's config.yaml and exports its
spans to a receiver in this process. The tour's at-risk statement is run three times as org_admin
-- the first read installs the code source's package -- and each run's request is broken down by
the stages its request span reports (``stage.<name>.ms``: landing, the code call, the engine's
statement, governance) and by every span it exported, longest first. The breakdown is printed
(run with ``-rP``) and kept as the test's ``at_risk_timing`` property."""

# Requirements: REQ-2099

from __future__ import annotations

import time
from pathlib import Path

import httpx
import pytest
import yaml

from tests.integration.test_churnprod_demo import ChurnStack
from tests.otlp_receiver import OtlpReceiver, ReceivedSpan

pytestmark = [pytest.mark.integration]

_DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "churnprod"
_STEPS = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]
(_AT_RISK,) = [s["sql"] for s in _STEPS if s["key"] == "churnprodAtRisk"]
_RUNS = 3


@pytest.fixture(scope="module")
def stack():
    built = ChurnStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def traced(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer
    from tests.port_lease import lease_ports

    (receiver_port,) = lease_ports(1)
    receiver = OtlpReceiver(port=receiver_port)
    receiver.start()
    home = tmp_path_factory.mktemp("churnprod_timing")
    srv = IsolatedServer(
        "churnprod_timing",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={
            **stack.env,
            "PROVISA_DATA_DIR": str(home / "provisa"),
            "PROVISA_RUNTIME_DEPS_CACHE": str(home / "runtime-deps"),
            "OTEL_SDK_DISABLED": "false",
            "OTEL_EXPORTER_OTLP_ENDPOINT": receiver.endpoint,
            "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
            "OTEL_SPAN_EXPORT_DELAY_MILLIS": "200",
            "PROVISA_TRACE_DETAIL": "debug",
        },
    )
    try:
        srv.start(timeout=400)
    except BaseException:
        receiver.stop()
        raise
    try:
        yield srv, receiver
    finally:
        srv.stop_process()
        receiver.stop()


@pytest.fixture(autouse=True)
def _requests_sent_here_start_no_trace():
    """The requests sent here carry no trace context of this process, so each is a trace of its
    own (see test_trace_detail_span_count_e2e)."""
    from opentelemetry.instrumentation.utils import suppress_instrumentation

    with suppress_instrumentation():
        yield


def _ms(span: ReceivedSpan) -> float:
    return (span.end_ns - span.start_ns) / 1e6


def _breakdown(run: int, wall_s: float, spans: list[ReceivedSpan]) -> str:
    """One run: its wall time, its request span's stage times and every span, longest first."""
    lines = [f"run {run}: {wall_s:.2f} s wall, {len(spans)} spans"]
    roots = [s for s in spans if not s.parent_span_id]
    for root in roots:
        lines.append(f"  request span {root.name}: {_ms(root):.1f} ms")
        stages = sorted(
            ((k, v) for k, v in root.attrs.items() if k.startswith("stage.")),
            key=lambda kv: kv[0],
        )
        lines += [f"    {k} = {v}" for k, v in stages]
    for span in sorted(spans, key=_ms, reverse=True)[:40]:
        facts = {k: v for k, v in span.attrs.items() if k.startswith(("codesource.", "db.rows"))}
        lines.append(f"  {_ms(span):10.1f} ms  {span.name}  {facts or ''}")
    return "\n".join(lines)


def test_where_the_at_risk_list_spends_its_time(traced, record_property):
    srv, receiver = traced
    report = []
    for run in range(1, _RUNS + 1):
        receiver.settle()
        before = {s.trace_id for s in receiver.snapshot()}
        started = time.perf_counter()
        resp = httpx.post(
            f"{srv.base_url}/data/sql",
            json={"sql": _AT_RISK},
            headers={"X-Provisa-Role": "org_admin"},
            timeout=900,
        )
        wall = time.perf_counter() - started
        assert resp.status_code == 200, resp.text
        assert len(resp.json()["data"]["sql"]) == 20
        spans = [s for s in receiver.settle() if s.trace_id not in before]
        report.append(_breakdown(run, wall, spans))
    text = "\n\n".join(report)
    print(f"\n=== churnprod at-risk timing ===\n{text}")
    record_property("at_risk_timing", text)
