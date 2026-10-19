# Copyright (c) 2026 Kenneth Stott
# Canary: 44c1f396-cb8e-45e1-9aa0-745dc633a25e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911, the three-device optimistic benchmark (demo/named/perf/METHODOLOGY.md): each device's
facts read off itself, device 1's server environment with every backend on device 3, telemetry off
or on per run, and the published point (telemetry off) over the repetitions."""

from __future__ import annotations

import json
import os
import socket
import sys
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import contract_model  # noqa: E402
import host_agent  # noqa: E402
import measurements  # noqa: E402
import optimistic_load as ol  # noqa: E402
import paging  # noqa: E402
import three_device as td  # noqa: E402

LSCPU = """Architecture:            x86_64
CPU(s):                  32
Thread(s) per core:      2
Core(s) per socket:      16
Socket(s):               1
"""


# --- the host agent ----------------------------------------------------------------------------


def test_lscpu_gives_physical_cores_not_threads() -> None:
    assert host_agent.parse_lscpu(LSCPU) == {
        "physical_cores": 16,
        "sockets": 1,
        "threads_per_core": 2,
    }


def test_host_facts_are_read_off_the_machine() -> None:
    meta = {
        "machine-type": "projects/123/machineTypes/c4d-standard-32",
        "zone": "projects/123/zones/us-east1-b",
    }
    facts = host_agent.host_facts(
        {"telemetry": "off"},
        lscpu=lambda: LSCPU,
        metadata=lambda path: meta[path],
        nics=lambda: {"ens4": "gve"},
    )
    assert facts["machine_type"] == "c4d-standard-32"
    assert facts["zone"] == "us-east1-b"
    assert facts["cpu"]["physical_cores"] == 16
    assert facts["nic_drivers"] == {"ens4": "gve"}
    assert facts["facts"] == {"telemetry": "off"}
    assert facts["errors"] == {}


def test_a_fact_that_cannot_be_read_is_an_error_never_a_guess() -> None:
    def no_metadata(path: str) -> str:
        raise OSError("metadata.google.internal unreachable")

    facts = host_agent.host_facts({}, lscpu=lambda: LSCPU, metadata=no_metadata, nics=lambda: {})
    assert facts["machine_type"] is None and facts["zone"] is None
    assert facts["errors"]["machine_type"] == "OSError: metadata.google.internal unreachable"


def test_nic_drivers_come_from_sysfs(tmp_path: Path) -> None:
    drivers = tmp_path / "drivers"
    (drivers / "gve").mkdir(parents=True)
    (tmp_path / "net" / "ens4" / "device").mkdir(parents=True)
    (tmp_path / "net" / "ens4" / "device" / "driver").symlink_to(drivers / "gve")
    (tmp_path / "net" / "lo").mkdir()
    assert host_agent.nic_drivers(tmp_path / "net") == {"ens4": "gve"}


@pytest.fixture
def listener() -> Iterator[tuple[str, int]]:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(256)
    stop = threading.Event()

    def accept() -> None:
        sock.settimeout(0.2)
        while not stop.is_set():
            try:
                conn, _ = sock.accept()
                conn.close()
            except TimeoutError:
                continue

    t = threading.Thread(target=accept, daemon=True)
    t.start()
    try:
        yield sock.getsockname()
    finally:
        stop.set()
        t.join()
        sock.close()


def test_the_round_trip_is_timed_tcp_connects(listener: tuple[str, int]) -> None:
    rtt = host_agent.tcp_rtt(*listener, connects=20)
    assert rtt["connects"] == 20 and rtt["port"] == listener[1]
    assert 0 < rtt["median_ms"] <= rtt["p99_ms"]


@pytest.fixture
def agent(listener: tuple[str, int]) -> Iterator[str]:
    server = host_agent.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        host_agent.handler(
            pid=os.getpid(),
            facts={"telemetry": "on", "engine": "pg"},
            rtt_targets={"backends": listener},
            docker_stats=lambda: "perf-postgresql-1 150.00%\nperf-redis-1 12.50%\n",
        ),
    )
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def test_the_agent_answers_its_facts_cpu_containers_and_round_trips(agent: str) -> None:
    host = httpx.get(f"{agent}/host").json()
    assert host["facts"] == {"telemetry": "on", "engine": "pg"}
    assert host["rtt_targets"] == ["backends"]
    assert httpx.get(f"{agent}/cpu").json()["pid"] == os.getpid()
    assert httpx.get(f"{agent}/rtt/backends").json()["connects"] == host_agent.RTT_CONNECTS
    paged = httpx.get(f"{agent}/paging").json()
    assert set(paged) in ({"error"}, {*paging.VMSTAT_FIELDS, "mem_available_kb", "majflt"})
    if "majflt" in paged:
        assert set(paged["majflt"]) == {"provisa"}
    stats = measurements.remote_docker_stats(f"{agent}/docker-stats")()
    assert measurements.parse_docker_stats(stats) == {
        "perf-postgresql-1": 1.5,
        "perf-redis-1": 0.125,
    }


def test_the_agent_answers_nothing_it_was_not_given(agent: str) -> None:
    assert httpx.get(f"{agent}/rtt/elsewhere").status_code == 404
    assert httpx.get(f"{agent}/anything").status_code == 404
    assert httpx.post(f"{agent}/host").status_code == 501


def test_an_agent_without_a_pid_has_no_cpu() -> None:
    server = host_agent.serve(("127.0.0.1", 0), pid=None, facts={}, rtt_targets={})
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        resp = httpx.get(f"http://127.0.0.1:{server.server_address[1]}/cpu")
        assert resp.status_code == 404 and "without --pid" in resp.json()["error"]
    finally:
        server.shutdown()
        server.server_close()


def test_the_agent_needs_an_explicit_listen_address(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["host_agent.py", "--fact", "device=backends"])
    with pytest.raises(SystemExit):
        host_agent.main()


def test_the_client_collects_every_devices_facts_and_round_trips(agent: str) -> None:
    devices = ol.collect_devices({"server": agent})
    assert devices["server"]["facts"]["telemetry"] == "on"
    assert set(devices["network"]) == {"client->server", "server->backends"}
    assert devices["network"]["server->backends"]["connects"] == host_agent.RTT_CONNECTS
    assert "cpu" in devices["client"]


def test_a_device_url_needs_its_port() -> None:
    with pytest.raises(contract_model.SetupError, match="--device server: the URL names no port"):
        ol.collect_devices(
            {"server": "http://10.0.0.5"},
            get=lambda url: {"rtt_targets": [], "facts": {}},
        )


# --- device 1's server ---------------------------------------------------------------------------

BASE = BENCH.parent / "config.yaml"


def test_telemetry_off_names_no_exporter_and_samples_nothing() -> None:
    config = yaml.safe_load(td.run_config(BASE.read_text(), telemetry="off", collector_url=None))
    assert config["observability"]["endpoint"] == ""
    assert config["observability"]["sample_rate"] == 0.0


def test_telemetry_on_exports_to_the_collector_at_the_configured_rate() -> None:
    base = yaml.safe_load(BASE.read_text())["observability"]["sample_rate"]
    config = yaml.safe_load(
        td.run_config(BASE.read_text(), telemetry="on", collector_url="http://10.0.0.4:24318")
    )
    assert config["observability"]["endpoint"] == "http://10.0.0.4:24318"
    assert config["observability"]["sample_rate"] == base


def _env(**kw: Any) -> dict[str, str]:
    args: dict[str, Any] = {
        "engine": "pg",
        "telemetry": "off",
        "backends_host": "10.0.0.4",
        "workers": 32,
        "config_path": "/x.yaml",
    }
    return td.server_env(**{**args, **kw})


def test_every_backend_is_on_device_3() -> None:
    env = _env()
    assert env["PROVISA_BENCH_POSTGRESQL_HOST"] == "10.0.0.4"
    assert env["REDIS_URL"] == "redis://10.0.0.4:26379"
    assert env["TENANT_DATABASE_URL"].endswith("@10.0.0.4:25634/provisa")
    assert env["PG_HOST"] == "10.0.0.4"
    assert env["PROVISA_ENGINE"] == "pg"
    assert env["PROVISA_ENGINE_URL"] == (
        "postgresql://provisa:provisa@10.0.0.4:25633/provisa_engine"
    )
    assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in env
    assert env["PROVISA_WORKERS"] == "32"


def test_the_engine_is_where_the_methodology_puts_it() -> None:
    duckdb = _env(engine="duckdb")
    assert duckdb["PROVISA_ENGINE"] == "duckdb" and "PROVISA_ENGINE_URL" not in duckdb
    trino = _env(engine="trino")
    assert (trino["TRINO_HOST"], trino["TRINO_PORT"]) == ("10.0.0.4", "28080")
    with pytest.raises(ValueError, match="engine must be one of"):
        _env(engine="spark")


def test_telemetry_on_exports_to_device_3() -> None:
    env = _env(telemetry="on", request_threads=2)
    assert env["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://10.0.0.4:24318"
    assert env["OTEL_EXPORTER_OTLP_PROTOCOL"] == "http/protobuf"
    assert env["PROVISA_REQUEST_THREADS"] == "2"


# --- the report over repetitions ---------------------------------------------------------------


def _step(c: int, rps: float, *, errors: int = 0, limited: bool = False, cpu: float = 1.0) -> dict:
    return {
        "concurrency": c,
        "req_per_s": rps,
        "p50_ms": 2.0,
        "p99_ms": 9.0,
        "errors": errors,
        "client_limited": limited,
        "server_cpu_ms_per_request": cpu,
        "client_cpu_cores": 3.0,
        "source_cpu_cores": {"bench-postgresql": 0.5},
        "backend_cpu_cores": {"perf-redis-1": 0.2},
        "paging": {
            "client": {"error": "FileNotFoundError: /proc/vmstat"},
            "server": {
                "pgmajfault": 12,
                "pswpin": 0,
                "pswpout": 3,
                "majflt": {"provisa": 7},
                "mem_available_mb_start": 900,
                "mem_available_mb_end": 850,
            },
        },
    }


def _run(root: Path, name: str, telemetry: str, peak: float, cpu: float) -> None:
    out = root / name / "optimistic"
    out.mkdir(parents=True)
    steps = [
        _step(1, 400.0, cpu=cpu),
        _step(64, peak, cpu=cpu),
        _step(256, peak * 1.5, errors=3, cpu=cpu),  # faster, with errors: not the point
        _step(512, peak * 1.2, limited=True, cpu=cpu),  # client-limited: not the point
    ]
    (out / "graphql.json").write_text(json.dumps({"transport": "graphql", "steps": steps}))
    devices = {"server": {"facts": {"telemetry": telemetry}}, "network": {}, "client": {}}
    (out / "devices.json").write_text(json.dumps(devices))


def test_the_published_point_is_telemetry_off_over_the_repetitions(tmp_path: Path) -> None:
    for rep, (off, on) in enumerate([(2000, 1800), (2100, 1850), (1900, 1750)], start=1):
        _run(tmp_path, f"{rep}-off", "off", off, 1.0)
        _run(tmp_path, f"{rep}-on", "on", on, 1.2)
    published, internal = td.build(tmp_path)
    g = published["transports"]["graphql"]
    assert published["caption"] == "single source, direct read; Telemetry OFF"
    assert g["repetitions"] == 3 and g["concurrency"] == [64, 64, 64]
    assert g["req_per_s"] == {"median": 2000, "min": 1900, "max": 2100}
    assert g["backend_cpu_cores"]["perf-redis-1"]["median"] == 0.2
    # paging beside CPU per request: each host's counters over the repetitions, or why not read
    server = g["paging"]["server"]
    assert server["pgmajfault"]["median"] == 12 and server["pswpout"]["max"] == 3
    assert server["majflt"] == {"provisa": {"median": 7, "min": 7, "max": 7}}
    assert server["mem_available_mb_start"]["median"] == 900
    assert server["mem_available_mb_end"]["median"] == 850
    assert g["paging"]["client"] == {
        "majflt": {},
        "errors": ["FileNotFoundError: /proc/vmstat"],
    }
    assert "telemetry_on" not in published and "impact_on_minus_off" not in published
    assert internal["label"] == "internal: not for publication"
    impact = internal["impact_on_minus_off"]["graphql"]
    assert impact["req_per_s"] == 1800 - 2000
    assert impact["server_cpu_ms_per_request"] == pytest.approx(0.2)


def test_a_run_whose_server_disagrees_on_telemetry_is_refused(tmp_path: Path) -> None:
    _run(tmp_path, "1-off", "on", 2000, 1.0)
    with pytest.raises(ValueError, match="1-off: the server says telemetry on, not off"):
        td.build(tmp_path)


def test_a_run_with_no_clean_step_is_refused(tmp_path: Path) -> None:
    out = tmp_path / "1-off" / "optimistic"
    out.mkdir(parents=True)
    report = {"transport": "graphql", "steps": [_step(1, 10.0, errors=1)]}
    (out / "graphql.json").write_text(json.dumps(report))
    (out / "devices.json").write_text(json.dumps({"server": {"facts": {"telemetry": "off"}}}))
    with pytest.raises(ValueError, match="1-off/graphql.json: no step without errors"):
        td.build(tmp_path)


# --- the contract's warm-up --------------------------------------------------------------------


def test_a_contract_may_state_a_warm_up() -> None:
    import setup_contract as sc

    raw = yaml.safe_load((BENCH / "setups" / "perf-stack.yaml").read_text())
    raw["load"]["warmup_s"] = 60
    setup = sc.setup_from_dict(
        raw,
        environ={"PROVISA_HTTP_BASE_URL": "http://localhost:8001"},
        known_transports=list(ol.TRANSPORTS),
    )
    assert setup.load.warmup_s == 60
    del raw["load"]["warmup_s"]
    assert (
        sc.setup_from_dict(
            raw,
            environ={"PROVISA_HTTP_BASE_URL": "http://localhost:8001"},
            known_transports=list(ol.TRANSPORTS),
        ).load.warmup_s
        == 0
    )


def test_the_three_device_contract_reaches_device_1_by_its_private_address() -> None:
    import setup_contract as sc

    raw = yaml.safe_load((BENCH / "setups" / "three-device.yaml").read_text())
    setup = sc.setup_from_dict(
        raw,
        environ={
            "PROVISA_HTTP_BASE_URL": "http://10.90.0.2:8001",
            "PROVISA_BENCH_SERVER_HOST": "10.90.0.2",
        },
        known_transports=list(ol.TRANSPORTS),
    )
    e = setup.endpoints
    assert {e.pgwire.host, e.bolt.host, e.flight.host, e.grpc.host} == {"10.90.0.2"}
    assert (setup.load.warmup_s, setup.load.window_s) == (60, 30)


def test_a_host_from_an_unset_variable_is_refused_naming_it() -> None:
    import setup_contract as sc

    raw = yaml.safe_load((BENCH / "setups" / "three-device.yaml").read_text())
    with pytest.raises(
        contract_model.SetupError,
        match="deployment.endpoints.pgwire.host: environment variable PROVISA_BENCH_SERVER_HOST",
    ):
        sc.setup_from_dict(
            raw,
            environ={"PROVISA_HTTP_BASE_URL": "http://10.90.0.2:8001"},
            known_transports=list(ol.TRANSPORTS),
        )


def test_device_1_starts_every_listener_the_contract_reaches() -> None:
    env = _env()
    assert (env["PROVISA_PGWIRE_PORT"], env["PROVISA_BOLT_PORT"]) == ("5439", "17687")


def test_the_driver_dry_run_runs_nothing_and_says_what_it_would() -> None:
    import os
    import subprocess

    out = subprocess.run(
        ["bash", str(BENCH / "three_device_run.sh"), "--reps", "1", "--dry-run"],
        env={
            **os.environ,
            "PERF_ZONE": "us-east1-b",
            "PERF_REPO": "/opt/provisa",
            "PERF_SERVER_IP": "10.90.0.2",
            "PERF_BACKENDS_IP": "10.90.0.4",
            "PATH": "/usr/bin:/bin",  # no gcloud: a dry run never calls it
        },
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert "[provisa-perf-backends]" in out and "[provisa-perf-server]" in out
    assert "orchestrate.sh serve --telemetry off --engine pg" in out
    assert out.index("--telemetry off") < out.index("--telemetry on")
    assert "--setup setups/three-device.yaml" in out
    assert "PROVISA_BENCH_SERVER_HOST=10.90.0.2" in out


_LIFECYCLE = BENCH / "bench_lifecycle.sh"
_STARTUP = BENCH.parents[3] / "terraform" / "perf" / "startup.sh.tftpl"


@pytest.mark.parametrize("missing", ["--project", "--zone", "--commit", "--ssh-cidr"])
def test_the_lifecycle_refuses_a_run_missing_what_it_needs(missing: str) -> None:
    import subprocess

    given = {
        "--project": "p",
        "--zone": "us-east1-b",
        "--commit": "abc",
        "--ssh-cidr": "203.0.113.10/32",
    }
    del given[missing]
    args = [a for k, v in given.items() for a in (k, v)]
    done = subprocess.run(
        ["bash", str(_LIFECYCLE), *args, "--dry-run"], capture_output=True, text=True
    )
    assert done.returncode == 2
    assert f"{missing} is required" in done.stdout


def test_the_lifecycle_destroys_on_any_exit_unless_kept() -> None:
    text = _LIFECYCLE.read_text()
    # destroy is armed before apply, so a failed apply or run still tears down what exists
    assert text.index("trap destroy EXIT") < text.index("apply -input=false -auto-approve")
    assert 'if [ -n "$KEEP" ]' in text


def test_a_device_is_ready_only_when_every_setup_step_passed() -> None:
    script = _STARTUP.read_text()
    assert "set -euo pipefail" in script
    # cleared at boot, written last: the marker names the commit only after every step
    assert script.index('rm -f "$READY"') < script.index("docker compose up seeder")
    assert script.rstrip().endswith('echo "${commit}" > "$READY"')


def test_the_results_go_where_the_lifecycle_says() -> None:
    import os
    import subprocess

    out = subprocess.run(
        ["bash", str(BENCH / "three_device_run.sh"), "--reps", "1", "--dry-run"],
        env={
            **os.environ,
            "PERF_ZONE": "us-east1-b",
            "PERF_REPO": "/opt/provisa",
            "PERF_SERVER_IP": "10.90.0.2",
            "PERF_BACKENDS_IP": "10.90.0.4",
            "PERF_LOCAL_OUT": "/runs/x/results",
            "PATH": "/usr/bin:/bin",
        },
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert "/runs/x/results/" in out and "--out /runs/x/results-report" in out
