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


def test_a_peers_round_trip_probe_is_never_dropped_by_the_agents_backlog() -> None:
    """A peer's probe is RTT_CONNECTS back-to-back connects; with the agent's accepting thread
    busy (here: not accepting at all), every one of them is queued by the kernel, so none waits on
    a dropped SYN's 1 s retransmit."""
    server = host_agent.serve(("127.0.0.1", 0), pid=None, facts={}, rtt_targets={})
    try:
        rtt = host_agent.tcp_rtt(*server.server_address[:2], connects=host_agent.RTT_CONNECTS)
        assert rtt["p99_ms"] < 500
    finally:
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
            "PERF_BUCKET": "bkt",
            "PERF_RUN_ID": "run1",
            "PATH": "/usr/bin:/bin",  # no gcloud: a dry run never calls it
        },
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert "[provisa-perf-backends]" in out and "[provisa-perf-server]" in out
    assert "orchestrate.sh serve --telemetry off --engine pg" in out
    # Started in the background, the serve gets SIGINT back at its default, so it stops on it.
    assert "nohup setsid env --default-signal=INT" in out
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
            "PERF_BUCKET": "bkt",
            "PERF_RUN_ID": "run1",
            "PERF_LOCAL_OUT": "/runs/x/results",
            "PATH": "/usr/bin:/bin",
        },
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert "/runs/x/results/" in out and "--out /runs/x/results-report" in out


# --- a governance pass (REQ-2092) --------------------------------------------------------------


def test_a_pass_run_states_its_variant_cache_and_load() -> None:
    import governance_variants as gv

    text = (BENCH / "setups" / "three-device.yaml").read_text()
    c = yaml.safe_load(
        td.variant_contract(
            text,
            telemetry="off",
            variant="all",
            cache="uncached",
            steps=[1, 16],
            window_s=15,
            warmup_s=15,
        )
    )
    assert c["knobs"]["governance"] == {**gv.ZERO, **gv.VARIANTS["all"], "telemetry": False}
    assert c["knobs"]["cache"] == {"probability": 0.0}
    assert c["knobs"]["fields"]["distribution"] == {"kind": "constant", "value": 5}
    assert (c["load"]["steps"], c["load"]["window_s"], c["load"]["warmup_s"]) == ([1, 16], 15, 15)
    with pytest.raises(ValueError, match="variant must be one of"):
        td.variant_contract(
            text, telemetry="off", variant="most", cache="cached", steps=[1], window_s=1, warmup_s=0
        )


def _pass_run(root: Path, variant: str, cache: str, cpu: float) -> None:
    out = root / f"1-{variant}-{cache}-off" / "optimistic"
    out.mkdir(parents=True)
    report = {"transport": "graphql", "steps": [_step(1, 400.0, cpu=cpu)]}
    (out / "graphql.json").write_text(json.dumps(report))
    (out / "devices.json").write_text(json.dumps({"server": {}}))


def test_the_pass_report_gives_each_variant_over_zero(tmp_path: Path) -> None:
    _pass_run(tmp_path, "zero", "cached", 1.0)
    _pass_run(tmp_path, "row_rules_2", "cached", 1.25)
    _pass_run(tmp_path, "zero_end", "cached", 1.1)
    rows = {r["variant"]: r for r in td.variant_rows(tmp_path)}
    assert rows["row_rules_2"]["over_zero_ms"] == 0.25
    assert rows["zero_end"]["over_zero_ms"] == 0.1
    assert rows["row_rules_2"]["paging"]["server"]["pgmajfault"] == 12


def test_the_pass_dry_run_runs_every_variant_and_cache_and_reports_the_pass() -> None:
    import os
    import subprocess

    out = subprocess.run(
        [
            "bash",
            str(BENCH / "three_device_run.sh"),
            "--reps",
            "1",
            "--variants",
            "zero,all",
            "--caches",
            "cached,uncached",
            "--telemetry",
            "off",
            "--steps",
            "1,16",
            "--window-s",
            "15",
            "--warmup-s",
            "15",
            "--dry-run",
        ],
        env={
            **os.environ,
            "PERF_ZONE": "us-east1-b",
            "PERF_REPO": "/opt/provisa",
            "PERF_SERVER_IP": "10.90.0.2",
            "PERF_BACKENDS_IP": "10.90.0.4",
            "PERF_BUCKET": "bkt",
            "PERF_RUN_ID": "run1",
            "PERF_LOCAL_OUT": "/runs/x/results",
            "PATH": "/usr/bin:/bin",
        },
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    runs = [line.split()[2] for line in out.splitlines() if line.startswith("=== run ")]
    assert runs == [
        "1-zero-cached-off",
        "1-zero-uncached-off",
        "1-all-cached-off",
        "1-all-uncached-off",
    ]
    assert "--variant all --cache uncached --steps 1,16 --window-s 15 --warmup-s 15" in out
    assert "--telemetry on" not in out
    assert "three_device.py variant-report --root /runs/x/results" in out


def test_a_pass_needs_all_its_flags() -> None:
    import os
    import subprocess

    done = subprocess.run(
        ["bash", str(BENCH / "three_device_run.sh"), "--variants", "zero", "--dry-run"],
        env={
            **os.environ,
            "PERF_ZONE": "z",
            "PERF_REPO": "/r",
            "PERF_SERVER_IP": "a",
            "PERF_BACKENDS_IP": "b",
            "PERF_BUCKET": "bkt",
            "PERF_RUN_ID": "run1",
            "PATH": "/usr/bin:/bin",
        },
        capture_output=True,
        text=True,
    )
    assert done.returncode == 2 and "a governance pass needs" in done.stdout


def test_the_lifecycle_sizes_every_device_with_one_flag() -> None:
    text = _LIFECYCLE.read_text()
    for device in ("server", "client", "backends"):
        assert f'-var "{device}_machine_type=$MACHINE_TYPE"' in text
    assert '-var "disk_gb=$DISK_GB"' in text


_FAKE_GCLOUD = r"""
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
S = ROOT / "cloud"
args = sys.argv[1:]
with (ROOT / "calls.log").open("a") as f:
    f.write("gcloud" + "".join(f" [{a}]" for a in args) + "\n")


def read(name: str, default: str = "") -> str:
    p = S / name
    return p.read_text() if p.exists() else default


def write(name: str, text: str) -> None:
    p = S / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


verb = args[:3]
if verb == ["compute", "images", "describe"]:
    if (S / "images" / args[3]).exists():
        print(args[3])
        sys.exit(0)
    sys.stderr.write(
        "ERROR: (gcloud.compute.images.describe) Could not fetch resource:\n"
        f" - The resource 'projects/p/global/images/{args[3]}' was not found\n"
    )
    sys.exit(1)
if verb == ["compute", "images", "create"]:
    write(f"images/{args[3]}", " ".join(args))
    sys.exit(0)
if verb == ["compute", "instances", "describe"]:
    print(read(f"status/{args[3]}", "RUNNING"))
    sys.exit(0)
if verb == ["compute", "instances", "start"]:
    if (S / "start_fails").exists():
        sys.exit(1)
    for name in args[3:]:
        if name.startswith("--"):
            break
        write(f"status/{name}", "RUNNING")
    sys.exit(0)
if verb == ["compute", "instances", "stop"]:
    write(f"status/{args[3]}", "STOPPED")
    sys.exit(0)
if args[:2] == ["compute", "scp"]:
    name = args[-2].rsplit("/", 1)[1]
    shutil.copytree(S / "fixture" / name, Path(args[-1]) / name)
    sys.exit(0)
if args[:2] == ["compute", "ssh"]:
    device, command = args[2], args[args.index("--command") + 1]
    if "journalctl" in command:
        print(f"setup log of {device}")
    elif "docker compose --profile three-device stop" in command:
        if (S / "compose_stop_fails").exists():
            sys.exit(1)
    elif "/var/lib/provisa-perf/ready" in command:
        if device not in read("never_ready").split():
            print(read("commit"))
    elif "run_benchmark.py" in command:
        name = re.search(r"--output-dir results/three-device/(\S+)", command).group(1)
        with (S / "benchmarks").open("a") as f:
            f.write(name + "\n")
        left = int(read(f"preempt/{name}", "0"))
        if left:
            write(f"preempt/{name}", str(left - 1))
            write("status/provisa-perf-client", "STOPPED")
            sys.exit(255)
        if (S / "fail_runs").exists():
            sys.exit(1)
sys.exit(0)
"""

_FAKE_GSUTIL = r"""
import glob
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
S = ROOT / "cloud" / "gcs"
args = [a for a in sys.argv[1:] if a != "-m"]
with (ROOT / "calls.log").open("a") as f:
    f.write("gsutil " + " ".join(sys.argv[1:]) + "\n")


def local(url: str) -> Path:
    return S / url.removeprefix("gs://")


if args[0] == "ls":
    hits = sorted(glob.glob(str(local(args[1]))))
    if not hits:
        sys.stderr.write("CommandException: One or more URLs matched no objects.\n")
        sys.exit(1)
    for hit in hits:
        print("gs://" + str(Path(hit).relative_to(S)))
elif args[:2] == ["cp", "-"]:
    local(args[2]).parent.mkdir(parents=True, exist_ok=True)
    local(args[2]).write_text(sys.stdin.read())
elif args[:2] == ["cp", "-r"]:
    src = Path(args[2])
    shutil.copytree(src, local(args[3].rstrip("/")) / src.name, dirs_exist_ok=True)
elif args[:2] == ["rsync", "-r"]:
    if not local(args[2]).exists():
        sys.exit(1)
    shutil.copytree(local(args[2]), args[3], dirs_exist_ok=True)
else:
    sys.exit(f"fake gsutil: {args}")
"""


def _fake_cloud(tmp_path: Path, commit: str, *, image: bool = True) -> Path:
    """terraform, gcloud and gsutil stand-ins on PATH (tmp_path/bin): each logs its arguments to
    tmp_path/calls.log; the cloud's state is files under tmp_path/cloud (images, instance status,
    the bucket under gcs/, each device's ready commit). The seed image for the commit is stored
    unless image is False. Returns the state directory."""
    bin_dir, state = tmp_path / "bin", tmp_path / "cloud"
    bin_dir.mkdir()
    (state / "images").mkdir(parents=True)
    (state / "commit").write_text(commit)
    if image:
        (state / "images" / _seed_image(commit)).write_text("")
    log = tmp_path / "calls.log"
    (bin_dir / "terraform").write_text(
        f"""#!/bin/bash
echo "terraform $*" >> {log}
case " $* " in
  *" output "*) echo '{{"server": "10.90.0.2", "client": "10.90.0.3", "backends": "10.90.0.4"}}' ;;
esac
"""
    )
    (bin_dir / "gcloud").write_text(f"#!{sys.executable}\n{_FAKE_GCLOUD}")
    (bin_dir / "gsutil").write_text(f"#!{sys.executable}\n{_FAKE_GSUTIL}")
    for tool in ("terraform", "gcloud", "gsutil"):
        (bin_dir / tool).chmod(0o755)
    return state


_REPO = BENCH.parents[3]


def _head() -> str:
    import subprocess

    return subprocess.run(
        ["git", "-C", str(_REPO), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _seed_image(commit: str) -> str:
    import seed_inputs

    return f"provisa-perf-seed-{seed_inputs.seed_hash(_REPO, commit)[:12]}"


def _lifecycle(tmp_path: Path, *args: str, script: Path | None = None) -> Any:
    import shutil
    import subprocess

    return subprocess.run(
        ["bash", str(script or _LIFECYCLE), *args],
        env={
            "PATH": f"{tmp_path / 'bin'}:{Path(shutil.which('python3')).parent}:/usr/bin:/bin",
            "HOME": str(tmp_path),
            "PROVISA_BENCH_RUNS": str(tmp_path / "runs"),
        },
        capture_output=True,
        text=True,
    )


def _calls(tmp_path: Path) -> list[str]:
    return (tmp_path / "calls.log").read_text().splitlines()


def test_a_waiting_device_is_held_up_and_every_setup_log_is_kept_before_destroy(
    tmp_path: Path,
) -> None:
    commit = _head()
    state = _fake_cloud(tmp_path, commit)
    (state / "never_ready").write_text("provisa-perf-backends")
    runs = tmp_path / "runs"
    done = _lifecycle(
        tmp_path,
        *("--project", "p", "--zone", "us-east1-b", "--commit", commit),
        *("--ssh-cidr", "203.0.113.10/32", "--ready-timeout", "0"),
    )
    assert done.returncode == 1, done.stdout + done.stderr
    assert f"provisa-perf-backends was not ready at {commit}" in done.stdout
    calls = _calls(tmp_path)
    polls = [c for c in calls if "/var/lib/provisa-perf/ready" in c]
    # server, client, then backends once before the deadline; each poll refreshes the idle hold
    assert len(polls) == 3
    assert all("sudo touch /var/lib/provisa-idle/hold;" in c for c in polls)
    journals = [i for i, c in enumerate(calls) if "journalctl -t provisa-perf-startup" in c]
    destroy = next(
        i for i, c in enumerate(calls) if c.startswith("terraform ") and " destroy " in c
    )
    assert len(journals) == 3 and max(journals) < destroy
    (run,) = runs.iterdir()
    for device in ("server", "client", "backends"):
        assert (
            run / f"startup-{device}.log"
        ).read_text() == f"setup log of provisa-perf-{device}\n"


def test_the_backends_device_alone_can_be_given_a_larger_machine(tmp_path: Path) -> None:
    """Its stores are sized for a large box: a cheap stage sizes the backends device apart, after
    --machine-type, and keeps the server and client small."""
    commit = _head()
    _fake_cloud(tmp_path, commit)
    done = _lifecycle(
        tmp_path,
        *("--project", "p", "--zone", "us-east1-b", "--commit", commit),
        *("--ssh-cidr", "203.0.113.10/32", "--dry-run"),
        *("--machine-type", "c4d-standard-4", "--backends-machine-type", "c4d-highmem-8"),
    )
    assert done.returncode == 0, done.stdout + done.stderr
    (plan,) = [c for c in _calls(tmp_path) if c.startswith("terraform ") and " plan " in c]
    assert "-var server_machine_type=c4d-standard-4" in plan
    assert "-var client_machine_type=c4d-standard-4" in plan
    # the later value is the one terraform keeps
    assert plan.index("backends_machine_type=c4d-standard-4") < plan.index(
        "backends_machine_type=c4d-highmem-8"
    )


def test_each_image_the_stack_builds_is_built_by_one_service() -> None:
    """Two services building one image tag at once fail ("image already exists"), so a tag is
    built by one service and the others only run it."""
    import collections

    import yaml

    compose = yaml.safe_load((BENCH.parent / "docker-compose.yml").read_text())
    builders = collections.Counter(
        svc["image"] for svc in compose["services"].values() if "build" in svc and "image" in svc
    )
    assert [tag for tag, n in builders.items() if n > 1] == []
    runners = {
        name: svc
        for name, svc in compose["services"].items()
        if "build" not in svc and svc.get("image") in builders
    }
    assert runners and all(svc.get("pull_policy") == "never" for svc in runners.values())


def test_no_remote_process_match_finds_the_shell_that_runs_it() -> None:
    """A remote command runs as one shell whose own command line is the command; `pgrep -f` or
    `pkill -f` with a pattern that matches that text finds the shell itself, so a check never
    starts what it guards (the backends agent never ran) and a kill signals its own shell."""
    import os
    import re
    import shlex
    import subprocess

    out = subprocess.run(
        ["bash", str(BENCH / "three_device_run.sh"), "--reps", "1", "--dry-run"],
        env={
            **os.environ,
            "PERF_ZONE": "us-east1-b",
            "PERF_REPO": "/opt/provisa",
            "PERF_SERVER_IP": "10.90.0.2",
            "PERF_BACKENDS_IP": "10.90.0.4",
            "PERF_BUCKET": "bkt",
            "PERF_RUN_ID": "run1",
            "PATH": "/usr/bin:/bin",
        },
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    commands = [block.split("\n", 1)[1] for block in out.split("\n[provisa-perf-")[1:]]
    checked = 0
    for command in commands:
        for match in re.finditer(r"p(?:grep|kill)\s+(?:-\w+\s+)*-f\s+(\S+)", command):
            pattern = shlex.split(match.group(1))[0]
            assert re.search(pattern, command) is None, (pattern, command)
            checked += 1
    assert checked > 0


# --- the seed job, the seed image, checkpoints and recovery (REQ-2092) ---------------------------

_SEED_LIFECYCLE = BENCH / "seed_lifecycle.sh"
_TF = BENCH.parents[3] / "terraform" / "perf"


def _seed_tree(tmp_path: Path) -> Path:
    """A copy of every seed input (and only those) under tmp_path/repo."""
    import shutil

    import seed_inputs

    root = tmp_path / "repo"
    for rel in [*seed_inputs.FILES, seed_inputs.COMPOSE]:
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(_REPO / rel, root / rel)
    return root


@pytest.mark.parametrize(
    "rel",
    [
        "demo/named/perf/generate_postgres.py",
        "demo/named/perf/generate_clickhouse.py",
        "demo/named/perf/generate_mongo.py",
        "demo/named/perf/generate_neo4j.py",
        "demo/named/perf/seed.py",
        "demo/named/perf/Dockerfile.seeder",
        "demo/named/perf/Dockerfile.postgres",
    ],
)
def test_a_change_to_any_seed_file_changes_the_seed_hash(tmp_path: Path, rel: str) -> None:
    import seed_inputs

    root = _seed_tree(tmp_path)
    before = seed_inputs.seed_hash(root)
    with (root / rel).open("a") as f:
        f.write("\n# changed\n")
    assert seed_inputs.seed_hash(root) != before


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("image: mongo:7", "image: mongo:8"),
        ("image: clickhouse/clickhouse-server:24.3", "image: clickhouse/clickhouse-server:24.8"),
        ("image: neo4j:5.19-community", "image: neo4j:5.26-community"),
        (
            "image: provisa-perf-postgres:16\n    build:",
            "image: provisa-perf-postgres:17\n    build:",
        ),
        ('"${PROVISA_BENCH_ORDERS:-20000000}"', '"${PROVISA_BENCH_ORDERS:-1000000}"'),
        ('"${PROVISA_BENCH_NEO4J_ORDERS:-2000000}"', '"${PROVISA_BENCH_NEO4J_ORDERS:-100}"'),
        ('"${PROVISA_BENCH_ITEMS_PER_ORDER:-4}"', '"${PROVISA_BENCH_ITEMS_PER_ORDER:-8}"'),
    ],
)
def test_an_engine_tag_or_scale_setting_changes_the_seed_hash(
    tmp_path: Path, old: str, new: str
) -> None:
    import seed_inputs

    root = _seed_tree(tmp_path)
    compose = root / seed_inputs.COMPOSE
    before = seed_inputs.seed_hash(root)
    assert compose.read_text().count(old) == 1
    compose.write_text(compose.read_text().replace(old, new))
    assert seed_inputs.seed_hash(root) != before


def test_the_postgres_engine_tag_in_its_dockerfile_changes_the_seed_hash(tmp_path: Path) -> None:
    import seed_inputs

    root = _seed_tree(tmp_path)
    dockerfile = root / "demo/named/perf/Dockerfile.postgres"
    before = seed_inputs.seed_hash(root)
    dockerfile.write_text(
        dockerfile.read_text().replace("ARG PG_IMAGE=postgres:16.15", "ARG PG_IMAGE=postgres:16.16")
    )
    assert seed_inputs.seed_hash(root) != before


def test_an_unrelated_change_leaves_the_seed_hash(tmp_path: Path) -> None:
    import seed_inputs

    root = _seed_tree(tmp_path)
    compose = root / seed_inputs.COMPOSE
    before = seed_inputs.seed_hash(root)
    (root / "demo/named/perf/config.yaml").write_text("anything: 1\n")
    (root / "demo/named/perf/bench").mkdir()
    (root / "demo/named/perf/bench/run_benchmark.py").write_text("print('changed')\n")
    # a compose edit outside the engines' tags and the seeder's scale: a non-engine image, a port
    text = compose.read_text()
    text = text.replace("image: redis:7.4-alpine", "image: redis:7.6-alpine")
    text = text.replace(
        "PROVISA_BENCH_POSTGRESQL_PORT:-25632", "PROVISA_BENCH_POSTGRESQL_PORT:-25640"
    )
    compose.write_text(text)
    assert seed_inputs.seed_hash(root) == before


def test_a_missing_seed_input_is_named(tmp_path: Path) -> None:
    import seed_inputs

    root = _seed_tree(tmp_path)
    (root / "demo/named/perf/seed.py").unlink()
    with pytest.raises(seed_inputs.SeedInputError, match="demo/named/perf/seed.py is missing"):
        seed_inputs.seed_hash(root)
    root = _seed_tree(tmp_path / "again")
    compose = root / seed_inputs.COMPOSE
    compose.write_text(
        compose.read_text().replace("      PROVISA_BENCH_ITEMS_PER_ORDER:", "      X:")
    )
    with pytest.raises(seed_inputs.SeedInputError, match="no PROVISA_BENCH_ITEMS_PER_ORDER"):
        seed_inputs.seed_hash(root)


def test_the_seed_hash_at_a_commit_is_that_of_its_files() -> None:
    import seed_inputs

    head = _head()
    assert seed_inputs.seed_hash(_REPO, head) == seed_inputs.seed_hash(_REPO, head)
    assert len(seed_inputs.seed_hash(_REPO, head)) == 64
    with pytest.raises(seed_inputs.SeedInputError, match="not readable at 0000000"):
        seed_inputs.seed_hash(_REPO, "0000000")


def test_a_run_without_the_seed_image_stops_before_any_apply(tmp_path: Path) -> None:
    import seed_inputs

    commit = _head()
    _fake_cloud(tmp_path, commit, image=False)
    done = _lifecycle(
        tmp_path,
        *("--project", "p", "--zone", "us-east1-b", "--commit", commit),
        *("--ssh-cidr", "203.0.113.10/32"),
    )
    assert done.returncode == 1, done.stdout + done.stderr
    seed_hash = seed_inputs.seed_hash(_REPO, commit)
    assert f"the seed inputs changed: there is no seed image {_seed_image(commit)}" in done.stdout
    assert f"seed hash {seed_hash}" in done.stdout and "seed_lifecycle.sh" in done.stdout
    assert not [c for c in _calls(tmp_path) if c.startswith("terraform ")]


def test_the_seed_image_boots_the_backends_device_alone(tmp_path: Path) -> None:
    commit = _head()
    _fake_cloud(tmp_path, commit)
    done = _lifecycle(
        tmp_path,
        *("--project", "p", "--zone", "us-east1-b", "--commit", commit),
        *("--ssh-cidr", "203.0.113.10/32", "--dry-run"),
    )
    assert done.returncode == 0, done.stdout + done.stderr
    (plan,) = [c for c in _calls(tmp_path) if c.startswith("terraform ") and " plan " in c]
    assert f"-var backends_image=projects/p/global/images/{_seed_image(commit)}" in plan
    assert "-var seed_mode=from_image" in plan and "-var image=" not in plan
    main = (_TF / "main.tf").read_text()
    assert "image = local.boot_image[each.key]" in main
    boot = main[main.index("boot_image = {") :]
    boot = boot[: boot.index("}")]
    assert "server   = var.image" in boot and "client   = var.image" in boot
    assert 'backends = var.seed_mode == "from_image" ? var.backends_image : var.image' in boot


def test_the_backends_device_can_be_on_demand_beside_spot_devices(tmp_path: Path) -> None:
    commit = _head()
    _fake_cloud(tmp_path, commit)
    done = _lifecycle(
        tmp_path,
        *("--project", "p", "--zone", "us-east1-b", "--commit", commit),
        *("--ssh-cidr", "203.0.113.10/32", "--dry-run"),
        *("--backends-provisioning-model", "ON_DEMAND"),
    )
    assert done.returncode == 0, done.stdout + done.stderr
    (plan,) = [c for c in _calls(tmp_path) if c.startswith("terraform ") and " plan " in c]
    assert "-var provisioning_model=SPOT" in plan
    assert "-var backends_provisioning_model=ON_DEMAND" in plan
    main = (_TF / "main.tf").read_text()
    assert "provisioning_model  = local.provisioning_model[each.key]" in main
    assert (
        "backends = var.backends_provisioning_model == null ? var.provisioning_model"
        " : var.backends_provisioning_model"
    ) in main


def test_the_seed_job_stores_the_image_and_destroys_the_device(tmp_path: Path) -> None:
    import seed_inputs

    commit = _head()
    state = _fake_cloud(tmp_path, commit, image=False)
    done = _lifecycle(
        tmp_path,
        *("--project", "p", "--zone", "us-east1-b", "--commit", commit),
        *("--ssh-cidr", "203.0.113.10/32", "--backends-provisioning-model", "ON_DEMAND"),
        script=_SEED_LIFECYCLE,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    calls = _calls(tmp_path)
    seed_hash = seed_inputs.seed_hash(_REPO, commit)
    image = _seed_image(commit)

    def at(pred: Any) -> int:
        return next(i for i, c in enumerate(calls) if pred(c))

    apply = at(lambda c: c.startswith("terraform ") and " apply " in c)
    assert '-var devices=["backends"]' in calls[apply] and "-var seed_mode=seed" in calls[apply]
    assert "-var backends_provisioning_model=ON_DEMAND" in calls[apply]
    ready = at(lambda c: "/var/lib/provisa-perf/ready" in c)
    assert "[provisa-perf-backends]" in calls[ready]
    compose_stop = at(lambda c: "sudo docker compose --profile three-device stop" in c)
    assert "[provisa-perf-backends]" in calls[compose_stop]
    stop = at(lambda c: "[instances] [stop] [provisa-perf-backends]" in c)
    create = at(lambda c: f"[images] [create] [{image}]" in c)
    destroy = at(lambda c: c.startswith("terraform ") and " destroy " in c)
    assert apply < ready < compose_stop < stop < create < destroy
    assert f"[--labels] [seed_hash={seed_hash[:63]}]" in calls[create]
    assert "[--source-disk] [provisa-perf-backends]" in calls[create]
    # only the backends device was ever polled or asked for its log
    assert not [c for c in calls if "provisa-perf-server" in c or "provisa-perf-client" in c]
    assert (state / "images" / image).exists()


def test_a_failed_compose_stop_fails_the_seed_job_by_name(tmp_path: Path) -> None:
    commit = _head()
    state = _fake_cloud(tmp_path, commit, image=False)
    (state / "compose_stop_fails").write_text("")
    done = _lifecycle(
        tmp_path,
        *("--project", "p", "--zone", "us-east1-b", "--commit", commit),
        *("--ssh-cidr", "203.0.113.10/32"),
        script=_SEED_LIFECYCLE,
    )
    assert done.returncode == 1
    assert "provisa-perf-backends: docker compose stop failed; no image stored" in done.stdout
    calls = _calls(tmp_path)
    assert not [c for c in calls if "[instances] [stop]" in c or "[images] [create]" in c]
    assert any(c.startswith("terraform ") and " destroy " in c for c in calls)
    assert not (state / "images" / _seed_image(commit)).exists()


def test_the_seed_job_does_nothing_when_the_image_exists(tmp_path: Path) -> None:
    commit = _head()
    _fake_cloud(tmp_path, commit)
    done = _lifecycle(
        tmp_path,
        *("--project", "p", "--zone", "us-east1-b", "--commit", commit),
        *("--ssh-cidr", "203.0.113.10/32"),
        script=_SEED_LIFECYCLE,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    assert f"seed image {_seed_image(commit)} for seed hash" in done.stdout
    assert "already exists; nothing to do" in done.stdout
    assert not [c for c in _calls(tmp_path) if c.startswith("terraform ")]


def _seed_step(tmp_path: Path, seed_mode: str, *, seeded: bool) -> Any:
    """The backends device's data-stack step of the startup script, rendered for seed_mode and
    run against a stand-in docker, its repository under tmp_path."""
    import subprocess

    script = _STARTUP.read_text()
    step = script[script.index('  SEEDED="$REPO_DIR') : script.index("  esac") + len("  esac")]
    step = step.replace("${seed_mode}", seed_mode)
    repo = tmp_path / "repo"
    (repo / "demo/named/perf/data").mkdir(parents=True)
    if seeded:
        (repo / "demo/named/perf/data/.seeded").write_text("seeded\n")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(f'#!/bin/bash\necho "docker $*" >> {tmp_path}/docker.log\n')
    (bin_dir / "docker").chmod(0o755)
    return subprocess.run(
        ["bash", "-c", f"set -euo pipefail\nREPO_DIR={repo}\n{step}\necho step-passed"],
        env={"PATH": f"{bin_dir}:/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )


def test_a_run_device_without_the_seed_is_refused_never_seeded(tmp_path: Path) -> None:
    done = _seed_step(tmp_path, "from_image", seeded=False)
    assert done.returncode == 1 and "step-passed" not in done.stdout
    assert "data/.seeded is absent, so this image carries no seed" in done.stdout
    assert not (tmp_path / "docker.log").exists()
    # the ready marker is written only after every step: a refused step never writes it
    script = _STARTUP.read_text()
    assert script.index("from_image)") < script.rindex('echo "${commit}" > "$READY"')


def test_a_run_device_with_the_seed_brings_the_stack_up_without_the_seeder(
    tmp_path: Path,
) -> None:
    done = _seed_step(tmp_path, "from_image", seeded=True)
    assert done.returncode == 0, done.stdout + done.stderr
    assert (tmp_path / "docker.log").read_text() == (
        "docker compose --profile three-device up -d --build --scale seeder=0"
        " --renew-anon-volumes\n"
    )


def test_the_seed_job_device_seeds(tmp_path: Path) -> None:
    done = _seed_step(tmp_path, "seed", seeded=False)
    assert done.returncode == 0, done.stdout + done.stderr
    assert (tmp_path / "docker.log").read_text().splitlines() == [
        "docker compose --profile three-device up -d --build",
        "docker compose up seeder",
    ]


def _run_fixtures(state: Path) -> None:
    _run(state / "fixture", "1-off", "off", 2000, 1.0)
    _run(state / "fixture", "1-on", "on", 1800, 1.2)


def _full_run(tmp_path: Path, commit: str) -> Any:
    return _lifecycle(
        tmp_path,
        *("--project", "p", "--zone", "us-east1-b", "--commit", commit),
        *("--ssh-cidr", "203.0.113.10/32", "--reps", "1"),
    )


def test_a_preempted_device_is_restarted_and_only_the_runs_not_done_are_repeated(
    tmp_path: Path,
) -> None:
    commit = _head()
    state = _fake_cloud(tmp_path, commit)
    _run_fixtures(state)
    (state / "preempt").mkdir()
    (state / "preempt" / "1-on").write_text("1")
    done = _full_run(tmp_path, commit)
    assert done.returncode == 0, done.stdout + done.stderr
    # 1-off checkpointed before the preemption, so the rerun skips it
    assert (state / "benchmarks").read_text().splitlines() == ["1-off", "1-on", "1-on"]
    assert "already checkpointed in gs://" in done.stdout and " 1-off " in done.stdout
    assert "=== run 1-off: checkpointed already, skipped ===" in done.stdout
    calls = _calls(tmp_path)
    starts = [c for c in calls if "[instances] [start]" in c]
    assert len(starts) == 1 and "[provisa-perf-client]" in starts[0]
    assert len([c for c in calls if c.startswith("terraform ") and " apply " in c]) == 1
    (run,) = (tmp_path / "runs").iterdir()
    bucket = f"provisa-perf-{run.name.lower()}-p"
    for name in ("1-off", "1-on"):
        assert (state / "gcs" / bucket / run.name / name / "DONE").read_text() == name
        # copied into the run directory before destroy
        assert (run / "checkpoints" / name / "optimistic" / "graphql.json").exists()
    assert (run / "results-report" / "published.json").exists()
    rsync = next(i for i, c in enumerate(calls) if c.startswith("gsutil rsync -r "))
    destroy = next(
        i for i, c in enumerate(calls) if c.startswith("terraform ") and " destroy " in c
    )
    assert rsync < destroy


def test_recovery_is_bounded_at_three(tmp_path: Path) -> None:
    commit = _head()
    state = _fake_cloud(tmp_path, commit)
    _run_fixtures(state)
    (state / "preempt").mkdir()
    (state / "preempt" / "1-off").write_text("99")
    done = _full_run(tmp_path, commit)
    assert done.returncode == 1
    assert "the run failed again after 3 recoveries" in done.stdout
    assert (state / "benchmarks").read_text().splitlines() == ["1-off"] * 4
    calls = _calls(tmp_path)
    assert len([c for c in calls if "[instances] [start]" in c]) == 3
    assert any(c.startswith("terraform ") and " destroy " in c for c in calls)


def test_a_device_that_does_not_start_is_replaced_from_its_image(tmp_path: Path) -> None:
    commit = _head()
    state = _fake_cloud(tmp_path, commit)
    _run_fixtures(state)
    (state / "preempt").mkdir()
    (state / "preempt" / "1-on").write_text("1")
    (state / "start_fails").write_text("")
    # the replaced device comes up running
    (tmp_path / "bin" / "terraform").write_text(
        (tmp_path / "bin" / "terraform").read_text()
        + f'case " $* " in *" apply "*) mkdir -p {state}/status;'
        f" echo RUNNING > {state}/status/provisa-perf-client ;; esac\n"
    )
    done = _full_run(tmp_path, commit)
    assert done.returncode == 0, done.stdout + done.stderr
    tf = [
        c
        for c in _calls(tmp_path)
        if c.startswith("terraform ") and (" apply " in c or " destroy " in c)
    ]
    # apply, the devices alone destroyed (the bucket kept), apply again, then the final destroy
    assert [" destroy " in c for c in tf] == [False, True, False, True]
    assert "-target=google_compute_instance.device" in tf[1]
    assert "-target" not in tf[3]
    assert (state / "benchmarks").read_text().splitlines() == ["1-off", "1-on", "1-on"]


def test_a_failure_with_every_device_running_is_not_retried(tmp_path: Path) -> None:
    commit = _head()
    state = _fake_cloud(tmp_path, commit)
    _run_fixtures(state)
    (state / "fail_runs").write_text("")
    done = _full_run(tmp_path, commit)
    assert done.returncode == 1
    assert "the run failed with every device running: not a preemption" in done.stdout
    assert (state / "benchmarks").read_text().splitlines() == ["1-off"]
    assert not [c for c in _calls(tmp_path) if "[instances] [start]" in c]


def test_the_dry_run_says_how_each_run_is_checkpointed() -> None:
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
            "PERF_BUCKET": "bkt",
            "PERF_RUN_ID": "run1",
            "PERF_LOCAL_OUT": "/runs/x/results",
            "PATH": "/usr/bin:/bin",
        },
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    for name in ("1-off", "1-on"):
        assert f"gsutil -m cp -r /runs/x/results/{name} gs://bkt/run1/" in out
        assert f"gsutil cp - gs://bkt/run1/{name}/DONE" in out
    # the run never seeds: the seeder is scaled to nothing, an unseeded device refused
    assert "--scale seeder=0" in out and "docker compose up seeder" not in out
    assert "test -f data/.seeded" in out


def test_a_serve_started_in_the_background_stops_on_sigint(tmp_path: Path) -> None:
    """A job a non-interactive shell starts in the background begins with SIGINT ignored, and a
    bash script cannot trap a signal ignored on entry: started as the run once started it, the
    serve ran on through its stop. Started with SIGINT at its default, as the run starts it now, it
    stops on SIGINT."""
    import shutil
    import subprocess
    import time

    script = tmp_path / "serve.sh"
    script.write_text(
        "#!/bin/bash\ntrap 'echo stopped >> \"$1\"; exit 0' INT TERM\nsleep 30 & wait $!\n"
    )
    script.chmod(0o755)
    env_bin = shutil.which("env")
    assert env_bin is not None

    def started(prefix: str, marker: Path) -> bool:
        """Start the script as ``prefix`` starts it, send it SIGINT; whether it stopped on it."""
        subprocess.run(
            ["bash", "-c", f"{prefix}{script} {marker} >/dev/null 2>&1 </dev/null &"], check=True
        )
        time.sleep(0.5)
        subprocess.run(["pkill", "-INT", "-f", f"serve.sh {marker}"], check=False)
        time.sleep(1.5)
        stopped = marker.exists() and "stopped" in marker.read_text()
        subprocess.run(["pkill", "-TERM", "-f", f"serve.sh {marker}"], check=False)
        return stopped

    assert not started("", tmp_path / "plain")
    assert started(f"{env_bin} --default-signal=INT ", tmp_path / "reset")
