# Copyright (c) 2026 Kenneth Stott
# Canary: 8c8c0587-3210-4ca7-9b68-3b9ad2f5f680
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The three-device optimistic benchmark (REQ-1911, demo/named/perf/METHODOLOGY.md).

``serve-env``: on device 1, the environment Provisa is started with (``orchestrate.sh serve``):
every backend on device 3, the engine (pg by default), the workers, and telemetry off or on. With
telemetry off no exporter is named and the sample rate is 0, so no span is recorded; with it on,
spans are exported to the collector on device 3. It writes the config the server reads (the perf
config with its observability block set for the run) and prints ``export`` lines.

``report``: from the repetitions the operator's driver collected (``three_device_run.sh``), the
published point per transport with telemetry off (median of the repetitions, minimum and maximum
beside it, captioned), and, internal only, the same with telemetry on and the difference.

    python three_device.py serve-env --engine pg --telemetry off --backends-host 10.90.0.4 \\
        --workers 32 --config-out ~/.provisa-bench/config.yaml
    python three_device.py report --root results/three-device --out results/three-device
"""

from __future__ import annotations

import argparse
import json
import shlex
import statistics
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

ENGINES = ("pg", "duckdb", "trino")
TELEMETRY = ("off", "on")
CAPTION = "single source, direct read; Telemetry OFF"
LABEL_INTERNAL = "internal: not for publication"

# Where each backend listens on device 3 (demo/named/perf/docker-compose.yml, its defaults).
PORTS = {
    "postgresql": 25632,
    "engine": 25633,
    "control_plane": 25634,
    "redis": 26379,
    "otlp": 24318,
    "trino": 28080,
    "clickhouse": 28623,
    "mongodb": 27317,
    "neo4j_http": 27974,
}
API_PORT = 8001
# Every listener the contract reaches on device 1 (setups/three-device.yaml). Bolt does not
# start unless its port is set (server.bolt_port defaults to 0); on one host a .env sets it.
BOLT_PORT = 17687


def run_config(base_text: str, *, telemetry: str, collector_url: str | None) -> str:
    """The perf config with its observability block set for a run: off names no endpoint and
    samples nothing; on exports to the collector at the sample rate the config states."""
    if telemetry not in TELEMETRY:
        raise ValueError(f"telemetry must be one of {', '.join(TELEMETRY)}, got {telemetry!r}")
    config = yaml.safe_load(base_text)
    obs = config["observability"]
    if telemetry == "off":
        obs["endpoint"] = ""
        obs["sample_rate"] = 0.0
    else:
        if not collector_url:
            raise ValueError("telemetry on needs the collector's URL")
        obs["endpoint"] = collector_url
    return yaml.safe_dump(config, sort_keys=False)


def server_env(
    *,
    engine: str,
    telemetry: str,
    backends_host: str,
    workers: int,
    config_path: str,
    request_threads: int | None = None,
) -> dict[str, str]:
    """The environment of Provisa on device 1: everything it reaches is on device 3."""
    if engine not in ENGINES:
        raise ValueError(f"engine must be one of {', '.join(ENGINES)}, got {engine!r}")
    if telemetry not in TELEMETRY:
        raise ValueError(f"telemetry must be one of {', '.join(TELEMETRY)}, got {telemetry!r}")
    h = backends_host
    cp = f"postgresql+psycopg://provisa:provisa@{h}:{PORTS['control_plane']}/provisa"
    env = {
        "PROVISA_CONFIG": config_path,
        "PROVISA_CONFIG_REPLACE": "true",
        "PROVISA_DEMO": "true",
        "PROVISA_WORKERS": str(workers),
        "PROVISA_LAUNCH_ID": str(uuid.uuid4()),
        "PROVISA_PGWIRE_PORT": "5439",
        "PROVISA_BOLT_PORT": str(BOLT_PORT),
        "PG_HOST": h,
        "PG_PORT": str(PORTS["control_plane"]),
        "PG_DATABASE": "provisa",
        "PG_USER": "provisa",
        "PG_PASSWORD": "provisa",
        "POSTGRES_HOST": h,
        "TENANT_DATABASE_URL": cp,
        "PLATFORM_DATABASE_URL": cp,
        "REDIS_URL": f"redis://{h}:{PORTS['redis']}",
        "PROVISA_BENCH_POSTGRESQL_HOST": h,
        "PROVISA_BENCH_POSTGRESQL_PORT": str(PORTS["postgresql"]),
        "PROVISA_BENCH_CLICKHOUSE_HOST": h,
        "PROVISA_BENCH_CLICKHOUSE_PORT": str(PORTS["clickhouse"]),
        "PROVISA_BENCH_MONGO_HOST": h,
        "PROVISA_BENCH_MONGO_PORT": str(PORTS["mongodb"]),
        "PROVISA_BENCH_NEO4J_HOST": h,
        "PROVISA_BENCH_NEO4J_HTTP_PORT": str(PORTS["neo4j_http"]),
        "PROVISA_ENGINE": engine,
        "PROVISA_BENCH_TELEMETRY": telemetry,
    }
    if engine == "pg":
        env["PROVISA_ENGINE_URL"] = (
            f"postgresql://provisa:provisa@{h}:{PORTS['engine']}/provisa_engine"
        )
    elif engine == "trino":
        env["TRINO_HOST"] = h
        env["TRINO_PORT"] = str(PORTS["trino"])
    if telemetry == "on":
        env["OTEL_EXPORTER_OTLP_ENDPOINT"] = collector_url(h)
        env["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"
        env["OTEL_SERVICE_NAME"] = "provisa"
    if request_threads is not None:
        env["PROVISA_REQUEST_THREADS"] = str(request_threads)
    return env


def collector_url(backends_host: str) -> str:
    return f"http://{backends_host}:{PORTS['otlp']}"


# --- the report over repetitions -------------------------------------------------------------


def published_step(report: Mapping[str, Any]) -> dict[str, Any] | None:
    """The step with the highest throughput that had no errors and was not client-limited."""
    if "error" in report:
        return None
    clean = [
        s
        for s in report["steps"]
        if "error" not in s and s["errors"] == 0 and not s["client_limited"]
    ]
    return max(clean, key=lambda s: s["req_per_s"]) if clean else None


_FIGURES = ("req_per_s", "p50_ms", "p99_ms", "server_cpu_ms_per_request", "client_cpu_cores")


def _spread(values: Sequence[float]) -> dict[str, float]:
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def _per_key(steps: Sequence[Mapping[str, Any]], field: str) -> dict[str, dict[str, float]]:
    keys = sorted({k for s in steps for k, v in (s.get(field) or {}).items() if v is not None})
    return {
        k: _spread([s[field][k] for s in steps if s.get(field, {}).get(k) is not None])
        for k in keys
    }


def summarize(steps: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "repetitions": len(steps),
        "concurrency": [s["concurrency"] for s in steps],
    }
    for f in _FIGURES:
        values = [s[f] for s in steps if s.get(f) is not None]
        if len(values) != len(steps):
            raise ValueError(f"{f}: missing from {len(steps) - len(values)} repetition(s)")
        out[f] = _spread(values)
    out["source_cpu_cores"] = _per_key(steps, "source_cpu_cores")
    out["backend_cpu_cores"] = _per_key(steps, "backend_cpu_cores")
    return out


def _runs(root: Path) -> dict[str, list[Path]]:
    """Run directories by telemetry setting: ``<root>/<rep>-<telemetry>/``."""
    out: dict[str, list[Path]] = {t: [] for t in TELEMETRY}
    for d in sorted(p for p in root.iterdir() if p.is_dir()):
        telemetry = d.name.rsplit("-", 1)[-1]
        if telemetry not in TELEMETRY:
            raise ValueError(f"{d.name}: a run directory is <rep>-<off|on>")
        out[telemetry].append(d)
    return out


def _devices_telemetry(run: Path) -> str:
    return json.loads((run / "optimistic" / "devices.json").read_text())["server"]["facts"][
        "telemetry"
    ]


def build(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """(published, internal): the published file holds the telemetry-off points only."""
    runs = _runs(root)
    if not runs["off"]:
        raise ValueError("no telemetry-off run: nothing to publish")
    for telemetry, dirs in runs.items():
        for d in dirs:
            stated = _devices_telemetry(d)
            if stated != telemetry:
                raise ValueError(f"{d.name}: the server says telemetry {stated}, not {telemetry}")
    by: dict[str, dict[str, Any]] = {}
    for telemetry, dirs in runs.items():
        steps: dict[str, list[Mapping[str, Any]]] = {}
        for d in dirs:
            for path in sorted((d / "optimistic").glob("*.json")):
                if path.name in ("devices.json", "idle.json", "resolved-names.json"):
                    continue
                report = json.loads(path.read_text())
                if "transport" not in report:
                    continue
                step = published_step(report)
                if step is None:
                    raise ValueError(f"{d.name}/{path.name}: no step without errors")
                steps.setdefault(report["transport"], []).append(step)
        by[telemetry] = {t: summarize(s) for t, s in steps.items()}
    devices = json.loads((runs["off"][0] / "optimistic" / "devices.json").read_text())
    published = {"caption": CAPTION, "devices": devices, "transports": by["off"]}
    impact = {
        t: {
            f: by["on"][t][f]["median"] - by["off"][t][f]["median"]
            for f in ("req_per_s", "p99_ms", "server_cpu_ms_per_request")
        }
        for t in by["off"]
        if t in by.get("on", {})
    }
    internal = {
        "label": LABEL_INTERNAL,
        "devices": devices,
        "telemetry_off": by["off"],
        "telemetry_on": by.get("on", {}),
        "impact_on_minus_off": impact,
    }
    return published, internal


def run_contract(contract_text: str, *, telemetry: str) -> str:
    """The setup contract as one run states it: ``knobs.governance.telemetry`` set to the
    server's (REQ-2092), which the run checks against the server's agent's ``telemetry`` fact."""
    if telemetry not in TELEMETRY:
        raise ValueError(f"telemetry must be one of {', '.join(TELEMETRY)}, got {telemetry!r}")
    contract = yaml.safe_load(contract_text)
    contract["knobs"]["governance"]["telemetry"] = telemetry == "on"
    return yaml.safe_dump(contract, sort_keys=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="The three-device optimistic benchmark")
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve-env", help="device 1: the server's environment, as export lines")
    serve.add_argument("--engine", choices=ENGINES, default="pg")
    serve.add_argument("--telemetry", choices=TELEMETRY, required=True)
    serve.add_argument("--backends-host", required=True, help="device 3's private address")
    serve.add_argument("--workers", type=int, required=True)
    serve.add_argument("--request-threads", type=int)
    serve.add_argument("--base-config", default=str(Path(__file__).parent.parent / "config.yaml"))
    serve.add_argument("--config-out", required=True)
    con = sub.add_parser("contract", help="device 2: the setup contract as one run states it")
    con.add_argument("--setup", required=True)
    con.add_argument("--telemetry", choices=TELEMETRY, required=True)
    con.add_argument("--out", required=True)
    rep = sub.add_parser("report", help="the published point and the telemetry impact")
    rep.add_argument("--root", required=True)
    rep.add_argument("--out", required=True)
    args = parser.parse_args()
    if args.command == "contract":
        out = Path(args.out).expanduser()
        out.write_text(run_contract(Path(args.setup).read_text(), telemetry=args.telemetry))
        return 0
    if args.command == "serve-env":
        url = collector_url(args.backends_host) if args.telemetry == "on" else None
        out = Path(args.config_out).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            run_config(
                Path(args.base_config).read_text(), telemetry=args.telemetry, collector_url=url
            )
        )
        env = server_env(
            engine=args.engine,
            telemetry=args.telemetry,
            backends_host=args.backends_host,
            workers=args.workers,
            config_path=str(out),
            request_threads=args.request_threads,
        )
        for k, v in env.items():
            print(f"export {k}={shlex.quote(v)}")
        return 0
    published, internal = build(Path(args.root))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "published.json").write_text(json.dumps(published, indent=2))
    (out / "internal.json").write_text(json.dumps(internal, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
