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
every backend on device 3, the engine (pg by default) beside Provisa on device 1, the workers, and
telemetry off or on. With
telemetry off no exporter is named and the sample rate is 0, so no span is recorded; with it on,
spans are exported to the collector on device 3. It writes the config the server reads (the perf
config with its observability block set for the run) and prints ``export`` lines.

``report``: from the units the operator's driver collected (``three_device_run.sh``), the
published point per transport with telemetry off (median of the repetitions, minimum and maximum
beside it, captioned), and, internal only, the same with telemetry on and the difference. Each
report states the commit of every run.

    python three_device.py serve-env --engine pg --telemetry off --backends-host 10.90.0.4 \\
        --workers 32 --config-out ~/.provisa-bench/config.yaml
    python three_device.py report --root results/three-device --out results/three-device \\
        --ledger results/three-device-ledger.json
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

import bench_sanity
import bench_units
import governance_variants
from bench_units import published_step

ENGINES = ("pg", "duckdb", "trino")
TELEMETRY = ("off", "on")
CAPTION = "single source, direct read; Telemetry OFF"
LABEL_INTERNAL = "internal: not for publication"
# Run directories apart from the repetitions: pgwire with no response cache (<rep>-uncached-off,
# reported beside the cached row) and a py-spy profile (profile-<transport>-off, internal).
UNCACHED_RUN = "-uncached-off"
PROFILE_RUN = "profile-"
PGWIRE_UNCACHED = "pgwire_uncached"
# The published pairs, each with what its row in a results table means: a transport name alone
# conflates the wire with the language it carries. Every other transport stays internal.
PUBLISHED = {
    "pgwire": "SQL over the PostgreSQL wire protocol, response cache on",
    PGWIRE_UNCACHED: "SQL over the PostgreSQL wire protocol, response cache off",
    "flight_sql": "SQL text in an Arrow Flight ticket. Flight carries SQL here, never Cypher",
    "graphql": "GraphQL over HTTP, POST /data/graphql",
    "bolt": "Cypher over Bolt",
    "grpc": "A typed per-table RPC over gRPC, no query language",
    "rest": "REST over HTTP, GET /data/rest, JSON",
    "jsonapi": "JSON:API over HTTP, GET /data/jsonapi",
}
TABLE_CAPTION = (
    "Each row names a language and the wire that carried it. HTTP appears under several rows: "
    "they share a wire and differ in encoding."
)

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
PUBLIC_URL = "https://bench.provisa.example"  # orchestrate.sh's, and local_proof.py's
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
    engine_host: str | None = None,
) -> dict[str, str]:
    """The environment of Provisa on device 1: everything it reaches is on device 3 but the
    engine, which is on ``engine_host`` (device 1's own address; unset, device 3)."""
    if engine not in ENGINES:
        raise ValueError(f"engine must be one of {', '.join(ENGINES)}, got {engine!r}")
    if telemetry not in TELEMETRY:
        raise ValueError(f"telemetry must be one of {', '.join(TELEMETRY)}, got {telemetry!r}")
    h = backends_host
    engine_at = engine_host or h
    cp = f"postgresql+psycopg://provisa:provisa@{h}:{PORTS['control_plane']}/provisa"
    env = {
        "PROVISA_CONFIG": config_path,
        "PROVISA_CONFIG_REPLACE": "true",
        "PROVISA_DEMO": "true",
        # The SPARQL vocabulary's subjects start with it, and the run's lookup reads that vocabulary.
        "PROVISA_PUBLIC_URL": PUBLIC_URL,
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
            f"postgresql://provisa:provisa@{engine_at}:{PORTS['engine']}/provisa_engine"
        )
    elif engine == "trino":
        env["TRINO_HOST"] = engine_at
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


_FIGURES = ("req_per_s", "p50_ms", "p99_ms", "server_cpu_ms_per_request", "client_cpu_cores")


def _spread(values: Sequence[float]) -> dict[str, float]:
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def _per_key(steps: Sequence[Mapping[str, Any]], field: str) -> dict[str, dict[str, float]]:
    keys = sorted({k for s in steps for k, v in (s.get(field) or {}).items() if v is not None})
    return {
        k: _spread([s[field][k] for s in steps if s.get(field, {}).get(k) is not None])
        for k in keys
    }


def _paging(steps: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Each host's paging over the repetitions (beside CPU per request, so a rise in it can be
    told from paging): the spread of every counter, and the reasons a host's was not read."""
    out: dict[str, Any] = {}
    for host in sorted({h for s in steps for h in s["paging"]}):
        read = [s["paging"][host] for s in steps if "error" not in s["paging"][host]]
        errors = sorted({s["paging"][host]["error"] for s in steps if "error" in s["paging"][host]})
        fields = (
            {
                f: _spread([p[f] for p in read])
                for f in (
                    "pgmajfault",
                    "pswpin",
                    "pswpout",
                    "mem_available_mb_start",
                    "mem_available_mb_end",
                )
            }
            if read
            else {}
        )
        classes = sorted({k for p in read for k in p["majflt"]})
        out[host] = {
            **fields,
            "majflt": {k: _spread([p["majflt"][k] for p in read]) for k in classes},
            **({"errors": errors} if errors else {}),
        }
    return out


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
    out["paging"] = _paging(steps)
    return out


def _runs(root: Path) -> dict[str, list[Path]]:
    """Unit directories by telemetry setting: ``<root>/<rep>-<transport>-<telemetry>/``."""
    out: dict[str, list[Path]] = {t: [] for t in TELEMETRY}
    for d in sorted(p for p in root.iterdir() if p.is_dir()):
        if d.name.endswith(UNCACHED_RUN) or d.name.startswith(PROFILE_RUN):
            continue
        telemetry = d.name.rsplit("-", 1)[-1]
        if telemetry not in TELEMETRY:
            raise ValueError(f"{d.name}: a run directory is <rep>-<off|on>")
        out[telemetry].append(d)
    return out


def _uncached_runs(root: Path) -> list[Path]:
    return sorted(d for d in root.iterdir() if d.is_dir() and d.name.endswith(UNCACHED_RUN))


def _transport_steps(run: Path) -> dict[str, Mapping[str, Any]]:
    """The published step of each transport a run measured."""
    steps: dict[str, Mapping[str, Any]] = {}
    for path in sorted((run / "optimistic").glob("*.json")):
        if path.name in ("devices.json", "idle.json", "resolved-names.json"):
            continue
        report = json.loads(path.read_text())
        if "transport" not in report:
            continue
        step = published_step(report)
        if step is None:
            raise ValueError(f"{run.name}/{path.name}: no step without errors")
        steps[report["transport"]] = step
    return steps


def _devices(run: Path) -> dict[str, Any]:
    return json.loads((run / "optimistic" / "devices.json").read_text())


def one_fleet(runs: Sequence[Path]) -> tuple[dict[str, Any], dict[str, str | None]]:
    """The devices of the first of ``runs`` and the commit each run served: a report over runs from
    two VMs is refused. Two commits are stated, not refused (bench_units.commit_block)."""
    first = _devices(runs[0])
    commits = {runs[0].name: first["server"]["facts"].get("commit")}
    for run in runs[1:]:
        devices = _devices(run)
        for name in ("server", "client", "backends"):
            if name in first or name in devices:
                bench_sanity.refuse_cross_vm(
                    first.get(name) or {},
                    devices.get(name) or {},
                    labels=(f"{runs[0].name} {name}", f"{run.name} {name}"),
                )
        commits[run.name] = devices["server"]["facts"].get("commit")
    return first, commits


def version_of(commits: Mapping[str, str | None]) -> str | list[str] | None:
    """The version a report opens with: the one commit, or every commit of a run across several."""
    distinct = sorted({c for c in commits.values() if c})
    if len(distinct) > 1:
        return distinct
    return distinct[0] if distinct else None


def build(
    root: Path, ledger: Mapping[str, Any] | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    """(published, internal): the published file holds the telemetry-off points only. Both state
    the commit of every run and, given the run's ledger, its stops and discarded units."""
    runs = _runs(root)
    if not runs["off"]:
        raise ValueError("no telemetry-off run: nothing to publish")
    for telemetry, dirs in runs.items():
        for d in dirs:
            stated = _devices(d)["server"]["facts"]["telemetry"]
            if stated != telemetry:
                raise ValueError(f"{d.name}: the server says telemetry {stated}, not {telemetry}")
    by: dict[str, dict[str, Any]] = {}
    for telemetry, dirs in runs.items():
        steps: dict[str, list[Mapping[str, Any]]] = {}
        for d in dirs:
            for transport, step in _transport_steps(d).items():
                steps.setdefault(transport, []).append(step)
        by[telemetry] = {t: summarize(s) for t, s in steps.items()}
    uncached = _uncached_runs(root)
    if uncached:
        by["off"][PGWIRE_UNCACHED] = summarize([_transport_steps(d)["pgwire"] for d in uncached])
    devices, commits = one_fleet([*(d for dirs in runs.values() for d in dirs), *uncached])
    stated = {**bench_units.commit_block(commits), **bench_units.stops_block(ledger)}
    published = {
        "version": version_of(commits),
        **stated,
        "caption": CAPTION,
        "table_caption": TABLE_CAPTION,
        "transport_captions": {t: PUBLISHED[t] for t in by["off"] if t in PUBLISHED},
        "devices": devices,
        "transports": {t: s for t, s in by["off"].items() if t in PUBLISHED},
    }
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
        "version": version_of(commits),
        **stated,
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


def narrowed_contract(
    contract_text: str,
    *,
    only: str,
    uncached: bool,
    steps: Sequence[int] | None,
    window_s: float | None,
    warmup_s: int | None,
) -> str:
    """The contract with one transport, for the pgwire uncached run and a py-spy profile."""
    contract = yaml.safe_load(contract_text)
    contract["transports"] = {only: contract["transports"][only]}
    if uncached:
        contract["knobs"]["cache"] = {"probability": 0.0}
    if steps is not None:
        contract["load"].update(steps=list(steps), window_s=window_s, warmup_s=warmup_s)
    return yaml.safe_dump(contract, sort_keys=False)


def variant_contract(
    contract_text: str,
    *,
    telemetry: str,
    variant: str,
    cache: str,
    steps: Sequence[int],
    window_s: float,
    warmup_s: int,
) -> str:
    """The contract as one run of a governance pass states it (REQ-2092): the variant's
    governance (governance_variants.py), the cache setting, the pass's load, and every request
    reading every listed column of the Postgres orders table, so a masked column is in it. The
    contract narrows to that table, as local_pass.py's does: governance is set up on every contract
    table, and a table no request reads only makes the pass's admin calls longer."""
    if variant not in governance_variants.VARIANTS:
        raise ValueError(f"variant must be one of {', '.join(governance_variants.VARIANTS)}")
    if cache not in governance_variants.CACHE:
        raise ValueError(f"cache must be one of {', '.join(governance_variants.CACHE)}")
    contract = yaml.safe_load(run_contract(contract_text, telemetry=telemetry))
    knobs = contract["knobs"]
    knobs["governance"] = governance_variants.governance(variant, telemetry=telemetry == "on")
    knobs["cache"] = {"probability": governance_variants.CACHE[cache]}
    source = contract["sources"]["bench-postgresql"]
    orders = next(t for t in source["tables"] if t["table"] == "orders")
    source["tables"] = [orders]
    source["joins"] = []
    contract["sources"] = {"bench-postgresql": source}
    contract["cross_source_joins"] = []
    knobs["source_weights"] = {"bench-postgresql": 1.0}
    knobs["route"] = {"bench-postgresql": knobs["route"]["bench-postgresql"]}
    knobs["fields"] = {
        "probability": 1.0,
        "distribution": {"kind": "constant", "value": len(orders["columns"])},
    }
    contract["load"].update(steps=list(steps), window_s=window_s, warmup_s=warmup_s)
    return yaml.safe_dump(contract, sort_keys=False)


def variant_rows(root: Path) -> list[dict[str, Any]]:
    """The pass's table (governance_variants.table) from its unit directories,
    ``<root>/<rep>-<variant>-<cache>-<transport>-<telemetry>/optimistic/<transport>.json``."""
    results: dict[str, list[Mapping[str, Any]]] = {}
    for d in sorted(p for p in root.iterdir() if p.is_dir()):
        variant, cache = d.name.split("-")[1:3]
        reports = []
        for path in sorted((d / "optimistic").glob("*.json")):
            report = json.loads(path.read_text())
            if "transport" in report:
                reports.append(report)
        results.setdefault(f"{variant}/{cache}", []).extend(reports)
    return governance_variants.table(results)


def variant_commits(root: Path) -> dict[str, str | None]:
    """The commit each unit of a pass served."""
    return {
        d.name: _devices(d)["server"]["facts"].get("commit")
        for d in sorted(p for p in root.iterdir() if p.is_dir())
    }


def _ledger(path: str | None) -> dict[str, Any] | None:
    return bench_units.load_ledger(Path(path)) if path else None


def main() -> int:
    parser = argparse.ArgumentParser(description="The three-device optimistic benchmark")
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve-env", help="device 1: the server's environment, as export lines")
    serve.add_argument("--engine", choices=ENGINES, default="pg")
    serve.add_argument("--telemetry", choices=TELEMETRY, required=True)
    serve.add_argument("--backends-host", required=True, help="device 3's private address")
    serve.add_argument("--engine-host", help="where the engine listens; default device 3")
    serve.add_argument("--workers", type=int, required=True)
    serve.add_argument("--request-threads", type=int)
    serve.add_argument("--base-config", default=str(Path(__file__).parent.parent / "config.yaml"))
    serve.add_argument("--config-out", required=True)
    con = sub.add_parser("contract", help="device 2: the setup contract as one run states it")
    con.add_argument("--setup", required=True)
    con.add_argument("--telemetry", choices=TELEMETRY, required=True)
    con.add_argument("--out", required=True)
    # a governance pass's run (REQ-2092): all or none of these
    con.add_argument("--variant", choices=list(governance_variants.VARIANTS))
    con.add_argument("--cache", choices=list(governance_variants.CACHE))
    con.add_argument("--steps", help="comma-separated concurrencies")
    con.add_argument("--window-s", type=float)
    con.add_argument("--warmup-s", type=int)
    # one transport (the pgwire uncached run, a profile), with or without that load
    con.add_argument("--only", help="the one transport to run")
    con.add_argument("--uncached", action="store_true")
    vrep = sub.add_parser("variant-report", help="a governance pass's table, internal only")
    vrep.add_argument("--root", required=True)
    vrep.add_argument("--out", required=True)
    vrep.add_argument("--ledger", help="the run's ledger: its stops and discarded units")
    rep = sub.add_parser("report", help="the published point and the telemetry impact")
    rep.add_argument("--root", required=True)
    rep.add_argument("--out", required=True)
    rep.add_argument("--ledger", help="the run's ledger: its stops and discarded units")
    args = parser.parse_args()
    if args.command == "contract":
        out = Path(args.out).expanduser()
        text = Path(args.setup).read_text()
        steps = [int(c) for c in args.steps.split(",")] if args.steps else None
        pass_args = (args.variant, args.cache, args.steps, args.window_s, args.warmup_s)
        if args.variant is None and args.cache is None:
            contract = run_contract(text, telemetry=args.telemetry)
            load = {"steps": steps, "window_s": args.window_s, "warmup_s": args.warmup_s}
        elif any(a is None for a in pass_args):
            parser.error("--variant, --cache, --steps, --window-s and --warmup-s go together")
        else:
            contract = variant_contract(
                text,
                telemetry=args.telemetry,
                variant=args.variant,
                cache=args.cache,
                steps=steps,
                window_s=args.window_s,
                warmup_s=args.warmup_s,
            )
            load = {"steps": None, "window_s": None, "warmup_s": None}
        if args.only:
            contract = narrowed_contract(contract, only=args.only, uncached=args.uncached, **load)
        out.write_text(contract)
        return 0
    if args.command == "variant-report":
        rows = variant_rows(Path(args.root))
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        commits = variant_commits(Path(args.root))
        (out / "variants.json").write_text(
            json.dumps(
                {
                    "label": LABEL_INTERNAL,
                    "version": version_of(commits),
                    **bench_units.commit_block(commits),
                    **bench_units.stops_block(_ledger(args.ledger)),
                    "rows": rows,
                },
                indent=2,
            )
        )
        for r in rows:
            paging = {
                h: (p if "error" in p else {k: p[k] for k in ("pgmajfault", "pswpin", "pswpout")})
                for h, p in r.get("paging", {}).items()
            }
            print(
                r["variant"],
                r["cache"],
                r["transport"],
                r["concurrency"],
                r.get("server_cpu_ms_per_request"),
                r.get("over_zero_ms"),
                r.get("error") or json.dumps(paging),
            )
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
            engine_host=args.engine_host,
        )
        for k, v in env.items():
            print(f"export {k}={shlex.quote(v)}")
        return 0
    published, internal = build(Path(args.root), _ledger(args.ledger))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "published.json").write_text(json.dumps(published, indent=2))
    (out / "internal.json").write_text(json.dumps(internal, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
