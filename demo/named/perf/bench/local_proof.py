# Copyright (c) 2026 Kenneth Stott
# Canary: b7971e16-1495-406b-a308-2f03c4b1dde4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Prove the benchmark against a small local stack (REQ-1911). Run it through the machine-wide
one-heavy-job lock:

    /Users/kennethstott/.claude/jobs/c770c3fd/tmp/heavy.sh \\
        .venv/bin/python3 demo/named/perf/bench/local_proof.py --output-dir ~/bench_local_proof

It provisions, runs and tears down everything inside this one command:

* its OWN compose project (``provisa-benchproof-<pid>``), derived from demo/named/perf/docker-compose.yml
  with the four sources on leased ports (tests/port_lease.py), stock images, tiny data
  (``generate_*.py --orders N``);
* its OWN isolated Provisa server subprocess (``tests/integration/isolated_server.py``: own org,
  SQLite control plane, embedded Redis, auth disabled, leased ports), with demo/named/perf/fragment.yaml
  as its config. Never the maintainer's local-dev instance, never the perf VM, no process is killed
  by pattern.

It then runs, and records in ``<output-dir>/proof.json`` and as a table: the name lookup; one request
per transport per source with every knob at zero; one with fields+filters+rows; one join per
join-capable transport; the stats-header time split; the docker-stats sampler; the declared-vs-
registry replication check and the audit-log route verification.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any

import yaml

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(REPO))

import contract_model  # noqa: E402
import lookup  # noqa: E402
import measurements  # noqa: E402
import optimistic_load as ol  # noqa: E402
import replication  # noqa: E402
import request_mix  # noqa: E402
import request_render  # noqa: E402
import setup_contract as sc  # noqa: E402

PERF = HERE.parent
SOURCES = ["bench-postgresql", "bench-clickhouse", "bench-mongodb", "bench-neo4j"]
SERVICE = {
    "bench-postgresql": "postgresql",
    "bench-clickhouse": "clickhouse",
    "bench-mongodb": "mongodb",
    "bench-neo4j": "neo4j",
}
ORDERS, ITEMS_PER_ORDER, NEO4J_ORDERS = 2000, 2, 200
MAX_LOAD = 12.0


def log(msg: str) -> None:
    print(f"[proof {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def wait_for_load(timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    while True:
        load = os.getloadavg()[0]
        if load < MAX_LOAD:
            log(f"1-minute load {load:.1f} < {MAX_LOAD:g}")
            return
        if time.time() > deadline:
            raise SystemExit(
                f"1-minute load still {load:.1f} after {timeout_s:g}s: not starting containers"
            )
        log(f"1-minute load {load:.1f}: waiting")
        time.sleep(30)


# --------------------------------------------------------------------------------------------
# The stack
# --------------------------------------------------------------------------------------------


def compose_file(ports: dict[str, int], tmp: Path) -> Path:
    """The perf demo's compose file with stock images, small settings, no bind mounts, no seeder, on
    leased ports."""
    raw = yaml.safe_load((PERF / "docker-compose.yml").read_text())
    services = {k: v for k, v in raw["services"].items() if k in SERVICE.values()}
    pg = services["postgresql"]
    pg.pop("build", None)
    pg.update(
        image="postgres:16",
        volumes=[],
        shm_size="256m",
        command=["postgres", "-c", "max_connections=200"],
    )
    pg["ports"] = [f"127.0.0.1:{ports['bench-postgresql']}:5432"]
    services["mongodb"]["volumes"] = []
    services["mongodb"]["ports"] = [f"127.0.0.1:{ports['bench-mongodb']}:27017"]
    ch = services["clickhouse"]
    ch["volumes"] = []
    ch["ports"] = [f"127.0.0.1:{ports['bench-clickhouse']}:8123"]
    neo = services["neo4j"]
    neo["volumes"] = []
    neo["environment"].update(
        NEO4J_server_memory_heap_max__size="512m", NEO4J_server_memory_pagecache_size="256m"
    )
    neo["ports"] = [
        f"127.0.0.1:{ports['neo4j-http']}:7474",
        f"127.0.0.1:{ports['neo4j-bolt']}:7687",
    ]
    for svc in services.values():
        svc.pop("restart", None)
        # not the perf demo's labels: `docker ps --filter label=com.provisa.demo=perf` lists the
        # maintainer's demo, and this stack must never look like it
        svc["labels"] = {"com.provisa.benchproof": "1"}
    path = tmp / "docker-compose.proof.yml"
    path.write_text(yaml.safe_dump({"services": services}, sort_keys=False))
    return path


def run(cmd: list[str], **kw: Any) -> subprocess.CompletedProcess:
    log("$ " + " ".join(cmd))
    return subprocess.run(cmd, check=True, text=True, **kw)


def seed(ports: dict[str, int]) -> None:
    env = {
        **os.environ,
        "PROVISA_BENCH_POSTGRESQL_HOST": "localhost",
        "PROVISA_BENCH_POSTGRESQL_PORT": str(ports["bench-postgresql"]),
        "PROVISA_BENCH_CLICKHOUSE_HOST": "localhost",
        "PROVISA_BENCH_CLICKHOUSE_PORT": str(ports["bench-clickhouse"]),
        "PROVISA_BENCH_MONGO_HOST": "localhost",
        "PROVISA_BENCH_MONGO_PORT": str(ports["bench-mongodb"]),
        "PROVISA_BENCH_NEO4J_HOST": "localhost",
        "PROVISA_BENCH_NEO4J_HTTP_PORT": str(ports["neo4j-http"]),
    }
    py = sys.executable
    for script, args in (
        (
            "generate_postgres.py",
            ["--orders", str(ORDERS), "--items-per-order", str(ITEMS_PER_ORDER)],
        ),
        ("generate_clickhouse.py", ["--orders", str(ORDERS)]),
        ("generate_mongo.py", ["--orders", str(ORDERS)]),
        (
            "generate_neo4j.py",
            ["--orders", str(NEO4J_ORDERS), "--items-per-order", str(ITEMS_PER_ORDER)],
        ),
    ):
        run([py, str(PERF / script), *args], env=env, cwd=str(PERF))


def _proof_server_class() -> Any:
    from tests.integration.isolated_server import IsolatedServer

    class ProofServer(IsolatedServer):
        """IsolatedServer with the proof's own config file. Its stock config writer copies the file
        and sets ``auth.provider`` on the copy, which a config that includes another file with an
        ``auth`` section refuses (only list sections merge)."""

        def _write_config(self) -> str:
            return self._config

        def stop_process(self) -> None:
            self._cfg_path = (
                None  # the config file is the proof's, not the server's: do not delete it
            )
            super().stop_process()

    return ProofServer


def _source_env(ports: dict[str, int]) -> dict[str, str]:
    return {
        "PROVISA_BENCH_POSTGRESQL_HOST": "localhost",
        "PROVISA_BENCH_POSTGRESQL_PORT": str(ports["bench-postgresql"]),
        "PROVISA_BENCH_CLICKHOUSE_HOST": "localhost",
        "PROVISA_BENCH_CLICKHOUSE_PORT": str(ports["bench-clickhouse"]),
        "PROVISA_BENCH_MONGO_HOST": "localhost",
        "PROVISA_BENCH_MONGO_PORT": str(ports["bench-mongodb"]),
        "PROVISA_BENCH_NEO4J_HOST": "localhost",
        "PROVISA_BENCH_NEO4J_HTTP_PORT": str(ports["neo4j-http"]),
    }


def start_server(ports: dict[str, int], tmp: Path) -> Any:
    wrapper = tmp / "provisa-with-perf.yaml"
    wrapper.write_text(
        yaml.safe_dump(
            {
                "includes": [
                    str(REPO / "config" / "provisa-install.yaml"),
                    str(PERF / "fragment.yaml"),
                ]
            }
        )
    )
    server = _proof_server_class()(
        f"benchproof{os.getpid()}",
        engine="duckdb",
        control_plane="sqlite",
        enable_pgwire=True,
        enable_bolt=True,
        await_flight=True,
        await_grpc=True,
        config=str(wrapper),
        env=_source_env(ports),
    )
    server.start(timeout=600)
    return server


# --------------------------------------------------------------------------------------------
# Phase 2: the same stack behind the simple auth provider, with the credentials the contract names
# --------------------------------------------------------------------------------------------

AUTH_USER, AUTH_PASSWORD_ENV = "benchuser", "BENCH_PROOF_PASSWORD"
AUTH_PASSWORD = "benchproof-password-1"  # the user's password on the proof server; the contract reads it from the env var


def start_auth_server(ports: dict[str, int], tmp: Path) -> Any:
    """A second isolated server over the same sources with auth.provider simple (a bcrypt user and a
    JWT secret). IsolatedServer forces auth off in its config copy, so its config writer is
    replaced."""
    import bcrypt

    password = AUTH_PASSWORD
    # the install config with its auth section replaced (an including file may not redefine a key the
    # included one has), plus the perf fragment
    cfg: dict[str, Any] = yaml.safe_load((REPO / "config" / "provisa-install.yaml").read_text())
    cfg["includes"] = [str(PERF / "fragment.yaml")]
    cfg["auth"] = {
        **cfg.get("auth", {}),
        "provider": "simple",
        "allow_simple_auth": True,
        "jwt_secret": "benchproof-jwt-secret-benchproof-jwt-secret",
        "simple": {
            "users": [
                {
                    "username": AUTH_USER,
                    "password_hash": bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode(),
                    "roles": ["org_admin"],
                }
            ]
        },
    }
    path = tmp / "provisa-auth.yaml"
    path.write_text(yaml.safe_dump(cfg))

    server = _proof_server_class()(
        f"benchproofauth{os.getpid()}",
        engine="duckdb",
        control_plane="sqlite",
        enable_pgwire=True,
        enable_bolt=True,
        await_flight=True,
        await_grpc=True,
        config=str(path),
        env=_source_env(ports),
    )
    server.start(timeout=600)
    return server


def start_replica_server(ports: dict[str, int], tmp: Path) -> Any:
    """A server whose configuration makes ClickHouse a replica source (replicate 0, TTL 60):
    the only way to set it, since the admin API refuses to change a source the config declares."""
    fragment = yaml.safe_load((PERF / "fragment.yaml").read_text())
    for src in fragment["sources"]:
        if src["id"] == "bench-clickhouse":
            src["replicate"] = 0
            src["cache_ttl"] = 60
    frag_path = tmp / "fragment-replica.yaml"
    frag_path.write_text(yaml.safe_dump(fragment))
    wrapper = tmp / "provisa-replica.yaml"
    wrapper.write_text(
        yaml.safe_dump(
            {"includes": [str(REPO / "config" / "provisa-install.yaml"), str(frag_path)]}
        )
    )
    server = _proof_server_class()(
        f"benchproofrep{os.getpid()}",
        engine="duckdb",
        control_plane="sqlite",
        enable_pgwire=True,
        enable_bolt=True,
        await_flight=True,
        await_grpc=True,
        config=str(wrapper),
        env=_source_env(ports),
    )
    server.start(timeout=600)
    return server


def replica_phase(
    ports: dict[str, int], tmp: Path, raw: dict[str, Any], out: Path
) -> dict[str, Any]:
    """ClickHouse declared and configured a replica: the registry check passes, and the audit log
    shows its requests served by the engine (against live, which the main phase showed direct)."""
    result: dict[str, Any] = {}
    server = start_replica_server(ports, tmp)
    try:
        contract = copy.deepcopy(raw)
        contract["sources"]["bench-clickhouse"]["replication"] = {
            "setting": "replica",
            "ttl_seconds": 60,
        }
        ep_raw = contract["deployment"]["endpoints"]
        ep_raw["pgwire"]["port"], ep_raw["bolt"]["port"] = server.pgwire_port, server.bolt_port
        ep_raw["flight"]["port"], ep_raw["grpc"]["port"] = server.flight_port, server.grpc_port
        ep_raw["http"]["base_url"] = server.base_url
        path = out / "proof-contract-replica.yaml"
        path.write_text(yaml.safe_dump(contract, sort_keys=False))
        os.environ["PROVISA_HTTP_BASE_URL"] = server.base_url
        unbound = sc.load_setup(path, known_transports=list(ol.TRANSPORTS))
        resolved = ol.resolve_names(
            argparse.Namespace(setup=str(path), resolved_names=None), unbound, out / "replica"
        )
        bound = sc.load_setup(
            path,
            known_transports=list(ol.TRANSPORTS),
            deployment=resolved,
            verify_replication=False,
        )
        result["mismatches"] = replication.mismatches(bound, resolved)
        result["registry_clickhouse"] = {
            "replicate": resolved.sources["bench-clickhouse"]["replicate"],
            "cache_ttl": resolved.sources["bench-clickhouse"]["cache_ttl"],
        }
        ep = ol.endpoints_from_setup(bound)
        result["route_verification"] = verify_routes(bound, resolved, ep, contract)
        rows = []
        for transport in ("pgwire", "data_sql", "graphql"):
            setup = variant(contract, resolved, "bench-clickhouse", transport)
            gen = request_mix.RequestGenerator(
                setup, transport, f"proof/replica/{transport}", cacheable=False
            )
            query = request_render.Renderer(setup).render(gen.next())
            ep_t = ol.Endpoints(**{**ep.__dict__, "setup": setup})
            row = {
                "transport": transport,
                "request": text_of(query, transport),
                **one_request(ep_t, transport, query, ep.role),
            }
            waited = 0
            while (
                row.get("status") == "ok" and row.get("rows") == 0 and waited < 120
            ):  # a replica is landed on first read
                time.sleep(10)
                waited += 10
                row = {
                    "transport": transport,
                    "request": row["request"],
                    "waited_s": waited,
                    **one_request(ep_t, transport, query, ep.role),
                }
            rows.append(row)
        result["requests"] = rows
    finally:
        server.stop_process()
    return result


JWT_SECRET = "benchproof-jwt-secret-benchproof-jwt-secret"


def credentials_phase(
    ports: dict[str, int],
    tmp: Path,
    project: str,
    raw: dict[str, Any],
    out: Path,
    proof: dict[str, Any],
) -> dict[str, Any]:
    """One request per transport on the Postgres source against a server with auth on (the simple
    provider), as ``AUTH_USER``, for both credential kinds: ``token`` (a JWT minted here with the
    provider's secret: HTTP, gRPC, Flight, Bolt-bearer) and ``password`` (pgwire and Bolt present
    the password; HTTP/gRPC/Flight log in first through /auth/login). Also the no-credential
    request, and the login route's own answer."""
    import httpx
    import jwt

    result: dict[str, Any] = {}
    proof["credentials"] = result  # kept even if a step below raises
    password = AUTH_PASSWORD
    server = start_auth_server(ports, tmp)
    try:
        base = proof_contract(server, project)
        os.environ["PROVISA_HTTP_BASE_URL"] = server.base_url
        unauth = httpx.post(
            server.base_url + "/data/graphql",
            json={"query": "{ __typename }"},
            headers={"X-Provisa-Role": "org_admin"},
            timeout=30,
        )
        result["no_credential_http_status"] = unauth.status_code
        probe = httpx.post(
            server.base_url + "/auth/login",
            json={"username": AUTH_USER, "password": password},
            timeout=30,
        )
        result["login_route"] = {"status": probe.status_code, "body": probe.text[:200]}
        token = jwt.encode(
            {"sub": AUTH_USER, "roles": ["org_admin"], "exp": int(time.time()) + 3600},
            JWT_SECRET,
            algorithm="HS256",
        )
        for kind, secret in (("token", token), ("password", password)):
            os.environ[AUTH_PASSWORD_ENV] = secret
            contract = copy.deepcopy(base)
            contract["deployment"]["credentials"] = (
                {"mode": "env", "kind": "token", "env": AUTH_PASSWORD_ENV}
                if kind == "token"
                else {
                    "mode": "env",
                    "kind": "password",
                    "user": AUTH_USER,
                    "env": AUTH_PASSWORD_ENV,
                }
            )
            path = out / f"proof-contract-auth-{kind}.yaml"
            path.write_text(yaml.safe_dump(contract, sort_keys=False))
            rows: list[dict[str, Any]] = []
            result[kind] = {"requests": rows}
            try:
                unbound = sc.load_setup(path, known_transports=list(ol.TRANSPORTS))
                resolved = ol.resolve_names(
                    argparse.Namespace(setup=str(path), resolved_names=None),
                    unbound,
                    out / f"auth-{kind}",
                )
                result[kind]["lookup"] = "ok"
            except Exception as exc:  # noqa: BLE001 - recorded: with the login route absent a password cannot log in
                result[kind]["lookup"] = ol._brief(exc)  # noqa: SLF001
                resolved = lookup.read_resolved(
                    out / "resolved-names.json"
                )  # the main phase's names: same tables
            bound = sc.load_setup(
                path,
                known_transports=list(ol.TRANSPORTS),
                deployment=resolved,
                verify_replication=False,
            )
            ep = ol.endpoints_from_setup(bound)
            for transport in ol.TRANSPORTS:
                row: dict[str, Any] = {"transport": transport}
                try:
                    setup = variant(contract, resolved, "bench-postgresql", transport)
                    ep_t = ol.Endpoints(**{**ep.__dict__, "setup": setup})
                    gen = request_mix.RequestGenerator(
                        setup, transport, f"proof/auth/{kind}/{transport}", cacheable=False
                    )
                    query = request_render.Renderer(setup).render(gen.next())
                    row["request"] = text_of(query, transport)
                    row.update(one_request(ep_t, transport, query, ep.role))
                except sc.SetupError as exc:
                    row.update(status="not exposed", detail=str(exc)[:200])
                rows.append(row)
    finally:
        server.stop_process()
    return result


# --------------------------------------------------------------------------------------------
# The proof contract: the perf contract pointed at the local stack, small domains, all live
# --------------------------------------------------------------------------------------------


def proof_contract(server: Any, project: str) -> dict[str, Any]:
    raw = yaml.safe_load((HERE / "setups" / "perf-stack.yaml").read_text())
    ep = raw["deployment"]["endpoints"]
    ep["pgwire"]["port"], ep["bolt"]["port"] = server.pgwire_port, server.bolt_port
    ep["flight"]["port"], ep["grpc"]["port"] = server.flight_port, server.grpc_port
    ep["http"]["base_url"] = server.base_url
    domains = {
        "bench-postgresql": {"order_id": ORDERS, "customer_id": ORDERS // 8},
        "bench-clickhouse": {"order_id": ORDERS, "customer_id": ORDERS // 8},
        "bench-mongodb": {"order_id": ORDERS, "customer_id": ORDERS // 8},
        "bench-neo4j": {"order_id": NEO4J_ORDERS, "customer_id": NEO4J_ORDERS // 8},
    }
    for sid, src in raw["sources"].items():
        src["replication"] = {"setting": "live", "ttl_seconds": 0}
        src["monitor"] = {"container": f"{project}-{SERVICE[sid]}-1"}
        for table in src["tables"]:
            for col in table["columns"]:
                dom = col.get("domain")
                if dom and dom["kind"] == "int_range":
                    dom["max"] = domains[sid][col["name"]]
    raw["load"]["steps"] = [1]
    raw["load"]["window_s"] = 2
    return raw


# --------------------------------------------------------------------------------------------
# The proof
# --------------------------------------------------------------------------------------------


def one_request(ep: ol.Endpoints, transport: str, query: Any, role: str) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        client = ol.CLIENTS[transport](ep)
        try:
            rows, cols, hit = client.call(query, role)
        finally:
            client.close()
        return {
            "status": "ok",
            "rows": rows,
            "columns": cols,
            "hit": hit,
            "ms": round((time.perf_counter() - t0) * 1000, 1),
        }
    except Exception as exc:  # noqa: BLE001 - the evidence table records the failure
        out = {"status": "error", "error": ol._brief(exc)}  # noqa: SLF001
        resp = getattr(exc, "response", None)
        if resp is not None:
            out["response_body"] = resp.text[:600]  # what the server said
        return out


def text_of(query: Any, transport: str) -> Any:
    language = contract_model.TRANSPORT_LANGUAGE[transport]
    return getattr(query, language)


def variant(
    raw: dict[str, Any], resolved: lookup.Resolved, source: str, transport: str, **knobs: Any
) -> contract_model.Setup:
    """The proof contract with all weight on ``source``, one transport and ``knobs`` set."""
    c = copy.deepcopy(raw)
    k = c["knobs"]
    k["source_weights"] = {s: 1.0 if s == source else 0.0 for s in c["sources"]}
    c["transports"] = {transport: {}}
    for name, value in knobs.items():
        k[name] = {"probability": 1, "distribution": {"kind": "constant", "value": value}}
    return sc.setup_from_dict(
        c,
        environ=os.environ,
        known_transports=list(ol.TRANSPORTS),
        deployment=resolved,
        verify_replication=False,
    )


def evidence_rows(
    raw: dict[str, Any], resolved: lookup.Resolved, ep_base: ol.Endpoints, role: str
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    shapes = {
        "zero": {},
        "fields+filters+rows": {"fields": 2, "filters": 1, "rows": 5},
    }
    for source in SOURCES:
        for transport in ol.TRANSPORTS:
            for shape, knobs in shapes.items():
                row = {"source": source, "transport": transport, "shape": shape}
                try:
                    setup = variant(raw, resolved, source, transport, **knobs)
                except contract_model.SetupError as exc:
                    row.update(status="not exposed", detail=str(exc)[:200])
                    rows.append(row)
                    continue
                ep = ol.Endpoints(**{**ep_base.__dict__, "setup": setup})
                gen = request_mix.RequestGenerator(
                    setup,
                    transport,
                    f"proof/{source}/{transport}/{shape}",
                    cacheable=ol.TRANSPORTS[transport][1] is not None,
                )
                spec = gen.next()
                query = request_render.Renderer(setup).render(spec)
                row["request"] = text_of(query, transport)
                row.update(one_request(ep, transport, query, role))
                rows.append(row)
    return rows


def join_rows(
    raw: dict[str, Any], resolved: lookup.Resolved, ep_base: ol.Endpoints, role: str
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for kind, knob in (
        ("same-source", "joins_same_source"),
        ("cross-source", "joins_cross_source"),
    ):
        for transport in contract_model.JOIN_TRANSPORTS:
            row = {"kind": kind, "transport": transport}
            try:
                c = copy.deepcopy(raw)
                c["knobs"]["source_weights"] = {
                    "bench-postgresql": 0.5 if kind == "cross-source" else 1.0,
                    "bench-clickhouse": 0.25 if kind == "cross-source" else 0.0,
                    "bench-mongodb": 0.25 if kind == "cross-source" else 0.0,
                    "bench-neo4j": 0.0,
                }
                c["transports"] = {transport: {}}
                c["knobs"][knob] = {
                    "probability": 1,
                    "distribution": {"kind": "constant", "value": 1},
                }
                setup = sc.setup_from_dict(
                    c,
                    environ=os.environ,
                    known_transports=list(ol.TRANSPORTS),
                    deployment=resolved,
                    verify_replication=False,
                )
                gen = request_mix.RequestGenerator(
                    setup, transport, f"proof/join/{kind}/{transport}", cacheable=True
                )
                spec = gen.next()
                query = request_render.Renderer(setup).render(spec)
                ep = ol.Endpoints(**{**ep_base.__dict__, "setup": setup})
                row["request"] = text_of(query, transport)
                row.update(one_request(ep, transport, query, role))
            except contract_model.SetupError as exc:
                row.update(status="not available", detail=str(exc)[:200])
            rows.append(row)
    return rows


def render_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    def cell(r: dict[str, Any], c: str) -> str:
        v = r.get(c, "")
        return " ".join(str(v).split())[:110]

    widths = [max([len(c), *(len(cell(r, c)) for r in rows)]) for c in columns]
    lines = ["  ".join(c.ljust(w) for c, w in zip(columns, widths))]
    lines += ["  ".join(cell(r, c).ljust(w) for c, w in zip(columns, widths)) for r in rows]
    return "\n".join(lines)


def main_phase(server: Any, raw: dict[str, Any], out: Path, proof: dict[str, Any]) -> None:
    """Lookup, requests, joins, time split, sampler, replication and route verification."""
    contract = out / "proof-contract.yaml"
    contract.write_text(yaml.safe_dump(raw, sort_keys=False))
    os.environ["PROVISA_HTTP_BASE_URL"] = server.base_url
    a = argparse.Namespace(setup=str(contract), resolved_names=None)
    unbound = sc.load_setup(contract, known_transports=list(ol.TRANSPORTS))
    resolved = ol.resolve_names(a, unbound, out)
    proof["lookup"] = {
        k: v.__dict__ | {"columns": {c: dict(s) for c, s in v.columns.items()}}
        for k, v in resolved.tables.items()
    }
    log("name lookup: done (resolved-names.json, lookup-responses.json written)")
    bound = sc.load_setup(
        contract,
        known_transports=list(ol.TRANSPORTS),
        deployment=resolved,
        verify_replication=False,
    )
    ep = ol.endpoints_from_setup(bound)
    role = ep.role

    proof["availability"] = {
        sid: {t.name: {lang: t.has(lang) for lang in lookup.LANGS} for t in src.tables}
        for sid, src in bound.sources.items()
    }
    rows = evidence_rows(raw, resolved, ep, role)
    proof["requests"] = rows
    proof["joins"] = join_rows(raw, resolved, ep, role)

    # the stats-header time split
    split: dict[str, Any] = {}
    import httpx

    with httpx.Client(
        base_url=server.base_url, timeout=120, headers={"X-Provisa-Role": role}
    ) as client:
        for transport in measurements.PROFILE_TRANSPORTS:
            try:
                setup = variant(raw, resolved, "bench-postgresql", transport)
                split[transport] = measurements.profile_transport(
                    setup, transport, client, role=role, requests=5
                )
            except Exception as exc:  # noqa: BLE001
                split[transport] = {"error": ol._brief(exc)}  # noqa: SLF001
    proof["time_split"] = split
    samples: dict[str, Any] = {}
    with httpx.Client(
        base_url=server.base_url, timeout=120, headers={"X-Provisa-Role": role}
    ) as client:
        for transport in measurements.PROFILE_TRANSPORTS:
            try:
                setup = variant(raw, resolved, "bench-postgresql", transport)
                gen = request_mix.RequestGenerator(
                    setup, transport, f"proof/stats/{transport}", cacheable=False
                )
                query = request_render.Renderer(setup).render(gen.next())
                body = measurements._send(transport, client, query, role)  # noqa: SLF001
                samples[transport] = measurements.extract_stats(transport, body)
            except Exception as exc:  # noqa: BLE001
                samples[transport] = {"error": ol._brief(exc)}  # noqa: SLF001
    proof["stats_samples"] = samples

    # the docker-stats sampler
    sampler = measurements.DockerStatsSampler(
        {sid: s.container for sid, s in bound.sources.items() if s.container}
    )
    sampler.sample_once()
    res = sampler.result()
    proof["docker_stats"] = {"cores": dict(res.cores), "samples": res.samples, "error": res.error}

    # declared vs the deployment's registry, and the audit-log route verification
    proof["replication_mismatches"] = replication.mismatches(bound, resolved)
    proof["route_verification"] = verify_routes(bound, resolved, ep, raw)
    proof["replication_apply"] = replication_apply(server, raw, resolved, ep)


def main() -> int:
    parser = argparse.ArgumentParser(description="Prove the benchmark against a small local stack")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--load-timeout", type=float, default=3600)
    parser.add_argument(
        "--phases",
        default="main,replica,credentials",
        help="main: lookup, requests, joins, time split, sampler, replication; credentials: the same "
        "stack behind the simple auth provider",
    )
    args = parser.parse_args()
    out = Path(args.output_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    wait_for_load(args.load_timeout)

    from tests.port_lease import lease_ports

    leased = lease_ports(6)
    ports = {
        "bench-postgresql": leased[0],
        "bench-clickhouse": leased[1],
        "bench-mongodb": leased[2],
        "neo4j-http": leased[3],
        "neo4j-bolt": leased[4],
    }
    project = f"provisa-benchproof-{os.getpid()}"
    tmp = Path(tempfile.mkdtemp(prefix="benchproof-"))
    compose = ["docker", "compose", "-p", project, "-f", str(compose_file(ports, tmp))]
    proof: dict[str, Any] = {"project": project, "ports": ports}
    server = None
    try:
        # one heavy source at a time: the small ones first
        for service in ("postgresql", "mongodb", "clickhouse", "neo4j"):
            run([*compose, "up", "-d", "--wait", service])
        seed(ports)
        phases = set(args.phases.split(","))
        server = start_server(ports, tmp)
        raw = proof_contract(server, project)
        if "main" in phases:
            main_phase(server, raw, out, proof)
        server.stop_process()
        server = None
        if "replica" in phases:
            proof["replica"] = replica_phase(ports, tmp, raw, out)
        if "credentials" in phases:
            credentials_phase(ports, tmp, project, raw, out, proof)
    except Exception:  # noqa: BLE001 - the proof records how it ended
        proof["fatal"] = traceback.format_exc()
        log(proof["fatal"])
        if server is not None:
            proof["server_stderr_tail"] = server.dump_stderr_debug()[-4000:]
    finally:
        if server is not None:
            server.stop_process()
        subprocess.run([*compose, "down", "-v", "--remove-orphans"], check=False)
        (out / "proof.json").write_text(json.dumps(proof, indent=2, default=str))
    for title, key, cols in (
        (
            "one request per transport per source",
            "requests",
            [
                "source",
                "transport",
                "shape",
                "status",
                "rows",
                "columns",
                "ms",
                "error",
                "response_body",
                "detail",
            ],
        ),
        ("joins", "joins", ["kind", "transport", "status", "rows", "columns", "error", "detail"]),
    ):
        if key in proof:
            print(f"\n== {title} ==\n{render_table(proof[key], cols)}")
    if "credentials" in proof:
        c = proof["credentials"]
        print("\n== credentials (simple auth provider) ==")
        print(
            "no credential, HTTP status:",
            c.get("no_credential_http_status"),
            "| login route:",
            c.get("login_route"),
        )
        for kind in ("token", "password"):
            if kind in c:
                print(f"-- kind {kind}: lookup {c[kind].get('lookup')}")
                print(
                    render_table(
                        c[kind].get("requests", []),
                        ["transport", "status", "rows", "columns", "ms", "error", "response_body"],
                    )
                )
    if "replica" in proof:
        print("\n== replica (ClickHouse configured as a replica source) ==")
        print(
            json.dumps(
                {
                    k: proof["replica"].get(k)
                    for k in ("mismatches", "registry_clickhouse", "route_verification")
                },
                indent=2,
                default=str,
            )[:2500]
        )
        print(
            render_table(
                proof["replica"].get("requests", []),
                ["transport", "status", "rows", "columns", "waited_s", "ms", "error"],
            )
        )
    for key in (
        "time_split",
        "docker_stats",
        "replication_mismatches",
        "route_verification",
        "fatal",
    ):
        if key in proof:
            print(f"\n== {key} ==\n{json.dumps(proof[key], indent=2, default=str)[:3000]}")
    return 1 if "fatal" in proof else 0


def replication_apply(
    server: Any, raw: dict[str, Any], before: lookup.Resolved, ep: ol.Endpoints
) -> dict[str, Any]:
    """Declare ClickHouse a replica (TTL 60), apply it through the admin API, check the registry now
    says so, send requests and check the audit route, then confirm the restore put every value back."""
    import httpx

    result: dict[str, Any] = {}
    declared = copy.deepcopy(raw)
    declared["sources"]["bench-clickhouse"]["replication"] = {
        "setting": "replica",
        "ttl_seconds": 60,
    }
    setup = sc.setup_from_dict(
        declared,
        environ=os.environ,
        known_transports=list(ol.TRANSPORTS),
        deployment=before,
        verify_replication=False,
    )

    def facts(r: lookup.Resolved) -> Any:
        return {
            "sources": dict(r.sources),
            "tables": {k: (t.replicate, t.cache_ttl) for k, t in r.tables.items()},
        }

    try:
        with httpx.Client(
            base_url=server.base_url, timeout=120, headers={"X-Provisa-Role": ep.role}
        ) as client:
            result["facts_before"] = facts(before)
            with replication.AdminReplication(client, ep.role).applied(setup, before):
                applied = lookup.resolve(*sc.identities(setup), lookup.fetch(client, ep.role))
                result["facts_applied"] = facts(applied)
                result["mismatches_after_apply"] = replication.mismatches(setup, applied)
                result["route_verification"] = verify_routes(setup, applied, ep, declared)
            restored = lookup.resolve(*sc.identities(setup), lookup.fetch(client, ep.role))
            result["facts_restored"] = facts(restored)
            result["restored_equals_before"] = facts(restored) == facts(before)
    except Exception as exc:  # noqa: BLE001 - the proof records it
        result["error"] = f"{type(exc).__name__}: {exc}"[:500]
    return result


def verify_routes(
    setup: contract_model.Setup, resolved: lookup.Resolved, ep: ol.Endpoints, raw: dict[str, Any]
) -> dict[str, Any]:
    """Send a few requests per source over pgwire, then read ops.queries and compare with the
    declared (live) setting."""
    import psycopg

    result: dict[str, Any] = {}
    domains = sorted(set(ol.resolved_domains(setup, resolved)))
    conn = psycopg.connect(
        host=ep.pgwire_host,
        port=ep.pgwire_port,
        user=ep.role,
        password="unused",
        dbname="provisa",
        autocommit=True,
    )
    try:
        audit = replication.audit_reader(conn, domains)
        baseline = audit.baseline()
        sent: list[request_mix.RequestSpec] = []
        for source in SOURCES:
            try:
                s = variant(raw, resolved, source, "pgwire")
            except contract_model.SetupError as exc:
                result[source] = {"status": "not exposed", "detail": str(exc)[:200]}
                continue
            gen = request_mix.RequestGenerator(
                s, "pgwire", f"proof/route/{source}", cacheable=False
            )
            renderer = request_render.Renderer(s)
            for _ in range(3):
                spec = gen.next()
                sql = renderer.render(spec).sql
                assert sql is not None
                conn.execute(sql).fetchall()  # type: ignore[arg-type]
                sent.append(spec)
        result["verification"] = replication.verify_routes(
            setup, replication.classes_of(setup, sent), audit, baseline
        )
    except Exception as exc:  # noqa: BLE001
        result["error"] = ol._brief(exc)  # noqa: SLF001
    finally:
        conn.close()
    return result


if __name__ == "__main__":
    raise SystemExit(main())
