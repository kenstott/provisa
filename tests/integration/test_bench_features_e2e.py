# Copyright (c) 2026 Kenneth Stott
# Canary: b74132fc-2225-4ddf-8d8c-9a77944b4cac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every benchmark feature, end to end, answering correctly before any measurement (REQ-1911,
REQ-2092).

The bench's own request generation (demo/named/perf/bench: request_mix's RequestGenerator, its
Renderer and optimistic_load's per-transport clients) is driven against a real stack: its own
compose project with the four sources (PostgreSQL, ClickHouse, MongoDB, Neo4j) on leased ports,
seeded small, and an isolated Provisa server on the perf demo's config -- the bench's local proof's
provisioning (local_proof.py), never the perf VMs, never the maintainer's local-dev instance.

For each knob the generator honours, and the governance, cache and join-replication knobs, a few
requests are sent over every transport that can spell them, and each answer is checked against the
expected rows read from the source itself (never through Provisa). A request carries no ORDER BY,
so its LIMIT names no fixed rows: an answer is correct when every row it returns is one of the
expected rows (a multiset), as many as the limit allows (min(limit, matching)), with the requested
columns; a join's every (parent, child) pair joins. A governance knob is also shown applied: the
rule's predicate in the governed SQL, the role the audit log records, the masked columns null.

Each request's latency is written to results/bench_features/latency.json, as data: nothing is
asserted of it.
"""

# Requirements: REQ-1911, REQ-2092

from __future__ import annotations

import argparse
import copy
import json
import os
import subprocess
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

pytestmark = [
    pytest.mark.integration,
    pytest.mark.e2e,
    pytest.mark.bench,
    pytest.mark.requires_clickhouse,
    pytest.mark.requires_mongodb,
    pytest.mark.requires_neo4j,
]

REPO = Path(__file__).resolve().parents[2]
BENCH = REPO / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))  # the bench's modules import each other by their own names

import contract_model as cm  # noqa: E402
import governance  # noqa: E402
import local_proof as lp  # noqa: E402
import optimistic_load as ol  # noqa: E402
import replication  # noqa: E402
import request_mix  # noqa: E402
import request_render  # noqa: E402
import setup_contract as sc  # noqa: E402

TRANSPORTS = tuple(ol.TRANSPORTS)  # all ten: graphql ... mcp
SOURCES = tuple(lp.SOURCES)
LATENCY_OUT = REPO / "results" / "bench_features" / "latency.json"
READER = "bench_feat_reader"  # the second contract role: a child of org_admin
PER_CELL = 3  # requests per (knob, transport[, source]) cell
# The roles the MCP server may act as over loopback (mcp.role, a pin): every role a request names --
# the contract's two, the governance knobs' deepest chain (role_depth 3) and derived roles (2).
MCP_ROLES = ",".join(
    [
        "org_admin",
        READER,
        *(cm.chain_role("org_admin", level) for level in (1, 2, 3)),
        *(f"{cm.derived_attribute(i)}__{cm.DERIVED_VALUE}" for i in (1, 2)),
    ]
)


# --------------------------------------------------------------------------------------------
# What the sources hold: the expected rows
# --------------------------------------------------------------------------------------------


def norm(value: Any) -> Any:
    """A value as answers and sources are compared: numbers as a rounded float (a gRPC int64 is a
    JSON string, a PostgreSQL numeric a Decimal), everything else as text."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        return round(float(value), 6)
    text = str(value)
    try:
        return round(float(text), 6)
    except ValueError:
        return text


def mongo_column_paths() -> dict[str, str]:
    """order_docs' registered columns, each to the document path it reads (its name when flat)."""
    config = yaml.safe_load((lp.PERF / "config.yaml").read_text())
    (table,) = [t for t in config["tables"] if t["table"] == "order_docs"]
    return {c["name"]: c.get("path") or c["name"] for c in table["columns"]}


def _at(doc: dict[str, Any], path: str) -> Any:
    value: Any = doc
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


class Oracle:
    """Every row of every contract table, read from its source once."""

    def __init__(self, ports: dict[str, int]) -> None:
        self.tables: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._read_postgres(ports["bench-postgresql"])
        self._read_clickhouse(ports["bench-clickhouse"])
        self._read_mongo(ports["bench-mongodb"])
        self._read_neo4j(ports["neo4j-http"])

    def _read_postgres(self, port: int) -> None:
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(
            host="127.0.0.1",
            port=port,
            user="provisa",
            password="provisa",
            dbname="provisa_bench",
            row_factory=dict_row,
        ) as conn:
            for table in ("orders", "order_items"):
                rows = conn.execute(f"SELECT * FROM public.{table}").fetchall()  # noqa: S608
                self.tables[("bench-postgresql", table)] = [dict(r) for r in rows]

    def _read_clickhouse(self, port: int) -> None:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host="127.0.0.1", port=port, username="default", password="provisa"
        )
        result = client.query("SELECT * FROM default.order_events")
        self.tables[("bench-clickhouse", "order_events")] = list(result.named_results())
        client.close()

    def _read_mongo(self, port: int) -> None:
        from pymongo import MongoClient

        # A registered column may read a nested field (shipping_carrier: path shipping.carrier):
        # each document is read as the perf demo's registration names its columns.
        paths = mongo_column_paths()
        client: MongoClient = MongoClient(host="127.0.0.1", port=port, directConnection=True)
        docs = client["provisa_bench"]["order_docs"].find({}, {"_id": 0})
        self.tables[("bench-mongodb", "order_docs")] = [
            {name: _at(d, path) for name, path in paths.items()} for d in docs
        ]
        client.close()

    def _read_neo4j(self, port: int) -> None:
        resp = httpx.post(
            f"http://127.0.0.1:{port}/db/neo4j/tx/commit",
            json={"statements": [{"statement": "MATCH (o:Order) RETURN properties(o) AS p"}]},
            timeout=120,
        )
        resp.raise_for_status()
        body = resp.json()
        assert not body["errors"], body["errors"]
        data = body["results"][0]["data"]
        self.tables[("bench-neo4j", "bench_order_node")] = [d["row"][0] for d in data]

    def rows(self, source: str, table: str) -> list[dict[str, Any]]:
        return self.tables[(source, table)]


# --------------------------------------------------------------------------------------------
# Checking an answer
# --------------------------------------------------------------------------------------------


def _key_map(table: cm.Table) -> dict[str, str]:
    """Each spelling of each column (its contract name and every language's) to its contract name."""
    out: dict[str, str] = {}
    for col in table.columns:
        out[col.name.lower()] = col.name
        for spelled in col.names.values():
            out[str(spelled).lower()] = col.name
    return out


def _projected(
    row: dict[str, Any], keys: dict[str, str], wanted: tuple[str, ...]
) -> tuple[Any, ...]:
    by_name = {keys[k.lower()]: v for k, v in row.items() if k.lower() in keys}
    missing = [c for c in wanted if c not in by_name]
    assert not missing, f"the answer has no {missing} (row {row})"
    return tuple(norm(by_name[c]) for c in wanted)


def expected_rows(
    oracle: Oracle, table: cm.Table, spec: request_mix.RequestSpec, masked: set[str]
) -> Counter:
    """The rows ``spec`` may answer, as a multiset of its columns' values; a masked column is null."""
    out: Counter = Counter()
    for row in oracle.rows(spec.source, spec.table):
        if all(norm(row.get(c)) == norm(v) for c, v in spec.filters):
            out[tuple(None if c in masked else norm(row.get(c)) for c in spec.columns)] += 1
    return out


def check_single(
    transport: str,
    table: cm.Table,
    spec: request_mix.RequestSpec,
    columns: list[str],
    rows: list[dict[str, Any]],
    oracle: Oracle,
    masked: set[str] = frozenset(),  # type: ignore[assignment]
) -> None:
    keys = _key_map(table)
    returned = {keys[c.lower()] for c in columns if c.lower() in keys}
    # An empty answer names no column on a transport that reads its columns off its rows (GraphQL,
    # Cypher, REST, JSON:API): the count check below still holds it to the expected rows.
    if rows or columns:
        if transport == "grpc":
            # gRPC answers every column of the row, whatever the read mask (optimistic_load.NOTES).
            assert set(spec.columns) <= returned, (columns, spec.columns)
        elif transport == "jsonapi":
            # a JSON:API resource always carries its id, the table's base column
            assert set(spec.columns) <= returned <= {*spec.columns, table.base_column}, (
                transport,
                columns,
                spec.columns,
            )
        else:
            assert returned == set(spec.columns), (transport, columns, spec.columns)
    expected = expected_rows(oracle, table, spec, masked)
    got = Counter(_projected(r, keys, spec.columns) for r in rows)
    extra = got - expected
    assert not extra, f"{transport}: rows not in {spec.source}.{spec.table}: {list(extra)[:3]}"
    assert sum(got.values()) == min(spec.rows, sum(expected.values())), (
        transport,
        sum(got.values()),
        spec.rows,
        sum(expected.values()),
    )


def check_join(
    transport: str,
    setup: cm.Setup,
    spec: request_mix.RequestSpec,
    rows: list[dict[str, Any]],
    oracle: Oracle,
) -> None:
    """One join step: every returned parent is a parent row the filters admit, and its child's
    base value is a child row whose join column equals some such parent's."""
    (step,) = spec.joins
    tables = {(sid, t.name): t for sid, s in setup.sources.items() for t in s.tables}
    parent_t = tables[(spec.source, spec.table)]
    child_t = tables[(step.child_source, step.child_table)]
    parents = [
        r
        for r in oracle.rows(spec.source, spec.table)
        if all(norm(r.get(c)) == norm(v) for c, v in spec.filters)
    ]
    children = oracle.rows(step.child_source, step.child_table)
    by_join: dict[Any, set[Any]] = {}
    for c in children:
        by_join.setdefault(norm(c.get(step.child_column)), set()).add(
            norm(c.get(child_t.base_column))
        )
    parent_keys = _key_map(parent_t)

    def joins(parent_values: tuple[Any, ...], child_base: Any) -> bool:
        for p in parents:
            if tuple(norm(p.get(c)) for c in spec.columns) == parent_values:
                if child_base in by_join.get(norm(p.get(step.parent_column)), set()):
                    return True
        return False

    pairs: list[tuple[tuple[Any, ...], Any]] = []
    child_alias = f"{child_t.name}_{child_t.base_column}".lower()
    for row in rows:
        own = _projected(row, parent_keys, spec.columns)
        nested = next((v for k, v in row.items() if k == step.graphql_field), None)
        if nested is not None or step.graphql_field in row:  # GraphQL: the child is nested
            kids = nested if isinstance(nested, list) else [nested] if nested else []
            child_keys = _key_map(child_t)
            for kid in kids:
                (base,) = _projected(kid, child_keys, (child_t.base_column,))
                pairs.append((own, base))
        else:
            base = next(norm(v) for k, v in row.items() if k.lower() == child_alias)
            pairs.append((own, base))
    bad = [p for p in pairs if not joins(*p)]
    assert not bad, f"{transport}: pairs that do not join: {bad[:3]}"
    assert len(rows) <= spec.rows, (transport, len(rows), spec.rows)
    assert pairs, f"{transport}: the join answered no pair"


# --------------------------------------------------------------------------------------------
# The stack
# --------------------------------------------------------------------------------------------


@dataclass
class Stack:
    server: Any
    raw: dict[str, Any]
    resolved: Any
    bound: cm.Setup
    ep: ol.Endpoints
    oracle: Oracle
    ports: dict[str, int]
    latency: list[dict[str, Any]] = field(default_factory=list)


_STACK_ENV = ("PG_HOST", "PG_PORT", "PG_USER", "PG_PASSWORD", "PROVISA_HTTP_BASE_URL")


@pytest.fixture(scope="module")
def stack() -> Iterator[Stack]:
    from tests.port_lease import lease_ports

    leased = lease_ports(6)
    ports = {
        "bench-postgresql": leased[0],
        "bench-clickhouse": leased[1],
        "bench-mongodb": leased[2],
        "neo4j-http": leased[3],
        "neo4j-bolt": leased[4],
        "mcp": leased[5],
    }
    project = f"provisa-benchfeat-{os.getpid()}"
    tmp = Path(tempfile.mkdtemp(prefix="benchfeat-"))
    compose = ["docker", "compose", "-p", project, "-f", str(lp.compose_file(ports, tmp))]
    saved = {k: os.environ.get(k) for k in _STACK_ENV}
    server = None
    built: Stack | None = None
    try:
        for service in ("postgresql", "mongodb", "clickhouse", "neo4j"):
            lp.run([*compose, "up", "-d", "--wait", service])
        lp.seed(ports)
        lp.control_plane_on_stack(ports)
        server = start_server(ports)
        raw = lp.proof_contract(server, project)
        raw["deployment"]["roles"] = [{"role": "org_admin", "weight": 1.0}]
        raw["deployment"]["endpoints"]["mcp"]["url"] = f"http://127.0.0.1:{ports['mcp']}/mcp"
        contract = tmp / "contract.yaml"
        contract.write_text(yaml.safe_dump(raw, sort_keys=False))
        os.environ["PROVISA_HTTP_BASE_URL"] = server.base_url
        unbound = sc.load_setup(contract, known_transports=list(TRANSPORTS))
        resolved = ol.resolve_names(
            argparse.Namespace(setup=str(contract), resolved_names=None), unbound, tmp
        )
        bound = sc.load_setup(
            contract,
            known_transports=list(TRANSPORTS),
            deployment=resolved,
            verify_replication=False,
        )
        built = Stack(
            server, raw, resolved, bound, ol.endpoints_from_setup(bound), Oracle(ports), ports
        )
        yield built
    finally:
        if built is not None:
            LATENCY_OUT.parent.mkdir(parents=True, exist_ok=True)
            LATENCY_OUT.write_text(json.dumps(built.latency, indent=1))
        if server is not None:
            server.stop_process()
        subprocess.run([*compose, "down", "-v", "--remove-orphans"], check=False)
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def start_server(ports: dict[str, int], **env: str) -> Any:
    """The isolated server on the perf demo's config over the stack's sources, every transport on,
    MCP on its own leased port (it is opt-in: PROVISA_MCP_PORT)."""
    server = lp._proof_server_class()(  # noqa: SLF001
        f"benchfeat{os.getpid()}{len(env)}",
        engine="duckdb",
        control_plane="own",
        enable_pgwire=True,
        enable_bolt=True,
        await_flight=True,
        await_grpc=True,
        config=str(lp.PERF / "config.yaml"),
        env={
            **lp._source_env(ports),  # noqa: SLF001
            "PROVISA_MCP_PORT": str(ports["mcp"]),
            # loopback: the harness's pinned roles apply to a call with no bearer (mcp_pin); off
            # loopback a bearer is required
            "PROVISA_MCP_HOST": "127.0.0.1",
            "PROVISA_MCP_ROLE": MCP_ROLES,
            **env,
        },
    )
    server.start(timeout=600)
    deadline = time.monotonic() + 120
    while True:  # the MCP server binds beside the app, after the app answers
        try:
            httpx.get(f"http://127.0.0.1:{ports['mcp']}/mcp", timeout=5)
            break
        except httpx.TransportError:
            assert time.monotonic() < deadline, "the MCP server did not come up"
            time.sleep(1)
    return server


def variant(
    stack: Stack,
    transport: str,
    *,
    weights: dict[str, float] | None = None,
    knobs: dict[str, Any] | None = None,
    raw_edit: Any = None,
) -> cm.Setup:
    """The contract with one transport, the source weights given (else all on PostgreSQL), no
    request opted into the response cache, each named distribution knob a constant, and
    ``raw_edit`` applied to the rest."""
    c = copy.deepcopy(stack.raw)
    k = c["knobs"]
    k["source_weights"] = weights or {s: 1.0 if s == "bench-postgresql" else 0.0 for s in SOURCES}
    c["transports"] = {transport: {}}
    # the contract opts every request into the response cache; a test that measures it opts in
    k["cache"] = {"probability": 0.0}
    for name, value in (knobs or {}).items():
        k[name] = {"probability": 1, "distribution": {"kind": "constant", "value": value}}
    if raw_edit is not None:
        raw_edit(c)
    return sc.setup_from_dict(
        c,
        environ=os.environ,
        known_transports=list(TRANSPORTS),
        deployment=stack.resolved,
        verify_replication=False,
    )


def specs(setup: cm.Setup, transport: str, stream: str, n: int = PER_CELL) -> Iterator[Any]:
    """``n`` (spec, rendered query) the bench would send: its own generator and renderer."""
    gen = request_mix.RequestGenerator(
        setup, transport, stream, cacheable=ol.TRANSPORTS[transport][1] is not None
    )
    renderer = request_render.Renderer(setup)
    for _ in range(n):
        spec = gen.next()
        yield spec, renderer.render(spec)


def send(
    stack: Stack, setup: cm.Setup, transport: str, query: Any, role: str, cell: str
) -> tuple[list[str], list[dict[str, Any]], bool | None]:
    ep = ol.Endpoints(**{**stack.ep.__dict__, "setup": setup})
    client = ol.CLIENTS[transport](ep)
    t0 = time.perf_counter()
    try:
        answer = client.rows(query, role)
    except httpx.HTTPStatusError as exc:
        raise AssertionError(
            f"{transport}: {exc.response.status_code} {exc.response.text[:600]}"
        ) from exc
    finally:
        client.close()
    stack.latency.append(
        {"cell": cell, "transport": transport, "ms": round((time.perf_counter() - t0) * 1e3, 2)}
    )
    return answer


def table_of(setup: cm.Setup, spec: request_mix.RequestSpec) -> cm.Table:
    return next(t for t in setup.sources[spec.source].tables if t.name == spec.table)


def spellable(query: Any, transport: str) -> bool:
    return getattr(query, cm.TRANSPORT_LANGUAGE[transport]) is not None


def run_cell(
    stack: Stack,
    transport: str,
    cell: str,
    setup: cm.Setup,
    *,
    role: str | None = None,
    masked: Mapping[str, list[str]] | None = None,
) -> int:
    """Send ``cell``'s requests and check each; returns how many were sent (0: none spellable)."""
    sent = 0
    for spec, query in specs(setup, transport, f"features/{cell}/{transport}"):
        if not spellable(query, transport):
            continue
        # the spec's role is already what it is sent as (RequestGenerator: request_role)
        as_role = role or spec.role
        columns, rows, _hit = send(stack, setup, transport, query, as_role, cell)
        if spec.joins:
            check_join(transport, setup, spec, rows, stack.oracle)
        else:
            table = table_of(setup, spec)
            # the masks are the table's own (AdminGovernance reports them by table name)
            own = set((masked or {}).get(table.name, ()))
            check_single(transport, table, spec, columns, rows, stack.oracle, own)
        sent += 1
    return sent


# --------------------------------------------------------------------------------------------
# fields, filters, rows; source_weights and tables.weight
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize(
    "shape",
    [
        {"fields": 1},
        {"fields": 3},
        {"filters": 1},
        {"filters": 2, "fields": 2},
        {"rows": 1},
        {"rows": 25},
    ],
    ids=["fields1", "fields3", "filters1", "filters2", "rows1", "rows25"],
)
def test_fields_filters_rows_answer_the_expected_rows(stack, transport, shape):
    cell = "+".join(f"{k}{v}" for k, v in shape.items())
    setup = variant(stack, transport, knobs=shape)
    assert run_cell(stack, transport, cell, setup) > 0


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_each_source_answers_over_each_transport_that_spells_it(stack, transport):
    """source_weights: each source alone. A source whose table the transport's language has no
    spelling for is not sent (the contract refuses that combination); every other one answers."""
    lang = cm.TRANSPORT_LANGUAGE[transport]
    spelled = [
        sid
        for sid, src in stack.bound.sources.items()
        if any(t.has(lang) and t.weight > 0 for t in src.tables)
    ]
    assert spelled, f"{transport} spells no source's table"
    for source in spelled:
        weights = {s: 1.0 if s == source else 0.0 for s in SOURCES}
        setup = variant(stack, transport, weights=weights, knobs={"fields": 2, "filters": 1})
        assert run_cell(stack, transport, f"source:{source}", setup) > 0, source


def _neo4j_filtered_on_customer_only(c: dict[str, Any]) -> None:
    for t in c["sources"]["bench-neo4j"]["tables"]:
        for col in t["columns"]:
            col.pop("domain", None)
            if col["name"] == "customer_id":
                col["domain"] = {"kind": "int_range", "min": 1, "max": 250000}


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_a_non_key_filter_on_a_row_level_neo4j_table_is_refused_by_name(stack, transport):
    """REQ-1915: bench_order_node is replicated row by row and Neo4j has no direct driver to apply
    a filter on a column other than the key, so such a read is refused with the reason, on every
    transport that spells the table -- never answered from the rows replicated so far."""
    table = next(t for t in stack.bound.sources["bench-neo4j"].tables)
    if not table.has(cm.TRANSPORT_LANGUAGE[transport]):
        return  # the contract sends nothing there (test_each_source_...)
    weights = {s: 1.0 if s == "bench-neo4j" else 0.0 for s in SOURCES}
    setup = variant(
        stack,
        transport,
        weights=weights,
        knobs={"filters": 1},
        raw_edit=_neo4j_filtered_on_customer_only,
    )
    sent = 0
    for spec, query in specs(setup, transport, f"features/row-level-refusal/{transport}"):
        assert [f[0] for f in spec.filters] == ["customer_id"]
        # each transport reports it in its own error type; the message is the same everywhere
        with pytest.raises(Exception, match="cannot apply this filter itself"):
            send(stack, setup, transport, query, spec.role, "row-level-refusal")
        sent += 1
    assert sent > 0


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_each_table_answers_by_its_weight(stack, transport):
    """tables.weight: order_items (weight 0 in the contract) read alone, its weight 1."""

    def only_items(c: dict[str, Any]) -> None:
        for t in c["sources"]["bench-postgresql"]["tables"]:
            t["weight"] = 1.0 if t["table"] == "order_items" else 0.0

    setup = variant(stack, transport, knobs={"fields": 2}, raw_edit=only_items)
    for spec, _ in specs(setup, transport, f"features/tables/{transport}"):
        assert spec.table == "order_items"
    assert run_cell(stack, transport, "tables.weight:order_items", setup) > 0


# --------------------------------------------------------------------------------------------
# joins, route, join replication
# --------------------------------------------------------------------------------------------


_CROSS = {
    "bench-postgresql": 0.5,
    "bench-clickhouse": 0.25,
    "bench-mongodb": 0.25,
    "bench-neo4j": 0,
}


@pytest.mark.parametrize("transport", cm.JOIN_TRANSPORTS)
@pytest.mark.parametrize("knob", ["joins_same_source", "joins_cross_source"])
def test_joins_answer_pairs_that_join(stack, transport, knob):
    weights = _CROSS if knob == "joins_cross_source" else None
    setup = variant(stack, transport, weights=weights, knobs={knob: 1, "fields": 2})
    assert run_cell(stack, transport, knob, setup) > 0


def _audit_engine(stack: Stack) -> Any:
    from provisa.core.database import create_engine_from_url

    return create_engine_from_url(stack.server.plane.url)


def _audit_baseline(stack: Stack) -> int:
    import sqlalchemy as sa

    engine = _audit_engine(stack)
    try:
        with engine.connect() as conn:
            return int(
                conn.execute(
                    sa.text(
                        f"SELECT COALESCE(MAX(id), 0) FROM org_{stack.server.org_id}.query_audit_log"
                    )
                ).scalar_one()
            )
    finally:
        engine.dispose()


def _audit_since(stack: Stack, baseline: int, at_least: int) -> list[dict[str, Any]]:
    """The audit rows the server wrote after ``baseline`` (the statement's role, route, transport
    and what was enforced on it), waiting for the writer (it inserts in batches) until there are
    ``at_least``."""
    import sqlalchemy as sa

    engine = _audit_engine(stack)
    query = sa.text(
        f"SELECT id, role_id, route, source, status_code, enforced "
        f"FROM org_{stack.server.org_id}.query_audit_log WHERE id > :b ORDER BY id"
    )
    deadline = time.monotonic() + 60
    try:
        while True:
            with engine.connect() as conn:
                rows = [dict(r._mapping) for r in conn.execute(query, {"b": baseline})]
            if len(rows) >= at_least or time.monotonic() > deadline:
                return rows
            time.sleep(0.5)
    finally:
        engine.dispose()


@pytest.mark.parametrize("transport", ("data_sql", "graphql", "cypher_http", "pgwire"))
@pytest.mark.parametrize("mode", ["direct", "federated"])
def test_a_route_hint_answers_the_same_rows_and_is_the_route_taken(stack, transport, mode):
    def route(c: dict[str, Any]) -> None:
        c["knobs"]["route"]["bench-postgresql"] = {
            "mode": mode,
            "federated_probability": 1.0 if mode == "federated" else 0.0,
        }

    setup = variant(stack, transport, knobs={"fields": 2}, raw_edit=route)
    baseline = _audit_baseline(stack)
    sent = run_cell(stack, transport, f"route:{mode}", setup)
    assert sent > 0
    routes = {r["route"] for r in _audit_since(stack, baseline, sent) if r["status_code"] < 400}
    assert routes == {"engine" if mode == "federated" else "direct"}, routes


@pytest.mark.parametrize("transport", cm.JOIN_TRANSPORTS)
def test_join_replication_answers_the_same_pairs(stack, transport):
    def replicated(c: dict[str, Any]) -> None:
        # both ends of every cross-source join read from replicas, and only such joins drawn
        for sid in ("bench-postgresql", "bench-clickhouse", "bench-mongodb"):
            c["sources"][sid]["replication"] = {"setting": "replica", "ttl_seconds": 600}
        c["knobs"]["join_replication"] = {"probability": 1, "weights": {"both_replica": 1.0}}

    setup = variant(
        stack,
        transport,
        weights=_CROSS,
        knobs={"joins_cross_source": 1, "fields": 2},
        raw_edit=replicated,
    )
    with httpx.Client(
        base_url=stack.server.base_url, timeout=600, headers={"X-Provisa-Role": stack.ep.role}
    ) as client:
        with replication.AdminReplication(client, stack.ep.role).applied(setup, stack.resolved):
            # a replica is built when the model declares it; a read before then is answered that
            # it is still being built (REQ-1915), so the cell waits for the builds
            deadline = time.monotonic() + 600
            while True:
                try:
                    sent = run_cell(stack, transport, "join_replication", setup)
                    break
                except Exception as exc:  # noqa: BLE001 - re-raised unless it is the build wait
                    text = str(getattr(getattr(exc, "response", None), "text", "")) + str(exc)
                    if "being built" not in text or time.monotonic() > deadline:
                        raise
                    time.sleep(5)
            assert sent > 0


# --------------------------------------------------------------------------------------------
# cache, cache_repetition
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "transport",
    [t for t in TRANSPORTS if ol.TRANSPORTS[t][1] is not None and t not in ("rest", "jsonapi")],
)
def test_a_cached_request_answers_the_same_rows_again(stack, transport):
    def cached(c: dict[str, Any]) -> None:
        c["knobs"]["cache"] = {"probability": 1.0}

    setup = variant(
        stack, transport, knobs={"fields": 2, "filters": 1, "cache_repetition": 1}, raw_edit=cached
    )
    for spec, query in specs(setup, transport, f"features/cache/{transport}", n=2):
        if not spellable(query, transport):
            continue
        assert spec.cached
        role = spec.role
        first = send(stack, setup, transport, query, role, "cache:first")
        again = send(stack, setup, transport, query, role, "cache:again")
        table = table_of(setup, spec)
        check_single(transport, table, spec, first[0], first[1], stack.oracle)
        keys = _key_map(table)
        assert Counter(_projected(r, keys, spec.columns) for r in again[1]) == Counter(
            _projected(r, keys, spec.columns) for r in first[1]
        )
        if again[2] is not None:  # the transport says whether it was a hit
            assert again[2] is True, f"{transport}: the repeat was not served from the cache"


# --------------------------------------------------------------------------------------------
# deployment.roles, load.mode
# --------------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def reader(stack) -> str:
    """The second contract role: a child of org_admin, so it reads what org_admin reads."""
    with httpx.Client(base_url=stack.server.base_url, timeout=60) as client:
        gov = governance.AdminGovernance(client, stack.ep.role)
        top = gov._roles()["org_admin"]  # noqa: SLF001 - the bench's own admin client
        gov._mutate(  # noqa: SLF001
            "createRole",
            "$input: RoleInput!",
            "input: $input",
            {
                "input": {
                    "id": READER,
                    "description": "The features test's second reader.",
                    "capabilities": list(top["capabilities"]),
                    "domainAccess": list(top["domainAccess"]),
                    "parentRoleId": "org_admin",
                }
            },
        )
    return READER


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_each_role_answers_and_the_audit_log_names_it(stack, reader, transport):
    def roles(c: dict[str, Any]) -> None:
        c["deployment"]["roles"] = [
            {"role": "org_admin", "weight": 0.5},
            {"role": reader, "weight": 0.5},
        ]

    setup = variant(stack, transport, knobs={"fields": 2}, raw_edit=roles)
    baseline = _audit_baseline(stack)
    sent_roles = []
    for spec, query in specs(setup, transport, f"features/roles/{transport}", n=6):
        if not spellable(query, transport):
            continue
        assert spec.role in ("org_admin", reader)
        columns, rows, _ = send(stack, setup, transport, query, spec.role, "deployment.roles")
        check_single(transport, table_of(setup, spec), spec, columns, rows, stack.oracle)
        sent_roles.append(spec.role)
    assert set(sent_roles) == {"org_admin", reader}, sent_roles
    recorded = {r["role_id"] for r in _audit_since(stack, baseline, len(sent_roles))}
    assert {"org_admin", reader} <= recorded, recorded


@pytest.mark.parametrize("mode", ["closed_loop", "open_loop"])
@pytest.mark.parametrize("transport", ("data_sql", "graphql"))
def test_load_mode_runs_a_step_with_no_failed_request(stack, transport, mode):
    def load(c: dict[str, Any]) -> None:
        c["load"]["mode"] = mode
        if mode == "open_loop":
            # one step: the offered rate over this many connections (contract_parse)
            c["load"]["connections"] = c["load"].pop("steps")[0]
            c["load"]["rate_per_s"] = 20

    setup = variant(stack, transport, knobs={"fields": 2, "filters": 1}, raw_edit=load)
    ep = ol.Endpoints(**{**stack.ep.__dict__, "setup": setup})
    step = ol.run_step(transport, ep, 2, 3.0, 2, None)
    stack.latency.append({"cell": f"load.mode:{mode}", "transport": transport, "step": step})
    assert "error" not in step, step
    assert step["errors"] == 0, step.get("first_error")
    assert step["requests"] > 0, step


# --------------------------------------------------------------------------------------------
# governance
# --------------------------------------------------------------------------------------------


def _governed(stack: Stack, transport: str, g: dict[str, Any], **knobs: Any) -> cm.Setup:
    def gov(c: dict[str, Any]) -> None:
        c["knobs"]["governance"] = {**c["knobs"]["governance"], **g}

    return variant(stack, transport, knobs=knobs or {"fields": 2}, raw_edit=gov)


def _table_ids(stack: Stack, setup: cm.Setup) -> dict[str, str]:
    """Each contract table's registered id, as the audit's ``enforced`` keys it (text)."""
    return {
        t.name: str(stack.resolved.tables[sc.table_identity(setup, sid, t).key].table_id)
        for sid, src in setup.sources.items()
        for t in src.tables
    }


# Each knob is set up once, its transports run under it (the fixture is module-scoped and
# parametrized, so pytest groups the tests by knob): setting one up rebuilds the model.
GOVERNANCE = ("row_rules", "masks", "redaction", "role_depth", "derived_roles")


@dataclass
class Governed:
    knob: str
    knobs: dict[str, Any]  # the governance knob values
    request_knobs: dict[str, Any]  # the request shape each transport is sent
    report: dict[str, Any]  # what AdminGovernance set up
    client: httpx.Client


def _orders_columns(stack: Stack) -> int:
    """How many columns orders is registered with (the share masks a count of these)."""
    tid = _table_ids(stack, stack.bound)["orders"]
    with httpx.Client(base_url=stack.server.base_url, timeout=60) as client:
        gov = governance.AdminGovernance(client, stack.ep.role)
        return len(gov._tables()[int(tid)]["columns"])  # noqa: SLF001 - the bench's own client


@pytest.fixture(scope="module", params=GOVERNANCE)
def governed(stack, request) -> Iterator[Governed]:
    knob = request.param
    request_knobs: dict[str, Any] = {"fields": 2}
    if knob == "row_rules":
        g: dict[str, Any] = {"row_rules_per_table": 2}
    elif knob == "masks":
        # one masked column of orders (its only nullable one no filter, join or base request
        # reads), every column requested so it is in each answer
        g = {"masked_column_share": 1 / _orders_columns(stack)}
        request_knobs = {
            "fields": next(
                len(t["columns"])
                for t in stack.raw["sources"]["bench-postgresql"]["tables"]
                if t["table"] == "orders"
            )
        }
    elif knob == "redaction":
        g = {"central_redaction": True}
    elif knob == "role_depth":
        g = {"role_depth": 3}
    else:
        g = {"derived_roles": 2}
    setup = _governed(stack, "data_sql", g, **request_knobs)
    with httpx.Client(base_url=stack.server.base_url, timeout=120) as client:
        with governance.AdminGovernance(client, stack.ep.role).applied(
            setup, "data_sql", stack.resolved
        ) as report:
            yield Governed(knob, g, request_knobs, report, client)


def _enforced(row: dict[str, Any]) -> dict[str, Any]:
    e = row["enforced"]
    return json.loads(e) if isinstance(e, str) else (e or {})


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_under_each_governance_knob_every_transport_answers_and_it_is_applied(
    stack, governed, transport
):
    """Each answer is the expected rows (a masked column null); and the knob was applied: the
    row rules and masks are what the audit records as enforced, the redaction rules are the org's,
    the deepest role and the derived roles are the ones the request acted as."""
    setup = _governed(stack, transport, governed.knobs, **governed.request_knobs)
    masked: dict[str, list[str]] = governed.report.get("masked_columns", {})
    request_role = setup.request_role(transport, stack.ep.role)
    if governed.knob == "redaction":
        rules = governed.client.get(
            "/admin/redaction", headers={"X-Provisa-Role": stack.ep.role}
        ).json()
        names = {r["name"] for r in rules["org"]}
        assert {r["name"] for r in governance.BENCH_REDACTION_RULES} <= names, names
    baseline = _audit_baseline(stack)
    sent = run_cell(stack, transport, f"governance.{governed.knob}", setup, masked=masked)
    assert sent > 0
    rows = [r for r in _audit_since(stack, baseline, sent) if r["status_code"] < 400]
    assert len(rows) >= sent, (sent, rows)
    orders = _table_ids(stack, setup)["orders"]
    for row in rows:
        enforced = _enforced(row)
        if governed.knob == "row_rules":
            # the tag rules (``<base column> IS NOT NULL``, two ANDed) are what was enforced
            rule = enforced["row_filters"].get(orders, {}).get("filter", "")
            assert rule.upper().count("IS NOT NULL") >= 2, (transport, rule)
        elif governed.knob == "masks":
            assert masked.get("orders"), governed.report
            assert set(masked["orders"]) <= set(enforced["masks"].get(orders, {})), enforced
        elif governed.knob == "role_depth":
            assert row["role_id"] == cm.chain_role(stack.ep.role, 3), (transport, row["role_id"])
        elif governed.knob == "derived_roles":
            attrs = [cm.derived_attribute(i) for i in (1, 2)]
            assert all(a in request_role for a in attrs), request_role
            evaluated = f"{row['role_id']} {' '.join(enforced.get('acting_roles', []))}"
            assert all(a in evaluated for a in attrs), (transport, evaluated)


# --------------------------------------------------------------------------------------------
# telemetry: a second server, exporting to the bench's no-op receiver
# --------------------------------------------------------------------------------------------


def test_with_telemetry_on_every_transport_answers_and_spans_arrive(stack):
    from tests.port_lease import lease_ports

    otlp, mcp = lease_ports(2)
    receiver = subprocess.Popen(  # noqa: S603 - the bench's own receiver
        [sys.executable, str(BENCH / "noop_otlp_receiver.py"), str(otlp), "--tally"],
    )
    server = None
    try:
        server = start_server(
            {**stack.ports, "mcp": mcp},
            OTEL_SDK_DISABLED="false",
            OTEL_EXPORTER_OTLP_ENDPOINT=f"http://127.0.0.1:{otlp}",
        )
        tel = copy.copy(stack)
        tel.server = server
        tel.latency = []
        tel.ep = ol.Endpoints(
            **{
                **stack.ep.__dict__,
                "http_base_url": server.base_url,
                "pgwire_port": server.pgwire_port,
                "bolt_port": server.bolt_port,
                "flight_port": server.flight_port,
                "grpc_port": server.grpc_port,
                "mcp_url": f"http://127.0.0.1:{mcp}/mcp",
            }
        )
        for transport in TRANSPORTS:
            setup = variant(tel, transport, knobs={"fields": 2})
            assert run_cell(tel, transport, "governance.telemetry", setup) > 0, transport
        stack.latency.extend(tel.latency)
        deadline = time.monotonic() + 60
        while True:
            stages = httpx.get(f"http://127.0.0.1:{otlp}/stages", timeout=10).json()
            if stages:
                break
            assert time.monotonic() < deadline, "no span reached the receiver"
            time.sleep(1)
    finally:
        if server is not None:
            server.stop_process()
        receiver.terminate()
        receiver.wait(10)
