# Copyright (c) 2026 Kenneth Stott
# Canary: d8466828-fff9-477f-9656-1dfb28e60d82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: the replication setting a run is measured under (live | replica), and the proof that
requests were served the way it says.

FIXTURE: the admin API, the deployment's answers and the audit log are fakes here; the real ones
are proven by the local integration run."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import contract_model  # noqa: E402
import deployment_fixture as fx  # noqa: E402
import lookup  # noqa: E402
import optimistic_load as ol  # noqa: E402
import replication as rp  # noqa: E402
import request_mix  # noqa: E402
import setup_contract as sc  # noqa: E402

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001"}
TRANSPORTS = list(ol.TRANSPORTS)


def _raw() -> dict[str, Any]:
    return json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))


def _setup(
    mutate: Any = None, deployment: Any = None, tmp_path: Path | None = None, verify: bool = False
) -> contract_model.Setup:
    raw = _raw()
    if mutate:
        mutate(raw)
    return sc.setup_from_dict(
        raw,
        environ=ENV,
        known_transports=TRANSPORTS,
        deployment=deployment or fx.build(),
        verify_replication=verify,
    )


def _declare(**settings: tuple[str, int]) -> Any:
    def mutate(raw: dict[str, Any]) -> None:
        for source, (setting, ttl) in settings.items():
            raw["sources"][f"bench-{source}"]["replication"] = {
                "setting": setting,
                "ttl_seconds": ttl,
            }

    return mutate


# ------------------------------------------------------------------ the contract


def test_the_perf_contract_declares_what_it_is_measured_under() -> None:
    setup = _setup()
    assert {
        s: (src.replication.setting, src.replication.ttl_seconds)
        for s, src in setup.sources.items()
    } == {
        "bench-postgresql": ("live", 0),
        "bench-clickhouse": ("replica", 300),
        "bench-mongodb": ("replica", 300),
        "bench-neo4j": ("replica", 300),
    }


def test_replication_is_required_per_source() -> None:
    def drop(raw: dict[str, Any]) -> None:
        del raw["sources"]["bench-postgresql"]["replication"]

    with pytest.raises(
        contract_model.SetupError, match="sources.bench-postgresql.replication: required"
    ):
        _setup(drop)


@pytest.mark.parametrize(
    "decl,msg",
    [
        ({"setting": "materialized", "ttl_seconds": 0}, "setting: must be one of live, replica"),
        ({"setting": "replica", "ttl_seconds": 0}, "a replica needs its refresh TTL"),
        ({"setting": "live", "ttl_seconds": 30}, "a live read has no replica TTL"),
        ({"setting": "live"}, "ttl_seconds: required"),
    ],
)
def test_replication_values(decl: dict, msg: str) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["sources"]["bench-postgresql"]["replication"] = decl

    with pytest.raises(contract_model.SetupError, match=msg):
        _setup(mutate)


def test_a_table_may_override_its_sources_setting() -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["sources"]["bench-postgresql"]["tables"][1]["replication"] = {
            "setting": "replica",
            "ttl_seconds": 60,
        }

    setup = _setup(mutate)
    orders, items = setup.sources["bench-postgresql"].tables
    assert rp.effective(setup, "bench-postgresql", orders).setting == "live"
    assert rp.effective(setup, "bench-postgresql", items) == contract_model.Replication(
        "replica", 60
    )


def test_the_old_landed_flag_is_gone() -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["sources"]["bench-postgresql"]["landed"] = True

    with pytest.raises(contract_model.SetupError, match="unknown key 'landed'"):
        _setup(mutate)


# ------------------------------------------------------------------ declared vs what the deployment has


def test_the_perf_contract_matches_the_perf_deployment() -> None:
    setup = _setup()
    resolved = lookup.resolve(*sc.identities(setup), fx.build())
    assert rp.mismatches(setup, resolved) == []


def test_a_declared_replica_the_deployment_reads_live_is_a_mismatch() -> None:
    setup = _setup(_declare(postgresql=("replica", 300)))
    resolved = lookup.resolve(*sc.identities(setup), fx.build())
    first, second = rp.mismatches(setup, resolved)  # orders and order_items both read live
    assert (
        "bench-postgresql/public.orders" in first
        and "declared replica" in first
        and "reads it live" in first
    )
    assert "bench-postgresql/public.order_items" in second


def test_a_declared_live_the_deployment_replicates_is_a_mismatch() -> None:
    setup = _setup(_declare(clickhouse=("live", 0)))
    resolved = lookup.resolve(*sc.identities(setup), fx.build())
    (m,) = rp.mismatches(setup, resolved)
    assert "bench-clickhouse/default.order_events" in m and "declared live" in m


def test_a_declared_ttl_that_is_not_the_deployments_is_a_mismatch() -> None:
    setup = _setup(_declare(clickhouse=("replica", 60)))
    resolved = lookup.resolve(*sc.identities(setup), fx.build())
    (m,) = rp.mismatches(setup, resolved)
    assert "TTL 60" in m and "deployment's is 300" in m


def test_a_table_value_beats_its_sources() -> None:
    tables = [dict(t) for t in fx.TABLES]
    tables[0]["prefer_materialized"] = True  # orders replicated although its source is not
    setup = _setup(
        lambda raw: raw["sources"]["bench-postgresql"]["tables"][0].update(
            replication={"setting": "replica", "ttl_seconds": 300}
        ),
        deployment=fx.build(tables=tables),
    )
    raw = fx.build(tables=tables)
    raw.admin["data"]["tables"][0]["cacheTtl"] = 300
    assert rp.mismatches(setup, lookup.resolve(*sc.identities(setup), raw)) == []


def test_binding_refuses_a_contract_the_deployment_does_not_have() -> None:
    with pytest.raises(contract_model.SetupError, match="declared replica"):
        _setup(_declare(postgresql=("replica", 300)), verify=True)


def test_applying_replication_skips_that_check_because_the_run_sets_it() -> None:
    assert _setup(_declare(postgresql=("replica", 300)), verify=False)


# ------------------------------------------------------------------ applying it through the admin API


class FakeAdmin:
    """An admin GraphQL endpoint holding the replication settings of the perf stack."""

    def __init__(
        self,
        mutations: tuple[str, ...] = (
            "updateSourcePreferMaterialized",
            "updateTablePreferMaterialized",
            "updateSourceCache",
            "updateTableCache",
        ),
    ) -> None:
        self.sources = {
            s["id"]: {
                "prefer": s["preferMaterialized"],
                "cache_enabled": True,
                "ttl": s["cacheTtl"],
            }
            for s in fx.SOURCES
        }
        self.tables = {
            t["id"]: {"prefer": t["prefer_materialized"], "ttl": t["cache_ttl"]} for t in fx.TABLES
        }
        self.mutations = mutations
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.fail_on: str | None = None

    def post(self, path: str, json: dict[str, Any], headers: dict[str, str]) -> Any:
        import httpx

        req = httpx.Request("POST", "http://x/admin/graphql")
        assert path == "/admin/graphql" and headers["X-Provisa-Role"]
        query, variables = json["query"], json.get("variables") or {}
        if "__schema" in query:
            body = {
                "data": {
                    "__schema": {"mutationType": {"fields": [{"name": m} for m in self.mutations]}}
                }
            }
        else:
            name = next(m for m in self.mutations if m in query)
            if name == self.fail_on:
                return httpx.Response(
                    200,
                    json={"data": {name: {"success": False, "message": "boom", "code": "x"}}},
                    request=req,
                )
            self.calls.append((name, variables))
            if name == "updateSourcePreferMaterialized":
                self.sources[variables["sourceId"]]["prefer"] = variables["value"]
            elif name == "updateTablePreferMaterialized":
                self.tables[variables["tableId"]]["prefer"] = variables["value"]
            elif name == "updateSourceCache":
                self.sources[variables["sourceId"]].update(
                    cache_enabled=variables["enabled"], ttl=variables["ttl"]
                )
            elif name == "updateTableCache":
                self.tables[variables["tableId"]]["ttl"] = variables["ttl"]
            body = {"data": {name: {"success": True, "message": "ok", "code": "ok"}}}
        return httpx.Response(200, json=body, request=req)


def _applier(admin: FakeAdmin) -> rp.AdminReplication:
    return rp.AdminReplication(admin, role="org_admin")


def _resolved(setup: contract_model.Setup) -> lookup.Resolved:
    return lookup.resolve(*sc.identities(setup), fx.build())


def test_apply_sets_each_declared_source_and_restores_it_afterwards() -> None:
    setup = _setup(_declare(postgresql=("replica", 120), clickhouse=("live", 0)))
    resolved, admin = _resolved(setup), FakeAdmin()
    before = (dict(admin.sources), dict(admin.tables))
    with _applier(admin).applied(setup, resolved):
        assert (
            admin.sources["bench-postgresql"]["prefer"] is True
            and admin.sources["bench-postgresql"]["ttl"] == 120
        )
        assert admin.sources["bench-clickhouse"]["prefer"] is False
    assert admin.sources["bench-postgresql"] == before[0]["bench-postgresql"]
    assert admin.sources["bench-clickhouse"] == before[0]["bench-clickhouse"]
    assert admin.tables == before[1]


def test_apply_restores_on_failure_inside_the_run() -> None:
    setup = _setup(_declare(postgresql=("replica", 120)))
    admin = FakeAdmin()
    with pytest.raises(RuntimeError, match="the run failed"):
        with _applier(admin).applied(setup, _resolved(setup)):
            raise RuntimeError("the run failed")
    assert (
        admin.sources["bench-postgresql"]["prefer"] is False
        and admin.sources["bench-postgresql"]["ttl"] is None
    )


def test_apply_restores_what_it_changed_when_a_later_change_fails() -> None:
    setup = _setup(_declare(postgresql=("replica", 120), clickhouse=("live", 0)))
    admin = FakeAdmin()
    admin.fail_on = "updateSourceCache"  # the TTL of the first source cannot be set
    with pytest.raises(rp.ReplicationError, match="updateSourceCache failed: boom"):
        with _applier(admin).applied(setup, _resolved(setup)):
            pass
    assert (
        admin.sources["bench-postgresql"]["prefer"] is False
        and admin.sources["bench-clickhouse"]["prefer"] is True
    )


def test_a_table_level_declaration_sets_the_table() -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["sources"]["bench-postgresql"]["tables"][1]["replication"] = {
            "setting": "replica",
            "ttl_seconds": 60,
        }

    setup, admin = _setup(mutate), FakeAdmin()
    with _applier(admin).applied(setup, _resolved(setup)):
        assert admin.tables[2]["prefer"] is True and admin.tables[2]["ttl"] == 60
    assert admin.tables[2] == {"prefer": None, "ttl": None}


def test_a_renamed_admin_mutation_is_named_in_the_error() -> None:
    setup = _setup()
    admin = FakeAdmin(mutations=("updateSourceReplicate",))
    with pytest.raises(rp.ReplicationError, match="no updateSourcePreferMaterialized mutation"):
        with _applier(admin).applied(setup, _resolved(setup)):
            pass


def test_without_route_verification_a_deployment_not_as_declared_is_refused() -> None:
    setup = _setup(_declare(postgresql=("replica", 300)))
    with pytest.raises(sc.SetupError, match="--apply-replication"):
        rp.require_declared_or_apply(setup, _resolved(setup), apply=False, routes_verified=False)
    assert (
        rp.require_declared_or_apply(setup, _resolved(setup), apply=True, routes_verified=False)
        == []
    )
    assert (
        rp.require_declared_or_apply(
            _setup(), _resolved(_setup()), apply=False, routes_verified=False
        )
        == []
    )


def test_with_route_verification_the_registry_difference_is_a_warning_the_audit_log_decides() -> (
    None
):
    """The admin API reports the stored value, not what a config-declared source's config says."""
    setup = _setup(_declare(postgresql=("replica", 300)))
    warnings = rp.require_declared_or_apply(
        setup, _resolved(setup), apply=False, routes_verified=True
    )
    assert len(warnings) == 2 and "declared replica" in warnings[0]


# ------------------------------------------------------------------ which route served the requests


def _spec(
    source: str, table: str, joins: tuple = (), cached: bool = False
) -> request_mix.RequestSpec:
    return request_mix.RequestSpec(source, table, ("c",), 1, cached, (), joins)


def test_expected_route_follows_the_setting() -> None:
    setup = _setup()
    assert (
        rp.expected_route(setup, _spec("bench-postgresql", "orders")) == "direct"
    )  # live, one source
    assert (
        rp.expected_route(setup, _spec("bench-clickhouse", "order_events")) == "engine"
    )  # a replica


def test_a_request_that_joins_a_replica_is_read_through_the_engine() -> None:
    setup = _setup()
    join = contract_model.JoinStep(
        0, "cross", "bench-clickhouse", "order_events", "order_id", "order_id", True, None, None
    )
    assert rp.expected_route(setup, _spec("bench-postgresql", "orders", (join,))) == "engine"


def test_live_across_sources_cannot_be_told_from_a_replica_by_route() -> None:
    setup = _setup(_declare(clickhouse=("live", 0)))
    join = contract_model.JoinStep(
        0, "cross", "bench-clickhouse", "order_events", "order_id", "order_id", True, None, None
    )
    assert rp.expected_route(setup, _spec("bench-postgresql", "orders", (join,))) is None


class FakeAudit:
    """ops.queries over a pgwire connection: rows (id, table_name, route, source)."""

    def __init__(self, rows: list[tuple[int, str, str, str]]) -> None:
        self.rows = rows
        self.polls = 0

    def baseline(self) -> int:
        return max((r[0] for r in self.rows), default=0)

    def since(self, baseline: int) -> list[tuple[str, str, str]]:
        self.polls += 1
        return [(t, route, src) for i, t, route, src in self.rows if i > baseline]


def _verify(
    setup: contract_model.Setup,
    sent: list[request_mix.RequestSpec],
    rows: list[tuple[int, str, str, str]],
    baseline: int = 0,
) -> dict[str, Any]:
    audit = FakeAudit(rows)
    return rp.verify_routes(
        setup, rp.classes_of(setup, sent), audit, baseline, wait_s=0.0, sleep=lambda s: None
    )


def test_requests_served_as_declared_are_verified() -> None:
    setup = _setup()
    sent = [_spec("bench-postgresql", "orders")] * 3 + [
        _spec("bench-clickhouse", "order_events")
    ] * 2
    rows = [(1, "orders", "direct", "sql")] * 3 + [(4, "order_events", "engine", "sql")] * 2
    rows = [(i + 1, t, r, s) for i, (_, t, r, s) in enumerate(rows)]
    out = _verify(setup, sent, rows)
    assert out["verified"] == 5 and out["unverified"] == 0 and out["mismatches"] == []
    assert out["by_table"]["orders"] == {"expected": "direct", "served": {"direct": 3}}
    assert out["by_table"]["order_events"] == {"expected": "engine", "served": {"engine": 2}}


def test_a_replica_served_live_is_a_mismatch_and_refused() -> None:
    setup = _setup()
    sent = [_spec("bench-clickhouse", "order_events")] * 2
    rows = [(1, "order_events", "direct", "sql"), (2, "order_events", "engine", "sql")]
    with pytest.raises(
        rp.RouteMismatch,
        match="order_events: declared replica, expected route engine, served direct x1",
    ):
        _verify(setup, sent, rows)


def test_live_served_from_the_engine_is_a_mismatch_and_refused() -> None:
    setup = _setup()
    with pytest.raises(
        rp.RouteMismatch, match="orders: declared live, expected route direct, served engine x1"
    ):
        _verify(setup, [_spec("bench-postgresql", "orders")], [(1, "orders", "engine", "sql")])


def test_cache_hits_are_set_aside_not_counted_as_a_route() -> None:
    setup = _setup()
    sent = [_spec("bench-postgresql", "orders", cached=True)] * 3
    rows = [
        (1, "orders", "direct", "sql"),
        (2, "orders", "cache", "sql"),
        (3, "orders", "cache", "sql"),
    ]
    out = _verify(setup, sent, rows)
    assert out["by_table"]["orders"]["served"] == {"direct": 1, "cache": 2}
    assert out["verified"] == 1 and out["cache_served"] == 2


def test_requests_that_join_are_counted_but_unverified() -> None:
    setup = _setup()
    join = contract_model.JoinStep(
        0, "same", "bench-postgresql", "order_items", "order_id", "order_id", True, None, None
    )
    out = _verify(
        setup,
        [_spec("bench-postgresql", "orders", (join,))] * 2,
        [(1, "orders", "direct", "sql"), (2, "orders", "direct", "sql")],
    )
    assert out["verified"] == 0 and out["unverified"] == 2 and out["mismatches"] == []


def test_only_rows_after_the_baseline_count() -> None:
    setup = _setup()
    out = _verify(
        setup,
        [_spec("bench-postgresql", "orders")],
        [(1, "orders", "engine", "sql"), (7, "orders", "direct", "sql")],
        baseline=5,
    )
    assert out["by_table"]["orders"]["served"] == {"direct": 1}


def test_the_audit_log_is_polled_until_every_request_is_in() -> None:
    setup = _setup()
    audit = FakeAudit([(1, "orders", "direct", "sql")])
    calls = {"n": 0}

    def sleep(_: float) -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            audit.rows.append((2, "orders", "direct", "sql"))

    out = rp.verify_routes(
        setup,
        rp.classes_of(setup, [_spec("bench-postgresql", "orders")] * 2),
        audit,
        0,
        wait_s=5.0,
        sleep=sleep,
        poll_s=0.1,
    )
    assert out["verified"] == 2 and audit.polls >= 3


def test_audit_rows_that_never_arrive_are_an_error() -> None:
    setup = _setup()
    with pytest.raises(rp.RouteMismatch, match="the audit log has 1 of 3 requests after"):
        rp.verify_routes(
            setup,
            rp.classes_of(setup, [_spec("bench-postgresql", "orders")] * 3),
            FakeAudit([(1, "orders", "direct", "sql")]),
            0,
            wait_s=0.2,
            sleep=lambda s: None,
            poll_s=0.1,
        )


def test_the_audit_query_reads_the_ops_queries_report() -> None:
    assert rp.AUDIT_SINCE_SQL.startswith(
        "SELECT table_name, route, source FROM ops.queries WHERE id > %s"
    )
    assert "domain_id = ANY(%s)" in rp.AUDIT_SINCE_SQL and "status_code < 400" in rp.AUDIT_SINCE_SQL
    assert rp.AUDIT_BASELINE_SQL == "SELECT COALESCE(MAX(id), 0) FROM ops.queries"


def test_the_classes_of_what_was_sent_are_plain_data() -> None:
    setup = _setup()
    join = contract_model.JoinStep(
        0, "same", "bench-postgresql", "order_items", "order_id", "order_id", True, None, None
    )
    sent = (
        [_spec("bench-postgresql", "orders")] * 2
        + [_spec("bench-clickhouse", "order_events")]
        + [_spec("bench-postgresql", "orders", (join,))]
    )
    classes = rp.classes_of(setup, sent)
    assert classes == {
        "sent": 4,
        "unverified": 1,
        "tables": {
            "orders": {"expected": "direct", "declared": "live", "requests": 2},
            "order_events": {"expected": "engine", "declared": "replica", "requests": 1},
        },
    }
    assert json.loads(json.dumps(classes)) == classes


def test_audit_reader_asks_for_the_perf_domains_and_ok_statements() -> None:
    seen: list[tuple[str, tuple]] = []

    class Conn:
        def execute(self, sql: str, params: tuple = ()) -> Any:
            seen.append((sql, params))
            from types import SimpleNamespace

            return SimpleNamespace(
                fetchone=lambda: (41,), fetchall=lambda: [("orders", "direct", "sql")]
            )

    reader = rp.audit_reader(Conn(), ["perf-bench"])
    assert reader.baseline() == 41
    assert reader.since(41) == [("orders", "direct", "sql")]
    assert seen[1] == (rp.AUDIT_SINCE_SQL, (41, ["perf-bench"]))


# ------------------------------------------------------------------ what the local run showed


def test_a_live_read_of_a_source_the_router_never_sends_direct_cannot_be_told_by_route() -> None:
    """order_docs (MongoDB) and bench_order_node (Neo4j) were served by the engine with the source
    declared live: the router has no direct driver for them (router.py VIRTUAL_SOURCES)."""
    setup = _setup(_declare(mongodb=("live", 0), neo4j=("live", 0)))
    assert rp.expected_route(setup, _spec("bench-mongodb", "order_docs")) is None
    assert rp.expected_route(setup, _spec("bench-neo4j", "bench_order_node")) is None
    assert (
        rp.expected_route(setup, _spec("bench-clickhouse", "order_events")) == "engine"
    )  # a replica
    assert setup.sources["bench-mongodb"].kind == "mongodb"


def test_such_requests_are_counted_and_not_called_mismatches() -> None:
    setup = _setup(_declare(mongodb=("live", 0)))
    out = _verify(
        setup,
        [_spec("bench-mongodb", "order_docs")] * 2,
        [(1, "order_docs", "engine", "sql"), (2, "order_docs", "engine", "sql")],
    )
    assert out["verified"] == 0 and out["by_table"]["order_docs"] == {
        "expected": None,
        "served": {"engine": 2},
    }


def test_a_refusal_by_the_deployment_leaves_nothing_to_restore() -> None:
    """The deployment refuses to change a source declared in its configuration file; the run stops
    with the deployment's own message and does not try to 'restore' what was never changed."""
    setup = _setup(_declare(postgresql=("replica", 120)))
    admin = FakeAdmin()
    refused = "Source 'bench-postgresql' is declared in the configuration file"
    original = admin.post

    def post(path: str, json: dict[str, Any], headers: dict[str, str]) -> Any:
        import httpx

        if "updateSourcePreferMaterialized" in json["query"] and "__schema" not in json["query"]:
            req = httpx.Request("POST", "http://x/admin/graphql")
            admin.calls.append(("refused", {}))
            return httpx.Response(
                200,
                json={
                    "data": {
                        "updateSourcePreferMaterialized": {
                            "success": False,
                            "message": refused,
                            "code": "x",
                        }
                    }
                },
                request=req,
            )
        return original(path, json=json, headers=headers)

    admin.post = post  # type: ignore[method-assign]
    with pytest.raises(
        rp.ReplicationError,
        match="updateSourcePreferMaterialized failed: Source 'bench-postgresql' is declared in the configuration file",
    ):
        with _applier(admin).applied(setup, _resolved(setup)):
            pass
    assert admin.calls == [("refused", {})]  # no restore was attempted
