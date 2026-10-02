# Copyright (c) 2026 Kenneth Stott
# Canary: ac539aca-27b8-499b-b4c0-23458dc3dd4b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Replication (REQ-826, REQ-1911): live reads or reads from a replica, as an operator setting.

Whether a table is read live or from its replica is not something a request can choose, and the
operator's setting is a floor no request gets around. A benchmark run is therefore measured *under*
a setting: the contract declares it per source (optionally per table), ``mismatches`` checks the
deployment really has it, ``AdminReplication.applied`` (``--apply-replication``) sets it through the
admin API for the run and restores the previous values afterwards, and ``verify_routes`` proves from
the audit log which route actually served the requests, refusing to report a figure whose requests
were served the other way.

Routes in the audit log (``query_audit_log.route``, read through the ops ``queries`` report,
``provisa/api/_meta_views.py``): ``direct`` (straight to the source), ``engine`` (through the
federation engine, which reads a replica of a replicated table), ``cache`` (the response cache),
``api``. The value is written at ``provisa/pgwire/_pipeline.py`` and ``provisa/api/data/endpoint.py``
from ``Route.<name>.lower()``.

The setting is ``replicate`` (REQ-826; ``update_source_replicate`` and ``update_table_replicate``
in ``provisa/api/admin/schema_mutation.py``): an integer — 0 Always, -1 Never, N > 0 Hot-N, not
set = Default. The contract's ``replica`` is 0 and its ``live`` is -1, the two values whose route
is certain. The admin mutations are named in ``MUTATIONS``.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import contract_model
import lookup
import request_mix
import setup_contract as sc

AUDIT_BASELINE_SQL = "SELECT COALESCE(MAX(id), 0) FROM ops.queries"
AUDIT_SINCE_SQL = (
    "SELECT table_name, route, source FROM ops.queries "
    "WHERE id > %s AND domain_id = ANY(%s) AND status_code < 400"
)
ROUTE_ENGINE, ROUTE_DIRECT, ROUTE_CACHE = "engine", "direct", "cache"

# Source types the router never sends DIRECT: they have no direct SQL driver and are always read
# through the engine, replicated or not (provisa/transpiler/router.py VIRTUAL_SOURCES at :48 and
# API_SOURCES at :45). For these the route cannot tell a live read from a replica read.
ALWAYS_ENGINE_SOURCE_TYPES = frozenset(
    {
        "sqlite",
        "mongodb",
        "cassandra",
        "redis",
        "neo4j",
        "delta_lake",
        "iceberg",
        "hive",
        "hive_s3",
        "google_sheets",
        "prometheus",
        "kafka",
        "openapi",
        "graphql_api",
        "grpc_api",
        "grpc_remote",
    }
)

# the admin mutations that set the setting, and the introspection name that proves they exist
MUTATIONS = {
    "source": "updateSourceReplicate",
    "table": "updateTableReplicate",
    "source_cache": "updateSourceCache",
    "table_cache": "updateTableCache",
}


class ReplicationError(RuntimeError):
    """The replication setting could not be read, set or restored."""


class RouteMismatch(RuntimeError):
    """Requests were served by a route other than the one the declared setting implies, or the audit
    log does not show them: no figure is reported for them."""


# --------------------------------------------------------------------------------------------
# What the contract declares vs what the deployment has
# --------------------------------------------------------------------------------------------


def effective(
    setup: contract_model.Setup, source: str, table: contract_model.Table
) -> contract_model.Replication:
    """A table's declared setting: its own, else its source's."""
    return table.replication or setup.sources[source].replication


def _is_replica(value: Any) -> bool | None:
    """The registry's setting as live (False) / replica (True); None: not set, inherit."""
    if value is None:
        return None
    if value == 0:
        return True  # replicate = Always
    if value == -1:
        return False  # replicate = Never
    raise ReplicationError(f"replicate = {value} (Hot-N) is neither live nor replica")


def _replicate_value(setting: str) -> int:
    """The ``replicate`` value for a contract setting: ``replica`` is Always (0), ``live`` is
    Never (-1) — the two values whose route does not depend on how busy the table is."""
    return 0 if setting == "replica" else -1


def _registry(resolved: lookup.Resolved, source: str, key: str) -> tuple[bool, int | None]:
    """The (is replica, TTL) the deployment holds for a table: its own value, else its source's."""
    names = resolved.tables[key]
    src = resolved.sources[source]
    own = _is_replica(names.replicate)
    replica = own if own is not None else bool(_is_replica(src["replicate"]))
    ttl = names.cache_ttl if names.cache_ttl is not None else src["cache_ttl"]
    return replica, ttl


def mismatches(setup: contract_model.Setup, resolved: lookup.Resolved) -> list[str]:
    """Where the deployment's replication differs from what the contract declares."""
    out: list[str] = []
    for sid, src in setup.sources.items():
        for table in src.tables:
            key = sc.table_identity(setup, sid, table).key
            declared = effective(setup, sid, table)
            replica, ttl = _registry(resolved, sid, key)
            has = "replica" if replica else "live"
            if declared.setting != has:
                out.append(
                    f"{key}: declared {declared.setting}, the deployment reads it "
                    f"{'from its replica' if replica else 'live'}"
                )
            elif declared.setting == "replica" and ttl != declared.ttl_seconds:
                out.append(f"{key}: declared TTL {declared.ttl_seconds}, the deployment's is {ttl}")
    return out


def require_declared_or_apply(
    setup: contract_model.Setup,
    resolved: lookup.Resolved,
    *,
    apply: bool,
    routes_verified: bool,
) -> list[str]:
    """Where the deployment's registry differs from the declaration, and what the run does about it.

    A deployment is only touched when the run was told to set it (``apply``). The registry is not
    authoritative for a source the deployment's configuration file declares: the admin API reports
    the stored value (the local run showed ``replicate`` unset for a source whose config
    sets it true, while the router followed the config). So when the routes are verified from the
    audit log, which is authoritative, a difference is returned as a warning for the caller to print
    and the audit decides; without route verification nothing else would catch it and it raises."""
    problems = mismatches(setup, resolved)
    if problems and not apply and not routes_verified:
        raise contract_model.SetupError(
            "the deployment is not as the contract declares: "
            + "; ".join(problems)
            + "; pass --apply-replication to let the run set it and restore it afterwards"
        )
    return [] if apply else problems


# --------------------------------------------------------------------------------------------
# Applying it through the admin API
# --------------------------------------------------------------------------------------------


@dataclass
class _Change:
    mutation: str
    variables: dict[str, Any]
    arguments: str  # GraphQL variable declarations
    call: str  # the mutation's argument list


class AdminReplication:
    """Sets the declared replication through ``/admin/graphql`` and restores the previous values.

    ``client`` is an httpx.Client for the deployment (its credential header set); ``role`` must hold
    ``source_registration`` and ``table_registration``, the rights those mutations are gated on."""

    def __init__(self, client: Any, role: str) -> None:
        self._client = client
        self._role = role
        self._checked = False

    def _post(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        resp = self._client.post(
            "/admin/graphql",
            json={"query": query, "variables": variables or {}},
            headers={"X-Provisa-Role": self._role},
        )
        resp.raise_for_status()
        body = resp.json()
        if body.get("errors"):
            raise ReplicationError(f"/admin/graphql: {str(body['errors'])[:300]}")
        return body["data"]

    def _check_mutations(self) -> None:
        if self._checked:
            return
        data = self._post("{ __schema { mutationType { fields { name } } } }")
        names = {f["name"] for f in data["__schema"]["mutationType"]["fields"]}
        for want in MUTATIONS.values():
            if want not in names:
                raise ReplicationError(
                    f"the admin API has no {want} mutation: the deployment predates the "
                    "replicate setting (REQ-826), or MUTATIONS in replication.py is out of date"
                )
        self._checked = True

    def _run(self, change: _Change) -> None:
        query = (
            f"mutation({change.arguments}) {{ {change.mutation}({change.call}) "
            "{ success message code } }"
        )
        result = self._post(query, change.variables)[change.mutation]
        if not result["success"]:
            raise ReplicationError(f"{change.mutation} failed: {result['message']}")

    # ---- the changes for a setting, and the ones that put it back
    def _source_changes(
        self, sid: str, replicate: int | None, ttl: int | None, cache_enabled: bool
    ) -> list[_Change]:
        return [
            _Change(
                MUTATIONS["source"],
                {"sourceId": sid, "value": replicate},
                "$sourceId: String!, $value: Int",
                "sourceId: $sourceId, replicate: $value",
            ),
            _Change(
                MUTATIONS["source_cache"],
                {"sourceId": sid, "enabled": cache_enabled, "ttl": ttl},
                "$sourceId: String!, $enabled: Boolean!, $ttl: Int",
                "sourceId: $sourceId, cacheEnabled: $enabled, cacheTtl: $ttl",
            ),
        ]

    def _table_changes(
        self, table_id: int, replicate: int | None, ttl: int | None
    ) -> list[_Change]:
        return [
            _Change(
                MUTATIONS["table"],
                {"tableId": table_id, "value": replicate},
                "$tableId: Int!, $value: Int",
                "tableId: $tableId, replicate: $value",
            ),
            _Change(
                MUTATIONS["table_cache"],
                {"tableId": table_id, "ttl": ttl},
                "$tableId: Int!, $ttl: Int",
                "tableId: $tableId, cacheTtl: $ttl",
            ),
        ]

    @contextmanager
    def applied(self, setup: contract_model.Setup, resolved: lookup.Resolved) -> Iterator[None]:
        """Set what the contract declares; put the previous values back on exit, whether the run
        succeeded, failed, or a later change could not be made."""
        self._check_mutations()
        undo: list[_Change] = []
        try:
            for sid, src in setup.sources.items():
                before = resolved.sources[sid]
                want = src.replication
                self._apply(
                    self._source_changes(
                        sid,
                        _replicate_value(want.setting),
                        want.ttl_seconds or None,
                        before["cache_enabled"],
                    ),
                    # put back exactly what the registry held, whatever it was
                    self._source_changes(
                        sid, before["replicate"], before["cache_ttl"], before["cache_enabled"]
                    ),
                    undo,
                )
                for table in src.tables:
                    if table.replication is None:
                        continue
                    key = sc.table_identity(setup, sid, table).key
                    names = resolved.tables[key]
                    if names.table_id is None:
                        raise ReplicationError(
                            f"the resolved names carry no registry id for {key}: ask the deployment again"
                        )
                    t = table.replication
                    self._apply(
                        self._table_changes(
                            names.table_id, _replicate_value(t.setting), t.ttl_seconds or None
                        ),
                        self._table_changes(names.table_id, names.replicate, names.cache_ttl),
                        undo,
                    )
            yield
        finally:
            failures = []
            for change in reversed(undo):
                try:
                    self._run(change)
                except Exception as exc:  # noqa: BLE001 - every restore is attempted, then reported
                    failures.append(f"{change.mutation} {change.variables}: {exc}")
            if failures:
                raise ReplicationError("could not restore: " + "; ".join(failures))

    def _apply(
        self, changes: Sequence[_Change], restores: Sequence[_Change], undo: list[_Change]
    ) -> None:
        """Make each change; its restore is queued once it has been made, so a change that was
        refused leaves nothing to restore."""
        for change, restore in zip(changes, restores):
            self._run(change)
            undo.append(restore)


# --------------------------------------------------------------------------------------------
# Which route served the requests
# --------------------------------------------------------------------------------------------


def expected_route(setup: contract_model.Setup, spec: request_mix.RequestSpec) -> str | None:
    """The route a request must have been served by under the declared settings, or None when the
    route cannot tell: live reads across several sources, and live reads of a source type the router
    never sends direct, go through the engine whether or not a table is replicated."""
    tables = {(sid, t.name): t for sid, s in setup.sources.items() for t in s.tables}
    members = [(spec.source, spec.table)] + [(j.child_source, j.child_table) for j in spec.joins]
    replica = any(
        effective(setup, sid, tables[(sid, name)]).setting == "replica" for sid, name in members
    )
    if replica:
        return ROUTE_ENGINE  # reads come from the replica on every engine path; never DIRECT
    if any(setup.sources[sid].kind in ALWAYS_ENGINE_SOURCE_TYPES for sid, _ in members):
        return None
    return ROUTE_DIRECT if len({sid for sid, _ in members}) == 1 else None


class _AuditReader:
    """Reads ``ops.queries`` over pgwire (see ``AUDIT_*_SQL``)."""

    def __init__(self, conn: Any, domain_ids: Sequence[str]) -> None:
        self._conn = conn
        self._domains = list(domain_ids)

    def baseline(self) -> int:
        return self._conn.execute(AUDIT_BASELINE_SQL).fetchone()[0]

    def since(self, baseline: int) -> list[tuple[str, str, str]]:
        return [
            tuple(r)
            for r in self._conn.execute(AUDIT_SINCE_SQL, (baseline, self._domains)).fetchall()
        ]


def audit_reader(conn: Any, domain_ids: Sequence[str]) -> _AuditReader:
    return _AuditReader(conn, domain_ids)


def classes_of(
    setup: contract_model.Setup, sent: Sequence[request_mix.RequestSpec]
) -> dict[str, Any]:
    """What a step sent, as the data ``verify_routes`` needs: per table the request's own table, the
    route the declared setting implies and how many requests; requests that join are counted but
    cannot be attributed to one table, so they are unverified."""
    tables = {(sid, t.name): t for sid, s in setup.sources.items() for t in s.tables}
    out: dict[str, Any] = {"sent": len(sent), "unverified": 0, "tables": {}}
    for spec in sent:
        if spec.joins:
            out["unverified"] += 1
            continue
        t = out["tables"].setdefault(
            spec.table,
            {
                "expected": expected_route(setup, spec),
                "declared": effective(
                    setup, spec.source, tables[(spec.source, spec.table)]
                ).setting,
                "requests": 0,
            },
        )
        t["requests"] += 1
    return out


def verify_routes(
    setup: contract_model.Setup,
    classes: Mapping[str, Any],
    audit: Any,
    baseline: int,
    *,
    wait_s: float = 30.0,
    poll_s: float = 0.5,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Compare the routes the audit log recorded since ``baseline`` with the declared settings.

    ``classes`` is ``classes_of`` what was sent. Verification is per table the request read (the
    audit row names the request's first registered table): a request that joins is counted but
    unverified, a response-cache hit is set aside. A replica served live, or live served through the
    engine, raises ``RouteMismatch``: the figures for those requests are not reported."""
    sent = classes["sent"]
    deadline = time.monotonic() + wait_s
    while True:
        rows = audit.since(baseline)
        if len(rows) >= sent or time.monotonic() >= deadline:
            break
        sleep(poll_s)
    if len(rows) < sent:
        raise RouteMismatch(f"the audit log has {len(rows)} of {sent} requests after {wait_s:g}s")
    served: dict[str, Counter[str]] = {}
    for table, route, _source in rows:
        served.setdefault(table, Counter())[route] += 1
    mismatches_: list[str] = []
    by_table: dict[str, Any] = {}
    verified = cache_served = 0
    for table, counts in served.items():
        info = classes["tables"].get(table)
        expected = info["expected"] if info else None
        by_table[table] = {"expected": expected, "served": dict(counts)}
        cache_served += counts.get(ROUTE_CACHE, 0)
        if expected is None:
            continue
        for route, n in counts.items():
            if route == ROUTE_CACHE:
                continue
            if route == expected:
                verified += n
            else:
                mismatches_.append(
                    f"{table}: declared {info['declared']}, expected route {expected}, "
                    f"served {route} x{n}"
                )
    if mismatches_:
        raise RouteMismatch("; ".join(mismatches_))
    return {
        "verified": verified,
        "unverified": classes["unverified"],
        "cache_served": cache_served,
        "mismatches": [],
        "by_table": by_table,
    }
