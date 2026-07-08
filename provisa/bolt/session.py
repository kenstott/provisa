# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3f1a2e-4c8b-4e9f-a5d2-1b6c3e8f2a4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bolt session: state machine, auth, query execution, result buffering."""

from __future__ import annotations

import logging
import os
from enum import Enum, auto
from typing import Any

from sqlalchemy import select

import provisa.bolt.messages as msg
from provisa.bolt.packstream import pack_message
from provisa.bolt.websocket import BoltWriter

log = logging.getLogger(__name__)

_BOLT_VERSION = "5.4"
_SERVER_AGENT = f"Neo4j/{_BOLT_VERSION} (Provisa)"
# Server→client hint: how long a client should wait for a server response before
# giving up. Federated reads (multi-source, cold Kafka/Iceberg) can be slow, so
# this is generous and configurable via PROVISA_BOLT_RECV_TIMEOUT (seconds).
_BOLT_RECV_TIMEOUT = int(os.environ.get("PROVISA_BOLT_RECV_TIMEOUT", "120"))


class State(Enum):
    NEGOTIATION = auto()
    AUTHENTICATION = auto()
    READY = auto()
    STREAMING = auto()
    TX_READY = auto()
    TX_STREAMING = auto()
    FAILED = auto()
    DEFUNCT = auto()


class BoltSession:
    def __init__(self, writer: BoltWriter, bolt_version: tuple[int, int]) -> None:
        self.writer = writer
        self.bolt_version = bolt_version
        self.state = State.AUTHENTICATION
        self.user_id: str | None = None
        # All role_ids the authenticated user holds; each surfaces as a "provisa_<role>" database.
        self.roles: list[str] = []
        # Active role for the current tx/run, chosen via the Bolt `db` field. Defaults to roles[0].
        self.role_id: str | None = None
        # db selected by the most recent BEGIN (explicit-tx path).
        self._tx_db: str | None = None
        # Buffered result from last RUN: list of column-ordered value lists
        self._result_columns: list[str] = []
        self._result_rows: list[list[Any]] = []
        self._pull_offset: int = 0

    # ── Response helpers ───────────────────────────────────────────────────────

    def _send(self, data: bytes) -> None:
        import logging as _logging
        from provisa.bolt.framing import write_message

        _logging.getLogger("uvicorn.error").warning(
            "[BOLT] send tag=0x%02X len=%d", data[1] if len(data) >= 2 else 0, len(data)
        )
        write_message(self.writer, data)

    def send_success(self, meta: dict | None = None) -> None:
        self._send(pack_message(msg.SUCCESS, meta or {}))

    def send_failure(self, code: str, message: str) -> None:
        self._send(pack_message(msg.FAILURE, {"code": code, "message": message}))
        self.state = State.FAILED

    def send_ignored(self) -> None:
        self._send(pack_message(msg.IGNORED))

    def send_record(self, values: list[Any]) -> None:
        self._send(pack_message(msg.RECORD, values))

    # ── Auth ───────────────────────────────────────────────────────────────────

    def _resolve_user(self, principal: str, credentials: str) -> tuple[str, list[str]] | None:
        """Return (user_id, role_ids) on success, None on failure.

        The role set becomes the user's selectable databases (provisa_<role>). Selecting
        a role narrows to that role's domain rights; the user can never exceed this set.
        """
        # An import failure is a server fault, not an auth failure — propagate it.
        from provisa.api.app import state as app_state

        if app_state.auth_config is None:
            # Fail closed: absent auth_config must never silently degrade to no-auth.
            raise RuntimeError("bolt auth_config not configured")
        provider = app_state.auth_config["provider"]
        all_roles = list(app_state.contexts.keys())

        if provider == "none" or not getattr(app_state, "auth_middleware_active", False):
            # No auth — every role is available; default to principal if it names a real role.
            ordered = (
                [principal, *[r for r in all_roles if r != principal]]
                if principal and principal in app_state.contexts
                else all_roles
            )
            return (principal or "anonymous", ordered) if ordered else None

        if provider != "simple":
            return None

        try:
            from provisa.auth.providers.simple import _provider_instance as auth_provider

            if auth_provider is None:
                return None
            auth_provider.login(principal, credentials)
            user = getattr(auth_provider, "_users", {}).get(principal, {})
            # Only roles that actually exist as compiled contexts are selectable.
            roles = [r for r in user.get("roles", []) if r in app_state.contexts]
            if not roles:
                return None
            return principal, roles
        except ValueError:
            return None

    def _resolve_db(self, db: Any) -> tuple[str, bool] | None:
        """Map a Bolt `db` value to (role_id, include_ops), or None if unauthorized.

        The db name encodes two axes:
          provisa_ops_<role> → role + ops/meta domains included
          provisa_<role>     → role + business domains only (ops/meta excluded)
        Empty / "system" / "provisa" → default role, business view.
        The role must be in the user's set; anything else → None.
        """
        if not self.roles:
            return None
        default = self.roles[0]
        if not db or db in ("system", "provisa"):
            return default, False
        if isinstance(db, str) and db.startswith("provisa_ops_"):
            role = db[len("provisa_ops_") :]
            return (role, True) if role in self.roles else None
        if isinstance(db, str) and db.startswith("provisa_"):
            role = db[len("provisa_") :]
            return (role, False) if role in self.roles else None
        return None

    # ── Message handlers ───────────────────────────────────────────────────────

    def handle_hello(self, fields: list[Any]) -> None:
        # Bolt 4.x: HELLO carries credentials; Bolt 5.x: HELLO has no credentials (LOGON follows)
        meta: dict = fields[0] if fields and isinstance(fields[0], dict) else {}
        major, _ = self.bolt_version
        if major < 5:
            # Auth inline with HELLO
            principal = meta.get("principal", "")
            credentials = meta.get("credentials", "")
            resolved = self._resolve_user(principal, credentials)
            if resolved is None:
                self.send_failure(
                    "Neo.ClientError.Security.Unauthorized",
                    f"Invalid credentials for principal {principal!r}",
                )
                return
            self.user_id, self.roles = resolved
            self.role_id = self.roles[0]
            self.state = State.READY
        else:
            # Bolt 5.x: wait for LOGON
            self.state = State.AUTHENTICATION

        self.send_success(
            {
                "server": _SERVER_AGENT,
                "connection_id": "bolt-provisa-1",
                "hints": {"connection.recv_timeout_seconds": _BOLT_RECV_TIMEOUT},
            }
        )

    def handle_logon(self, fields: list[Any]) -> None:
        meta: dict = fields[0] if fields and isinstance(fields[0], dict) else {}
        principal = meta.get("principal", "")
        credentials = meta.get("credentials", "")
        resolved = self._resolve_user(principal, credentials)
        if resolved is None:
            self.send_failure(
                "Neo.ClientError.Security.Unauthorized",
                f"Invalid credentials for principal {principal!r}",
            )
            return
        self.user_id, self.roles = resolved
        self.role_id = self.roles[0]
        self.state = State.READY
        self.send_success({})

    def handle_logoff(self) -> None:
        self.user_id = None
        self.roles = []
        self.role_id = None
        self.state = State.AUTHENTICATION
        self.send_success({})

    def handle_reset(self) -> None:
        self._result_columns = []
        self._result_rows = []
        self._pull_offset = 0
        if self.state != State.DEFUNCT:
            self.state = State.READY if self.role_id else State.AUTHENTICATION
        self.send_success({})

    def handle_begin(self, fields: list[Any]) -> None:
        if self.state == State.FAILED:
            self.send_ignored()
            return
        meta: dict = fields[0] if fields and isinstance(fields[0], dict) else {}
        self._tx_db = meta.get("db")
        self.state = State.TX_READY
        self.send_success({})

    def handle_commit(self) -> None:
        if self.state == State.FAILED:
            self.send_ignored()
            return
        self._tx_db = None
        self.state = State.READY
        self.send_success({})

    def handle_rollback(self) -> None:
        if self.state == State.FAILED:
            self.send_ignored()
            return
        self._tx_db = None
        self.state = State.READY
        self.send_success({})

    async def handle_run(self, fields: list[Any]) -> None:
        cypher: str = fields[0] if fields else ""
        parameters: dict = fields[1] if len(fields) > 1 and isinstance(fields[1], dict) else {}
        extra: dict = fields[2] if len(fields) > 2 and isinstance(fields[2], dict) else {}

        # db selection: autocommit RUN carries `db` in extra; explicit-tx inherits BEGIN's db.
        db = extra.get("db", self._tx_db)
        resolved = self._resolve_db(db) if self.roles else None

        import logging as _logging

        _logging.getLogger("uvicorn.error").warning(
            "[BOLT] RUN cypher=%r db=%r resolved=%r state=%s",
            cypher,
            db,
            resolved,
            self.state.name,
        )

        if self.state == State.FAILED:
            self.send_ignored()
            return

        if not self.roles:
            self.send_failure("Neo.ClientError.Security.Unauthorized", "Not authenticated")
            return

        if resolved is None:
            self.send_failure(
                "Neo.ClientError.Database.DatabaseNotFound",
                f"Database {db!r} does not exist or is not accessible",
            )
            return

        role_id, include_ops = resolved

        try:
            columns, rows = await _execute_cypher(
                cypher, parameters, role_id, include_ops=include_ops, roles=self.roles
            )
        except PermissionError as exc:
            self.send_failure("Neo.ClientError.Security.Forbidden", str(exc))
            return
        except Exception as exc:
            import logging as _logging
            import traceback as _tb

            _logging.getLogger("uvicorn.error").warning(
                "[BOLT] RUN failed: %s\n%s", exc, _tb.format_exc()
            )
            self.send_failure("Neo.ClientError.Statement.SyntaxError", str(exc))
            return

        self._result_columns = columns
        self._result_rows = rows
        self._pull_offset = 0

        in_tx = self.state in (State.TX_READY, State.TX_STREAMING)
        self.state = State.TX_STREAMING if in_tx else State.STREAMING
        self.send_success({"fields": columns, "t_first": 0})

    def handle_pull(self, fields: list[Any]) -> None:
        if self.state == State.FAILED:
            self.send_ignored()
            return
        if self.state not in (State.STREAMING, State.TX_STREAMING):
            self.send_failure(
                "Neo.ClientError.Request.Invalid",
                f"Cannot PULL in state {self.state.name}",
            )
            return

        meta: dict = fields[0] if fields and isinstance(fields[0], dict) else {}
        n = meta.get("n", -1)

        _dbg = logging.getLogger("uvicorn.error")
        _dbg.warning(
            "[BOLT] PULL n=%d offset=%d total_rows=%d", n, self._pull_offset, len(self._result_rows)
        )
        rows_sent = 0
        while self._pull_offset < len(self._result_rows):
            if n != -1 and rows_sent >= n:
                break
            row = self._result_rows[self._pull_offset]
            _dbg.warning("[BOLT] PULL sending record row=%r", row)
            self.send_record(row)
            self._pull_offset += 1
            rows_sent += 1

        has_more = self._pull_offset < len(self._result_rows)
        in_tx = self.state == State.TX_STREAMING
        if not has_more:
            self.state = State.TX_READY if in_tx else State.READY
        self.send_success({"has_more": has_more, "t_last": 0, "type": "r"})

    def handle_discard(self, fields: list[Any]) -> None:
        if self.state == State.FAILED:
            self.send_ignored()
            return
        meta: dict = fields[0] if fields and isinstance(fields[0], dict) else {}
        n = meta.get("n", -1)
        if n == -1:
            self._pull_offset = len(self._result_rows)
        else:
            self._pull_offset = min(self._pull_offset + n, len(self._result_rows))
        in_tx = self.state == State.TX_STREAMING
        has_more = self._pull_offset < len(self._result_rows)
        if not has_more:
            self.state = State.TX_READY if in_tx else State.READY
        self.send_success({"has_more": has_more})

    def handle_route(self) -> None:
        import logging as _logging

        _logging.getLogger("uvicorn.error").warning("[BOLT] ROUTE received")
        self.send_success(
            {
                "rt": {
                    "ttl": 300,
                    "db": "neo4j",
                    "servers": [
                        {"addresses": ["localhost:17687"], "role": "WRITE"},
                        {"addresses": ["localhost:17687"], "role": "READ"},
                        {"addresses": [], "role": "ROUTE"},
                    ],
                }
            }
        )

    def handle_telemetry(self) -> None:
        self.send_success({})


# ── Cypher execution ───────────────────────────────────────────────────────────


def _bolt_label_map(ctx: Any, role_id: str, include_ops: bool, app_state: Any) -> Any:
    """Build a CypherLabelMap scoped to the role's domain rights (the hard ceiling).

    include_ops=False additionally drops system/meta/ops domains (the "provisa_<role>"
    business view); include_ops=True keeps them ("provisa_ops_<role>"). The role's
    domain_access is always applied first, so no db name can exceed the role's rights.
    """
    from provisa.core import domain_policy
    from provisa.cypher.label_map import CypherLabelMap

    role = getattr(app_state, "roles", {}).get(role_id, {})
    cache = getattr(app_state, "schema_build_cache", {})
    base = CypherLabelMap.from_schema(
        ctx,
        domain_access=role.get("domain_access"),
        all_tables=cache.get("tables"),
        all_relationships=cache.get("relationships"),
        all_column_types=cache.get("column_types"),
        source_catalogs=getattr(app_state, "source_catalogs", None),
    )
    if include_ops:
        return base

    # Business view: drop system/meta/ops-domain nodes (and any relationship touching them).
    # from_schema's domain_access only gates cross-domain node addition — it does not filter the
    # base node set — so the exclusion must happen here, post-build.
    sys_ids = set(domain_policy.system_domain_ids())
    biz_nodes = {tn: nm for tn, nm in base.nodes.items() if (nm.domain_id or "") not in sys_ids}
    biz_rels = {
        k: rm
        for k, rm in base.relationships.items()
        if rm.source_label in biz_nodes and rm.target_label in biz_nodes
    }
    nodes_by_table: dict[str, list[str]] = {}
    for tn, nm in biz_nodes.items():
        nodes_by_table.setdefault(nm.table_label, []).append(tn)
    biz_domains = {
        dl: [tn for tn in tns if tn in biz_nodes]
        for dl, tns in base.domains.items()
        if any(tn in biz_nodes for tn in tns)
    }
    biz_aliases = {
        rt: [rm for rm in rms if rm.source_label in biz_nodes and rm.target_label in biz_nodes]
        for rt, rms in base.aliases.items()
    }
    biz_aliases = {rt: rms for rt, rms in biz_aliases.items() if rms}
    return CypherLabelMap(
        nodes=biz_nodes,
        relationships=biz_rels,
        domains=biz_domains,
        nodes_by_table=nodes_by_table,
        aliases=biz_aliases,
    )


def _show_databases_rows(roles: list[str]) -> tuple[list[str], list[list[Any]]]:
    """One database per (view × role): provisa_<role> (business) and provisa_ops_<role>."""
    cols = [
        "name",
        "type",
        "aliases",
        "access",
        "address",
        "role",
        "writer",
        "requestedStatus",
        "currentStatus",
        "statusMessage",
        "default",
        "home",
        "constituents",
    ]
    default_role = roles[0] if roles else None
    rows: list[list[Any]] = []
    for r in roles:
        for name in (f"provisa_{r}", f"provisa_ops_{r}"):
            is_home = name == f"provisa_{default_role}"
            rows.append(
                [
                    name,
                    "standard",
                    [],
                    "read-write",
                    "localhost:17687",
                    "primary",
                    True,
                    "online",
                    "online",
                    "",
                    is_home,
                    is_home,
                    [],
                ]
            )
    return cols, rows


def _system_query(
    cypher: str,
    ctx: Any,
    role_id: str,
    include_ops: bool,
    app_state: Any,
    roles: list[str] | None = None,
) -> tuple[list[str], list[list[Any]]] | None:
    """Handle Neo4j Browser system/catalog queries. Return None to fall through."""
    import logging as _logging

    _dbg = _logging.getLogger("uvicorn.error")
    q = cypher.strip()

    q_upper = q.upper()

    # SHOW DATABASES / SHOW DEFAULT DATABASE — one db per (view × role) the user holds.
    if q_upper.startswith("SHOW DATABASE") or q_upper.startswith("SHOW DEFAULT DATABASE"):
        _dbg.warning("[BOLT] _system_query: intercepted SHOW DATABASES roles=%r", roles)
        return _show_databases_rows(roles or ([role_id] if role_id else []))

    # SHOW ALIASES
    if q_upper.startswith("SHOW ALIASES"):
        _dbg.warning("[BOLT] _system_query: intercepted SHOW ALIASES")
        return ["name", "database", "location", "url", "user"], []

    # SHOW PROCEDURES / SHOW FUNCTIONS
    if q_upper.startswith("SHOW PROCEDURES") or q_upper.startswith("SHOW FUNCTIONS"):
        _dbg.warning("[BOLT] _system_query: intercepted SHOW PROCEDURES/FUNCTIONS")
        return ["name", "description", "signature"], []

    # SHOW TRANSACTIONS / SHOW SETTINGS / SHOW INDEXES / SHOW CONSTRAINTS
    if (
        q_upper.startswith("SHOW TRANSACTIONS")
        or q_upper.startswith("SHOW SETTINGS")
        or q_upper.startswith("SHOW INDEXES")
        or q_upper.startswith("SHOW CONSTRAINTS")
    ):
        _dbg.warning(
            "[BOLT] _system_query: intercepted SHOW %s", q.split()[1] if len(q.split()) > 1 else ""
        )
        return [], []

    # db.labels / db.relationshipTypes / db.propertyKeys compound query
    if "db.labels()" in q or "db.relationshipTypes()" in q or "db.propertyKeys()" in q:
        label_map = _bolt_label_map(ctx, role_id, include_ops, app_state)
        labels = sorted(
            {nm.table_label for nm in label_map.nodes.values()}
            | {nm.domain_label for nm in label_map.nodes.values() if nm.domain_label}
        )
        rel_types = sorted({rm.rel_type for rm in label_map.relationships.values()})
        prop_keys = sorted({prop for nm in label_map.nodes.values() for prop in nm.properties})
        _dbg.warning("[BOLT] _system_query: db.labels=%r rel_types=%r", labels, rel_types)
        rows: list[list[Any]] = []
        if "db.labels()" in q:
            rows.append([{"name": "labels", "data": labels}])
        if "db.relationshipTypes()" in q:
            rows.append([{"name": "relationshipTypes", "data": rel_types}])
        if "db.propertyKeys()" in q:
            rows.append([{"name": "propertyKeys", "data": prop_keys}])
        return ["result"], rows

    # dbms.components() — version info
    if "dbms.components()" in q:
        _dbg.warning("[BOLT] _system_query: intercepted dbms.components()")
        return ["name", "versions", "edition"], [
            ["Neo4j Kernel (Provisa)", ["5.3.0"], "community"],
        ]

    # CALL dbms.showCurrentUser() — Browser uses this to identify the logged-in user
    if "dbms.showCurrentUser()" in q:
        _dbg.warning("[BOLT] _system_query: intercepted dbms.showCurrentUser()")
        return ["username", "roles", "flags"], [[role_id, list(roles or [role_id]), []]]

    # CALL dbms.* catch-all (must be after specific handlers above)
    if q_upper.startswith("CALL DBMS.") or q_upper.startswith("CALL DB."):
        _dbg.warning("[BOLT] _system_query: intercepted CALL DBMS/DB.*")
        return [], []

    return None


async def _graph_counts(
    ctx: Any, role_id: str, include_ops: bool, app_state: Any
) -> tuple[int, int]:
    """Total node/rel counts: sum of per-label count(n) and per-rel-type count(r).

    Mirrors /data/graph-counts (REQ-392); view-labels over the same physical rows
    are counted per-label, matching the internal graph browser.
    """
    import asyncio

    label_map = _bolt_label_map(ctx, role_id, include_ops, app_state)
    node_labels = [nm.label for nm in label_map.nodes.values()]
    seen: set[str] = set()
    rel_types: list[str] = []
    for rel in label_map.relationships.values():
        if rel.rel_type not in seen:
            seen.add(rel.rel_type)
            rel_types.append(rel.rel_type)

    async def _count(cypher: str) -> int:
        try:
            _cols, rows = await _execute_cypher(cypher, {}, role_id, include_ops=include_ops)
            return int(rows[0][0]) if rows and rows[0] else 0
        except Exception:
            return 0

    node_results = await asyncio.gather(
        *[_count(f"MATCH (n:{lbl}) RETURN count(n) AS cnt") for lbl in node_labels]
    )
    rel_results = await asyncio.gather(
        *[_count(f"MATCH ()-[r:{rt}]->() RETURN count(r) AS cnt") for rt in rel_types]
    )
    return sum(node_results), sum(rel_results)


async def _impute_relationships(
    parameters: dict, ctx: Any, role_id: str, include_ops: bool, app_state: Any
) -> tuple[list[str], list[list[Any]]]:
    """Impute edges among the Browser's visible nodes (REQ-345).

    The Browser sends integer node IDs in $existingNodeIds/$newNodeIds. Resolve them
    to (label, pk) via node_ids, then for every relationship pair whose endpoints are
    both visible run MATCH (a)-[r]->(b) WHERE a.pk IN [...] AND b.pk IN [...] RETURN r.
    Each returned edge carries integer node IDs (re-registered idempotently), so they
    line up with the nodes already on the Browser canvas.
    """
    from provisa.compiler.naming import apply_cql_property as _cql_prop

    tenant_db = getattr(app_state, "tenant_db", None)
    if tenant_db is None:
        return ["r"], []

    raw_ids = list(parameters.get("existingNodeIds") or []) + list(
        parameters.get("newNodeIds") or []
    )
    int_ids: list[int] = []
    for v in raw_ids:
        try:
            int_ids.append(int(v))
        except (ValueError, TypeError):
            pass
    if not int_ids:
        return ["r"], []

    label_map = _bolt_label_map(ctx, role_id, include_ops, app_state)
    nm_by_label = {nm.label: nm for nm in label_map.nodes.values()}

    from provisa.core.schema_org import node_ids

    async with tenant_db.acquire() as conn:
        result = await conn.execute_core(
            select(node_ids.c.id, node_ids.c.label, node_ids.c.composite_id).where(
                node_ids.c.id.in_(int_ids)
            )
        )
        pg_rows = [dict(r._mapping) for r in result.fetchall()]

    by_label: dict[str, list[Any]] = {}
    for r in pg_rows:
        if nm_by_label.get(r["label"]) is None:
            continue
        pk_str = r["composite_id"].rsplit("|", 1)[-1]
        val: Any = int(pk_str) if pk_str.lstrip("-").isdigit() else pk_str
        by_label.setdefault(r["label"], []).append(val)

    visible = set(by_label.keys())

    def _cql_literal(v: Any) -> str:
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"

    edges: list[list[Any]] = []
    seen: set[Any] = set()
    for rel in label_map.relationships.values():
        src_label = label_map.nodes[rel.source_label].label
        tgt_label = label_map.nodes[rel.target_label].label
        if src_label not in visible or tgt_label not in visible:
            continue
        src_nm = label_map.nodes[rel.source_label]
        tgt_nm = label_map.nodes[rel.target_label]
        src_prop = _cql_prop(src_nm.id_column)
        tgt_prop = _cql_prop(tgt_nm.id_column)
        src_ids = ", ".join(_cql_literal(i) for i in by_label[src_label])
        tgt_ids = ", ".join(_cql_literal(i) for i in by_label[tgt_label])
        query = (
            f"MATCH (a:{src_label})-[r:{rel.rel_type}]->(b:{tgt_label})"
            f" WHERE a.{src_prop} IN [{src_ids}] AND b.{tgt_prop} IN [{tgt_ids}] RETURN r"
        )
        try:
            _cols, rrows = await _execute_cypher(query, {}, role_id, include_ops=include_ops)
        except Exception:
            continue
        for row in rrows:
            edge = row[0] if row else None
            if isinstance(edge, dict):
                eid = edge.get("identity")
                if eid not in seen:
                    seen.add(eid)
                    edges.append([edge])
    return ["r"], edges


async def _execute_cypher(
    cypher: str,
    parameters: dict,
    role_id: str,
    include_ops: bool = True,
    roles: list[str] | None = None,
) -> tuple[list[str], list[list[Any]]]:
    """Run Cypher through the Provisa pipeline; return (columns, rows-of-values)."""
    from provisa.api.app import state as app_state
    from provisa.cypher.assembler import (
        assemble_rows,
        register_node_ids,
        register_rel_ids,
        to_serializable,
    )
    from provisa.cypher.params import CypherParamError, bind_params, collect_param_names
    from provisa.cypher.parser import CypherParseError, parse_cypher
    from provisa.cypher.translator import (
        CypherCrossSourceError,
        CypherTranslateError,
        cypher_to_sql,
    )
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.compiler.sql_gen import make_semantic_sql
    from provisa.pgwire._pipeline import _govern_and_route_compiled, _execute_plan

    ctx = app_state.contexts.get(role_id)
    if ctx is None:
        raise PermissionError("Schema not loaded")

    result = _system_query(cypher, ctx, role_id, include_ops, app_state, roles)
    if result is not None:
        return result

    # Browser sysinfo node/rel totals — compute real counts (matches the internal graph browser).
    _q = cypher.strip()
    if "count(*)" in _q and "'nodes'" in _q and "'relationships'" in _q:
        node_count, rel_count = await _graph_counts(ctx, role_id, include_ops, app_state)
        return ["result"], [
            [{"name": "nodes", "data": node_count}],
            [{"name": "relationships", "data": rel_count}],
        ]

    # Browser auto-complete-relationships probe — impute edges among visible nodes (REQ-345).
    #   MATCH (a)-[r]->(b) WHERE id(a) IN $existingNodeIds AND id(b) IN $newNodeIds RETURN r
    if "$existingNodeIds" in cypher and "$newNodeIds" in cypher:
        return await _impute_relationships(parameters, ctx, role_id, include_ops, app_state)

    # Try write path first; fall through to read path if it doesn't parse as a write.
    from provisa.cypher.write_translator import CypherWriteParseError, parse_cypher_write

    try:
        parse_cypher_write(cypher)
        return await _execute_write_cypher(cypher, role_id, ctx, include_ops, app_state)
    except CypherWriteParseError:
        pass

    try:
        ast = parse_cypher(cypher)
    except CypherParseError as exc:
        raise ValueError(str(exc)) from exc

    label_map = _bolt_label_map(ctx, role_id, include_ops, app_state)

    param_names = collect_param_names(cypher)
    try:
        bind_params(param_names, parameters)
    except CypherParamError as exc:
        raise ValueError(str(exc)) from exc

    try:
        sql_ast, ordered_params, graph_vars = cypher_to_sql(ast, label_map, parameters)
    except (CypherCrossSourceError, CypherTranslateError) as exc:
        raise ValueError(str(exc)) from exc

    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)

    try:
        sql_str = sql_ast.sql(dialect="postgres")
    except Exception as exc:
        raise RuntimeError(f"SQL generation failed: {exc}") from exc

    import logging as _logging

    _logging.getLogger("uvicorn.error").warning("[BOLT] cypher_sql=%s", sql_str)
    semantic_sql = make_semantic_sql(sql_str, ctx)
    _logging.getLogger("uvicorn.error").warning("[BOLT] semantic_sql=%s", semantic_sql)
    resolved_params = [parameters.get(name) for name in ordered_params]

    plan = await _govern_and_route_compiled(
        semantic_sql,
        role_id,
        exec_params=resolved_params or None,
    )
    result = await _execute_plan(plan)
    raw_rows = [dict(zip(result.column_names, row)) for row in result.rows]
    assembled = assemble_rows(raw_rows, graph_vars)
    serializable = [to_serializable(r) for r in assembled]

    _tenant_db = getattr(app_state, "tenant_db", None)
    await register_node_ids(serializable, _tenant_db)
    await register_rel_ids(serializable, _tenant_db)

    columns = list(raw_rows[0].keys()) if raw_rows else []
    rows = [[row.get(col) for col in columns] for row in serializable]
    return columns, rows


async def _execute_write_cypher(
    cypher: str, role_id: str, ctx: Any, include_ops: bool, app_state: Any
) -> tuple[list[str], list[list[Any]]]:
    from provisa.cypher.write_translator import (
        CypherWriteParseError,
        WriteTranslator,
        parse_cypher_write,
    )
    from provisa.pgwire._pipeline import _govern_and_route_compiled, _execute_plan

    try:
        write_ast = parse_cypher_write(cypher)
    except CypherWriteParseError as exc:
        raise ValueError(str(exc)) from exc

    label_map = _bolt_label_map(ctx, role_id, include_ops, app_state)
    translator = WriteTranslator(label_map)
    sql = translator.translate(write_ast)

    plan = await _govern_and_route_compiled(sql, role_id)
    result = await _execute_plan(plan)
    rows = [list(row) for row in result.rows]
    return result.column_names, rows
