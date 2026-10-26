# Copyright (c) 2026 Kenneth Stott
# Canary: 9f1d00ab-ade5-46f3-b223-e984b94be18b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Governance a run is measured under (REQ-2092): set up in the deployment, removed afterwards.

The contract's ``knobs.governance`` (``contract_model.Governance``) says what the request is
governed by. ``AdminGovernance.applied(setup, transport, resolved)`` creates it through the
deployment's admin API for one transport's run and removes it afterwards, the run failing or not,
the way ``replication.AdminReplication`` sets and restores replication. Everything it creates is
named with ``contract_model.GOVERNANCE_PREFIX``; one left by an interrupted run is refused by name.

What it does, for each of the contract's roles R (``Setup.request_role``):

- ``role_depth`` d: roles ``bench_gov_R_d1`` (parent R) ... ``bench_gov_R_d<d>``, each with R's
  capabilities and domain access (``createRole``, parent first; ``deleteRole``, leaf first). The
  request acts as the deepest; the rules and masks below are on it.
- ``derived_roles`` k: attributes ``bench_gov_attr1..k`` declared (``PUT /admin/derived-roles``);
  the request names ``bench_gov_attr<i>__v`` beside its data role.
- ``row_rules_per_table`` n: for every contract table, n tags of its own, each assigned to it and
  carrying a tag-level row rule ``<base column> IS NOT NULL`` for the request's role. Tag rules on
  a table are ANDed (``security/row_filters.resolve_table_rules``); a table-level rule is one per
  table and role. Every row is kept, so the answer is the optimistic one.
- ``masked_column_share`` s: on every contract table, round(s x its columns) columns masked for the
  request's role with a constant NULL (``Mask`` type ``constant``: a projection rewrite on any type
  that keeps every row). They are taken in the contract's column order from the nullable columns
  no filter, join or base read uses; too few is refused by name. A mask is set by saving the whole
  table (``updateTable``), as the table editor does (``table_input``): the mask goes first in the
  column's list, and the list as it was is saved back afterwards.
- ``central_redaction``: the org's redaction rules with the bench's appended (``PUT
  /admin/redaction``), restored afterwards. The built-in rules are always in force; off changes
  nothing.

``telemetry`` is the server's own, set when it starts: not set up here (``check_telemetry``).
"""

# Requirements: REQ-2092

from __future__ import annotations

from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from typing import Any

import contract_model as cm
import lookup
import setup_contract as sc

#: The rules ``central_redaction`` adds: what an org's own list commonly holds.
BENCH_REDACTION_RULES = (
    {"name": f"{cm.GOVERNANCE_PREFIX}_email", "pattern": r"[\w.+-]+@[\w-]+\.[\w.-]+"},
    {"name": f"{cm.GOVERNANCE_PREFIX}_card", "pattern": r"\b(?:\d[ -]?){13,16}\b"},
    {"name": f"{cm.GOVERNANCE_PREFIX}_ssn", "pattern": r"\b\d{3}-\d{2}-\d{4}\b"},
)

#: The mask a masked column gets: NULL, on any data type.
NULL_MASK = {"type": "constant", "pattern": None, "replace": None, "value": None, "precision": None}


class GovernanceError(RuntimeError):
    """The governance could not be set up or removed; the message says what and why."""


# --------------------------------------------------------------------------------------------
# A table, read and saved the way the table editor does it (provisa/api/mcp/table_edit.py
# table_input, provisa-ui/src/pages/tables/helpers.ts buildTableUpdateInput)
# --------------------------------------------------------------------------------------------

_MASK_FIELDS = "to mask { type pattern replace value precision }"
COLUMN_FIELDS = (
    "columnName visibleTo alias description dataType path embedding embeddingModel "
    "nativeFilterType nativeFilterRequired isPrimaryKey isForeignKey isAlternateKey scope "
    f"epochUnit fake fakeStable syntheticRule masks {{ {_MASK_FIELDS} }}"
)
TABLE_FIELDS = (
    "id sourceId domainId schemaName tableName alias description watermarkColumn changeSignal "
    "probeQuery probeType viewSql dqContract profilerSourceId queryTemplate fileGlob "
    "sourceFileColumn importSet arguments landingFilter loadProtected offPeakWindow offPeakTz "
    "materialize mvRefreshInterval mvDebounceQuiet mvDebounceMaxDelay pushDebounceQuiet "
    "pushDebounceMaxDelay mvConsistency mvPreprocess businessKey validTime "
    "recordedTime mvPersist mvIncremental mvCalendar mvGrain mvAllowedLateness "
    "mvExpectedEvents mvBusinessDayGrain modelingRole modelingHistory productId enableAggregates "
    "enableGroupBy uniqueConstraints { name columns } "
    "live { strategy watermarkColumn pollInterval queryId kafka { topic format keyColumn landingTimeColumn } "
    "outputs { type topic keyColumn bootstrapServers role } } "
    f"columns {{ {COLUMN_FIELDS} }}"
)
TABLES_QUERY = f"{{ tables {{ {TABLE_FIELDS} }} }}"

# Strings the editor sends as null when empty.
_TABLE_EMPTY_IS_NULL = (
    "alias",
    "description",
    "watermarkColumn",
    "changeSignal",
    "probeQuery",
    "probeType",
    "viewSql",
    "dqContract",
    "profilerSourceId",
    "queryTemplate",
    "fileGlob",
    "sourceFileColumn",
    "importSet",
    "landingFilter",
    "mvPreprocess",
    "mvCalendar",
    "mvGrain",
)
_TABLE_AS_IS = (
    "sourceId",
    "domainId",
    "schemaName",
    "tableName",
    "loadProtected",
    "offPeakWindow",
    "offPeakTz",
    "materialize",
    "mvRefreshInterval",
    "mvDebounceQuiet",
    "mvDebounceMaxDelay",
    "pushDebounceQuiet",
    "pushDebounceMaxDelay",
    "mvConsistency",
    "validTime",
    "recordedTime",
    "mvPersist",
    "mvIncremental",
    "mvAllowedLateness",
    "mvExpectedEvents",
    "mvBusinessDayGrain",
    "modelingRole",
    "modelingHistory",
    "enableAggregates",
    "enableGroupBy",
)
_COLUMN_EMPTY_IS_NULL = (
    "alias",
    "description",
    "dataType",
    "path",
    "nativeFilterType",
    "fake",
    "syntheticRule",
)
_COLUMN_AS_IS = (
    "embedding",
    "embeddingModel",
    "nativeFilterRequired",
    "isPrimaryKey",
    "isForeignKey",
    "isAlternateKey",
    "scope",
    "epochUnit",
    "fakeStable",
)


def column_input(c: Mapping[str, Any], masks: list[dict] | None = None) -> dict[str, Any]:
    """The ``ColumnInput`` that saves column ``c`` back, with ``masks`` in place of its own."""
    out: dict[str, Any] = {
        "name": c["columnName"],
        "visibleTo": list(c["visibleTo"] or []),
        "masks": [
            # The editor sends no why: the stored one stands (REQ-2094).
            {
                "to": list(e["to"]),
                "mask": None if e["mask"] is None else dict(e["mask"]),
                "why": None,
            }
            for e in (c["masks"] or [] if masks is None else masks)
        ],
    }
    out.update({k: c[k] or None for k in _COLUMN_EMPTY_IS_NULL})
    out.update({k: c[k] for k in _COLUMN_AS_IS})
    return out


def table_input(
    t: Mapping[str, Any], masks: Mapping[str, list[dict]] | None = None
) -> dict[str, Any]:
    """The ``TableInput`` that saves table ``t`` (as ``TABLES_QUERY`` reads it) back unchanged,
    but for the columns named in ``masks``, saved with those mask lists."""
    masks = masks or {}
    live = t["live"]
    out: dict[str, Any] = {k: t[k] or None for k in _TABLE_EMPTY_IS_NULL}
    out.update({k: t[k] for k in _TABLE_AS_IS})
    out.update(
        {
            "arguments": dict(t["arguments"] or {}),
            "businessKey": list(t["businessKey"] or []),
            # REQ-970: the MV row-identity key IS the table's primary key, as the editor derives it
            "mvPrimaryKey": [c["columnName"] for c in t["columns"] if c["isPrimaryKey"]],
            # REQ-1443 clause 10: a checker table's product is derived and refused on the row
            "productId": None if t["dqContract"] else t["productId"],
            "live": None
            if live is None
            else {
                "strategy": live["strategy"],
                "watermarkColumn": live["watermarkColumn"],
                "pollInterval": live["pollInterval"],
                "kafka": dict(live["kafka"])
                if live["strategy"] == "kafka" and live["kafka"] is not None
                else None,
                "queryId": live["queryId"],
                "outputs": [dict(o) for o in live["outputs"]],
            },
            "uniqueConstraints": [
                {"name": u["name"], "columns": list(u["columns"])} for u in t["uniqueConstraints"]
            ],
            "columns": [column_input(c, masks.get(c["columnName"])) for c in t["columns"]],
        }
    )
    return out


# --------------------------------------------------------------------------------------------
# The plan: what one transport's governance creates
# --------------------------------------------------------------------------------------------


def masked_columns(setup: cm.Setup, table: cm.Table, nullable: Mapping[str, bool], share: float):
    """The columns of ``table`` ``share`` of its columns masks, in the contract's order: nullable,
    and not read by a filter, a join or the base request."""
    wanted = round(share * len(nullable))
    if wanted == 0:
        return []
    joined = {
        col
        for j in (*setup.cross_source_joins, *(j for s in setup.sources.values() for j in s.joins))
        for tbl, col in ((j.left_table, j.left_column), (j.right_table, j.right_column))
        if tbl == table.name
    }
    eligible = [
        c.name
        for c in table.columns
        if nullable.get(c.name)
        and c.name != table.base_column
        and not c.filterable
        and c.name not in joined
    ]
    if len(eligible) < wanted:
        raise GovernanceError(
            f"table {table.name}: masked_column_share {share} masks {wanted} of its "
            f"{len(nullable)} columns, but only {len(eligible)} are nullable and read by no "
            f"filter, join or base request ({', '.join(eligible) or 'none'})"
        )
    return eligible[:wanted]


def rule_tag(table_id: int, index: int) -> str:
    return f"{cm.GOVERNANCE_PREFIX}_rule{index}_t{table_id}"


# --------------------------------------------------------------------------------------------
# The admin API
# --------------------------------------------------------------------------------------------


class AdminGovernance:
    """Sets up a transport's governance through the admin API and removes it afterwards.

    ``client`` is an httpx.Client for the deployment (its credential header set); ``role`` (the
    contract's first) must hold ``user_management`` (roles, derived roles), ``masking_config`` and
    ``view_governance`` (rules, masks), the tag editor's right and ``org_settings`` (redaction)."""

    def __init__(self, client: Any, role: str) -> None:
        self._client = client
        self._role = role

    # ---- calls
    def _headers(self) -> dict[str, str]:
        return {"X-Provisa-Role": self._role}

    def _graphql(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        resp = self._client.post(
            "/admin/graphql",
            json={"query": query, "variables": variables or {}},
            headers=self._headers(),
        )
        if resp.status_code != 200:
            raise GovernanceError(f"/admin/graphql answered {resp.status_code}: {resp.text[:300]}")
        body = resp.json()
        if body.get("errors"):
            raise GovernanceError(f"/admin/graphql: {str(body['errors'])[:300]}")
        return body["data"]

    def _mutate(self, name: str, args: str, call: str, variables: dict[str, Any]) -> None:
        query = f"mutation({args}) {{ {name}({call}) {{ success message code }} }}"
        result = self._graphql(query, variables)[name]
        if not result["success"]:
            raise GovernanceError(f"{name} failed: {result['message']} ({result['code']})")

    def _rest(self, method: str, path: str, body: Any = None) -> Any:
        resp = self._client.request(method, path, json=body, headers=self._headers())
        if resp.status_code >= 400:
            raise GovernanceError(f"{method} {path} answered {resp.status_code}: {resp.text[:300]}")
        return resp.json() if resp.content else None

    # ---- reads
    def _roles(self) -> dict[str, dict[str, Any]]:
        data = self._graphql("{ roles { id capabilities domainAccess parents } }")
        return {r["id"]: r for r in data["roles"]}

    def _tables(self) -> dict[int, dict[str, Any]]:
        return {t["id"]: t for t in self._graphql(TABLES_QUERY)["tables"]}

    def _derived(self) -> set[str]:
        return {d["attribute"] for d in self._rest("GET", "/admin/derived-roles")["roles"]}

    # ---- the steps, each returning how it is undone
    def _create_roles(self, setup: cm.Setup, depth: int) -> Callable[[], None]:
        roles = self._roles()
        created: list[str] = []

        def undo() -> None:
            for rid in reversed(created):
                self._mutate("deleteRole", "$id: String!", "id: $id", {"id": rid})

        try:
            for r in (r for r in setup.roles if r.weight > 0):
                top = roles.get(r.role)
                if top is None:
                    raise GovernanceError(f"role {r.role}: not a role of the deployment")
                for level in range(1, depth + 1):
                    rid = cm.chain_role(r.role, level)
                    if rid in roles:
                        raise GovernanceError(
                            f"role {rid} exists: left by an earlier run; delete it first"
                        )
                    self._mutate(
                        "createRole",
                        "$input: RoleInput!",
                        "input: $input",
                        {
                            "input": {
                                "id": rid,
                                "description": f"Benchmark governance: {level} below {r.role}",
                                "capabilities": list(top["capabilities"]),
                                "domainAccess": list(top["domainAccess"]),
                                "parents": [cm.chain_role(r.role, level - 1)],
                            }
                        },
                    )
                    created.append(rid)
        except BaseException:
            undo()
            raise
        return undo

    def _declare_derived(self, count: int) -> Callable[[], None]:
        existing = self._derived()
        declared: list[str] = []

        def undo() -> None:
            for attr in reversed(declared):
                self._rest("DELETE", f"/admin/derived-roles/{attr}")

        try:
            for i in range(1, count + 1):
                attr = cm.derived_attribute(i)
                if attr in existing:
                    raise GovernanceError(
                        f"derived role {attr} is declared: left by an earlier run; remove it first"
                    )
                self._rest(
                    "PUT",
                    f"/admin/derived-roles/{attr}",
                    {"description": f"Benchmark governance: derived role {i}"},
                )
                declared.append(attr)
        except BaseException:
            undo()
            raise
        return undo

    def _row_rules(
        self, tables: list[tuple[cm.Table, int]], roles: list[str], count: int
    ) -> Callable[[], None]:
        done: list[Callable[[], None]] = []

        def undo() -> None:
            for step in reversed(done):
                step()

        try:
            for table, tid in tables:
                for i in range(1, count + 1):
                    tag = rule_tag(tid, i)
                    self._mutate(
                        "createTag",
                        "$input: TagInput!",
                        "input: $input",
                        {
                            "input": {
                                "id": tag,
                                "description": f"Benchmark governance: row rule {i}",
                                "appliesTo": ["table"],
                            }
                        },
                    )
                    done.append(
                        lambda tag=tag: self._mutate(
                            "deleteTag", "$id: String!", "id: $id", {"id": tag}
                        )
                    )
                    assignment = {"tagId": tag, "objectType": "table", "tableId": tid}
                    self._mutate(
                        "assignTag",
                        "$input: TagAssignmentInput!",
                        "input: $input",
                        {"input": assignment},
                    )
                    done.append(
                        lambda a=assignment: self._mutate(
                            "unassignTag",
                            "$input: TagAssignmentInput!",
                            "input: $input",
                            {"input": a},
                        )
                    )
                    for role in roles:
                        self._mutate(
                            "upsertRlsRule",
                            "$input: RLSRuleInput!",
                            "input: $input",
                            {
                                "input": {
                                    "tagId": tag,
                                    "roleId": role,
                                    "filterExpr": f"{table.base_column} IS NOT NULL",
                                    "description": f"Benchmark governance: row rule {i}, every row",
                                }
                            },
                        )
                        done.append(
                            lambda tag=tag, role=role: self._mutate(
                                "deleteRlsRule",
                                "$roleId: String!, $tagId: String",
                                "roleId: $roleId, tagId: $tagId",
                                {"roleId": role, "tagId": tag},
                            )
                        )
        except BaseException:
            undo()
            raise
        return undo

    def _masks(
        self, setup: cm.Setup, tables: list[tuple[cm.Table, int]], roles: list[str], share: float
    ) -> tuple[Callable[[], None], dict[str, list[str]]]:
        stored = self._tables()
        saved: list[dict[str, Any]] = []
        masked: dict[str, list[str]] = {}

        def save(t: dict[str, Any], masks: Mapping[str, list[dict]]) -> None:
            self._mutate(
                "updateTable",
                "$input: TableInput!",
                "input: $input",
                {"input": table_input(t, masks)},
            )

        def undo() -> None:
            for t in reversed(saved):
                save(t, {})

        try:
            for table, tid in tables:
                t = stored[tid]
                # A column is nullable unless it is the primary key: the rule the compiled
                # schema types columns by (provisa/api/app_schema_build.py, is_nullable)
                nullable = {c["columnName"]: not c["isPrimaryKey"] for c in t["columns"]}
                names = masked_columns(setup, table, nullable, share)
                if not names:
                    continue
                own = {c["columnName"]: list(c["masks"] or []) for c in t["columns"]}
                save(t, {n: [{"to": roles, "mask": dict(NULL_MASK)}, *own[n]] for n in names})
                saved.append(t)
                masked[table.name] = names
        except BaseException:
            undo()
            raise
        return undo, masked

    def _redaction(self) -> Callable[[], None]:
        own = [
            {"name": r["name"], "pattern": r["pattern"], "reference": r["reference"]}
            for r in self._rest("GET", "/admin/redaction")["org"]
        ]
        if any(r["name"].startswith(cm.GOVERNANCE_PREFIX) for r in own):
            raise GovernanceError(
                "the org's redaction rules hold the bench's: left by an earlier run"
            )
        self._rest("PUT", "/admin/redaction", {"rules": [*own, *BENCH_REDACTION_RULES]})
        return lambda: self._rest("PUT", "/admin/redaction", {"rules": own})

    # ---- the whole
    @contextmanager
    def applied(
        self, setup: cm.Setup, transport: str, resolved: lookup.Resolved
    ) -> Generator[dict[str, Any]]:
        """Set up ``transport``'s governance for the block, and remove it after, whatever the
        block does. Yields what was set up, for the report."""
        g = setup.governance(transport)
        if not g.changes_deployment:
            yield {"governance": "none set up (every knob at zero)"}
            return
        roles = sorted({cm.chain_role(r.role, g.role_depth) for r in setup.roles if r.weight > 0})
        tables = [
            (t, resolved.tables[sc.table_identity(setup, sid, t).key].table_id)
            for sid, src in setup.sources.items()
            for t in src.tables
        ]
        undo: list[Callable[[], None]] = []
        report: dict[str, Any] = {"request_roles": roles}
        try:
            if g.role_depth:
                undo.append(self._create_roles(setup, g.role_depth))
            if g.derived_roles:
                undo.append(self._declare_derived(g.derived_roles))
            if g.row_rules_per_table:
                undo.append(self._row_rules(tables, roles, g.row_rules_per_table))
                report["row_rules_per_table"] = g.row_rules_per_table
            if g.masked_column_share:
                restore, masked = self._masks(setup, tables, roles, g.masked_column_share)
                undo.append(restore)
                report["masked_columns"] = masked
            if g.central_redaction:
                undo.append(self._redaction())
                report["central_redaction"] = [r["name"] for r in BENCH_REDACTION_RULES]
            yield report
        finally:
            failures = []
            for step in reversed(undo):
                try:
                    step()
                except GovernanceError as exc:
                    failures.append(str(exc))
            if failures:
                raise GovernanceError(
                    "governance not fully removed; remove by hand what is named: "
                    + "; ".join(failures)
                )


def check_telemetry(setup: cm.Setup, devices: Mapping[str, Any] | None) -> str:
    """The contract's telemetry against the run's device facts (``--device``: a device whose
    agent states ``--fact telemetry=on|off``): refused when one differs. Without such a fact, it is
    as declared, not checked."""
    declared = "on" if setup.knobs.governance.telemetry else "off"
    stated = {
        name: str(device["facts"]["telemetry"])
        for name, device in (devices or {}).items()
        if isinstance(device, Mapping) and "telemetry" in (device.get("facts") or {})
    }
    if not stated:
        return f"telemetry {declared}: as declared, not checked (no device states it)"
    wrong = {name: v for name, v in stated.items() if v != declared}
    if wrong:
        raise cm.SetupError(
            f"knobs.governance.telemetry is {declared}, but the server was started with telemetry "
            + ", ".join(f"{v} ({name})" for name, v in sorted(wrong.items()))
        )
    return f"telemetry {declared}: as the server states"
