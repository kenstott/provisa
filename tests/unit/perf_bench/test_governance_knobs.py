# Copyright (c) 2026 Kenneth Stott
# Canary: 42f40ed0-b789-426c-b9e0-4b540d68cfe4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2092: the benchmark contract's governance knobs -- parsed, sent as the request's role, set up
in the deployment for a transport's run and removed after it."""

from __future__ import annotations

import dataclasses
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import contract_model as cm  # noqa: E402
import deployment_fixture as fx  # noqa: E402
import governance  # noqa: E402
import lookup  # noqa: E402
import optimistic_load as ol  # noqa: E402
import request_mix  # noqa: E402
import setup_contract as sc  # noqa: E402

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001", "BENCH_TOKEN": "t"}
TRANSPORTS = list(ol.TRANSPORTS)
GOVERNED = {
    "row_rules_per_table": 2,
    "masked_column_share": 0.25,
    "central_redaction": True,
    "role_depth": 2,
    "derived_roles": 1,
}


def _raw() -> dict[str, Any]:
    return json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))


def _load(raw: dict[str, Any], tmp_path: Path) -> cm.Setup:
    path = tmp_path / "setup.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


def _fails(raw: dict[str, Any], tmp_path: Path, message: str) -> None:
    with pytest.raises(cm.SetupError) as exc:
        _load(raw, tmp_path)
    assert message in str(exc.value)


def _governed(raw: dict[str, Any], **knobs: Any) -> dict[str, Any]:
    raw["knobs"]["governance"].update(knobs)
    return raw


# ---------------------------------------------------------------- parsing


def test_every_governance_knob_at_zero_is_the_optimistic_request(tmp_path: Path) -> None:
    setup = _load(_raw(), tmp_path)
    for transport in setup.transports:
        assert setup.governance(transport) == cm.OPTIMISTIC_GOVERNANCE
        assert not setup.governance(transport).changes_deployment
        assert setup.request_role(transport, "org_admin") == "org_admin"


def test_the_knobs_are_read_as_declared(tmp_path: Path) -> None:
    setup = _load(_governed(_raw(), **GOVERNED, telemetry=True), tmp_path)
    assert setup.governance("graphql") == cm.Governance(2, 0.25, True, 2, 1, True)
    assert setup.governance("graphql").changes_deployment


def test_the_governance_section_is_required(tmp_path: Path) -> None:
    raw = _raw()
    del raw["knobs"]["governance"]
    _fails(raw, tmp_path, "knobs.governance: required")


@pytest.mark.parametrize(
    ("knob", "value", "message"),
    [
        ("row_rules_per_table", -1, "knobs.governance.row_rules_per_table"),
        (
            "masked_column_share",
            1.5,
            "knobs.governance.masked_column_share: must be between 0 and 1",
        ),
        ("central_redaction", "yes", "knobs.governance.central_redaction: must be true or false"),
        ("role_depth", 1.5, "knobs.governance.role_depth"),
        ("derived_roles", -2, "knobs.governance.derived_roles"),
        ("telemetry", 1, "knobs.governance.telemetry: must be true or false"),
    ],
)
def test_a_malformed_knob_is_refused_by_name(
    tmp_path: Path, knob: str, value: Any, message: str
) -> None:
    _fails(_governed(_raw(), **{knob: value}), tmp_path, message)


def test_an_unknown_governance_knob_is_refused(tmp_path: Path) -> None:
    _fails(_governed(_raw(), grants_expire=True), tmp_path, "unknown key 'grants_expire'")


def test_a_transport_override_governs_that_transport_only(tmp_path: Path) -> None:
    raw = _raw()
    override = {k: v for k, v in GOVERNED.items()}
    raw["transports"]["rest"] = {"knobs": {"governance": override}}
    setup = _load(raw, tmp_path)
    assert setup.governance("rest") == cm.Governance(2, 0.25, True, 2, 1, False)
    assert setup.governance("graphql") == cm.OPTIMISTIC_GOVERNANCE


def test_an_override_takes_its_telemetry_from_the_server(tmp_path: Path) -> None:
    raw = _governed(_raw(), telemetry=True)
    raw["transports"]["rest"] = {"knobs": {"governance": dict(GOVERNED)}}
    assert _load(raw, tmp_path).governance("rest").telemetry is True


def test_an_override_may_not_name_telemetry(tmp_path: Path) -> None:
    raw = _raw()
    raw["transports"]["rest"] = {"knobs": {"governance": {**GOVERNED, "telemetry": True}}}
    _fails(raw, tmp_path, "transports.rest.knobs.governance.telemetry: the server's own")


@pytest.mark.parametrize("knob", ["role_depth", "derived_roles"])
def test_a_created_role_needs_the_unsecured_header(tmp_path: Path, knob: str) -> None:
    raw = _governed(_raw(), **{knob: 1})
    raw["deployment"]["credentials"] = {"mode": "env", "kind": "token", "env": "BENCH_TOKEN"}
    _fails(raw, tmp_path, "need deployment.credentials.mode none")


# ---------------------------------------------------------------- the request's role


def test_the_request_acts_as_the_deepest_role_with_its_derived_roles_beside_it() -> None:
    g = cm.Governance(0, 0.0, False, 3, 2, False)
    assert cm.governed_role("org_admin", g) == (
        "bench_gov_org_admin_d3,bench_gov_attr1__v,bench_gov_attr2__v"
    )
    assert cm.governed_role("org_admin", cm.Governance(0, 0.0, False, 1, 0, False)) == (
        "bench_gov_org_admin_d1"
    )


def test_every_request_of_a_governed_transport_is_sent_as_the_governed_role(tmp_path: Path) -> None:
    raw = _raw()
    raw["transports"]["graphql"] = {"knobs": {"governance": {**GOVERNED, "derived_roles": 0}}}
    setup = _load(raw, tmp_path)
    governed = request_mix.RequestGenerator(setup, "graphql", "s", cacheable=True)
    plain = request_mix.RequestGenerator(setup, "data_sql", "s", cacheable=True)
    assert {governed.next().role for _ in range(20)} == {"bench_gov_org_admin_d2"}
    assert {plain.next().role for _ in range(20)} == {"org_admin"}
    assert governed.base_spec(True, "org_admin").role == "bench_gov_org_admin_d2"


def test_the_request_itself_is_unchanged_by_governance(tmp_path: Path) -> None:
    raw = _raw()
    raw["transports"]["graphql"] = {"knobs": {"governance": dict(GOVERNED)}}
    governed = _load(raw, tmp_path)
    plain = _load(_raw(), tmp_path)
    a = request_mix.RequestGenerator(governed, "graphql", "s", cacheable=True)
    b = request_mix.RequestGenerator(plain, "graphql", "s", cacheable=True)
    for _ in range(10):
        x, y = a.next(), b.next()
        assert dataclasses.replace(x, role=None) == dataclasses.replace(y, role=None)


# ---------------------------------------------------------------- the deployment's side


class _Resp:
    def __init__(self, status: int, body: Any) -> None:
        self.status_code = status
        self._body = body
        self.text = json.dumps(body)
        self.content = self.text.encode() if body is not None else b""

    def json(self) -> Any:
        return self._body


def _column(name: str, masks: list | None = None) -> dict[str, Any]:
    return {
        "columnName": name,
        "visibleTo": ["org_admin"],
        "alias": "",
        "description": "",
        "dataType": "",
        "path": "",
        "embedding": False,
        "embeddingModel": None,
        "nativeFilterType": "",
        "nativeFilterRequired": None,
        "isPrimaryKey": name.endswith("_id") and name != "customer_id",
        "isForeignKey": False,
        "isAlternateKey": False,
        "scope": "domain",
        "epochUnit": None,
        "fake": "",
        "fakeStable": False,
        "syntheticRule": "",
        "masks": masks or [],
    }


def _table(tid: int, columns: list[dict[str, Any]]) -> dict[str, Any]:
    t: dict[str, Any] = {k: None for k in governance._TABLE_EMPTY_IS_NULL}
    t.update({k: None for k in governance._TABLE_AS_IS})
    t.update(
        {
            "id": tid,
            "sourceId": "src",
            "domainId": "bench",
            "schemaName": "public",
            "tableName": f"t{tid}",
            "arguments": None,
            "businessKey": [],
            "productId": None,
            "live": None,
            "uniqueConstraints": [],
            "columns": columns,
            # the server's own values where its table type declares one (types.py TableType)
            "materialize": False,
            "mvRefreshInterval": 300,
            "mvDebounceQuiet": 0.0,
            "mvDebounceMaxDelay": 5.0,
            "pushDebounceQuiet": 0.0,
            "pushDebounceMaxDelay": 5.0,
            "mvConsistency": "shared",
            "mvPersist": "replace",
            "mvIncremental": False,
            "mvAllowedLateness": 0.0,
            "mvBusinessDayGrain": False,
            "enableAggregates": False,
            "enableGroupBy": False,
        }
    )
    return t


def _valid_against_the_admin_schema(query: str, variables: dict[str, Any] | None) -> None:
    """What the bench sends is what the deployment's admin schema takes: its fields, arguments and
    each variable's value (a table save's whole input among them)."""
    from graphql import OperationDefinitionNode, parse, validate
    from graphql.execution.values import get_variable_values

    from provisa.api.admin.schema import admin_schema

    schema = admin_schema._schema
    document = parse(query)
    errors = validate(schema, document)
    assert not errors, f"{query[:200]}: {errors}"
    op = next(d for d in document.definitions if isinstance(d, OperationDefinitionNode))
    coerced = get_variable_values(schema, op.variable_definitions or (), variables or {})
    assert not isinstance(coerced, list), f"{query[:200]}: {coerced}"


class _Admin:
    """The deployment's admin API, recording each change."""

    def __init__(self, tables: dict[int, dict[str, Any]], fail_on: str | None = None) -> None:
        self.tables = tables
        self.fail_on = fail_on
        self.calls: list[tuple[str, Any]] = []
        self.roles = {
            "org_admin": {
                "id": "org_admin",
                "capabilities": ["query_development"],
                "domainAccess": ["*"],
                "parentRoleId": None,
            }
        }
        self.derived: list[str] = []
        self.org_rules = [{"name": "own", "pattern": "x+", "reference": None, "source": "org"}]

    def post(self, path: str, json: dict[str, Any], headers: dict[str, str]) -> _Resp:
        assert path == "/admin/graphql" and headers == {"X-Provisa-Role": "org_admin"}
        query = json["query"]
        _valid_against_the_admin_schema(query, json.get("variables"))
        if query.startswith("{ roles"):
            return _Resp(200, {"data": {"roles": list(self.roles.values())}})
        if query.startswith("{ tables"):
            return _Resp(200, {"data": {"tables": list(self.tables.values())}})
        name = query.split("{ ", 1)[1].split("(", 1)[0]
        self.calls.append((name, json["variables"]))
        if name == self.fail_on:
            return _Resp(200, {"data": {name: {"success": False, "message": "no", "code": "x"}}})
        if name == "createRole":
            self.roles[json["variables"]["input"]["id"]] = json["variables"]["input"]
        return _Resp(200, {"data": {name: {"success": True, "message": "", "code": None}}})

    def request(self, method: str, path: str, json: Any, headers: dict[str, str]) -> _Resp:
        self.calls.append((f"{method} {path}", json))
        if path == "/admin/derived-roles":
            return _Resp(200, {"roles": [{"attribute": a} for a in self.derived], "source": "org"})
        if path == "/admin/redaction" and method == "GET":
            return _Resp(200, {"org": self.org_rules})
        return _Resp(200, None)


def _resolved(setup: cm.Setup) -> lookup.Resolved:
    return lookup.resolve(*sc.identities(setup), fx.build())


def _governed_setup(tmp_path: Path, **knobs: Any) -> cm.Setup:
    """The perf contract with graphql governed by ``knobs`` alone (the rest at zero)."""
    raw = _raw()
    override = {**dict.fromkeys(GOVERNED, 0), "central_redaction": False, **knobs}
    raw["transports"]["graphql"] = {"knobs": {"governance": override}}
    return _load(raw, tmp_path)


def _deployment_tables(setup: cm.Setup, resolved: lookup.Resolved) -> dict[int, dict[str, Any]]:
    """Every contract table as the admin ``tables`` query answers it, its columns nullable."""
    out = {}
    for sid, src in setup.sources.items():
        for t in src.tables:
            tid = resolved.tables[sc.table_identity(setup, sid, t).key].table_id
            out[tid] = _table(tid, [_column(c.name) for c in t.columns])
    return out


def test_zero_governance_touches_nothing(tmp_path: Path) -> None:
    setup = _load(_raw(), tmp_path)
    admin = _Admin({})
    with governance.AdminGovernance(admin, "org_admin").applied(
        setup, "graphql", _resolved(setup)
    ) as report:
        assert report == {"governance": "none set up (every knob at zero)"}
    assert admin.calls == []


def test_roles_are_created_parent_first_and_removed_leaf_first(tmp_path: Path) -> None:
    setup = _governed_setup(tmp_path, role_depth=2)
    admin = _Admin({})
    with governance.AdminGovernance(admin, "org_admin").applied(
        setup, "graphql", _resolved(setup)
    ) as report:
        assert report["request_roles"] == ["bench_gov_org_admin_d2"]
        created = [v["input"] for n, v in admin.calls if n == "createRole"]
        assert [(r["id"], r["parentRoleId"]) for r in created] == [
            ("bench_gov_org_admin_d1", "org_admin"),
            ("bench_gov_org_admin_d2", "bench_gov_org_admin_d1"),
        ]
        assert all(r["capabilities"] == ["query_development"] for r in created)
    deleted = [v["id"] for n, v in admin.calls if n == "deleteRole"]
    assert deleted == ["bench_gov_org_admin_d2", "bench_gov_org_admin_d1"]


def test_a_role_left_by_an_earlier_run_is_refused_by_name(tmp_path: Path) -> None:
    setup = _governed_setup(tmp_path, role_depth=1)
    admin = _Admin({})
    admin.roles["bench_gov_org_admin_d1"] = {"id": "bench_gov_org_admin_d1"}
    with pytest.raises(governance.GovernanceError, match="bench_gov_org_admin_d1 exists"):
        with governance.AdminGovernance(admin, "org_admin").applied(
            setup, "graphql", _resolved(setup)
        ):
            pass


def test_derived_roles_are_declared_and_removed(tmp_path: Path) -> None:
    setup = _governed_setup(tmp_path, derived_roles=2)
    admin = _Admin({})
    with governance.AdminGovernance(admin, "org_admin").applied(setup, "graphql", _resolved(setup)):
        pass
    changes = [n for n, _ in admin.calls if not n.startswith("GET")]
    assert changes == [
        "PUT /admin/derived-roles/bench_gov_attr1",
        "PUT /admin/derived-roles/bench_gov_attr2",
        "DELETE /admin/derived-roles/bench_gov_attr2",
        "DELETE /admin/derived-roles/bench_gov_attr1",
    ]


def test_each_table_gets_its_row_rules_each_keeping_every_row(tmp_path: Path) -> None:
    setup = _governed_setup(tmp_path, row_rules_per_table=2)
    resolved = _resolved(setup)
    admin = _Admin({})
    with governance.AdminGovernance(admin, "org_admin").applied(setup, "graphql", resolved):
        rules = [v["input"] for n, v in admin.calls if n == "upsertRlsRule"]
    tables = [t for src in setup.sources.values() for t in src.tables]
    assert len(rules) == 2 * len(tables)
    orders = next(t for t in tables if t.name == "orders")
    oid = resolved.tables[sc.table_identity(setup, "bench-postgresql", orders).key].table_id
    assert [r for r in rules if r["tagId"].endswith(f"_t{oid}")] == [
        {
            "tagId": f"bench_gov_rule{i}_t{oid}",
            "roleId": "org_admin",
            "filterExpr": "order_id IS NOT NULL",
            "description": f"Benchmark governance: row rule {i}, every row",
        }
        for i in (1, 2)
    ]
    assigned = [v["input"] for n, v in admin.calls if n == "assignTag"]
    assert {"tagId": f"bench_gov_rule1_t{oid}", "objectType": "table", "tableId": oid} in assigned
    names = [n for n, _ in admin.calls]
    for made, undone in (
        ("createTag", "deleteTag"),
        ("assignTag", "unassignTag"),
        ("upsertRlsRule", "deleteRlsRule"),
    ):
        assert names.count(made) == names.count(undone) > 0


def _plain(masks: list[dict]) -> list[dict]:
    """Mask entries as the bench saves them, without the empty why the editor sends with each
    (REQ-2094: no why sent keeps the stored one)."""
    return [{k: v for k, v in e.items() if not (k == "why" and v is None)} for e in masks]


def test_masks_go_first_on_the_columns_no_filter_or_join_reads_and_are_put_back(
    tmp_path: Path,
) -> None:
    setup = _governed_setup(tmp_path, masked_column_share=0.25)
    resolved = _resolved(setup)
    tables = _deployment_tables(setup, resolved)
    orders = next(t for src in setup.sources.values() for t in src.tables if t.name == "orders")
    oid = resolved.tables[sc.table_identity(setup, "bench-postgresql", orders).key].table_id
    own = [{"to": ["analyst"], "mask": None}]
    tables[oid]["columns"][-1]["masks"] = own  # amount: an analyst sees it unmasked
    admin = _Admin(tables)
    with governance.AdminGovernance(admin, "org_admin").applied(
        setup, "graphql", resolved
    ) as report:
        pass
    saves = [v["input"] for n, v in admin.calls if n == "updateTable"]
    # orders has 5 columns: a quarter is 1; order_id is the base, customer_id, region and status
    # are filtered, so amount is the one
    assert report["masked_columns"]["orders"] == ["amount"]
    first = next(s for s in saves if s["tableName"] == f"t{oid}")
    amount = next(c for c in first["columns"] if c["name"] == "amount")
    assert _plain(amount["masks"]) == [{"to": ["org_admin"], "mask": governance.NULL_MASK}, *own]
    assert all(c["masks"] == [] for c in first["columns"] if c["name"] != "amount")
    restored = [s for s in saves if s["tableName"] == f"t{oid}"][-1]
    assert _plain(next(c for c in restored["columns"] if c["name"] == "amount")["masks"]) == own


def test_too_few_maskable_columns_is_refused_by_name(tmp_path: Path) -> None:
    setup = _governed_setup(tmp_path, masked_column_share=1.0)
    resolved = _resolved(setup)
    admin = _Admin(_deployment_tables(setup, resolved))
    with pytest.raises(governance.GovernanceError, match="masked_column_share 1.0 masks"):
        with governance.AdminGovernance(admin, "org_admin").applied(setup, "graphql", resolved):
            pass


def test_central_redaction_extends_the_org_rules_and_restores_them(tmp_path: Path) -> None:
    setup = _governed_setup(tmp_path, central_redaction=True)
    admin = _Admin({})
    with governance.AdminGovernance(admin, "org_admin").applied(setup, "graphql", _resolved(setup)):
        pass
    puts = [body for n, body in admin.calls if n == "PUT /admin/redaction"]
    own = [{"name": "own", "pattern": "x+", "reference": None}]
    assert puts == [{"rules": [*own, *governance.BENCH_REDACTION_RULES]}, {"rules": own}]


def test_everything_set_up_is_removed_when_a_step_fails(tmp_path: Path) -> None:
    setup = _governed_setup(tmp_path, role_depth=1, derived_roles=1, row_rules_per_table=1)
    admin = _Admin({}, fail_on="upsertRlsRule")
    with pytest.raises(governance.GovernanceError, match="upsertRlsRule failed"):
        with governance.AdminGovernance(admin, "org_admin").applied(
            setup, "graphql", _resolved(setup)
        ):
            pytest.fail("the block runs only once everything is set up")
    names = [n for n, _ in admin.calls]
    assert names.count("deleteRole") == 1
    assert "DELETE /admin/derived-roles/bench_gov_attr1" in names
    assert names.count("deleteTag") == names.count("createTag") == 1
    assert names.count("unassignTag") == names.count("assignTag") == 1


def test_everything_is_removed_when_the_run_fails(tmp_path: Path) -> None:
    setup = _governed_setup(tmp_path, role_depth=1, row_rules_per_table=1)
    admin = _Admin({})
    with pytest.raises(RuntimeError, match="the run"):
        with governance.AdminGovernance(admin, "org_admin").applied(
            setup, "graphql", _resolved(setup)
        ):
            raise RuntimeError("the run")
    names = [n for n, _ in admin.calls]
    assert names.count("deleteRole") == 1
    assert names.count("deleteRlsRule") == names.count("upsertRlsRule") > 0


# ---------------------------------------------------------------- the table save


def _snake(name: str) -> str:
    return "".join(f"_{c.lower()}" if c.isupper() else c for c in name)


def _as_read_type(value: Any) -> Any:
    """``value`` (the ``tables`` query's JSON) as the admin read type the MCP mapper takes."""
    if isinstance(value, dict):
        return SimpleNamespace(
            **{
                # a JSON scalar (the table's arguments) stays a mapping
                _snake(k): v if k == "arguments" else _as_read_type(v)
                for k, v in value.items()
            }
        )
    if isinstance(value, list):
        return [_as_read_type(v) for v in value]
    return value


def _camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(p.title() for p in rest)


def _as_json(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {_camel(f.name): _as_json(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, list):
        return [_as_json(v) for v in value]
    if isinstance(value, dict):
        return {k: _as_json(v) for k, v in value.items()}
    return value


def test_the_table_save_is_the_editor_s(tmp_path: Path) -> None:
    """The bench saves a table back as the table editor does (provisa/api/mcp/table_edit.py
    table_input): every field the editor sends, as it sends it."""
    from provisa.api.mcp import table_edit

    t = _table(
        7,
        [
            _column("order_id"),
            _column("amount", masks=[{"to": ["a"], "mask": dict(governance.NULL_MASK)}]),
        ],
    )
    t.update(
        {
            "alias": "Orders",
            "description": "every order",
            "arguments": {"since": "2026-01-01"},
            "materialize": True,
            "businessKey": ["order_id"],
            "enableAggregates": True,
            "uniqueConstraints": [{"name": "u", "columns": ["order_id"]}],
            "live": {
                "strategy": "kafka",
                "watermarkColumn": None,
                "pollInterval": 10,
                "queryId": None,
                "kafka": {
                    "topic": "orders",
                    "format": "json",
                    "keyColumn": "order_id",
                    "landingTimeColumn": "landed_at",
                },
                "outputs": [
                    {
                        "type": "sse",
                        "topic": None,
                        "keyColumn": None,
                        "bootstrapServers": None,
                        "role": None,
                    }
                ],
            },
        }
    )
    read = _as_read_type(t)
    read.stored_product_id = t["productId"]
    from provisa.api.admin.types import ColumnInput, TableInput

    def _defaults(input_type: type) -> dict[str, Any]:
        """What a field holds when it is left out: the input's own default."""
        return {
            _camel(f.name): f.default
            for f in dataclasses.fields(input_type)
            if f.default is not dataclasses.MISSING
        }

    def _sent(fields: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
        return {
            k: v
            for k, v in fields.items()
            if v not in (None, [], {}) and not (k in defaults and v == defaults[k])
        }

    table_defaults, column_defaults = _defaults(TableInput), _defaults(ColumnInput)

    editor = _sent(_as_json(table_edit.table_input(read)), table_defaults)
    ours = _sent(governance.table_input(t), table_defaults)
    editor_columns = editor.pop("columns")
    our_columns = ours.pop("columns")
    assert ours == editor
    for ec, oc in zip(editor_columns, our_columns, strict=True):
        assert _sent(oc, column_defaults) == _sent(ec, column_defaults)


# ---------------------------------------------------------------- telemetry and the run


def test_telemetry_is_checked_against_what_the_server_states(tmp_path: Path) -> None:
    setup = _load(_raw(), tmp_path)
    assert "not checked" in governance.check_telemetry(setup, None)
    devices = {"server": {"facts": {"telemetry": "off"}}, "network": {}}
    assert governance.check_telemetry(setup, devices) == "telemetry off: as the server states"
    with pytest.raises(cm.SetupError, match="started with telemetry on"):
        governance.check_telemetry(setup, {"server": {"facts": {"telemetry": "on"}}})


def test_a_governed_transport_runs_inside_its_governance_and_reports_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    setup = _governed_setup(tmp_path, row_rules_per_table=1)
    seen: list[str] = []

    def _run(transport: str, *a: Any) -> dict[str, Any]:
        seen.append(f"run {transport}")
        return {
            "transport": transport,
            "cached": True,
            "cache_probability": 1.0,
            "shape": {"rows": 1, "columns": 1},
            "steps": [],
        }

    from contextlib import contextmanager

    @contextmanager
    def _govern(transport: str):
        seen.append(f"set up {transport}")
        yield {"request_roles": ["org_admin"]}
        seen.append(f"removed {transport}")

    monkeypatch.setattr(ol, "run_transport", _run)
    reports = ol.run_all(
        ol.endpoints_from_setup(setup),
        tmp_path / "out",
        ["graphql"],
        (1,),
        1.0,
        1,
        None,
        govern=_govern,
    )
    assert seen == ["set up graphql", "run graphql", "removed graphql"]
    assert reports[0]["governance"]["row_rules_per_table"] == 1
    assert reports[0]["governance"]["request_roles"] == ["org_admin"]


def test_a_governed_transport_needs_the_admin_api(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    setup = _governed_setup(tmp_path, row_rules_per_table=1)
    monkeypatch.setattr(ol, "run_transport", lambda t, *a: {"transport": t, "steps": []})
    with pytest.raises(cm.SetupError, match="transports.graphql: its governance is set up"):
        ol.run_all(
            ol.endpoints_from_setup(setup), tmp_path / "out", ["graphql"], (1,), 1.0, 1, None
        )


def test_a_three_device_run_states_the_server_s_telemetry_in_its_contract(tmp_path: Path) -> None:
    import three_device

    text = (BENCH / "setups" / "three-device.yaml").read_text()
    on = yaml.safe_load(three_device.run_contract(text, telemetry="on"))
    off = yaml.safe_load(three_device.run_contract(text, telemetry="off"))
    assert on["knobs"]["governance"]["telemetry"] is True
    assert off["knobs"]["governance"]["telemetry"] is False
    on["knobs"]["governance"]["telemetry"] = False
    assert on == off == yaml.safe_load(text)
    with pytest.raises(ValueError, match="telemetry must be one of off, on"):
        three_device.run_contract(text, telemetry="maybe")
