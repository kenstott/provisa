# Copyright (c) 2026 Kenneth Stott
# Canary: 5c9e1a7d-3f62-4b84-9d0a-e6b2f8c47a19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-413: the discovery run's API on a real model store (SQLite), the model faked at its client
boundary and the source tables' reads faked at the governed read the profiler makes as the org
admin: system domains never included, a never-profiled table classified for the run and nothing
recorded, the request count before Run for every table a domain can hold (one that requires a
parameter and is never read, one with no column, one that cannot be read), and the run's model
chosen for that run only."""

# Requirements: REQ-413, REQ-464, REQ-1986, REQ-2147

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from sqlalchemy import func, insert, select

from provisa.api.admin import discovery
from provisa.api.errors import ApiError
from provisa.compiler.naming import domain_to_sql_name
from provisa.core import schema_org as so
from provisa.core.database import Database, create_engine_from_url
from provisa.core.parameters import ParameterRequired, missing_reason
from provisa.core.read_refusal import SourceUnavailable
from provisa.discovery import gather
from provisa.discovery.columns import NOT_READ, ColumnFacts, classes_meet
from provisa.discovery.direction import Sampled
from provisa.llm.client import Completion
from provisa.profiler.run import ProfileError

_REQUEST = cast(
    Any, SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id="steward-1")))
)
_TABLES = (
    "sources",
    "domains",
    "glossary_terms",
    "registered_tables",
    "table_columns",
    "tag_assignments",
    "relationships",
    "relationship_candidates",
    "profiler_classifications",
    "glossary_term_refs",
    "rls_rules",
    "roles",
    "data_products",
    "discovery_runs",
    "glossary_term_domains",
    "glossary_term_edges",
    "glossary_term_experts",
)
_KEY_ENVS = ("ANTHROPIC_API_KEY", "OPENAI_API_KEY", "MISTRAL_API_KEY")


#: Each registered table as the org admin reads it, by id: its domain and its columns' names.
_PUBLISHED: dict[int, tuple[str, list[str]]] = {}


@pytest.fixture(autouse=True)
def _nothing_published():
    _PUBLISHED.clear()


class _Reads:
    """The seam every read of a table's values goes through: the profiler's governed read as the
    org admin. Every column read holds the values given. A table in ``never_read`` requires a
    parameter: it has no row count, no profile and no values here, any read of it is recorded in
    ``attempted`` and refused as the pipeline refuses a read that gives a required parameter no
    value. A table in ``failing`` fails as given; a table in ``unpublished`` is not in the org
    admin's schema; a column in ``outside`` is registered and not held by the relation read."""

    def __init__(self, values: dict[str, list[Any]]):
        self.values = values
        self.sql: list[str] = []
        self.never_read: set[str] = set()
        self.attempted: list[str] = []
        self.failing: dict[str, Exception] = {}
        self.unpublished: set[str] = set()
        self.outside: set[str] = set()

    def refuse(self, table: str) -> None:
        if table in self.never_read:
            self.attempted.append(table)
            raise ParameterRequired(table, missing_reason(["orderId"]))
        if table in self.failing:
            raise self.failing[table]

    def resolve_target(self, state: Any, table_id: int, table_name: str, tags: Any) -> Any:
        if table_name in self.never_read:
            self.attempted.append(table_name)  # resolved only to be read
        if table_name in self.unpublished:
            raise ProfileError(f"table {table_name!r} (id {table_id}) is not readable by org_admin")
        domain, columns = _PUBLISHED[table_id]
        return SimpleNamespace(
            pgwire_name=f"{domain_to_sql_name(domain)}.{table_name}",
            columns=[
                SimpleNamespace(physical=c, name=c.lower())
                for c in columns
                if c not in self.outside
            ],
        )

    async def governed(self, sql: str) -> tuple[list[str], list[tuple]]:
        self.sql.append(sql)
        self.refuse(sql.split(' FROM "', 1)[1].split('"."', 1)[1].split('"', 1)[0])
        cols = sql.split("SELECT ", 1)[1].split(" FROM", 1)[0]
        names = [c.strip().strip('"') for c in cols.split(",")]
        depth = max(len(self.values.get(n, [])) for n in names)
        rows = [tuple(self.values.get(n, [None] * depth)[i] for n in names) for i in range(depth)]
        return names, rows


async def _store(url: str = "sqlite+pysqlite:///:memory:") -> Database:
    db = Database(create_engine_from_url(url), name="discovery-run")
    with db.engine.begin() as conn:
        so.metadata.create_all(conn, tables=[so.metadata.tables[n] for n in _TABLES])
    async with db.acquire() as conn:
        await conn.execute_core(insert(so.sources).values(id="pg", type="postgresql"))
        for d in ("sales", "crm", "pet-store", "meta", "ops", "ontology"):
            await conn.execute_core(insert(so.domains).values(id=d))
    return db


async def _table(db: Database, tid: int, domain: str, name: str, columns: list[tuple]) -> None:
    """``columns``: (name, type, primary key), or with a fourth, the kind of the required
    parameter the column is."""
    _PUBLISHED[tid] = (domain, [c[0] for c in columns])
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(so.registered_tables).values(
                id=tid, source_id="pg", domain_id=domain, schema_name="public", table_name=name
            )
        )
        for col_name, data_type, pk, *parameter in columns:
            await conn.execute_core(
                insert(so.table_columns).values(
                    table_id=tid,
                    domain_id=domain,
                    column_name=col_name,
                    data_type=data_type,
                    is_primary_key=pk,
                    native_filter_type=parameter[0] if parameter else None,
                    native_filter_required=True if parameter else None,
                )
            )


class _World:
    def __init__(self, monkeypatch, db: Database, *, config: dict, api_keys: dict[str, str]):
        self.db = db
        self.reads = _Reads(
            {"id": [1, 2, 3], "customer_id": [1, 1, 2], "name": ["a", "b", "c"], "ref": ["x"]}
        )
        self.classified: list[dict] = []
        self.model_calls: list[tuple[str, str, int]] = []
        self.answers: list[str] = []
        self.on_call: Any = None
        self.launched: list[tuple] = []
        self.fk_found: list[Any] = []
        for env in _KEY_ENVS:
            monkeypatch.delenv(env, raising=False)
        monkeypatch.setattr(discovery, "require_capability_request", lambda request, cap: None)
        monkeypatch.setattr(
            discovery,
            "state",
            SimpleNamespace(model_db=db, source_pools={}, source_types={}),
        )

        async def profiled(conn, table_id, table_name):
            if table_name in self.reads.never_read:
                self.reads.attempted.append(table_name)  # it has no profile to look up
            return None  # never profiled: classified during the run

        async def classify_table(state, **kwargs):
            self.classified.append(kwargs)
            self.reads.refuse(kwargs["table_name"])
            return []

        async def org_config(pool):
            return config

        async def org_keys(pool):
            return dict(api_keys)

        async def no_jev(pool):
            return None

        def respond(client, vendor, model, prompt, system, max_tokens, *args, **kwargs):
            self.model_calls.append((vendor, model, max_tokens))
            if self.on_call is not None:
                self.on_call(len(self.model_calls))
            return Completion(
                self.answers.pop(0) if self.answers else "[]", vendor, model, None, False
            )

        async def fk(source_pools, source_types, conn, table_ids):
            return [c for c in self.fk_found if c.source_table_id in table_ids]

        monkeypatch.setattr(discovery, "_launch", lambda *args: self.launched.append(args))
        monkeypatch.setattr("provisa.discovery.run.collect_fk_for_tables", fk)
        monkeypatch.setattr(gather, "_profiled_classes", profiled)
        monkeypatch.setattr("provisa.profiler.run.classify_table", classify_table)
        monkeypatch.setattr("provisa.profiler.run.resolve_target", self.reads.resolve_target)
        monkeypatch.setattr("provisa.profiler.run._governed", self.reads.governed)
        monkeypatch.setattr("provisa.core.org_settings.resolve_org_config", org_config)
        monkeypatch.setattr("provisa.core.org_secrets.read_org_api_keys", org_keys)
        monkeypatch.setattr("provisa.core.org_secrets.resolve_jev_api_key", no_jev)
        monkeypatch.setattr("provisa.llm.client.ProvisaLLMClient._respond", respond)


_ASSIGNED = {"ai_models": {"relationship_inference": {"vendor": "openai", "model": "gpt-test"}}}


def _scope(**over: Any) -> discovery.RunScope:
    body: dict[str, Any] = {"kinds": ["semantics"], "source_domains": ["sales", "crm"]}
    body.update(over)
    return discovery.RunScope.model_validate(body)


async def _orders_customers(db: Database) -> None:
    await _table(
        db, 1, "sales", "orders", [("id", "integer", True), ("customer_id", "integer", False)]
    )
    await _table(db, 2, "crm", "customers", [("id", "integer", True), ("name", "text", False)])


# --- system domains --------------------------------------------------------------------------------


async def test_system_domains_are_never_included_even_when_asked():
    db = await _store()
    await _orders_customers(db)
    await _table(db, 3, "meta", "tables", [("id", "integer", True)])
    await _table(db, 4, "ops", "queries", [("id", "integer", True)])
    await _table(db, 5, "ontology", "terms", [("id", "integer", True)])
    async with db.acquire() as conn:
        by_domain = await gather.scope_table_ids(conn, ["sales", "meta", "ops", "ontology"], None)
        by_table = await gather.scope_table_ids(conn, [], [1, 3, 4, 5])
    assert by_domain == [1]
    assert by_table == [1]


async def test_tables_default_to_every_table_of_the_chosen_domains():
    db = await _store()
    await _orders_customers(db)
    async with db.acquire() as conn:
        assert await gather.scope_table_ids(conn, ["sales", "crm"], None) == [1, 2]
        assert await gather.scope_table_ids(conn, ["sales", "crm"], [2]) == [2]


# --- classification during the run ------------------------------------------------------------------


async def test_a_never_profiled_table_is_classified_with_the_profiler_defaults_and_nothing_is_recorded(
    monkeypatch,
):
    db = await _store()
    await _orders_customers(db)
    world = _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k"})
    await discovery.count_run(_REQUEST, _scope())
    assert [
        (c["table_id"], c["values_per_column"], c["sample_above_cells"]) for c in world.classified
    ] == [
        (1, 500, None),
        (2, 500, None),
    ]
    async with db.acquire() as conn:
        recorded = (
            await conn.execute_core(select(func.count()).select_from(so.profiler_classifications))
        ).scalar_one()
    assert recorded == 0


# --- the request count ---------------------------------------------------------------------------------


async def test_the_count_is_given_by_group_before_any_model_call(monkeypatch):
    db = await _store()
    await _orders_customers(db)
    world = _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k"})
    counted = await discovery.count_run(_REQUEST, _scope())
    assert world.model_calls == []
    assert counted["requests"] == 1
    assert counted["by_group"] == {"whole_number": 1}
    assert counted["model"]["vendor"] == "openai" and counted["model"]["model"] == "gpt-test"
    assert counted["suggestions_per_request"] == 4_096 // 150


async def test_foreign_keys_alone_count_no_request(monkeypatch):
    db = await _store()
    await _orders_customers(db)
    _World(monkeypatch, db, config=_ASSIGNED, api_keys={})
    counted = await discovery.count_run(_REQUEST, _scope(kinds=["foreign_keys"]))
    assert counted["requests"] == 0 and counted["model"] is None


async def test_the_count_is_sized_from_the_chosen_models_limits(monkeypatch):
    db = await _store()
    await _orders_customers(db)
    config = {
        **_ASSIGNED,
        "ai_models": {
            **_ASSIGNED["ai_models"],
            "model_limits": {"mistral": {"tiny": {"input_tokens": 650, "output_tokens": 300}}},
        },
    }
    _World(monkeypatch, db, config=config, api_keys={"openai": "k", "mistral": "m"})
    assigned = await discovery.count_run(_REQUEST, _scope())
    chosen = await discovery.count_run(_REQUEST, _scope(vendor="mistral", model="tiny"))
    assert chosen["model"]["input_tokens"] == 650 and chosen["model"]["output_tokens"] == 300
    assert chosen["suggestions_per_request"] == 2
    assert chosen["requests"] > assigned["requests"]


# --- a table that requires a parameter: never read, reduced information (REQ-1986) ----------------

_ORDER_BY_ID, _ORDERS, _PET_BY_ID, _PETS = 1, 2, 3, 4
_PARAMETERIZED = {"get_order_by_id", "get_pet_by_id"}


async def _pet_store(db: Database) -> None:
    """A domain as the default demo's pet-store: its id hyphenated, two API tables that each
    require a parameter beside two tables read whole."""
    await _table(
        db,
        _ORDER_BY_ID,
        "pet-store",
        "get_order_by_id",
        [
            ("_nf_orderId", "integer", False, "path_param"),
            ("id", "integer", True),
            ("petId", "integer", False),
        ],
    )
    await _table(
        db, _ORDERS, "pet-store", "orders", [("id", "integer", True), ("pet_id", "integer", False)]
    )
    await _table(
        db,
        _PET_BY_ID,
        "pet-store",
        "get_pet_by_id",
        [("_nf_petId", "integer", False, "path_param"), ("name", "text", False)],
    )
    await _table(db, _PETS, "pet-store", "pets", [("id", "integer", True), ("name", "text", False)])


def _pet_world(monkeypatch, db: Database) -> _World:
    world = _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k"})
    world.reads.never_read = set(_PARAMETERIZED)
    return world


async def _facts(db: Database, ids: list[int]) -> dict[str, Any]:
    async with db.acquire() as conn:
        tables = await gather.table_facts(discovery.state, cast(Any, conn), ids)
    return {t.table_name: t for t in tables}


async def test_the_count_is_answered_for_a_scope_holding_a_table_that_requires_a_parameter(
    monkeypatch,
):
    """The dialog's count on the default demo (pet-store, shelter): get_order_by_id requires
    orderId, classifying it ran a count the pipeline refused (ParameterRequired), and the count
    route answered an unhandled error."""
    db = await _store()
    await _pet_store(db)
    world = _pet_world(monkeypatch, db)
    counted = await discovery.count_run(_REQUEST, _scope(source_domains=["pet-store"]))
    assert world.reads.attempted == []
    assert counted["source_tables"] == counted["target_tables"] == 4
    assert counted["tables_not_read"] == 2
    assert counted["requests"] >= 1


async def test_a_table_that_requires_a_parameter_is_known_from_its_registration_and_never_read(
    monkeypatch,
):
    db = await _store()
    await _pet_store(db)
    world = _pet_world(monkeypatch, db)
    facts = await _facts(db, [_ORDER_BY_ID, _ORDERS])
    order = facts["get_order_by_id"]
    assert world.reads.attempted == []
    assert order.reduced and order.parameters == (("orderId", "integer"),)
    assert [c.required_parameter for c in order.columns] == ["orderId", None, None]
    assert {c.classes for c in order.columns} == {(NOT_READ,)}
    assert {c.samples for c in order.columns} == {()}
    assert not any(c.sensitive for c in order.columns)
    # Only the table read whole is classified and sampled.
    assert [c["table_id"] for c in world.classified] == [_ORDERS]
    assert world.reads.sql == ['SELECT "id", "pet_id" FROM "pet_store"."orders" LIMIT 20']
    assert not facts["orders"].reduced and facts["orders"].parameters == ()


@pytest.mark.parametrize(
    ("source", "target", "not_read"),
    [
        pytest.param([_ORDER_BY_ID], [_ORDERS], 1, id="parameterized-source"),
        pytest.param([_ORDERS], [_ORDER_BY_ID], 1, id="parameterized-target"),
        pytest.param([_ORDER_BY_ID], [_PET_BY_ID], 2, id="both-sides-parameterized"),
        pytest.param(
            [_ORDER_BY_ID, _ORDERS, _PET_BY_ID, _PETS], None, 2, id="mixed-with-ordinary-tables"
        ),
    ],
)
async def test_relationship_requests_are_planned_on_reduced_information(
    monkeypatch, source, target, not_read
):
    db = await _store()
    await _pet_store(db)
    world = _pet_world(monkeypatch, db)
    scope = _scope(
        source_domains=["pet-store"],
        source_tables=source,
        target_same_as_source=target is None,
        target_domains=[] if target is None else ["pet-store"],
        target_tables=target,
    )
    counted = await discovery.count_run(_REQUEST, scope)
    assert world.reads.attempted == []
    assert counted["tables_not_read"] == not_read
    assert counted["by_group"]["whole_number"] >= 1
    if not_read == len(set(source) | set(target or [])):
        assert world.classified == [] and world.reads.sql == []  # nothing in scope is read
    _, prepared = await discovery._prepared(scope.spec())
    sent = "\n".join(r.prompt(prepared.k) for r in prepared.requests)
    assert (
        '"required_parameters":[{"column":"_nf_orderId","parameter":"orderId","type":"integer"}],'
        '"values":"not_read"'
    ) in sent
    # The parameter is a compared column, with the class of a column never read and no values.
    assert '{"name":"_nf_orderId","description":null,"classes":["not_read"],"tags":[]}' in sent
    assert "'not_read'" in sent  # the instructions say what it means


def test_a_column_never_read_is_compared_with_every_class():
    def column(classes: tuple[str, ...]) -> ColumnFacts:
        return ColumnFacts("c", "integer", None, (), classes, False, (), False, frozenset())

    assert classes_meet(column((NOT_READ,)), column(("email",)))
    assert classes_meet(column(("unknown",)), column((NOT_READ,)))
    assert classes_meet(column((NOT_READ,)), column((NOT_READ,)))
    assert not classes_meet(column(("unknown",)), column(("email",)))


async def test_the_sample_of_a_table_that_requires_a_parameter_is_refused_unread(monkeypatch):
    db = await _store()
    await _pet_store(db)
    world = _pet_world(monkeypatch, db)
    world.reads.values["id"] = [1, 1, 2]
    facts = await _facts(db, [_ORDER_BY_ID, _ORDERS])
    read = gather.uniqueness_reader(discovery.state)
    with pytest.raises(gather.TableUnreadable, match="requires a parameter"):
        await read(facts["get_order_by_id"], "id")
    assert world.reads.attempted == []
    assert await read(facts["orders"], "id") == Sampled(rows=3, repeats=True)
    assert world.reads.sql[-1] == (
        'SELECT "id" FROM "pet_store"."orders" WHERE "id" IS NOT NULL LIMIT 500'
    )


# --- the count for every other table a domain can hold -------------------------------------------


async def test_a_table_with_no_column_is_counted_and_never_read(monkeypatch):
    db = await _store()
    await _pet_store(db)
    await _table(db, 5, "pet-store", "empty_view", [])
    world = _pet_world(monkeypatch, db)
    world.reads.unpublished = {"empty_view"}
    counted = await discovery.count_run(_REQUEST, _scope(source_domains=["pet-store"]))
    assert counted["source_tables"] == 5
    assert [c["table_id"] for c in world.classified] == [_ORDERS, _PETS]


async def test_a_column_the_relation_read_does_not_hold_sends_no_values(monkeypatch):
    db = await _store()
    await _table(
        db, 1, "pet-store", "employees", [("id", "integer", True), ("manager", "text", False)]
    )
    world = _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k"})
    world.reads.values["manager"] = ["m"]
    world.reads.outside = {"manager"}
    facts = await _facts(db, [1])
    assert world.reads.sql == ['SELECT "id" FROM "pet_store"."employees" LIMIT 20']
    assert [c.samples for c in facts["employees"].columns] == [("1", "2", "3"), ()]


async def test_a_column_is_read_by_the_name_it_is_published_under(monkeypatch):
    db = await _store()
    await _table(db, 1, "pet-store", "orders", [("petId", "integer", False)])
    world = _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k"})
    world.reads.values["petid"] = [7, 8]
    facts = await _facts(db, [1])
    assert world.reads.sql == ['SELECT "petid" FROM "pet_store"."orders" LIMIT 20']
    assert facts["orders"].columns[0].samples == ("7", "8")


@pytest.mark.parametrize(
    ("refusal", "at", "reason"),
    [
        pytest.param(None, "schema", "is not readable by org_admin", id="not-in-the-schema"),
        pytest.param(
            ConnectionError("no connection pool for source pet-store-sqlite"),
            "classification",
            "no connection pool",
            id="source-with-no-pool",
        ),
        pytest.param(
            SourceUnavailable("pet-store-sqlite"),
            "classification",
            "could not answer",
            id="source-unavailable",
        ),
        pytest.param(
            ParameterRequired("pets", missing_reason(["status"])),
            "classification",
            "requires the parameter(s) ['status']",
            id="a-parameter-its-registration-does-not-record",
        ),
        pytest.param(
            ValueError("the read stops at 10000 rows and the table has more"),
            "sample",
            "stops at 10000 rows",
            id="row-cap-on-the-sample-read",
        ),
    ],
)
async def test_a_table_whose_read_is_refused_is_refused_by_name(monkeypatch, refusal, at, reason):
    db = await _store()
    await _pet_store(db)
    world = _pet_world(monkeypatch, db)
    if at == "schema":
        world.reads.unpublished = {"pets"}
    else:
        world.reads.failing = {"pets": refusal}
    if at == "sample":

        async def profiled(conn, table_id, table_name):
            return {}  # profiled: no classification read, so the sample read is the first

        monkeypatch.setattr(gather, "_profiled_classes", profiled)
    with pytest.raises(ApiError) as caught:
        await discovery.count_run(_REQUEST, _scope(source_domains=["pet-store"]))
    assert caught.value.status_code == 422
    assert caught.value.code == "discovery.table_unreadable"
    assert caught.value.params["table"] == "pets"
    assert reason in caught.value.params["reason"]
    assert "pets" in caught.value.detail and "Leave it out" in caught.value.detail


async def test_a_chosen_model_whose_vendor_has_no_key_is_refused(monkeypatch):
    db = await _store()
    await _orders_customers(db)
    _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k"})
    with pytest.raises(ApiError) as caught:
        await discovery.count_run(_REQUEST, _scope(vendor="mistral", model="large"))
    assert caught.value.code == "discovery.semantics_unavailable"


async def test_the_models_list_names_every_vendor_with_a_key_and_the_assigned_model(monkeypatch):
    db = await _store()
    _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k", "mistral": "m"})
    models = await discovery.discovery_models(_REQUEST)
    assert models["vendors"] == ["mistral", "openai"]
    assert models["assigned"]["vendor"] == "openai" and models["assigned"]["has_key"] is True
    assert models["assigned"]["input_tokens"] == 32_000  # an unknown model: the default pair
    assert models["semantics_available"] is True


# --- the background run ----------------------------------------------------------------------------


def _suggestion(src: tuple[int, str], tgt: tuple[int, str], confidence: float = 0.9) -> dict:
    return {
        "source_table_id": src[0],
        "source_column": src[1],
        "target_table_id": tgt[0],
        "target_column": tgt[1],
        "cardinality": "many-to-one",
        "confidence": confidence,
        "reasoning": "orders name their customer",
        "suggested_name": "purchase-history",
    }


async def _run(world: _World, scope: discovery.RunScope) -> dict:
    """Start a run and do its background work here, then read it back."""
    started = (await discovery.start_run(_REQUEST, scope))["run"]
    assert started["status"] == "running"
    (run_id, spec, model) = world.launched.pop()
    await discovery.run_discovery(run_id, spec, model)
    return (await discovery.get_run(_REQUEST, run_id))["run"]


async def _candidates(db: Database) -> list[dict]:
    async with db.acquire() as conn:
        rows = (await conn.execute_core(select(so.relationship_candidates))).fetchall()
    return [dict(r._mapping) for r in rows]


async def test_a_run_makes_exactly_the_requests_counted(monkeypatch, tmp_path):
    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    await _orders_customers(db)
    await _table(
        db, 3, "sales", "stores", [("id", "integer", True), ("region_id", "integer", False)]
    )
    config = {
        **_ASSIGNED,
        "ai_models": {
            **_ASSIGNED["ai_models"],
            "model_limits": {"openai": {"gpt-test": {"input_tokens": 650, "output_tokens": 4096}}},
        },
    }
    world = _World(monkeypatch, db, config=config, api_keys={"openai": "k"})
    counted = await discovery.count_run(_REQUEST, _scope())
    assert counted["requests"] > 1
    run = await _run(world, _scope())
    assert run["status"] == "done"
    assert len(world.model_calls) == counted["requests"]
    assert run["requests_done"] == run["requests_total"] == counted["requests"]


async def test_a_run_stores_its_suggestions_and_reports_what_each_kind_found(monkeypatch, tmp_path):
    from provisa.discovery.analyzer import RelationshipCandidate

    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    await _orders_customers(db)
    await _table(
        db, 3, "sales", "stores", [("id", "integer", True), ("region_id", "integer", False)]
    )
    world = _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k"})
    world.fk_found = [
        RelationshipCandidate(3, "region_id", 2, "id", "many-to-one", 1.0, "Foreign key constraint")
    ]
    import json as _json

    world.answers = [_json.dumps([_suggestion((1, "customer_id"), (2, "id"))])]
    run = await _run(world, _scope(kinds=["foreign_keys", "semantics"]))
    assert run["status"] == "done"
    assert run["model"] == "openai:gpt-test"
    report = run["report"]
    assert report["foreign_keys"] == {"found": 1, "suggested": 1}
    assert report["semantics"]["suggested"] == 1
    assert run["suggestions"] == 2
    stored = {
        (c["source_table_id"], c["source_column"], c["target_table_id"], c["target_column"])
        for c in await _candidates(db)
    }
    assert stored == {(1, "customer_id", 2, "id"), (3, "region_id", 2, "id")}
    assert {c["scope"] for c in await _candidates(db)} == {f"run:{run['id']}"}


async def test_stop_ends_the_run_after_the_request_in_flight_and_keeps_what_was_found(
    monkeypatch, tmp_path
):
    import json as _json

    import sqlalchemy as sa

    url = f"sqlite+pysqlite:///{tmp_path / 'store.db'}"
    db = await _store(url)
    await _orders_customers(db)
    await _table(
        db, 3, "sales", "stores", [("id", "integer", True), ("region_id", "integer", False)]
    )
    config = {
        **_ASSIGNED,
        "ai_models": {
            **_ASSIGNED["ai_models"],
            "model_limits": {"openai": {"gpt-test": {"input_tokens": 650, "output_tokens": 4096}}},
        },
    }
    world = _World(monkeypatch, db, config=config, api_keys={"openai": "k"})
    counted = await discovery.count_run(_REQUEST, _scope())
    assert counted["requests"] > 1
    world.answers = [_json.dumps([_suggestion((1, "customer_id"), (2, "id"))])]
    started = (await discovery.start_run(_REQUEST, _scope()))["run"]

    def press_stop(calls: int) -> None:
        # Stop pressed while the first request is in flight, as another worker would: on the row.
        engine = sa.create_engine(url)
        with engine.begin() as conn:
            conn.execute(
                sa.update(so.discovery_runs)
                .where(so.discovery_runs.c.id == started["id"])
                .values(stop_requested=True)
            )
        engine.dispose()

    world.on_call = press_stop
    (run_id, spec, model) = world.launched.pop()
    await discovery.run_discovery(run_id, spec, model)
    run = (await discovery.get_run(_REQUEST, run_id))["run"]
    assert len(world.model_calls) == 1
    assert run["status"] == "stopped"
    assert run["report"]["semantics"]["stopped"] is True
    assert run["requests_done"] == 1 and run["requests_total"] == counted["requests"]
    assert [(c["source_column"], c["target_column"]) for c in await _candidates(db)] == [
        ("customer_id", "id")
    ]


async def test_stop_through_the_api_marks_the_run_and_a_second_stop_after_it_ended_is_refused(
    monkeypatch, tmp_path
):
    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    await _orders_customers(db)
    world = _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k"})
    started = (await discovery.start_run(_REQUEST, _scope()))["run"]
    asked = (await discovery.stop_run(_REQUEST, started["id"]))["run"]
    assert asked["stop_requested"] is True and asked["status"] == "running"
    (run_id, spec, model) = world.launched.pop()
    await discovery.run_discovery(run_id, spec, model)
    assert world.model_calls == []
    ended = (await discovery.get_run(_REQUEST, run_id))["run"]
    assert ended["status"] == "stopped"
    with pytest.raises(ApiError) as caught:
        await discovery.stop_run(_REQUEST, run_id)
    assert caught.value.code == "discovery.run_ended"


async def test_a_run_whose_model_fails_every_request_reports_each_and_finishes(
    monkeypatch, tmp_path
):
    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    await _orders_customers(db)
    world = _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k"})

    def boom(calls: int) -> None:
        raise RuntimeError("vendor unavailable")

    world.on_call = boom
    run = await _run(world, _scope())
    assert run["status"] == "done"
    assert run["report"]["semantics"]["failed"] == [
        {"group": "whole_number", "error": "vendor unavailable"}
    ]


async def test_a_run_left_by_an_earlier_launch_is_reported_failed(monkeypatch, tmp_path):
    from provisa.discovery import runs as runs_repo

    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k"})
    async with db.acquire() as conn:
        run_id = await runs_repo.start(conn, scope={}, model=None, started_by=None)
        await conn.execute_core(
            so.discovery_runs.update().where(so.discovery_runs.c.id == run_id).values(launch="gone")
        )
    run = (await discovery.get_run(_REQUEST, run_id))["run"]
    assert run["status"] == "failed" and run["error"] == runs_repo.RESTARTED


async def test_the_latest_runs_are_listed_newest_first(monkeypatch, tmp_path):
    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    await _orders_customers(db)
    _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k"})
    first = (await discovery.start_run(_REQUEST, _scope()))["run"]
    second = (await discovery.start_run(_REQUEST, _scope(kinds=["foreign_keys"])))["run"]
    listed = (await discovery.list_runs(_REQUEST))["runs"]
    assert [r["id"] for r in listed] == [second["id"], first["id"]]
    assert second["model"] is None


# --- accept ---------------------------------------------------------------------------------------------


async def _flags(db: Database, table_id: int, column: str) -> tuple[bool, bool, bool]:
    async with db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(
                    so.table_columns.c.is_foreign_key,
                    so.table_columns.c.is_primary_key,
                    so.table_columns.c.is_alternate_key,
                ).where(
                    so.table_columns.c.table_id == table_id,
                    so.table_columns.c.column_name == column,
                )
            )
        ).one()
    return bool(row[0]), bool(row[1]), bool(row[2])


@pytest.mark.parametrize("target_has_a_key", [False, True])
async def test_accept_marks_the_referring_column_a_foreign_key_and_nothing_else(
    monkeypatch, tmp_path, target_has_a_key
):
    from provisa.discovery import candidates as candidates_repo

    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    # The target column is no key; with target_has_a_key the table has another primary key, where
    # the derivation would mark an alternate key.
    await _table(db, 1, "sales", "orders", [("id", "integer", True), ("store_code", "text", False)])
    await _table(
        db,
        2,
        "sales",
        "stores",
        [("id", "integer", target_has_a_key), ("code", "text", False)],
    )
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(so.relationship_candidates).values(
                source_table_id=1,
                source_column="store_code",
                target_table_id=2,
                target_column="code",
                cardinality="many-to-one",
                confidence=0.9,
                reasoning="codes match",
                suggested_name="order-store",
                scope="run:1",
            )
        )
        accepted = await candidates_repo.accept(conn, 1)
    assert accepted["relationship_id"] == "order-store"
    assert await _flags(db, 1, "store_code") == (True, False, False)
    assert await _flags(db, 2, "code") == (False, False, False)
    assert await _flags(db, 2, "id") == (False, target_has_a_key, False)
    async with db.acquire() as conn:
        rel = (await conn.execute_core(select(so.relationships))).one()._mapping
    assert (
        rel["source_table_id"],
        rel["source_column"],
        rel["target_table_id"],
        rel["target_column"],
    ) == (
        1,
        "store_code",
        2,
        "code",
    )
    assert rel["cardinality"] == "many-to-one"


# --- a run over a table that requires a parameter (REQ-1986) --------------------------------------


async def test_a_parameter_referring_to_another_tables_key_is_suggested_and_marked(
    monkeypatch, tmp_path
):
    """The expected good outcome: get_order_by_id's required parameter orderId refers to
    orders.id, found with no value of get_order_by_id read, and the suggestion says so."""
    import json as _json

    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    await _pet_store(db)
    world = _pet_world(monkeypatch, db)
    answer = _suggestion((_ORDER_BY_ID, "_nf_orderId"), (_ORDERS, "id"))
    answer["reasoning"] = "the parameter names an order"
    world.answers = [_json.dumps([answer])]
    run = await _run(world, _scope(source_domains=["pet-store"]))
    assert run["status"] == "done", run
    assert world.reads.attempted == []
    assert run["report"]["on_reduced_information"] == 1
    assert run["report"]["tables_not_read"] == 2
    (stored,) = await _candidates(db)
    assert (stored["source_table_id"], stored["source_column"]) == (_ORDER_BY_ID, "_nf_orderId")
    assert (stored["target_table_id"], stored["target_column"]) == (_ORDERS, "id")
    assert stored["reasoning"] == (
        "the parameter names an order; no values were compared: get_order_by_id requires a "
        "parameter and is never read"
    )
    # The list the suggestions table reads says so too, from the tables' registration.
    (listed,) = await discovery.list_candidates(_REQUEST)
    assert listed["id"] == stored["id"] and listed["reduced_information"] is True


async def test_a_suggestion_between_tables_read_whole_is_not_marked(monkeypatch, tmp_path):
    import json as _json

    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    await _orders_customers(db)
    world = _World(monkeypatch, db, config=_ASSIGNED, api_keys={"openai": "k"})
    world.answers = [_json.dumps([_suggestion((1, "customer_id"), (2, "id"))])]
    run = await _run(world, _scope())
    assert run["report"]["on_reduced_information"] == 0 and run["report"]["tables_not_read"] == 0
    (listed,) = await discovery.list_candidates(_REQUEST)
    assert listed["reduced_information"] is False
    assert listed["reasoning"] == "orders name their customer"


async def test_the_glossary_term_bound_to_a_column_is_sent_whether_or_not_its_table_is_read(
    monkeypatch,
):
    db = await _store()
    await _pet_store(db)
    async with db.acquire() as conn:
        await conn.execute_core(insert(so.glossary_terms).values(id=1, name="Order"))
        for table_id, column in ((_ORDER_BY_ID, "_nf_orderId"), (_ORDERS, "id")):
            await conn.execute_core(
                insert(so.glossary_term_refs).values(
                    term_id=1, table_id=table_id, column_name=column
                )
            )
    world = _pet_world(monkeypatch, db)
    facts = await _facts(db, [_ORDER_BY_ID, _ORDERS])
    assert world.reads.attempted == []
    assert facts["get_order_by_id"].column("_nf_orderId").term == "Order"
    assert facts["orders"].column("id").term == "Order"
    assert facts["orders"].column("pet_id").term is None
    _, prepared = await discovery._prepared(
        _scope(source_domains=["pet-store"], source_tables=[_ORDER_BY_ID, _ORDERS]).spec()
    )
    sent = prepared.requests[0].prompt(prepared.k)
    assert (
        '{"name":"_nf_orderId","description":null,"classes":["not_read"],"tags":[],"term":"Order"}'
    ) in sent
    assert '"tags":[],"term":"Order","sample_values":["1","2","3"]' in sent


async def test_a_model_whose_limits_hold_no_request_is_refused_saying_why(monkeypatch):
    db = await _store()
    await _orders_customers(db)
    config = {
        **_ASSIGNED,
        "ai_models": {
            **_ASSIGNED["ai_models"],
            "model_limits": {"openai": {"gpt-test": {"input_tokens": 650, "output_tokens": 100}}},
        },
    }
    _World(monkeypatch, db, config=config, api_keys={"openai": "k"})
    with pytest.raises(ApiError) as caught:
        await discovery.count_run(_REQUEST, _scope())
    assert caught.value.code == "discovery.model_has_no_room"
    assert caught.value.params["model"] == "openai:gpt-test"
    assert "100 tokens holds no suggestion" in caught.value.params["reason"]


async def test_a_direction_is_decided_without_reading_a_table_that_requires_a_parameter(
    monkeypatch, tmp_path
):
    """Both directions proposed between two columns that are no key, with equal confidence: the
    uniqueness sample would decide, and is not read when a side requires a parameter."""
    import json as _json

    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    await _pet_store(db)
    world = _pet_world(monkeypatch, db)
    world.answers = [
        _json.dumps(
            [
                _suggestion((_ORDER_BY_ID, "petId"), (_ORDERS, "pet_id")),
                _suggestion((_ORDERS, "pet_id"), (_ORDER_BY_ID, "petId")),
            ]
        )
    ]
    run = await _run(world, _scope(source_domains=["pet-store"]))
    assert run["status"] == "done", run
    assert world.reads.attempted == []
    assert not any("IS NOT NULL" in sql for sql in world.reads.sql)
    assert run["report"]["decided_by"] == {"fixed_order": 1}
    assert run["report"]["on_reduced_information"] == 1
    (stored,) = await _candidates(db)
    assert stored["reasoning"].endswith(
        "no values were compared: get_order_by_id requires a parameter and is never read"
    )
