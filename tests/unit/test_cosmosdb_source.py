# Copyright (c) 2026 Kenneth Stott
# Canary: 77bbeab0-f6a6-4597-bbc9-96d722e53757
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Azure Cosmos DB for NoSQL as a source (REQ-1982): its connection (account key or an Entra
service principal), its container and query tables, their parameters (a query's @names, the
partition key) sent beside the query and never inside it, and its documents checked on the way
out. The SDK is stood in for: no account is reached."""

# Requirements: REQ-1982, REQ-1986

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.federation.replica_address import ReplicaRoutes

from provisa.core.models import Column, Table
from provisa.cosmosdb.fetch import CosmosConnection, CosmosSourceInvalid, iter_query_batches
from provisa.cosmosdb.table import CosmosTableRefused, check_table, plan

_ENDPOINT = "https://acct.documents.azure.com:443/"


def _source(**kw) -> SimpleNamespace:
    fields = {
        "id": "cosmos",
        "host": _ENDPOINT,
        "database": "shop",
        "password": "s3cret",
        "federation_hints": {},
    }
    fields.update(kw)
    return SimpleNamespace(**fields)


# --- the connection ---


def test_the_account_key_signs_in():
    conn = CosmosConnection.from_source(_source())
    assert (conn.endpoint, conn.database, conn.auth, conn.secret) == (
        _ENDPOINT,
        "shop",
        "key",
        "s3cret",
    )
    assert conn._credential() == "s3cret"


def test_an_entra_service_principal_signs_in(monkeypatch):
    monkeypatch.setenv("COSMOS_SECRET", "client-secret")
    conn = CosmosConnection.from_source(
        _source(
            password="${env:COSMOS_SECRET}",
            federation_hints={"auth": "entra", "tenant_id": "t1", "client_id": "c1"},
        )
    )
    with patch("azure.identity.ClientSecretCredential") as credential:
        conn._credential()
    credential.assert_called_once_with(
        tenant_id="t1", client_id="c1", client_secret="client-secret"
    )


@pytest.mark.parametrize(
    ("kw", "reason"),
    [
        ({"host": "acct.documents.azure.com"}, "account endpoint URL"),
        ({"database": ""}, "database is required"),
        ({"password": ""}, "account key is required"),
        ({"federation_hints": {"auth": "entra", "tenant_id": "t1"}}, "needs client_id"),
        ({"federation_hints": {"auth": "sas"}}, "auth is 'key' or 'entra'"),
    ],
)
def test_a_source_that_cannot_sign_in_is_refused_naming_it(kw, reason):
    with pytest.raises(CosmosSourceInvalid, match=f"'cosmos'.*{reason}"):
        CosmosConnection.from_source(_source(**kw))


def test_a_read_sends_its_parameters_beside_the_query_and_drops_system_fields():
    container = MagicMock()
    pages = [[{"id": "1", "_rid": "x", "_etag": "e", "_ts": 5, "total": 2}]]
    container.query_items.return_value.by_page.return_value = iter(pages)
    conn = MagicMock(spec=CosmosConnection)
    conn.container.return_value = container
    batches = list(
        iter_query_batches(
            conn, "orders", "SELECT * FROM c WHERE c.s = @s", [{"name": "@s", "value": "x"}], 50
        )
    )
    assert batches == [[{"id": "1", "_ts": 5, "total": 2}]]
    container.query_items.assert_called_once_with(
        "SELECT * FROM c WHERE c.s = @s",
        parameters=[{"name": "@s", "value": "x"}],
        max_item_count=50,
        enable_cross_partition_query=True,
    )


def test_a_read_given_the_partition_key_reads_one_partition():
    container = MagicMock()
    container.query_items.return_value.by_page.return_value = iter([])
    conn = MagicMock(spec=CosmosConnection)
    conn.container.return_value = container
    list(iter_query_batches(conn, "orders", "SELECT * FROM c", [], 50, partition_key="c-7"))
    kwargs = container.query_items.call_args.kwargs
    assert kwargs["partition_key"] == "c-7" and "enable_cross_partition_query" not in kwargs


# --- tables as registered ---

_QUERY = json.dumps(
    {
        "container": "orders",
        "query": "SELECT c.id, c.total FROM c WHERE c.status = @status AND c.total > @min",
    }
)


def _param(name: str, kind: str, data_type: str = "text") -> Column:
    return Column(
        name=f"_nf_{name}",
        visible_to=[],
        data_type=data_type,
        native_filter_type=kind,
        native_filter_required=False,
    )


def _table(query: str | None = _QUERY, *params: Column) -> Table:
    return Table(
        source_id="cosmos",
        domain_id="shop",
        schema_name="shop",
        table_name="big_orders" if query else "orders",
        query_template=query,
        columns=[
            Column(name="id", visible_to=["analyst"], data_type="text"),
            Column(name="total", visible_to=["analyst"], data_type="numeric"),
            *params,
        ],
    )


def test_a_query_table_declares_each_of_its_parameters():
    query = check_table(
        _table(_QUERY, _param("status", "query_arg"), _param("min", "query_arg", "numeric"))
    )
    assert query is not None and query.container == "orders"


@pytest.mark.parametrize(
    ("params", "reason"),
    [
        ((_param("status", "query_arg"),), r"names \['min'\], which the table declares"),
        (
            (
                _param("status", "query_arg"),
                _param("min", "query_arg"),
                _param("extra", "query_arg"),
            ),
            r"\['extra'\] appear nowhere",
        ),
        ((_param("status", "query_param"),), "query_arg or partition_key"),
    ],
)
def test_a_query_table_that_cannot_be_read_is_refused(params, reason):
    with pytest.raises(CosmosTableRefused, match=reason):
        check_table(_table(_QUERY, *params))


def test_a_container_table_takes_its_partition_key_and_no_query_arguments():
    assert check_table(_table(None, _param("customerId", "partition_key"))) is None
    # A hierarchical key: one parameter per level.
    assert (
        check_table(_table(None, _param("a", "partition_key"), _param("b", "partition_key")))
        is None
    )
    with pytest.raises(CosmosTableRefused, match="no query to take arguments"):
        check_table(_table(None, _param("status", "query_arg")))


def _state_table(query: str | None, *params: tuple[str, str, str]) -> dict:
    return {
        "id": 3,
        "source_id": "cosmos",
        "schema_name": "shop",
        "table_name": "big_orders" if query else "orders",
        "query_template": query,
        "columns": [
            {"column_name": "id", "data_type": "text", "native_filter_type": None, "path": None},
            {
                "column_name": "total",
                "data_type": "numeric",
                "native_filter_type": None,
                "path": None,
            },
            {
                "column_name": "placed",
                "data_type": "date",
                "native_filter_type": None,
                "path": "meta.placed",
            },
            *(
                {
                    "column_name": f"_nf_{n}",
                    "data_type": t,
                    "native_filter_type": k,
                    "native_filter_required": False,
                    "required_by_steward": False,
                    "path": None,
                }
                for n, k, t in params
            ),
        ],
    }


def test_a_query_reads_with_its_arguments_as_json_parameters():
    table = _state_table(_QUERY, ("status", "query_arg", "text"), ("min", "query_arg", "numeric"))
    container, query, parameters, partition = plan(table, {"status": '{"$ne": 1}', "min": "9.5"})
    assert (container, partition) == ("orders", None)
    assert query == json.loads(_QUERY)["query"]  # the text is never written into
    assert parameters == [
        {"name": "@status", "value": '{"$ne": 1}'},  # operator syntax stays a string
        {"name": "@min", "value": 9.5},
    ]


def test_a_hierarchical_key_is_read_by_the_levels_given_from_the_top():
    table = _state_table(
        None, ("tenant", "partition_key", "text"), ("customerId", "partition_key", "text")
    )
    both = {"tenant": "t1", "customerId": "c-7"}
    assert plan(table, both)[3] == ["t1", "c-7"]
    assert plan(table, {"tenant": "t1"})[3] == ["t1"]  # a prefix of the key
    assert plan(table, {})[3] is None
    with pytest.raises(CosmosTableRefused, match="'customerId' is given without the level above"):
        plan(table, {"customerId": "c-7"})


def test_a_container_read_given_its_partition_key_reads_that_partition():
    table = _state_table(None, ("customerId", "partition_key", "text"))
    assert plan(table, {"customerId": "c-7"}) == ("orders", "SELECT * FROM c", [], "c-7")
    assert plan(table, {}) == ("orders", "SELECT * FROM c", [], None)


async def test_a_read_checks_its_documents_reading_dates_held_as_text():
    from provisa.cosmosdb.table import read

    table = _state_table(None, ("customerId", "partition_key", "text"))
    docs = [{"id": "o1", "total": 12.5, "meta": {"placed": "2025-03-02"}}, {"id": "o2", "total": 3}]
    with (
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[_source()]),
        ),
        patch("provisa.cosmosdb.fetch.iter_query_batches", return_value=iter([docs])) as ran,
    ):
        answer = await read(SimpleNamespace(), table, {"customerId": "c-7"})
    assert ran.call_args.args[1:] == ("orders", "SELECT * FROM c", [], 1000, "c-7")
    assert answer.rows == [
        {"id": "o1", "total": Decimal("12.5"), "placed": date(2025, 3, 2)},
        {"id": "o2", "total": Decimal("3"), "placed": None},
    ]
    assert answer.columns == [("id", "text"), ("total", "numeric"), ("placed", "date")]


async def test_a_required_partition_key_left_out_is_refused_before_any_read():
    from provisa.api.data.materialization import _materialize_api_to_engine_cache

    table = _state_table(None, ("customerId", "partition_key", "text"))
    table["columns"][-1]["required_by_steward"] = True
    state = SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={"cosmos": "cosmosdb"},
        replica_routes=ReplicaRoutes(),
        tables=[table],
    )
    with patch("provisa.cosmosdb.table.read", AsyncMock()) as ran:
        with pytest.raises(
            ValueError, match=r"'orders' requires the parameter\(s\) \['customerId'\]"
        ):
            await _materialize_api_to_engine_cache(
                "SELECT * FROM orders", state, nf_args={}, table_ids=[3]
            )
    ran.assert_not_awaited()


async def test_a_parameterized_cosmos_table_with_a_ttl_of_its_own_is_landed():
    """A partition key is a parameter: the rows are kept only for a TTL the table sets itself,
    under the arguments that read them (REQ-1986)."""
    from provisa.api.data.materialization import _materialize_api_to_engine_cache
    from provisa.api_source.engine_cache import CacheLocation
    from provisa.mongodb.pipeline_table import PipelineRead

    table = {**_state_table(None, ("customerId", "partition_key", "text")), "cache_ttl": 300}
    engine = MagicMock()
    engine.land_source_table = AsyncMock()
    state = SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={"cosmos": "cosmosdb"},
        replica_routes=ReplicaRoutes(),
        tables=[table],
        federation_engine=engine,
        source_cache={},
        response_cache_default_ttl=300,
    )
    answer = PipelineRead(rows=[{"id": "o1"}], columns=[("id", "text")])
    loc = CacheLocation("cat", "sch", "relational")
    with (
        patch("provisa.cosmosdb.table.read", AsyncMock(return_value=answer)) as ran,
        patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
        patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
        patch("provisa.api_source.engine_cache.ensure_cache_schema"),
        patch("provisa.api_source.engine_cache.analyze_cache_table", AsyncMock()),
        patch("provisa.api_source.engine_cache.schedule_drop"),
        patch("provisa.api_source.engine_cache.cache_table_name", return_value="c_1") as named,
    ):
        rewrites, _, _ = await _materialize_api_to_engine_cache(
            "SELECT * FROM orders", state, nf_args={"customerId": "c-7"}, table_ids=[3]
        )
    assert rewrites == {"orders": (loc, "c_1")}
    ran.assert_awaited_once()
    assert named.call_args.args == ("cosmos", "pipeline:orders", {"customerId": "c-7"})


async def test_a_parameterized_cosmos_table_with_no_ttl_of_its_own_is_read_per_statement():
    """REQ-1986: with no TTL of its own it is read for each statement and inlined into it --
    nothing landed or reused, whatever the source keeps."""
    from provisa.api.data.materialization import _materialize_api_to_engine_cache
    from provisa.mongodb.pipeline_table import PipelineRead

    table = _state_table(None, ("customerId", "partition_key", "text"))
    engine = MagicMock()
    engine.land_source_table = AsyncMock()
    state = SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={"cosmos": "cosmosdb"},
        replica_routes=ReplicaRoutes(),
        tables=[table],
        federation_engine=engine,
        source_cache={"cosmos": {"cache_ttl": 600}},
        response_cache_default_ttl=300,
    )
    answer = PipelineRead(rows=[{"id": "o1"}], columns=[("id", "text")])
    with patch("provisa.cosmosdb.table.read", AsyncMock(return_value=answer)) as ran:
        for _ in range(2):
            rewrites, inline, _ = await _materialize_api_to_engine_cache(
                "SELECT * FROM orders", state, nf_args={"customerId": "c-7"}, table_ids=[3]
            )
            assert rewrites == {} and inline["orders"].rows == [{"id": "o1"}]
    assert ran.await_count == 2
    engine.land_source_table.assert_not_awaited()


# --- loaders ---


async def test_a_container_table_with_a_parameter_has_no_whole_table_to_load():
    from provisa.cosmosdb.loader import make_cosmosdb_loader

    table = SimpleNamespace(
        table_name="orders",
        query_template=None,
        columns=[
            SimpleNamespace(
                name="_nf_customerId", data_type="text", native_filter_type="partition_key"
            )
        ],
    )
    with pytest.raises(ValueError, match="no whole table"):
        await make_cosmosdb_loader()(_source(), table)


def _keyed_table() -> SimpleNamespace:
    return SimpleNamespace(
        table_name="orders",
        query_template=None,
        columns=[
            SimpleNamespace(name="id", data_type="text", native_filter_type=None, path=None),
            SimpleNamespace(
                name="customerId", data_type="text", native_filter_type=None, path=None
            ),
        ],
    )


async def test_a_keyed_fetch_on_id_and_partition_key_is_point_reads():
    from provisa.cosmosdb.loader import make_cosmosdb_keyed_loader

    with (
        patch("provisa.cosmosdb.fetch.partition_key_paths", return_value=["/customerId"]),
        patch(
            "provisa.cosmosdb.fetch.read_points", return_value=[{"id": "o1", "customerId": "c-7"}]
        ) as points,
        patch("provisa.cosmosdb.fetch.iter_query_batches") as queried,
    ):
        rows = await make_cosmosdb_keyed_loader()(
            _source(), _keyed_table(), ["customerId", "id"], [("c-7", "o1")]
        )
    assert points.call_args.args[1:] == ("orders", [("o1", "c-7")])
    queried.assert_not_called()
    assert rows == [{"id": "o1", "customerId": "c-7"}]


async def test_a_keyed_fetch_on_another_key_is_one_parameterized_query():
    from provisa.cosmosdb.loader import make_cosmosdb_keyed_loader

    with (
        patch("provisa.cosmosdb.fetch.partition_key_paths", return_value=["/customerId"]),
        patch("provisa.cosmosdb.fetch.read_points") as points,
        patch("provisa.cosmosdb.fetch.iter_query_batches", return_value=iter([[]])) as queried,
    ):
        await make_cosmosdb_keyed_loader()(_source(), _keyed_table(), ["id"], [("o1",), ("o2",)])
    points.assert_not_called()
    query, parameters = queried.call_args.args[2], queried.call_args.args[3]
    assert query == 'SELECT * FROM c WHERE (c["id"] = @k0_0) OR (c["id"] = @k1_0)'
    assert parameters == [{"name": "@k0_0", "value": "o1"}, {"name": "@k1_0", "value": "o2"}]


# --- what Register Table offers ---


async def test_a_hierarchical_keys_suggested_parameters_make_a_table_that_registers():
    """Introspection suggests one parameter per level of a hierarchical key; the table built
    from them was refused ("a table has one partition key parameter")."""
    from provisa.cosmosdb import introspect

    docs = [{"id": "o1", "tenant": "t1", "customerId": "c-7", "total": 3.5}]
    with (
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[_source()]),
        ),
        patch("provisa.cosmosdb.fetch.sample", return_value=docs),
        patch(
            "provisa.cosmosdb.fetch.partition_key_paths", return_value=["/tenant", "/customerId"]
        ),
    ):
        columns = await introspect.columns(SimpleNamespace(), "cosmos", "orders")
    suggested = [
        Column(
            name=c["name"],
            visible_to=[],
            data_type=c["data_type"],
            path=c["path"],
            native_filter_type=c["native_filter_type"],
            native_filter_required=c["native_filter_required"],
        )
        for c in columns or []
    ]
    levels = [c.name for c in suggested if c.native_filter_type == "partition_key"]
    assert levels == ["_nf_tenant", "_nf_customerId"]
    table = Table(
        source_id="cosmos",
        domain_id="shop",
        schema_name="shop",
        table_name="orders",
        columns=suggested,
    )
    assert check_table(table) is None


async def test_a_containers_columns_are_suggested_with_its_partition_key_as_a_parameter():
    from provisa.cosmosdb import introspect

    docs = [{"id": "o1", "customerId": "c-7", "total": 3.5, "ship": {"city": "Oslo"}, "_ts": 1}]
    with (
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[_source()]),
        ),
        patch("provisa.cosmosdb.fetch.sample", return_value=docs),
        patch("provisa.cosmosdb.fetch.partition_key_paths", return_value=["/customerId"]),
    ):
        columns = await introspect.columns(SimpleNamespace(), "cosmos", "orders")
    by = {c["name"]: c for c in columns or []}
    assert by["ship_city"]["path"] == "ship.city"
    assert by["_ts"]["data_type"] == "integer"
    key = by["_nf_customerId"]
    assert (key["native_filter_type"], key["native_filter_required"], key["data_type"]) == (
        "partition_key",
        False,
        "text",
    )


def test_a_timestamp_column_reads_an_iso_string():
    from provisa.mongodb.pipeline_rows import OutputColumn, PipelineOutputMismatch, project

    cols = [OutputColumn("at", "timestamp")]
    assert project("t", {"at": "2025-03-02T10:00:00Z"}, cols, dates_as_text=True) == {
        "at": datetime(2025, 3, 2, 10, tzinfo=timezone.utc)
    }
    with pytest.raises(PipelineOutputMismatch, match="column 'at'"):
        project("t", {"at": "yesterday"}, cols, dates_as_text=True)


# --- the native change signal: the container's change feed ---


class _Feed:
    """A container whose change feed gives ``batches`` in turn, with an etag continuation."""

    def __init__(self, batches: list[list[dict]]) -> None:
        self._batches = list(batches)
        self.asked: list[dict] = []
        self.client_connection = SimpleNamespace(last_response_headers={"etag": "0"})

    def query_items_change_feed(self, **kw):
        self.asked.append(kw)
        self.client_connection.last_response_headers = {"etag": str(len(self.asked))}
        return iter(self._batches.pop(0) if self._batches else [])


async def test_the_change_feed_is_read_from_now_and_carried_by_its_continuation():
    from provisa.cosmosdb.change_feed import open_change_feed

    container = _Feed([[], [{"id": "o1"}, {"id": "o2"}], []])
    connection = SimpleNamespace(container=lambda name: container)
    async with open_change_feed(connection=connection, container="orders", wait_ms=1) as feed:
        assert container.asked == [{"start_time": "Now"}]  # placed at now when opened
        assert await feed.try_next() == {"id": "o1"}
        assert await feed.try_next() == {"id": "o2"}
        assert await feed.try_next() is None  # quiet: none arrived within the wait
    assert container.asked[1:] == [{"continuation": "1"}, {"continuation": "2"}]


def test_a_cosmos_table_follows_its_change_feed_and_needs_nothing_of_the_account():
    import asyncio

    from provisa.mongodb.change_feed import follows_change_feed, require_change_feed

    assert follows_change_feed("cosmosdb", "native", None)
    assert not follows_change_feed("cosmosdb", "ttl", "native")
    source = SimpleNamespace(id="cosmos", type=SimpleNamespace(value="cosmosdb"))
    asyncio.run(require_change_feed(source))  # no server question is asked


def test_a_table_read_under_arguments_has_no_change_feed_to_follow():
    table = _table(None, _param("customerId", "partition_key"))
    table.change_signal = "native"
    with pytest.raises(CosmosTableRefused, match="change feed follows a whole container"):
        check_table(table)
    whole = _table(None)
    whole.change_signal = "native"
    assert check_table(whole) is None
