# Copyright (c) 2026 Kenneth Stott
# Canary: 9b6c0e34-5d17-42fa-8e90-71bc3d5a802f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1069: the two standards-first targets map the same snapshot without losing governance."""

# Requirements: REQ-1068, REQ-1069, REQ-1070, REQ-1071

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import httpx
import pytest

from provisa.api.metadata_egress import build_snapshot, metadata_egress
from provisa.api.metadata_egress.openlineage import (
    DATASET_EVENT_URL,
    PRODUCER,
    RUN_EVENT_URL,
    OpenLineageEgress,
    to_events,
)
from provisa.api.metadata_egress.openmetadata import (
    CLASSIFICATION,
    RELATIONSHIP_PROPERTY,
    OpenMetadataEgress,
    to_entities,
    to_lineage_requests,
)
from provisa.api.metadata_egress.registry import registered_providers
from provisa.core.models import (
    Cardinality,
    Column,
    Domain,
    MetadataEgressConfig,
    ProvisaConfig,
    Relationship,
    RLSRule,
    Role,
    Source,
    SourceType,
    Table,
)

EVENT_TIME = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
MASK_PATTERN = r"(\d{3})-(\d{2})-(\d{4})"
RLS_FILTER = "region_id = current_setting('provisa.region')"


def _table(name: str, columns: list[Column], view_sql: str | None = None) -> Table:
    return Table(
        source_id="wh",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        columns=columns,
        view_sql=view_sql,
        materialize=view_sql is not None,
    )


@pytest.fixture
def snapshot():
    config = ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="Warehouse")],
        domains=[
            Domain(id="sales", description="Sales", steward="data-steward"),
            Domain(id="lab", description="Lab"),
        ],
        tables=[
            _table(
                "orders",
                [
                    Column(name="id", data_type="integer", visible_to=["analyst", "admin"]),
                    Column(
                        name="amount",
                        data_type="numeric",
                        visible_to=["analyst", "admin"],
                        description="Order total",
                    ),
                    Column(
                        name="customer_id", data_type="integer", visible_to=["analyst", "admin"]
                    ),
                    Column(
                        name="ssn",
                        data_type="text",
                        visible_to=["admin"],
                        mask_type="regex",
                        mask_pattern=MASK_PATTERN,
                        mask_replace="XXX",
                        unmasked_to=["admin"],
                    ),
                ],
            ),
            _table(
                "customers",
                [Column(name="id", data_type="integer", visible_to=["analyst", "admin"])],
            ),
            _table(
                "order_totals",
                [
                    Column(name="id", data_type="integer", visible_to=["analyst", "admin"]),
                    Column(name="net", data_type="numeric", visible_to=["analyst", "admin"]),
                ],
                view_sql="SELECT orders.id AS id, orders.amount * 2 AS net FROM orders",
            ),
        ],
        relationships=[
            Relationship(
                id="orders-to-customers",
                source_table_id="orders",
                target_table_id="customers",
                source_column="customer_id",
                target_column="id",
                cardinality=Cardinality.many_to_one,
                owner="join-steward",
                version=3,
                needs_review=True,
            )
        ],
        roles=[
            Role(id="analyst", capabilities=[], domain_access=["*"]),
            Role(id="admin", capabilities=[], domain_access=["*"]),
        ],
        rls_rules=[RLSRule(table_id="orders", role_id="analyst", filter=RLS_FILTER)],
    )
    return build_snapshot(config, org_id="acme", dialect="postgres")


def _egress_config(provider: str) -> MetadataEgressConfig:
    return MetadataEgressConfig(
        enabled=True, provider=provider, endpoint="https://catalog.example/", timeout_seconds=5
    )


# --- registry -------------------------------------------------------------------------


def test_both_standards_targets_are_resolvable_by_name():
    assert {"openlineage", "openmetadata"} <= set(registered_providers())
    assert isinstance(metadata_egress(_egress_config("openlineage")), OpenLineageEgress)
    assert isinstance(metadata_egress(_egress_config("openmetadata")), OpenMetadataEgress)


# --- OpenLineage ----------------------------------------------------------------------


def test_openlineage_emits_a_static_dataset_event_per_table(snapshot):
    events = to_events(snapshot, event_time=EVENT_TIME)
    datasets = [e for e in events if e.kind == "dataset"]
    assert [e.payload["dataset"]["name"] for e in datasets] == [
        "wh.public.orders",
        "wh.public.customers",
        "wh.public.order_totals",
    ]
    # A static dataset event carries no run: a base table is not produced by a Provisa job,
    # and asserting one would invent a process the catalog would then display.
    assert all("run" not in e.payload and "eventType" not in e.payload for e in datasets)
    assert all(e.payload["producer"] == PRODUCER for e in events)


def test_openlineage_event_names_its_own_type_in_the_schema_url(snapshot):
    """A consumer deduces the event shape from ``schemaURL``.

    Marquez reads a bare spec URL as a RunEvent and rejects the dataset event for a null run,
    so the anchor is what makes a static dataset event acceptable, not decoration.
    """
    events = to_events(snapshot, event_time=EVENT_TIME)
    by_kind = {e.kind: e.payload["schemaURL"] for e in events}
    assert by_kind["dataset"] == DATASET_EVENT_URL
    assert by_kind["lineage"] == RUN_EVENT_URL
    assert by_kind["dataset"] != by_kind["lineage"]


def test_openlineage_dataset_event_carries_schema_ownership_and_description(snapshot):
    events = to_events(snapshot, event_time=EVENT_TIME)
    orders = next(
        e for e in events if e.payload.get("dataset", {}).get("name") == "wh.public.orders"
    )
    facets = orders.payload["dataset"]["facets"]
    fields = {f["name"]: f for f in facets["schema"]["fields"]}
    assert fields["amount"]["type"] == "numeric"
    assert fields["amount"]["description"] == "Order total"
    assert facets["ownership"]["owners"] == [{"name": "data-steward", "type": "STEWARD"}]
    assert facets["provisa_domain"]["domainId"] == "sales"


def test_openlineage_column_lineage_facet_names_input_field_and_transform(snapshot):
    events = to_events(snapshot, event_time=EVENT_TIME)
    run = next(e for e in events if e.kind == "lineage")
    assert run.payload["eventType"] == "COMPLETE"
    assert run.payload["job"]["name"] == "wh.public.order_totals"
    assert [(i["namespace"], i["name"]) for i in run.payload["inputs"]] == [
        ("provisa://acme", "wh.public.orders")
    ]
    fields = run.payload["outputs"][0]["facets"]["columnLineage"]["fields"]
    net = fields["net"]["inputFields"][0]
    assert net["name"] == "wh.public.orders"
    assert net["field"] == "amount"
    assert net["transformations"][0]["type"] == "DIRECT"
    assert "amount" in net["transformations"][0]["description"]


def test_openlineage_run_event_repeats_the_schema_of_every_dataset_it_names(snapshot):
    """A run event replaces the facets of the datasets it names.

    Sending the output with columnLineage alone drops the schema the dataset event registered,
    and a consumer that resolves column lineage against known columns is then left with none.
    """
    events = to_events(snapshot, event_time=EVENT_TIME)
    run = next(e for e in events if e.kind == "lineage")
    output = run.payload["outputs"][0]
    assert {f["name"] for f in output["facets"]["schema"]["fields"]} == {"id", "net"}
    assert "columnLineage" in output["facets"]
    upstream = run.payload["inputs"][0]
    assert "amount" in {f["name"] for f in upstream["facets"]["schema"]["fields"]}


def test_openlineage_run_id_is_stable_across_publishes(snapshot):
    first = to_events(snapshot, event_time=EVENT_TIME)
    second = to_events(snapshot, event_time=datetime(2027, 1, 1, tzinfo=UTC))
    run_ids = [
        [e.payload["run"]["runId"] for e in events if e.kind == "lineage"]
        for events in (first, second)
    ]
    assert run_ids[0] == run_ids[1]


def test_openlineage_namespace_is_org_scoped(snapshot):
    events = to_events(snapshot, event_time=EVENT_TIME)
    assert {e.payload["dataset"]["namespace"] for e in events if e.kind == "dataset"} == {
        "provisa://acme"
    }


def test_openlineage_governance_facet_carries_signals_without_rule_bodies(snapshot):
    events = to_events(snapshot, event_time=EVENT_TIME)
    orders = next(
        e for e in events if e.payload.get("dataset", {}).get("name") == "wh.public.orders"
    )
    signals = orders.payload["dataset"]["facets"]["provisa_governance"]["signals"]
    by_signal = {s["signal"] for s in signals}
    assert by_signal == {"masked", "rls_restricted", "visibility_restricted"}
    masked = next(s for s in signals if s["signal"] == "masked")
    assert masked["asset"] == "wh.public.orders.ssn"
    assert masked["exemptRoles"] == ["admin"]
    text = json.dumps([e.payload for e in events])
    assert MASK_PATTERN not in text
    assert RLS_FILTER not in text


def test_openlineage_relationship_facet_carries_owner_and_review_flag(snapshot):
    events = to_events(snapshot, event_time=EVENT_TIME)
    orders = next(
        e for e in events if e.payload.get("dataset", {}).get("name") == "wh.public.orders"
    )
    rel = orders.payload["dataset"]["facets"]["provisa_relationships"]["relationships"][0]
    assert rel["id"] == "orders-to-customers"
    assert rel["target"] == "wh.public.customers"
    assert rel["owner"] == "join-steward"
    assert rel["needsReview"] is True


@pytest.mark.asyncio
async def test_openlineage_publish_posts_every_event_and_counts_by_kind(snapshot, monkeypatch):
    posted: list[tuple[str, dict]] = []

    async def _post(self, url, json=None, headers=None):
        posted.append((url, json))
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    result = await OpenLineageEgress(_egress_config("openlineage")).publish(snapshot)
    assert result.ok
    assert result.published == {"dataset": 3, "lineage": 1}
    assert {url for url, _ in posted} == {"https://catalog.example/api/v1/lineage"}


@pytest.mark.asyncio
async def test_openlineage_publish_reports_the_rejected_asset_and_continues(snapshot, monkeypatch):
    """One dataset the catalog refuses must not cost the publish the rest (REQ-1068)."""

    async def _post(self, url, json=None, headers=None):
        request = httpx.Request("POST", url)
        if json.get("dataset", {}).get("name") == "wh.public.customers":
            return httpx.Response(422, text="bad dataset", request=request)
        return httpx.Response(200, request=request)

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    result = await OpenLineageEgress(_egress_config("openlineage")).publish(snapshot)
    assert not result.ok
    assert [e.asset.fqn() for e in result.errors] == ["wh.public.customers"]
    assert "422" in result.errors[0].message
    assert result.published == {"dataset": 2, "lineage": 1}


# --- OpenMetadata ---------------------------------------------------------------------


def test_openmetadata_emits_containers_before_the_tables_that_reference_them(snapshot):
    kinds = [e.kind for e in to_entities(snapshot)]
    assert kinds.index("classification") < kinds.index("tag")
    assert kinds.index("tag") < kinds.index("table")
    assert kinds.index("service") < kinds.index("database") < kinds.index("schema")
    assert kinds.index("schema") < kinds.index("table")


def test_openmetadata_table_entity_carries_columns_tags_and_relationship_extension(snapshot):
    entities = to_entities(snapshot)
    orders = next(e for e in entities if e.kind == "table" and e.asset.fqn() == "wh.public.orders")
    assert orders.body["databaseSchema"] == "wh.default.public"
    columns = {c["name"]: c for c in orders.body["columns"]}
    assert columns["amount"]["dataTypeDisplay"] == "numeric"
    assert [t["tagFQN"] for t in columns["ssn"]["tags"]] == [
        f"{CLASSIFICATION}.masked",
        f"{CLASSIFICATION}.visibility_restricted",
    ]
    assert [t["tagFQN"] for t in orders.body["tags"]] == [f"{CLASSIFICATION}.rls_restricted"]
    # A JSON document, not a nested object: OpenMetadata's own tabular custom property caps a
    # table at three columns and a relationship carries eight fields.
    rel = json.loads(orders.body["extension"][RELATIONSHIP_PROPERTY])[0]
    assert rel["owner"] == "join-steward"
    assert rel["version"] == 3


def test_openmetadata_unstewarded_domain_publishes_with_no_owner(snapshot):
    entities = to_entities(snapshot)
    domains = {e.body["name"]: e for e in entities if e.kind == "domain"}
    # The owner reference itself is filled in at publish time from the id the server assigns the
    # steward; what the entity carries is which steward it belongs to.
    assert domains["sales"].owned_by == "data-steward"
    assert "owners" not in domains["sales"].body
    assert domains["lab"].owned_by is None
    assert "governance pending" in domains["lab"].body["description"]
    users = [e for e in entities if e.kind == "user"]
    assert [u.body["name"] for u in users] == ["data-steward"]
    # A steward is a Provisa identity, not a mailbox, so the address it publishes under is at a
    # reserved domain that cannot receive mail.
    assert users[0].body["email"].endswith("@provisa.invalid")
    kinds = [e.kind for e in entities]
    assert kinds.index("user") < kinds.index("domain")


def test_openmetadata_lineage_groups_columns_under_one_table_edge(snapshot):
    requests = to_lineage_requests(snapshot)
    assert len(requests) == 1
    edge = requests[0].body["edge"]
    # Named by FQN here; publish() swaps in the UUID the server assigned the table, because
    # OpenMetadata's AddLineage addresses an endpoint by id.
    assert edge["fromEntity"]["fullyQualifiedName"] == "wh.default.public.orders"
    assert edge["toEntity"]["fullyQualifiedName"] == "wh.default.public.order_totals"
    assert "id" not in edge["fromEntity"]
    columns = {c["toColumn"]: c for c in edge["lineageDetails"]["columnsLineage"]}
    assert set(columns) == {
        "wh.default.public.order_totals.id",
        "wh.default.public.order_totals.net",
    }
    net = columns["wh.default.public.order_totals.net"]
    assert net["fromColumns"] == ["wh.default.public.orders.amount"]
    assert "amount" in net["function"]


def test_openmetadata_never_hands_the_catalog_source_credentials(snapshot):
    """OpenMetadata must reach the data only through Provisa; a connection config with real
    credentials would give it an ungoverned path to the source."""
    service = next(e for e in to_entities(snapshot) if e.kind == "service")
    config = service.body["connection"]["config"]
    assert config["type"] == "CustomDatabase"
    assert set(config) == {"type", "sourcePythonClass"}


def test_openmetadata_payload_carries_no_rule_bodies(snapshot):
    text = json.dumps([e.body for e in (*to_entities(snapshot), *to_lineage_requests(snapshot))])
    assert MASK_PATTERN not in text
    assert RLS_FILTER not in text
    assert "current_setting" not in text


@pytest.mark.asyncio
def _entity_response(url: str, body: dict) -> dict:
    """What an OpenMetadata upsert echoes back.

    Two ids matter to the adapter: a table's, because it addresses lineage endpoints by the UUID
    the server assigns, and a user's, because it addresses a domain's owner the same way.
    """
    if url.endswith("/users"):
        return {"id": str(uuid.uuid5(uuid.NAMESPACE_URL, body["name"]))}
    if not url.endswith("/tables"):
        return {}
    fqn = f"{body['databaseSchema']}.{body['name']}"
    return {"id": str(uuid.uuid5(uuid.NAMESPACE_URL, fqn)), "fullyQualifiedName": fqn}


async def _type_get(self, url, params=None, headers=None):
    """The metadata-type lookups the relationship custom property is registered against."""
    name = url.rsplit("/", 1)[-1]
    return httpx.Response(
        200, json={"id": str(uuid.uuid5(uuid.NAMESPACE_URL, name))}, request=httpx.Request("GET", url)
    )


async def test_openmetadata_publish_upserts_every_entity(snapshot, monkeypatch):
    seen: list[str] = []

    async def _put(self, url, json=None, headers=None):
        seen.append(url)
        return httpx.Response(
            200, json=_entity_response(url, json), request=httpx.Request("PUT", url)
        )

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    monkeypatch.setattr(httpx.AsyncClient, "get", _type_get)
    result = await OpenMetadataEgress(_egress_config("openmetadata")).publish(snapshot)
    assert result.ok
    assert result.published["table"] == 3
    assert result.published["lineage"] == 1
    assert result.published["domain"] == 2
    assert result.published["user"] == 1
    assert all(url.startswith("https://catalog.example/api/v1/") for url in seen)
    # The custom-property registration is a schema change on the target, not an asset, so it
    # is sent but never counted as something published.
    assert result.total_published() == len([url for url in seen if "/metadata/types/" not in url])


@pytest.mark.asyncio
async def test_openmetadata_publish_reports_the_rejected_entity(snapshot, monkeypatch):
    async def _put(self, url, json=None, headers=None):
        request = httpx.Request("PUT", url)
        if url.endswith("/tables") and json["name"] == "order_totals":
            return httpx.Response(400, text="rejected", request=request)
        return httpx.Response(200, json=_entity_response(url, json), request=request)

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    monkeypatch.setattr(httpx.AsyncClient, "get", _type_get)
    result = await OpenMetadataEgress(_egress_config("openmetadata")).publish(snapshot)
    # Two failures, not one: the rejected table, and the lineage edge into it — which is
    # reported rather than sent, because an edge whose endpoint the catalog never accepted has
    # no id to address and would come back as a confusing second table-level error.
    assert [e.asset.fqn() for e in result.errors] == [
        "wh.public.order_totals",
        "wh.public.order_totals.net",
    ]
    assert "was not upserted" in result.errors[1].message
    assert result.published["table"] == 2
    assert "lineage" not in result.published


@pytest.mark.asyncio
async def test_openmetadata_publish_reports_a_steward_whose_user_the_catalog_refused(
    snapshot, monkeypatch
):
    """REQ-609: a domain whose steward could not be published must not publish as unstewarded."""

    async def _put(self, url, json=None, headers=None):
        request = httpx.Request("PUT", url)
        if url.endswith("/users"):
            return httpx.Response(403, text="refused", request=request)
        return httpx.Response(200, json=_entity_response(url, json), request=request)

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    monkeypatch.setattr(httpx.AsyncClient, "get", _type_get)
    result = await OpenMetadataEgress(_egress_config("openmetadata")).publish(snapshot)
    assert [e.asset.fqn() for e in result.errors] == ["data-steward", "sales"]
    assert "ownership cannot be published" in result.errors[1].message
    # The unstewarded domain is unaffected — it has no owner to lose.
    assert result.published["domain"] == 1


@pytest.mark.asyncio
async def test_openmetadata_publish_reports_relationships_it_could_not_declare(
    snapshot, monkeypatch
):
    """A custom property the target refuses costs the relationships, not every table."""
    sent: list[dict] = []

    async def _put(self, url, json=None, headers=None):
        request = httpx.Request("PUT", url)
        if "/metadata/types/" in url:
            return httpx.Response(403, text="refused", request=request)
        if url.endswith("/tables"):
            sent.append(json)
        return httpx.Response(200, json=_entity_response(url, json), request=request)

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    monkeypatch.setattr(httpx.AsyncClient, "get", _type_get)
    result = await OpenMetadataEgress(_egress_config("openmetadata")).publish(snapshot)
    assert [e.asset.fqn() for e in result.errors] == [RELATIONSHIP_PROPERTY]
    assert "were not published" in result.errors[0].message
    assert result.published["table"] == 3
    assert not any("extension" in body for body in sent)
