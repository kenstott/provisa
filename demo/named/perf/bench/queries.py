# Copyright (c) 2026 Kenneth Stott
# Canary: 185bf825-c6e3-4e3d-8e6c-eb6c7546edab
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The perf-demo benchmark query matrix — one definition per logical query, run over whichever
transports it applies to. Two axes, not flat "transports": WIRE TRANSPORT (pgwire / Bolt / HTTP /
gRPC — Arrow Flight is itself gRPC-transport) x ENCODING on that transport (SQL rows / Cypher-JSON /
GraphQL-JSON / REST-JSON / Protobuf / Arrow). Concretely: sql = pgwire, cypher = Bolt, http =
Cypher-over-HTTP (POST /data/cypher, REST/Cypher-JSON encoding on the HTTP transport), graphql =
POST /data/graphql (GraphQL-JSON encoding on the SAME HTTP transport http already uses), flight =
Arrow Flight (gRPC transport + Arrow encoding), grpc = Provisa's OTHER gRPC-based data-access
surface, provisa/grpc/server.py's generated ProvisaService (gRPC transport + Protobuf encoding) — a
distinct point in the same space from Flight, not an unrelated category. Schema/table names match
demo/named/perf/generate_*.py and fragment.yaml exactly.

Chokepoints covered (see conversation design): large result set (both single-source and federated —
large_scan vs large_federated_join), tiny rapid lookups, single-source vs cross-engine federation,
aggregation pushdown, Neo4j-source materialize cost, and Cypher-over-Bolt vs SQL-over-pgwire for both
a single-source-indexed pattern and a cross-engine pattern.

``graphql`` query text is verified against the real auto-generated schema (provisa/compiler/
schema_gen.py, schema_inputs.py, aggregate_gen.py) — root field names camelCase the table name
(default global_gql_naming_convention == "apollo_graphql", provisa/api/app.py:258-259), where-arg
filters are ``{field: {eq: value}}``/``{gte:}``/``{lte:}`` (provisa/compiler/type_map.py's
FILTER_TYPE_MAP), and group-by aggregation is ``{field}GroupBy(by: [...]) { groupKey aggregate
{ count sum { col } } }`` (schema_gen.py's _build_group_by_query_field + aggregate_gen.py's
build_agg_fields_type). No relationships are registered between orders/order_events/order_docs in
fragment.yaml (only the Neo4j bench_* edge tables have any), so federated_join/large_federated_join
cannot be a single nested GraphQL selection — they run as multiple aliased root fields in ONE
request instead (endpoint.py's _handle_query: "Multiple root fields are executed independently and
merged"), which still issues one round trip per iteration like the SQL join does, just without a
server-side JOIN.

``grpc`` query text targets provisa/grpc/server.py's generated ``ProvisaService`` (package
provisa.v1, RPCs ``Query{Type}``/``Query{Type}GroupBy``, verified against provisa/grpc/proto_gen.py
+ query_ir.py). CONFIRMED GAP, not a guess: neither RPC's ``filter`` field is ever read server-side
(grep provisa/grpc/*.py for "request.filter" — zero hits; query_ir.py's grpc_table_to_semantic_sql
only forwards ``limit``, grpc_table_to_group_by_graphql_text only forwards ``by``/``funcs``/
``include``). Every query needing a WHERE (point_lookup, federated_join, cypher_*) is therefore not
expressible over this transport today without silently returning the wrong rows — those are left
with ``grpc=None``. Only the matrix's two filter-free queries (large_scan's full unfiltered scan,
single_source_aggregation's un-filtered GROUP BY) get a ``grpc`` spec.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Query:
    id: str
    category: str
    description: str
    sql: str | None = None
    cypher: str | None = None
    # GraphQL query text (POST /data/graphql) — same HTTP transport as `cypher`'s http surface,
    # GraphQL-JSON encoding instead of Cypher-JSON. See module docstring for field-naming/filter
    # verification.
    graphql: str | None = None
    # Native gRPC spec (provisa.v1.ProvisaService), or None if not expressible — see module
    # docstring's CONFIRMED GAP note. {"mode": "scan", "type_name": "OrderItems"} for a plain
    # Query{Type} streaming scan; {"mode": "group_by", "type_name": "Orders", "by": ["region"]}
    # for a Query{Type}GroupBy streaming call.
    grpc: dict | None = None
    iterations: int = 20  # repeat this many times sequentially, for latency percentiles
    params: dict = field(default_factory=dict)


QUERIES: list[Query] = [
    Query(
        id="point_lookup",
        category="tiny_rapid",
        description="Single-row point lookup by primary key — per-query overhead floor",
        sql="SELECT * FROM perf_bench.orders WHERE order_id = :order_id",
        # WHERE clause, not an inline {order_id: $order_id} property-map filter: confirmed live
        # (pg_stat_activity, GCP perf run) that Provisa's Cypher-to-SQL translator silently drops
        # the inline map filter, executing an unfiltered full table scan instead of a point lookup.
        cypher="""
            MATCH (o:orders)
            WHERE o.order_id = $order_id
            RETURN o.order_id AS order_id, o.customer_id AS customer_id, o.region AS region,
                   o.status AS status, o.amount AS amount
        """,
        # camelCase root field/args verified against schema_gen.py/schema_inputs.py — see module
        # docstring. Variable name matches the SQL/Cypher params dict key (order_id) so the same
        # `q.params` dict feeds all three transports' request bodies unchanged.
        graphql="""
            query($order_id: Int!) {
              orders(where: {orderId: {eq: $order_id}}) {
                orderId
                customerId
                region
                status
                amount
              }
            }
        """,
        params={"order_id": 12345},
        iterations=200,
    ),
    Query(
        id="large_scan",
        category="large_result",
        description="order_items scan (2M-row LIMIT, shrunk from the full ~80M) — "
        "streaming/memory/pagination cost",
        # TEMPORARY (2026-09-26): the full unfiltered ~80M-row scan takes ~41min/iteration —
        # workable for the FINAL calibrated numbers, but every bug-fix cycle needing a real
        # end-to-end run against it (three found and fixed this session: the pgwire cursor
        # head-peek bug, the pgwire row encoder, pg_advisory_unlock_all) burns 2+ hours just to
        # find out whether a fix worked. LIMIT 2000000 so debugging iterations stay fast; restore
        # the unfiltered scan (this comment + the two lines below) once no further server-side
        # bugs are expected on this path.
        # sql="SELECT * FROM perf_bench.order_items",
        sql="SELECT * FROM perf_bench.order_items LIMIT 2000000",
        # No GraphQL text: a GraphQL response over the redirect threshold comes back as an S3
        # file manifest, not inline JSON (endpoint.py:223-231) — a different response shape from
        # every other transport's inline-JSON large_scan run, so it wouldn't measure the same
        # thing. See run_benchmark.py's GraphqlTransport docstring.
        grpc={
            "mode": "scan",
            "type_name": "OrderItems",
        },  # unfiltered scan — matches this SQL exactly
        iterations=3,
    ),
    Query(
        id="single_source_aggregation",
        category="aggregation",
        description="Revenue rollup by region over orders — pushdown vs pull-then-aggregate",
        sql="SELECT region, sum(amount) AS revenue, count(*) AS n FROM perf_bench.orders GROUP BY region",
        # ordersGroupBy: schema_gen.py's _build_group_by_query_field (gated by fragment.yaml's
        # orders.enable_group_by: true). aggregate.sum.amount == revenue, aggregate.count == n —
        # sum's sub-field is the raw column name "amount" (aggregate_gen.py keys AggregateFields
        # sub-messages by physical col_name, not camelCase).
        graphql="""
            query {
              ordersGroupBy(by: [region]) {
                groupKey
                aggregate {
                  count
                  sum { amount }
                }
              }
            }
        """,
        # Same shape server-side: Query{Type}GroupBy's `by` also un-filtered — this SQL has no
        # WHERE either, so the gap doesn't affect this query.
        grpc={"mode": "group_by", "type_name": "Orders", "by": ["region"]},
        iterations=10,
    ),
    Query(
        id="federated_join",
        category="federated",
        description="orders (PG) join order_events (ClickHouse) join order_docs (Mongo) on order_id",
        sql="""
            SELECT o.order_id, o.amount, e.event_type, d.status
            FROM perf_bench.orders o
            JOIN perf_bench.order_events e ON e.order_id = o.order_id
            JOIN perf_bench.order_docs d ON d.order_id = o.order_id
            WHERE o.order_id BETWEEN :lo AND :hi
        """,
        # No relationship is registered between orders/order_events/order_docs in fragment.yaml
        # (only the Neo4j bench_* edge tables have any), so this can't be one nested GraphQL
        # selection — three aliased root fields in ONE request instead (still one HTTP round
        # trip per iteration, like the SQL join). See module docstring.
        graphql="""
            query($lo: Int!, $hi: Int!) {
              orders(where: {orderId: {gte: $lo, lte: $hi}}) {
                orderId
                amount
              }
              orderEvents(where: {orderId: {gte: $lo, lte: $hi}}) {
                orderId
                eventType
              }
              orderDocs(where: {orderId: {gte: $lo, lte: $hi}}) {
                orderId
                status
              }
            }
        """,
        params={"lo": 1, "hi": 1000},
        iterations=10,
    ),
    Query(
        id="large_federated_join",
        category="large_federated",
        description=(
            "orders (PG) join order_events (ClickHouse) join order_docs (Mongo) on order_id, "
            "1M-order range — large-result counterpart to federated_join, so cross-engine "
            "merge/serialization cost at scale is measured, not just single-source large_scan"
        ),
        sql="""
            SELECT o.order_id, o.amount, e.event_type, d.status
            FROM perf_bench.orders o
            JOIN perf_bench.order_events e ON e.order_id = o.order_id
            JOIN perf_bench.order_docs d ON d.order_id = o.order_id
            WHERE o.order_id BETWEEN :lo AND :hi
        """,
        # No GraphQL text: same redirect-to-S3-manifest reasoning as large_scan (this range
        # spans ~1M orders per table, well over any sane inline-JSON threshold) — see
        # large_scan's comment and GraphqlTransport's docstring.
        params={"lo": 1, "hi": 1_000_000},
        iterations=3,  # matches large_scan's iteration count — this is also an expensive query
    ),
    Query(
        id="neo4j_materialize_cold",
        category="materialize_cost",
        description="First read of a Neo4j-source table after cache expiry — one Cypher HTTP call + flatten + land",
        sql="SELECT count(*) FROM perf_bench.bench_order_node",
        iterations=1,
    ),
    Query(
        id="neo4j_materialize_warm",
        category="materialize_cost",
        description="Repeat read of the same Neo4j-source table within TTL — local materialized scan",
        sql="SELECT count(*) FROM perf_bench.bench_order_node",
        iterations=10,
    ),
    Query(
        id="cypher_single_source",
        category="cypher_vs_sql_single_source",
        description="Multi-hop pattern resolved entirely within indexed Postgres orders/order_items",
        sql="""
            SELECT o.order_id, o.customer_id, i.sku, i.quantity
            FROM perf_bench.orders o
            JOIN perf_bench.order_items i ON i.order_id = o.order_id
            WHERE o.customer_id = :customer_id
        """,
        # See point_lookup's comment on the inline property-map-filter translator bug.
        cypher="""
            MATCH (o:orders)<-[:BELONGS_TO]-(i:order_items)
            WHERE o.customer_id = $customer_id
            RETURN o.order_id AS order_id, o.customer_id AS customer_id, i.sku AS sku, i.quantity AS quantity
        """,
        params={"customer_id": 4242},
        iterations=20,
    ),
    Query(
        id="cypher_cross_engine",
        category="cypher_vs_sql_cross_engine",
        description="Same multi-hop shape, but requires the Neo4j-materialized replica alongside Postgres",
        sql="""
            SELECT c.customer_id, o.order_id, p.product_id
            FROM perf_bench.bench_placed_edge pl
            JOIN perf_bench.bench_customer_node c ON c.customer_id = pl.customer_id
            JOIN perf_bench.bench_order_node o ON o.order_id = pl.order_id
            JOIN perf_bench.bench_contains_edge ce ON ce.order_id = o.order_id
            JOIN perf_bench.bench_product_node p ON p.product_id = ce.product_id
            WHERE c.customer_id = :customer_id
        """,
        # See point_lookup's comment on the inline property-map-filter translator bug.
        cypher="""
            MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product)
            WHERE c.customer_id = $customer_id
            RETURN c.customer_id AS customer_id, o.order_id AS order_id, p.product_id AS product_id
        """,
        params={"customer_id": 4242},
        iterations=20,
    ),
    # concurrency_ramp lives in artillery-concurrency-ramp.yml, not here — an open-model
    # (arrival-rate) load generator, not this module's closed-model sequential/threaded runs.
    # See that file's header for why the distinction matters for this one category.
]
