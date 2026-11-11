# Connector audit against the connector tree

Audit of every connector on origin/main (3a2385fee9) against REQ-2235, amended 2026-11-11 in 726750e867. Read-only: no product code changed. The machine-readable list is `connectors.json` beside this file.

## Summary

The audit finds 101 entries. The tree has two roots and today the code honors one of them. The SQL family is a real root (45 entries). The code resolver exists as four language runtimes, and exactly one connector, Spark, is a connector.yaml on top of one. The API resolvers exist as hand-written generic sources, none of them built on a code resolver yet, and the OData one does not exist.

| Base | Entries | Of which |
|---|---|---|
| SQL family | 45 | 27 databases and warehouses, 9 files and lakes, 8 engine-native non-SQL stores, the custom-connector extension point |
| Code resolver | 6 | 4 language runtimes, 1 branded connector (Spark), 1 engine-attach path for built hosts that REQ-2100 says is retired |
| API resolver, OpenAPI/REST | 2 | the generic source and 1 brand (Stripe, in Python, not a connector.yaml) |
| API resolver, GraphQL | 4 | the generic source and 3 brands (GitHub, GitLab, Shopify, in Python) |
| API resolver, gRPC | 1 | the generic source |
| API resolver, OData | 1 | does not exist; listed as a gap |
| Hand-built | 22 | 16 external systems, 6 Provisa-owned sources |
| Metadata-only | 18 | SourceType stubs with no connector (18 of the 20 stub types; Stripe and Shopify count as brands above) |
| Materialize readers | 2 | sheets_reader, motherduck_reader. freshness_reader is not a connector and is left out |

Branded connectors proper (a connector.yaml plus overrides): 1 (Spark). Branded in Python code instead of YAML: 4 (Stripe, GitHub, GitLab, Shopify). The brief's tree makes both kinds one thing, so 4 of the 5 need a move.

### Hand-built list and reasons

| Connector | Reason it is hand-built | Would sit on |
|---|---|---|
| sharepoint | adapter-in-Java; no OData resolver exists | OData resolver (Graph) |
| salesforce | logic is SOQL translation in a Java adapter | OpenAPI resolver (Salesforce publishes REST specs) or Python code resolver |
| servicenow | Table API adapter is Java; the OpenAPI reader exists beside it, so the two overlap | OpenAPI resolver (instance publishes OAS3) |
| jira | a table is a form (project + issue type); catalog logic is Jira-specific | OpenAPI resolver (Atlassian publishes a spec); forms are a source-specific catalog |
| splunk | SPL pushdown in a Java adapter | OpenAPI/REST resolver or Python code resolver |
| cloudops | three SDKs, one catalog; no API description to read | Python code resolver (three SDKs) |
| govdata | single vendor API; adapter in Java | Python code resolver or OpenAPI |
| google_workspace | mail landing and sync state are bespoke (mail_sync_state, mail_write) | OpenAPI/Discovery resolver, or Python code resolver |
| microsoft_365 | mail landing and sync state are bespoke | OData resolver (Graph) |
| quickbooks | its query language is a custom push dialect | OpenAPI resolver (OAuth, a query language in the entity endpoint) |
| cosmosdb | SDK-only, no engine catalog | Node or Python code resolver |
| qdrant | live-only: its vector query cannot be reproduced over a replica | stays hand-built, or a code resolver with a live-only tag |
| neo4j | graph model; SQL is never handed to it | Python code resolver |
| sparql | SPARQL is its own query language; contract implemented in its own code | Python code resolver |
| websocket | a stream, not a table read | Node or Python code resolver with @stream |
| rss | a feed, not a table read | Python code resolver with @stream |
| logs | Provisa-owned row shape (_file_id, _line_offset, _raw) | stays hand-built |
| ingest | Provisa-owned inbound endpoint, not a read of a system | stays hand-built |
| dbt | not a data connector | none |
| soda | Provisa-owned check runner | none |
| great_expectations | Provisa-owned check runner | none |
| data_profiler | Provisa-owned | none |

The seven Calcite pgwire adapters (sharepoint, salesforce, servicenow, jira, splunk, cloudops, govdata) are the largest block. Their logic is Java, outside the Python tree, so moving them means rewriting rather than re-basing.

### Biggest gaps between today and the tree

- **No source contract in code.** The pieces exist apart: `federation/connector_contract.py` (capabilities, plan, call), `codesource/manifest.py` (@requires), `grpc_remote_contract.py`, `api_source/contract.py`, `graphql_remote/contract.py`. No one module states catalog, rows, pushdown, requires, login and host together, so nothing can generate a page from it yet.
- **The API resolvers are not instances of a code resolver.** OpenAPI, GraphQL and gRPC each carry their own loader, mapper and executor in provisa/openapi, graphql_remote and grpc_remote.
- **OData has no resolver.** provisa/api/odata is the OData front door. Microsoft Graph (sharepoint, microsoft_365) is the natural first source.
- **No per-base conformance suite.** Conformance today is engine-level (test_governance_parity_e2e, test_cross_vendor_parity_e2e, REQ-827) plus a harness (connector_source_harness.py). Every connector's tests are bespoke. There is no suite a branded connector could inherit.
- **Brands are Python, not connector.yaml.** Four brands live in brands.py; Spark is the only connector.yaml.
- **Seven Java adapters** behind pgwire sit outside the tree.
- **Live verification is thin outside SQL.** Of the hand-built and API entries, only Salesforce, ServiceNow and the warehouse-lane sources run against a live service in CI. Jira has no integration test at all. Microsoft 365, Google Workspace, QuickBooks and Shopify have live tests that no CI lane runs.
- **Pushdown is not stated per source.** The summaries in connectors.json come from module docstrings and the base. A generator needs each connector to declare its own.

### Open questions

- **Do engine-native non-SQL stores (MongoDB, Cassandra, Redis, Elasticsearch, Pinot, Druid, Prometheus, Kafka) belong to the SQL family?** The audit puts them there because Trino and DuckDB read them natively. Option A (recommended): keep them there as a named sub-group. Option B: treat them as hand-built. Option C: move them under the code resolver. It changes 8 entries.
- **Are files and lakes (csv, parquet, files, Delta, Iceberg, Hive) in the SQL family?** The earlier wording of REQ-2235 listed files and object storage as a base; the 2026-11-11 text does not. The audit follows the new text and puts them in the SQL family.
- **Is the engine attach of code hosts retired?** REQ-2100 (2026-10-23) says no engine attaches a code host, yet engine.py and trino_connectors.py still register DuckDBCodeHostConnector and TrinoCodeHostConnector for the four languages. Either the registrations are dead or the amendment is.
- **Calcite adapters: rewrite or wrap?** Moving them to a base means rewriting Java in Python/Node. The alternative is a rule that a host serving the contract over pgwire counts as an instance of a code resolver regardless of language.
- **The 20 stub SourceTypes.** Each is marked with a candidate base (mostly OpenAPI brands, REQ-1923). The candidates are unverified: each vendor spec must be well typed before it qualifies.
- **Source types for brands.** `stripe` and `shopify` are both SourceType stubs and brands. GitHub and GitLab are brands only. Pick one identity per brand.

### How the lane and test columns were built

Tests are the files whose name contains the source type, plus a few aliases found by hand (Delta, HiveServer2, the file sources). A file named for an engine mixes several sources, so duckdb, trino and hive are marked not resolved. The lane is inferred from the pytest markers in those files and the lane definitions in scripts/ci/run_lane.py. It is not read from a CI run, and a source tested inside a multi-source file may have no entry here. Treat `none` as `not found by name`, and check it before acting on it.

Live lanes: warehouse is nightly with repo secrets against real cloud services. salesforce, athena and servicenow run on demand only. Fabric, Microsoft 365, Google Workspace, QuickBooks and Shopify have live tests that are in no CI lane. Every other lane runs containers.

## Per-connector table

### SQL family (read natively by the serving engines) (45)

| Name | What it is today | Needs from the base to move | Tests | Live lane |
|---|---|---|---|---|
| postgresql | SQL family: engine attach/scan (duckdb, trino, clickhouse, pg); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 1 unit, 1 integ: steps/steps_postgresql_deployment.py, unit/test_control_plane_postgresql_only.py | core/app |
| mysql | SQL family: engine attach/scan (duckdb, trino, clickhouse); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 1 unit, 0 integ: unit/test_mysql_discovery_binds_through_its_driver.py | none |
| mariadb | SQL family: engine attach/scan (trino); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 0 unit, 1 integ: integration/test_mariadb_source_e2e.py | core/app |
| singlestore | SQL family: engine attach/scan (trino); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 5 unit, 2 integ: integration/test_singlestore_bulk_load_e2e.py, integration/test_singlestore_source_e2e.py | warehouse |
| sqlserver | SQL family: engine attach/scan (duckdb, trino); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 0 unit, 1 integ: integration/test_sqlserver_source_e2e.py | core/app |
| oracle | SQL family: engine attach/scan (duckdb, trino); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 4 unit, 1 integ: integration/test_oracle_source_e2e.py, unit/grpc_encoder/oracle.py | core/app |
| firebird | SQL family: engine attach/scan (duckdb) | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 0 unit, 1 integ: integration/test_firebird_source_e2e.py | core/app |
| duckdb | SQL family: engine attach/scan (duckdb); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 1 unit, 0 integ: unit/test_duckdb_source_introspection.py | not resolved |
| motherduck | SQL family: engine attach/scan (duckdb) | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 5 unit, 3 integ: integration/test_embedded_pg_duckdb_motherduck_e2e.py, integration/test_motherduck_duckdb_e2e.py | warehouse |
| saphana | SQL family: no engine reach; direct driver | nothing to move; reach is driver-only or landing-only, so the page must say it is not engine-attached. | 0 unit, 0 integ | none |
| jdbc | SQL family: no engine reach; direct driver | nothing to move; reach is driver-only or landing-only, so the page must say it is not engine-attached. | 5 unit, 6 integ: integration/test_jdbc_driver_e2e.py, integration/test_jdbc_introspection_pgwire.py | isolated or core/app |
| cockroachdb | SQL family: engine attach/scan (trino); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 0 unit, 1 integ: integration/test_cockroachdb_source_e2e.py | core/app |
| yugabytedb | SQL family: engine attach/scan (trino); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 0 unit, 1 integ: integration/test_yugabytedb_source_e2e.py | core/app |
| greenplum | SQL family: engine attach/scan (trino); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 0 unit, 1 integ: integration/test_greenplum_source_e2e.py | core/app |
| tidb | SQL family: engine attach/scan (trino); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 0 unit, 1 integ: integration/test_tidb_source_e2e.py | core/app |
| snowflake | SQL family: engine attach/scan (duckdb, trino, pg); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 8 unit, 7 integ: integration/snowflake_warehouse.py, integration/test_metadata_export_snowflake_horizon_e2e.py | warehouse |
| bigquery | SQL family: engine attach/scan (duckdb, trino, pg); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 4 unit, 2 integ: integration/test_bigquery_federation_engine_e2e.py, integration/test_replica_target_bigquery.py | warehouse |
| databricks | SQL family: engine attach/scan (duckdb, trino, pg); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 7 unit, 7 integ: integration/databricks_uc.py, integration/databricks_warehouse.py | warehouse |
| fabric | SQL family: engine attach/scan (duckdb); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 3 unit, 4 integ: integration/fabric_capacity.py, integration/fabric_external_link.py | none in CI |
| synapse | SQL family: engine attach/scan (duckdb); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 0 unit, 3 integ: integration/synapse_provision.py, integration/test_synapse_federation_engine_e2e.py | warehouse |
| redshift | SQL family: engine attach/scan (trino); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 1 unit, 3 integ: integration/redshift_cluster.py, integration/test_redshift_pipeline_e2e.py | warehouse |
| athena | SQL family: no engine reach; direct driver | nothing to move; reach is driver-only or landing-only, so the page must say it is not engine-attached. | 1 unit, 2 integ: integration/test_athena_pipeline_e2e.py, integration/test_athena_source_e2e.py | athena |
| trino | SQL family: no engine reach; direct driver | nothing to move; reach is driver-only or landing-only, so the page must say it is not engine-attached. | 0 unit, 0 integ | not resolved |
| clickhouse | SQL family: engine attach/scan (duckdb, trino); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 9 unit, 10 integ: integration/test_clickhouse_engine_attach_names_per_org.py, integration/test_clickhouse_runtime_e2e.py | isolated or core/app |
| exasol | SQL family: engine attach/scan (trino); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 1 unit, 3 integ: integration/test_exasol_direct_driver_e2e.py, integration/test_exasol_pipeline_e2e.py | isolated or core/app |
| hiveserver2 | SQL family: no engine reach; direct driver | nothing to move; reach is driver-only or landing-only, so the page must say it is not engine-attached. | 0 unit, 1 integ: integration/test_hive_direct_driver_e2e.py | not resolved |
| sqlite | SQL family: engine attach/scan (duckdb, trino, clickhouse); direct driver | nothing to move: it is the root. To list: a stated per-type pushdown summary and a base conformance mark. | 0 unit, 2 integ: integration/test_duckdb_sqlite_source_e2e.py, integration/test_embedded_pg_sqlite_fdw_e2e.py | isolated or core/app |
| csv | SQL family: engine-read file (duckdb, trino, clickhouse, pg) | nothing to move; the engines read it natively. | 3 unit, 0 integ: unit/test_file_source_adapter.py, unit/test_file_source_locations.py | none named for it |
| parquet | SQL family: engine-read file (duckdb, trino, clickhouse, pg) | nothing to move; the engines read it natively. | 3 unit, 0 integ: unit/test_file_source_adapter.py, unit/test_file_source_locations.py | none named for it |
| files | SQL family: engine-read file set (duckdb, trino, clickhouse, pg) | nothing to move; the engines read it natively. | 9 unit, 2 integ: integration/test_files_demo.py, integration/test_files_pgwire_replica.py | isolated or core/app |
| delta_lake | SQL family: engine-read lake (duckdb, trino, clickhouse, pg) | nothing to move; the engines read it natively. | 2 unit, 1 integ: integration/test_duckdb_delta_source_e2e.py, unit/test_delta_config.py | not resolved |
| iceberg | SQL family: engine-read lake (duckdb, trino, clickhouse, pg) | nothing to move; the engines read it natively. | 7 unit, 3 integ: integration/test_embedded_pg_duckdb_iceberg_e2e.py, integration/test_profiler_iceberg_block_sample_e2e.py | isolated or core/app |
| hive | SQL family: engine-read lake (trino) | nothing to move; the engines read it natively. | 0 unit, 2 integ: integration/test_hive_pipeline_e2e.py, integration/test_hive_source_e2e.py | not resolved |
| hive_s3 | SQL family: engine-read lake (trino) | nothing to move; the engines read it natively. | 0 unit, 2 integ: integration/test_hive_s3_pipeline_e2e.py, integration/test_hive_s3_source_e2e.py | hive_s3 |
| airport | SQL family: engine-read Arrow Flight server (duckdb) | nothing to move; the engines read it natively. | 2 unit, 3 integ: integration/test_airport_credential_e2e.py, integration/test_airport_service_e2e.py | isolated or core/app |
| google_sheets | SQL family: engine-read spreadsheet (duckdb, trino); landed by events/sheets_reader.py where the engine has no gsheets extension | nothing to move; the engines read it natively. | 0 unit, 1 integ: integration/test_google_sheets_source_e2e.py | warehouse |
| mongodb | Engine catalog connector (duckdb, trino, clickhouse) with a per-type adapter in provisa/mongodb/source.py; Trino/DuckDB read it natively, path columns resolved by the connector | BOUNDARY CASE: native to an engine, but not SQL. Stays here unless the maintainer wants non-SQL stores under the code resolver (open question 1). | 4 unit, 5 integ: integration/test_mongodb_change_feed_deployment.py, integration/test_mongodb_change_stream_replica_e2e.py | datastores |
| cassandra | Engine catalog connector (trino) with a per-type adapter in provisa/cassandra/source.py; Trino/DuckDB read it natively, path columns as the adapter maps them | BOUNDARY CASE: native to an engine, but not SQL. Stays here unless the maintainer wants non-SQL stores under the code resolver (open question 1). | 1 unit, 4 integ: integration/test_cassandra_native_fetch.py, integration/test_cassandra_pipeline_e2e.py | isolated or core/app |
| redis | Engine catalog connector (trino) with a per-type adapter in provisa/redis/source.py; Trino/DuckDB read it natively, path columns as the adapter maps them | BOUNDARY CASE: native to an engine, but not SQL. Stays here unless the maintainer wants non-SQL stores under the code resolver (open question 1). | 5 unit, 3 integ: integration/test_redis_native_fetch.py, integration/test_redis_pipeline_e2e.py | isolated or core/app |
| elasticsearch | Engine catalog connector (trino) with a per-type adapter in provisa/elasticsearch/source.py; Trino/DuckDB read it natively, path columns resolved by the connector | BOUNDARY CASE: native to an engine, but not SQL. Stays here unless the maintainer wants non-SQL stores under the code resolver (open question 1). | 1 unit, 5 integ: integration/test_demo_stores_elasticsearch.py, integration/test_elasticsearch_introspect.py | datastores |
| pinot | Engine catalog connector (trino) with a per-type adapter in provisa/pinot/source.py; Trino/DuckDB read it natively, path columns as the adapter maps them | BOUNDARY CASE: native to an engine, but not SQL. Stays here unless the maintainer wants non-SQL stores under the code resolver (open question 1). | 0 unit, 2 integ: integration/test_pinot_pipeline_e2e.py, integration/test_pinot_source_e2e.py | isolated or core/app |
| druid | Engine catalog connector (trino) with a per-type adapter in provisa/druid/source.py; Trino/DuckDB read it natively, path columns as the adapter maps them | BOUNDARY CASE: native to an engine, but not SQL. Stays here unless the maintainer wants non-SQL stores under the code resolver (open question 1). | 0 unit, 2 integ: integration/test_druid_pipeline_e2e.py, integration/test_druid_source_e2e.py | isolated or core/app |
| prometheus | Engine catalog connector (trino) with a per-type adapter in provisa/prometheus/source.py; Trino/DuckDB read it natively, path columns as the adapter maps them | BOUNDARY CASE: native to an engine, but not SQL. Stays here unless the maintainer wants non-SQL stores under the code resolver (open question 1). | 2 unit, 2 integ: integration/test_prometheus_native_fetch.py, integration/test_prometheus_pipeline_e2e.py | datastores |
| kafka | Engine catalog connector (trino) with a per-type adapter in provisa/kafka/source.py; Trino/DuckDB read it natively, path columns as the adapter maps them | BOUNDARY CASE: native to an engine, but not SQL. Stays here unless the maintainer wants non-SQL stores under the code resolver (open question 1). | 15 unit, 7 integ: e2e/test_kafka_flow.py, integration/test_change_events_kafka_e2e.py | kafka |
| custom_connectors | federation/custom_connectors.py: config-driven pg_fdw, duckdb_attach, duckdb_scan and clickhouse_* kinds (REQ-1177/1178); config/custom_connectors.yaml ships empty | nothing; the SQL family's own extension point. Tests: test_custom_connectors_e2e.py, _mongo_e2e.py, _pg_e2e.py. | 0 unit, 3 integ: integration/test_custom_connectors_e2e.py, integration/test_custom_connectors_mongo_e2e.py | core/app |

### Code resolvers and what hangs off them (6)

| Name | What it is today | Needs from the base to move | Tests | Live lane |
|---|---|---|---|---|
| node | Code resolver, Node (Bun, Deno): @table/@command/@stream tags, dev mode in a sandboxed worker or built mode on a host over pgwire (REQ-2098, REQ-2100, REQ-2221) | This is the base. It needs the source contract written once, with the conformance suite and the page generator reading its tags. | 10 unit, 3 integ: integration/test_node_reader.py, integration/test_node_worker.py | core/app |
| python | Code resolver, Python: @table/@command/@stream tags, dev mode in a sandboxed worker or built mode on a host over pgwire (REQ-2098, REQ-2100, REQ-2221) | This is the base. It needs the source contract written once, with the conformance suite and the page generator reading its tags. | 6 unit, 1 integ: integration/test_python_worker.py, unit/grpc_encoder/python_ref.py | core/app |
| go | Code resolver, Go: @table/@command/@stream tags, dev mode in a sandboxed worker or built mode on a host over pgwire (REQ-2098, REQ-2100, REQ-2221) | This is the base. It needs the source contract written once, with the conformance suite and the page generator reading its tags. | 3 unit, 3 integ: integration/test_code_services_contract_go.py, integration/test_go_install_live.py | core/app |
| rust | Code resolver, Rust: @table/@command/@stream tags, dev mode in a sandboxed worker or built mode on a host over pgwire (REQ-2098, REQ-2100, REQ-2221) | This is the base. It needs the source contract written once, with the conformance suite and the page generator reading its tags. | 3 unit, 1 integ: integration/test_rust_worker.py, unit/test_rust_image_linker.py | core/app |
| spark | connectors/spark/connector.yaml plus provisa_spark (pyspark-client); built by `provisa connector build` (REQ-2221, REQ-2225) | nothing; it is the model branded connector. | 0 unit, 1 integ: integration/test_spark_connector_e2e.py | core/app |
| codehost_engine_attach | federation/codehost_connectors.py (DuckDBCodeHostConnector, PgCodeHostConnector) and TrinoCodeHostConnector, still registered for node/python/go/rust in engine.py and trino_connectors.py | REQ-2100 (Amended 2026-10-23) says no engine attaches a code host and reads are landed. Either the code is dead and can go, or the amendment and the registrations disagree. See open question 3. | 0 unit, 0 integ | none |

### API resolver: OpenAPI/REST (2)

| Name | What it is today | Needs from the base to move | Tests | Live lane |
|---|---|---|---|---|
| openapi | Hand-written generic source (provisa/openapi, provisa/api_source): spec to tables and commands, paging, replica-only. NOT yet an instance of the code resolver | Becomes an instance of the Python or Node code resolver, emitting the source contract (catalog, rows, pushdown, @requires, @login) from the spec. Today it implements its own loader, mapper and executor. | 9 unit, 4 integ: integration/test_openapi_pet_by_status.py, integration/test_openapi_source.py | isolated or core/app |
| brand:stripe | Stripe brand in Python (provisa/openapi/brands.py): pinned OpenAPI spec (provisa/openapi/_known_specs/stripe.json.gz) + auth + version header (REQ-1923). Not a connector.yaml yet | Move the brand to a connector.yaml (REQ-1923 -> REQ-2235); brands.py becomes data. The SourceType enum entry for stripe is a stub beside the brand. | 0 unit, 0 integ | none |

### API resolver: GraphQL (4)

| Name | What it is today | Needs from the base to move | Tests | Live lane |
|---|---|---|---|---|
| graphql_remote | Hand-written generic source (provisa/graphql_remote): introspection, mapper, executor, by-keys fetch; replica-only. NOT yet a code-resolver instance | Same as OpenAPI: re-express introspection and the mapper as a code-resolver instance behind the source contract. | 11 unit, 4 integ: integration/test_graphql_remote_integration.py, integration/test_graphql_remote_row_materialize.py | isolated or core/app |
| brand:github | GitHub brand in Python (provisa/graphql_remote/brands.py): baked schema (graphql_remote/_known_schemas/github.json.gz) (REQ-1923). Not a connector.yaml yet | Move the brand to a connector.yaml (REQ-1923 -> REQ-2235); brands.py becomes data. The SourceType enum entry for github does not exist (brand id only). | 1 unit, 0 integ: unit/test_graphql_remote_github_registration.py | none |
| brand:gitlab | GitLab brand in Python (provisa/graphql_remote/brands.py): baked schema (gitlab.json.gz) (REQ-1923). Not a connector.yaml yet | Move the brand to a connector.yaml (REQ-1923 -> REQ-2235); brands.py becomes data. The SourceType enum entry for gitlab does not exist (brand id only). | 0 unit, 0 integ | none |
| brand:shopify | Shopify brand in Python (provisa/graphql_remote/brands.py): baked schema (shopify.json.gz); per-store endpoint and API version (REQ-1923). Not a connector.yaml yet | Move the brand to a connector.yaml (REQ-1923 -> REQ-2235); brands.py becomes data. The SourceType enum entry for shopify is a stub beside the brand. | 2 unit, 1 integ: integration/test_shopify_source_live_e2e.py, unit/test_graphql_brand_shopify.py | none in CI |

### API resolver: gRPC (1)

| Name | What it is today | Needs from the base to move | Tests | Live lane |
|---|---|---|---|---|
| grpc_remote | Hand-written generic source (provisa/grpc_remote, provisa/source_adapters/grpc_remote_adapter.py, grpc_remote_contract.py); the only source with an adapter-contract module | Same: instance of a code resolver reading the proto; the adapter contract file is a ready seed for the source contract. | 4 unit, 1 integ: steps/steps_grpc_remote_schema_connector_req_322_329.py, unit/test_grpc_remote_cache.py | core/app |

### API resolver: OData (1)

| Name | What it is today | Needs from the base to move | Tests | Live lane |
|---|---|---|---|---|
| odata | DOES NOT EXIST as a source. provisa/api/odata is the OData FRONT door Provisa serves, not a source it reads | Build it: an OData resolver reading $metadata (CSDL) and pushing $filter/$select/$top. Microsoft Graph (sharepoint, microsoft_365) is the first customer. | 0 unit, 0 integ | none |

### Hand-built (22)

| Name | What it is today | Needs from the base to move | Tests | Live lane |
|---|---|---|---|---|
| sharepoint | Microsoft Graph via a Calcite adapter behind pgwire (federation/pgwire_replica, TrinoSharepointConnector). Needs Java adapter and OData-shaped Graph. | Would sit on: OData resolver (Graph). Reason hand-built: adapter-in-Java; no OData resolver exists. | 1 unit, 3 integ: integration/test_sharepoint_duckdb_attach.py, integration/test_sharepoint_source_e2e.py | warehouse |
| salesforce | Salesforce via a Calcite adapter behind pgwire; SOQL pushdown verified live; writes through pgwire (REQ-1946). | Would sit on: OpenAPI resolver (Salesforce publishes REST specs) or Python code resolver. Reason hand-built: logic is SOQL translation in a Java adapter. | 1 unit, 1 integ: integration/test_salesforce_source_e2e.py, unit/test_salesforce_connector.py | salesforce |
| servicenow | ServiceNow Table API via a Calcite adapter behind pgwire (REQ-1954); provisa/openapi/servicenow.py already reads the instance's own OpenAPI documents. | Would sit on: OpenAPI resolver (instance publishes OAS3). Reason hand-built: Table API adapter is Java; the OpenAPI reader exists beside it, so the two overlap. | 3 unit, 2 integ: integration/test_servicenow_pipeline_e2e.py, integration/test_servicenow_source_e2e.py | servicenow |
| jira | Jira Cloud via a Calcite adapter (module jira), forms as tables, JQL pushed down (REQ-2069). | Would sit on: OpenAPI resolver (Atlassian publishes a spec); forms are a source-specific catalog. Reason hand-built: a table is a form (project + issue type); catalog logic is Jira-specific. | 1 unit, 0 integ: unit/test_jira_connector.py | none |
| splunk | Splunk via a Calcite adapter behind pgwire (REQ-1730). | Would sit on: OpenAPI/REST resolver or Python code resolver. Reason hand-built: SPL pushdown in a Java adapter. | 1 unit, 4 integ: integration/test_splunk_duckdb_attach.py, integration/test_splunk_pipeline_e2e.py | isolated or core/app |
| cloudops | Azure, AWS, GCP resource inventory through the clouds' SDKs behind pgwire (REQ-1947), read only. | Would sit on: Python code resolver (three SDKs). Reason hand-built: three SDKs, one catalog; no API description to read. | 1 unit, 1 integ: integration/test_cloudops_source_e2e.py, unit/test_cloudops_connector.py | warehouse |
| govdata | AskAmerica (REQ-540), Calcite adapter behind pgwire, Trino read behind TRINO_READS_PGWIRE_ADAPTER_LIVE. | Would sit on: Python code resolver or OpenAPI. Reason hand-built: single vendor API; adapter in Java. | 2 unit, 3 integ: integration/test_govdata_source.py, integration/test_govdata_trino_source_e2e.py | warehouse |
| google_workspace | Mail, calendar, tasks through Google APIs; landing (mail_landing.py, mail_units.py), writes through its pgwire server (REQ-1946, REQ-1994). | Would sit on: OpenAPI/Discovery resolver, or Python code resolver. Reason hand-built: mail landing and sync state are bespoke (mail_sync_state, mail_write). | 6 unit, 2 integ: integration/test_google_workspace_calendar_live_e2e.py, integration/test_google_workspace_live_e2e.py | none in CI |
| microsoft_365 | Mail, calendar, directory through Graph (provisa/microsoft365: graph.py, mail.py, directory.py); writes through pgwire (REQ-1994). | Would sit on: OData resolver (Graph). Reason hand-built: mail landing and sync state are bespoke. | 4 unit, 2 integ: integration/test_microsoft365_calendar_live_e2e.py, integration/test_microsoft365_live_e2e.py | none in CI |
| quickbooks | QuickBooks Online Accounting API query endpoint, STARTPOSITION/MAXRESULTS paging, simple conditions pushed (REQ-2197). | Would sit on: OpenAPI resolver (OAuth, a query language in the entity endpoint). Reason hand-built: its query language is a custom push dialect. | 2 unit, 1 integ: integration/test_quickbooks_source_live_e2e.py, unit/test_quickbooks_sign_in.py | none in CI |
| cosmosdb | Read by its SDK and landed; no driver (REQ-1982); VIRTUAL_SOURCES. | Would sit on: Node or Python code resolver. Reason hand-built: SDK-only, no engine catalog. | 1 unit, 2 integ: integration/test_cosmosdb_emulator_search_spike.py, integration/test_cosmosdb_source_e2e.py | isolated or core/app |
| qdrant | Own driver and SQLGlot dialect (provisa/qdrant), live-only (REQ-2000), reports a complete dataset for similarity top-k. | Would sit on: stays hand-built, or a code resolver with a live-only tag. Reason hand-built: live-only: its vector query cannot be reproduced over a replica. | 3 unit, 1 integ: integration/test_qdrant_source_e2e.py, unit/test_qdrant_direct.py | isolated or core/app |
| neo4j | Own Bolt driver (executor/drivers/neo4j.py), keyed loader; VIRTUAL_SOURCES, not translated SQL (issue #119). | Would sit on: Python code resolver. Reason hand-built: graph model; SQL is never handed to it. | 6 unit, 7 integ: integration/test_neo4j_config_persist.py, integration/test_neo4j_exec.py | neo4j |
| sparql | Own client (provisa/sparql, sparql_query), complete-dataset reads under the contract. | Would sit on: Python code resolver. Reason hand-built: SPARQL is its own query language; contract implemented in its own code. | 15 unit, 5 integ: integration/test_sparql_config_persist.py, integration/test_sparql_endpoint.py | datastores |
| websocket | External WebSocket feed: connect, subscribe, receive events; landed. | Would sit on: Node or Python code resolver with @stream. Reason hand-built: a stream, not a table read. | 2 unit, 2 integ: integration/test_websocket_rss_integration.py, steps/steps_phase_at_websocket_rss_sources_req_338_344.py | isolated or core/app |
| rss | RSS/Atom poll, watermark by pubDate/updated; landed. | Would sit on: Python code resolver with @stream. Reason hand-built: a feed, not a table read. | 1 unit, 2 integ: integration/test_websocket_rss_integration.py, steps/steps_phase_at_websocket_rss_sources_req_338_344.py | isolated or core/app |
| logs | Log files parsed into one table, landed (provisa/logs); read by file offset. | Would sit on: stays hand-built. Reason hand-built: Provisa-owned row shape (_file_id, _line_offset, _raw). | 2 unit, 0 integ: unit/test_flight_rpc_logs_unexpected.py, unit/test_logs_source.py | none |
| ingest | Governed HTTP push receiver (provisa/ingest); the receiver is the source. | Would sit on: stays hand-built. Reason hand-built: Provisa-owned inbound endpoint, not a read of a system. | 5 unit, 2 integ: integration/test_ingest_push_integration.py, steps/steps_ingest_sources_governed_http_push_receiver_req_331_337.py | isolated or core/app |
| dbt | dbt formats written from the governed model (REQ-1967): an output, not a read. | Reason hand-built: not a data connector. | 7 unit, 0 integ: unit/test_dbt_connector_target.py, unit/test_dbt_products.py | none |
| soda | Data-quality checker run against a table through Provisa pgwire, results landed (Elastic 2.0, self-host). | Reason hand-built: Provisa-owned check runner. | 1 unit, 0 integ: unit/test_soda_hosted_gate.py | none |
| great_expectations | Data-quality checker, results landed as a governed table (Apache 2.0). | Reason hand-built: Provisa-owned check runner. | 0 unit, 0 integ | none |
| data_profiler | Built-in profiler schedule, results in the control plane (REQ-1934). | Reason hand-built: Provisa-owned. | 0 unit, 1 integ: integration/test_data_profiler_e2e.py | core/app |

### Metadata-only (stub source types) (18)

| Name | What it is today | Needs from the base to move | Tests | Live lane |
|---|---|---|---|---|
| airtable | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: openapi brand per REQ-1923 (the preferred route for the Supabase wrappers catalog). Airtable publishes a REST API with a typed spec (to verify). Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| auth0 | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: openapi brand per REQ-1923 (the preferred route for the Supabase wrappers catalog). Management API OpenAPI. Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| aws_cognito | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: python code resolver per REQ-1923 (the preferred route for the Supabase wrappers catalog). AWS SDK; no API description in the REQ-1923 sense. Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| calcom | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: openapi brand per REQ-1923 (the preferred route for the Supabase wrappers catalog). . Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| calendly | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: openapi brand per REQ-1923 (the preferred route for the Supabase wrappers catalog). . Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| clerk | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: openapi brand per REQ-1923 (the preferred route for the Supabase wrappers catalog). . Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| cloudflare_d1 | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: python code resolver per REQ-1923 (the preferred route for the Supabase wrappers catalog). D1 is SQLite over Cloudflare API; may belong in the SQL family via its HTTP API. Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| dynamodb | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: python code resolver per REQ-1923 (the preferred route for the Supabase wrappers catalog). AWS SDK, key-value API. Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| firebase | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: python code resolver per REQ-1923 (the preferred route for the Supabase wrappers catalog). Firestore SDK. Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| gravatar | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: openapi brand per REQ-1923 (the preferred route for the Supabase wrappers catalog). . Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| hubspot | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: openapi brand per REQ-1923 (the preferred route for the Supabase wrappers catalog). HubSpot publishes OpenAPI specs. Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| infura | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: python code resolver per REQ-1923 (the preferred route for the Supabase wrappers catalog). JSON-RPC, not REST. Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| logflare | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: openapi brand per REQ-1923 (the preferred route for the Supabase wrappers catalog). . Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| notion | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: openapi brand per REQ-1923 (the preferred route for the Supabase wrappers catalog). . Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| orb | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: openapi brand per REQ-1923 (the preferred route for the Supabase wrappers catalog). . Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| paddle | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: openapi brand per REQ-1923 (the preferred route for the Supabase wrappers catalog). . Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| s3_vectors | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: python code resolver per REQ-1923 (the preferred route for the Supabase wrappers catalog). AWS SDK. Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |
| slack | SourceType enum entry only (REQ-1871/1874 stub): registrable, but no connector, dialect or driver reaches it | Would sit on: openapi brand per REQ-1923 (the preferred route for the Supabase wrappers catalog). Slack Web API; spec exists, typing to verify. Verify the vendor spec is well typed before committing to it. | 0 unit, 0 integ | none |

### Materialize readers (2)

| Name | What it is today | Needs from the base to move | Tests | Live lane |
|---|---|---|---|---|
| events/sheets_reader | provisa/events/sheets_reader.py: lands a Google Sheets source's replicas through the Sheets API where the engine has no gsheets extension (REQ-1998) | It is a read path of google_sheets, not a connector. Would disappear if google_sheets sat on an OpenAPI or code resolver. | 0 unit, 1 integ: integration/test_google_sheets_source_e2e.py | warehouse |
| events/motherduck_reader | provisa/events/motherduck_reader.py: lands MotherDuck replicas through its Postgres-wire endpoint where the engine cannot attach (REQ-1998) | A read path of motherduck inside the SQL family; stays. | 5 unit, 3 integ: integration/test_embedded_pg_duckdb_motherduck_e2e.py, integration/test_motherduck_duckdb_e2e.py | warehouse |

Tests columns show counts and up to two files; connectors.json holds up to twelve per list. Base conformance tests are listed in connectors.json under `base_conformance`: for the SQL family they are the engine parity suites, for code resolvers the contract tests.
