# Copyright (c) 2026 Kenneth Stott
# Canary: f1b2c3d4-e5f6-7a8b-9c0d-e1f2a3b4c5d6
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd configuration for generated feature files.

All .feature files in this directory are auto-collected. Step definitions live in
tests/steps/. Unimplemented steps call pytest.skip — CI sees them as skipped rather
than failing, giving visibility without blocking.
"""

import sys
from pathlib import Path

# Make step definitions importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from pytest_bdd import scenarios  # type: ignore[import]

# Import stub step definitions so pytest-bdd can register them
import tests.steps.generated_stubs  # type: ignore[import]  # noqa: F401

# Collect every .feature file in this directory
import tests.steps.steps_query_governance  # type: ignore[import]  # noqa: F401

import tests.steps.steps_security  # type: ignore[import]  # noqa: F401

import tests.steps.steps_abac_approval_hook  # type: ignore[import]  # noqa: F401

import tests.steps.steps_two_stage_compiler_governed_sql  # type: ignore[import]  # noqa: F401

import tests.steps.steps_rate_limiting  # type: ignore[import]  # noqa: F401

import tests.steps.steps_audit_logging  # type: ignore[import]  # noqa: F401

import tests.steps.steps_authentication  # type: ignore[import]  # noqa: F401

import tests.steps.steps_registration_governance  # type: ignore[import]  # noqa: F401

import tests.steps.steps_jsonb_api_sources  # type: ignore[import]  # noqa: F401

import tests.steps.steps_views_governed_computed_datasets  # type: ignore[import]  # noqa: F401

import tests.steps.steps_column_path_extraction  # type: ignore[import]  # noqa: F401

import tests.steps.steps_naming_schema  # type: ignore[import]  # noqa: F401

import tests.steps.steps_auto_materialized_relationships  # type: ignore[import]  # noqa: F401

import tests.steps.steps_semantic_layer_semantic_model  # type: ignore[import]  # noqa: F401

import tests.steps.steps_domain_model  # type: ignore[import]  # noqa: F401

import tests.steps.steps_kafka_sources  # type: ignore[import]  # noqa: F401

import tests.steps.steps_query_api_sources_neo4j_sparql  # type: ignore[import]  # noqa: F401

import tests.steps.steps_graphql_remote_schema_connector_req_307_313  # type: ignore[import]  # noqa: F401

import tests.steps.steps_openapi_auto_registration_connector  # type: ignore[import]  # noqa: F401

import tests.steps.steps_grpc_remote_schema_connector_req_322_329  # type: ignore[import]  # noqa: F401

import tests.steps.steps_ingest_sources_governed_http_push_receiver_req_331_337  # type: ignore[import]  # noqa: F401

import tests.steps.steps_phase_at_websocket_rss_sources_req_338_344  # type: ignore[import]  # noqa: F401

import tests.steps.steps_file_lake_sources  # type: ignore[import]  # noqa: F401

import tests.steps.steps_vector_search  # type: ignore[import]  # noqa: F401

import tests.steps.steps_source_connectors  # type: ignore[import]  # noqa: F401

import tests.steps.steps_govdata_sources  # type: ignore[import]  # noqa: F401

import tests.steps.steps_json_api_remote_schema_connector  # type: ignore[import]  # noqa: F401

import tests.steps.steps_compiler_schema  # type: ignore[import]  # noqa: F401

import tests.steps.steps_mutation_execution  # type: ignore[import]  # noqa: F401

import tests.steps.steps_sqlglot_transpilation  # type: ignore[import]  # noqa: F401

import tests.steps.steps_aggregates  # type: ignore[import]  # noqa: F401

import tests.steps.steps_orderby_alignment  # type: ignore[import]  # noqa: F401

import tests.steps.steps_tracked_functions_custom_mutations  # type: ignore[import]  # noqa: F401

import tests.steps.steps_graphql_variable_defaults  # type: ignore[import]  # noqa: F401

import tests.steps.steps_cypher_query_frontend_phase_au  # type: ignore[import]  # noqa: F401

import tests.steps.steps_natural_language_query_service_phase_av  # type: ignore[import]  # noqa: F401

import tests.steps.steps_graph_analytics_pipeline  # type: ignore[import]  # noqa: F401

import tests.steps.steps_cypher_mutations  # type: ignore[import]  # noqa: F401

import tests.steps.steps_execution_routing  # type: ignore[import]  # noqa: F401

import tests.steps.steps_hot_tables_redis_cached_lookups  # type: ignore[import]  # noqa: F401

import tests.steps.steps_materialized_view_lifecycle  # type: ignore[import]  # noqa: F401

import tests.steps.steps_hot_table_auto_detection  # type: ignore[import]  # noqa: F401

import tests.steps.steps_warm_tables_local_ssd_via_trino_file_cache  # type: ignore[import]  # noqa: F401

import tests.steps.steps_federation_performance  # type: ignore[import]  # noqa: F401

import tests.steps.steps_cache  # type: ignore[import]  # noqa: F401

import tests.steps.steps_output_delivery  # type: ignore[import]  # noqa: F401

import tests.steps.steps_large_result_redirect_ctas  # type: ignore[import]  # noqa: F401

import tests.steps.steps_arrow_flight  # type: ignore[import]  # noqa: F401

import tests.steps.steps_api_integration  # type: ignore[import]  # noqa: F401

import tests.steps.steps_jdbc_odbc_integration  # type: ignore[import]  # noqa: F401

import tests.steps.steps_query_development_tools  # type: ignore[import]  # noqa: F401

import tests.steps.steps_sql_multi_protocol_client_access  # type: ignore[import]  # noqa: F401

import tests.steps.steps_automatic_persisted_queries_apq  # type: ignore[import]  # noqa: F401

import tests.steps.steps_pgwire_server  # type: ignore[import]  # noqa: F401

import tests.steps.steps_grpc  # type: ignore[import]  # noqa: F401

import tests.steps.steps_dataset_change_events  # type: ignore[import]  # noqa: F401

import tests.steps.steps_kafka_sinks_table_view_publishing  # type: ignore[import]  # noqa: F401

import tests.steps.steps_subscriptions  # type: ignore[import]  # noqa: F401

import tests.steps.steps_live_query_engine_unified_subscription_sink_delivery  # type: ignore[import]  # noqa: F401

import tests.steps.steps_infrastructure  # type: ignore[import]  # noqa: F401

import tests.steps.steps_opentelemetry_instrumentation  # type: ignore[import]  # noqa: F401

import tests.steps.steps_installer_packaging  # type: ignore[import]  # noqa: F401

import tests.steps.steps_hasura_migration_converters  # type: ignore[import]  # noqa: F401

import tests.steps.steps_hasura_v2_parity_low_complexity_features  # type: ignore[import]  # noqa: F401

import tests.steps.steps_hasura_v2_parity_medium_complexity_features  # type: ignore[import]  # noqa: F401

scenarios(".")
