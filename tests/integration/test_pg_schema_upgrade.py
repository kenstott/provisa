# Copyright (c) 2026 Kenneth Stott
# Canary: a8abb703-feab-49f9-9801-97804a2108f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org schema created before this week's columns upgrades in place on PostgreSQL.

V1 ships no migrations: ``schema.sql`` re-runs at every org runtime build, so every column added
after a schema was first created must reach it either through a hand-written
``ADD COLUMN IF NOT EXISTS`` block or through the metadata reconciliation ``init_schema`` now runs
after the script. cloud-dev found the gap the hard way — its API crash-looped on
``column "body_encoding" does not exist`` — so this test strips a fresh org schema back to that
older shape and proves both paths restore it, on a real PostgreSQL.
"""

from __future__ import annotations

import os
import re
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
import sqlalchemy as sa

from provisa.core import model_change
from provisa.core.db import add_missing_columns, init_schema

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()
_ORG = "upgrade_probe"
_SCHEMA = f"org_{_ORG}"
_OLD_CHECK = "CHECK (type IN ('openapi', 'graphql_api', 'grpc_api'))"


async def _columns(conn, table: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{_SCHEMA}' AND table_name = '{table}'"
    )
    return {r["column_name"] for r in rows}


@pytest_asyncio.fixture
async def old_shape(tenant_db):
    """A fresh org schema, then regressed to the pre-REQ-1668 shape."""
    await init_schema(tenant_db, _SCHEMA_SQL, org_id=_ORG)
    async with tenant_db.acquire() as conn:
        await conn.execute(f"SET search_path TO {_SCHEMA}")
        await conn.execute("ALTER TABLE api_endpoints DROP COLUMN body_encoding")
        await conn.execute("ALTER TABLE api_endpoints DROP COLUMN query_template")
        await conn.execute("ALTER TABLE api_endpoints DROP COLUMN response_normalizer")
        await conn.execute("ALTER TABLE sources DROP COLUMN password_ref")
        await conn.execute("ALTER TABLE api_sources DROP CONSTRAINT api_sources_type_check")
        await conn.execute(
            f"ALTER TABLE api_sources ADD CONSTRAINT api_sources_type_check {_OLD_CHECK}"
        )
        assert "body_encoding" not in await _columns(conn, "api_endpoints")
    yield tenant_db
    async with tenant_db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")


async def test_init_schema_upgrades_an_older_org_schema_in_place(old_shape):
    await init_schema(old_shape, _SCHEMA_SQL, org_id=_ORG)
    async with old_shape.acquire() as conn:
        await conn.execute(f"SET search_path TO {_SCHEMA}")
        cols = await _columns(conn, "api_endpoints")
        assert {"body_encoding", "query_template", "response_normalizer"} <= cols
        assert "password_ref" in await _columns(conn, "sources")
        # the widened type check admits the query-API kinds an older schema refused
        await conn.execute(
            "INSERT INTO api_sources (id, type, base_url, auth) "
            "VALUES ('probe_neo4j', 'neo4j', 'http://neo4j:7474', '{}')"
        )
        await conn.execute("DELETE FROM api_sources WHERE id = 'probe_neo4j'")


async def test_metadata_reconciliation_restores_a_column_schema_sql_never_alters(old_shape):
    """The reconciliation is what covers a column nobody wrote an ALTER block for: run it alone,
    scoped to the org schema, and the metadata's column comes back typed like schema.sql's."""
    with old_shape.engine.begin() as sa_conn:
        from provisa.core import schema_org

        add_missing_columns(sa_conn, schema_org.metadata.sorted_tables, _SCHEMA)
    async with old_shape.acquire() as conn:
        rows = await conn.fetch(
            "SELECT column_name, data_type, column_default FROM information_schema.columns "
            f"WHERE table_schema = '{_SCHEMA}' AND table_name = 'sources' "
            "AND column_name = 'password_ref'"
        )
        assert len(rows) == 1
        assert rows[0]["data_type"] == "text"
        assert rows[0]["column_default"] == "''::text"
        assert {"body_encoding", "query_template"} <= await _columns(conn, "api_endpoints")


# --- reference_documents: schema.sql upgrades it on its own ------------------------------------
#
# REQ-2185's index-job columns. init_schema's reconciliation would add them too, but without
# index_status's NOT NULL, so this applies the script alone: schema.sql is what has to carry them.

_REFERENCE_DOCUMENT_INDEX_COLUMNS = {
    "index_status": ("text", "NO", "'ready'::text"),
    "index_error": ("text", "YES", None),
    "index_step": ("text", "YES", None),
    "index_done": ("integer", "YES", None),
    "index_total": ("integer", "YES", None),
    "index_launch": ("text", "YES", None),
    "index_progress_at": ("timestamp with time zone", "YES", None),
    "pending_content": ("bytea", "YES", None),
    "terms_note": ("text", "YES", None),
}


@pytest_asyncio.fixture
async def old_reference_documents(tenant_db):
    """An org schema whose reference_documents predates the index job, holding one document."""
    await init_schema(tenant_db, _SCHEMA_SQL, org_id=_ORG)
    async with tenant_db.acquire() as conn:
        await conn.execute(f"SET search_path TO {_SCHEMA}")
        for column in _REFERENCE_DOCUMENT_INDEX_COLUMNS:
            await conn.execute(f"ALTER TABLE reference_documents DROP COLUMN {column}")
        await conn.execute(
            "INSERT INTO reference_documents "
            "(title, source_kind, source, media_type, content_sha256, status, embedding_model) "
            "VALUES ('Old', 'file', 'old.pdf', 'application/pdf', 'abc', 'kept', 'model')"
        )
    yield tenant_db
    async with tenant_db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")


async def test_schema_sql_adds_the_index_columns_to_an_older_reference_documents(
    old_reference_documents,
):
    async with old_reference_documents.acquire() as conn:
        await conn.execute(f"SET search_path TO {_SCHEMA}")
        async with model_change.layout():
            await conn.execute(_SCHEMA_SQL)
        rows = await conn.fetch(
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            f"WHERE table_schema = '{_SCHEMA}' AND table_name = 'reference_documents'"
        )
        live = {
            r["column_name"]: (r["data_type"], r["is_nullable"], r["column_default"]) for r in rows
        }
        added = {name: live.get(name) for name in _REFERENCE_DOCUMENT_INDEX_COLUMNS}
        assert added == _REFERENCE_DOCUMENT_INDEX_COLUMNS
        # the document that was already there is ready: its passages exist
        existing = await conn.fetch("SELECT index_status FROM reference_documents")
        assert [r["index_status"] for r in existing] == ["ready"]


# --- a metadata table schema.sql does not create ---------------------------------------------
#
# ``schema_org`` declares tables ``schema.sql`` has no CREATE for (``provisa_sources``,
# ``query_audit_log``, ``sla_breach_log``, ``source_catalog_cache``). The reconciliation creates
# them: in the org's schema, typed as the PostgreSQL plane declares them. Each test below runs on
# a database of its own, so "nothing in public" is a statement about this code and nothing else.

_PG = (
    f"postgresql+psycopg://{os.environ.get('PG_USER', 'provisa')}:"
    f"{os.environ.get('PG_PASSWORD', 'provisa')}@{os.environ.get('PG_HOST', 'localhost')}:"
    f"{os.environ.get('PG_PORT', '5432')}"
)
_NOT_IN_SCHEMA_SQL = (
    "provisa_sources",
    "query_audit_log",
    "sla_breach_log",
    "source_catalog_cache",
)


@pytest_asyncio.fixture
async def own_database():
    """A ``Database`` on a PostgreSQL database created for the test, and that database's URL."""
    from provisa.core.database import Database, create_engine_from_url

    name = f"schema_probe_{uuid.uuid4().hex[:10]}"
    admin = sa.create_engine(
        f"{_PG}/{os.environ.get('PG_DATABASE', 'provisa')}", isolation_level="AUTOCOMMIT"
    )
    with admin.connect() as conn:
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    db = Database(create_engine_from_url(f"{_PG}/{name}"), name="org", search_path=_SCHEMA)
    try:
        yield db
    finally:
        await db.close()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


def _tables_by_schema(db, names) -> set[tuple[str, str]]:
    with db.engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_name = ANY(:names) AND table_type = 'BASE TABLE'"
            ),
            {"names": list(names)},
        ).fetchall()
    return {(r[0], r[1]) for r in rows}


def _column_shapes(db, schema: str, table: str) -> list[tuple]:
    """(name, type, nullable, default) per column, the default with its schema qualifier removed."""
    with db.engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT column_name, udt_name, is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_schema = :schema AND table_name = :table ORDER BY column_name"
            ),
            {"schema": schema, "table": table},
        ).fetchall()
    return [(r[0], r[1], r[2], re.sub(r"\b\w+\.(?=\w+_seq)", "", r[3] or "")) for r in rows]


async def test_a_table_schema_sql_does_not_create_lands_in_the_org_schema(own_database):
    from provisa.api._meta_views import _ops_table_usage_ddl
    from provisa.audit.query_log import AUDIT_SCHEMA_SQL, init_audit_schema
    from provisa.core import schema_org

    db = own_database
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)

    names = [t.name for t in schema_org.metadata.sorted_tables]
    found = _tables_by_schema(db, names)
    assert {schema for schema, _ in found} == {_SCHEMA}, sorted(found)
    assert {name for _, name in found} == set(names)

    # Every JSON column of the metadata is JSONB in the org schema, created or pre-existing.
    with db.engine.connect() as conn:
        json_columns = conn.execute(
            sa.text(
                "SELECT table_name, column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = :schema AND data_type IN ('json', 'jsonb')"
            ),
            {"schema": _SCHEMA},
        ).fetchall()
    live = {(r[0], r[1]): r[2] for r in json_columns}
    for name in _NOT_IN_SCHEMA_SQL:
        for column in schema_org.metadata.tables[name].columns:
            if isinstance(column.type, sa.JSON):
                assert live[(name, column.name)] == "jsonb", (name, column.name)
    assert "json" not in set(live.values()), sorted(k for k, v in live.items() if v == "json")

    # The audit log this created is the table its own DDL declares, column for column: build that
    # DDL's table in a schema of its own and compare.
    with db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA audit_reference")
        conn.exec_driver_sql("SET LOCAL search_path TO audit_reference")
        conn.exec_driver_sql(AUDIT_SCHEMA_SQL.split("DO $$")[0])
    declared = _column_shapes(db, "audit_reference", "query_audit_log")
    # The record's columns: the statement, the tables it read by id and by name, and its outcome,
    # and the provenance a reader of the
    # record is answered with (model stamp and commit, the rules enforced, why it was routed where
    # it was, the sources read and the age of the data).
    assert {shape[0] for shape in declared} == {
        "id",
        "tenant_id",
        "user_id",
        "role_id",
        "query_hash",
        "query_text_enc",
        "table_ids",
        "table_names",
        "source",
        "status_code",
        "duration_ms",
        "route",
        "row_count",
        "trace_id",
        "model_stamp",
        "model_commit",
        "enforced",
        "route_reason",
        "sources",
        "data_age",
        "region",  # REQ-1922: the region whose data answered
        "as_of",  # REQ-1163: the time travel the statement read at
        "as_recorded",
        "logged_at",
    }
    assert _column_shapes(db, _SCHEMA, "query_audit_log") == declared
    with db.engine.begin() as conn:
        conn.exec_driver_sql("DROP SCHEMA audit_reference CASCADE")

    # Its initializer then adds what is its own, and what is built on the table works.
    await init_audit_schema(db, org_id=_ORG)
    with db.engine.begin() as conn:
        rules = conn.execute(
            sa.text(
                "SELECT rulename FROM pg_rules WHERE schemaname = :schema "
                "AND tablename = 'query_audit_log' ORDER BY 1"
            ),
            {"schema": _SCHEMA},
        ).fetchall()
        assert [r[0] for r in rules] == ["no_delete_audit", "no_update_audit"]
        conn.exec_driver_sql(f'SET LOCAL search_path TO "{_SCHEMA}"')
        conn.exec_driver_sql(_ops_table_usage_ddl())

    # A second start changes nothing.
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    await init_audit_schema(db, org_id=_ORG)
    assert _tables_by_schema(db, names) == found


async def test_a_table_missing_from_an_org_schema_is_recreated_there(own_database):
    db = own_database
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    with db.engine.begin() as conn:
        conn.exec_driver_sql(f'DROP TABLE "{_SCHEMA}".provisa_sources')
    assert _tables_by_schema(db, ["provisa_sources"]) == set()
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    assert _tables_by_schema(db, ["provisa_sources"]) == {(_SCHEMA, "provisa_sources")}


async def test_the_catalog_cache_table_is_created_in_the_schema_its_readers_use(own_database):
    from provisa.discovery.catalog_cache import CachedTable, ensure_table, read_cache, write_cache

    db = own_database  # scoped to the org schema, as a tenant plane is
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    with db.engine.begin() as conn:
        conn.exec_driver_sql(f'DROP TABLE "{_SCHEMA}".source_catalog_cache')
    await ensure_table(db)
    assert _tables_by_schema(db, ["source_catalog_cache"]) == {(_SCHEMA, "source_catalog_cache")}
    await write_cache(db, "src", "public", [CachedTable("public", "orders", ["id"], None)])
    cached = await read_cache(db, "src", "public")
    assert cached is not None and [t.table_name for t in cached] == ["orders"]


async def test_two_starts_reconciling_one_org_schema_at_once_both_succeed(own_database):
    """A newly provisioned org is initialized by its provisioning and by the first request that
    reaches it, at once. The second reconcile starts while the first has created the missing
    tables and not yet committed: it must wait and then find them, not create them again."""
    import threading

    from provisa.core import schema_org

    db = own_database
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    with db.engine.begin() as conn:
        conn.exec_driver_sql(f'DROP TABLE "{_SCHEMA}".provisa_sources')

    tables = schema_org.metadata.sorted_tables
    second: list[BaseException | str] = []

    def _second_start() -> None:
        try:
            with db.engine.begin() as conn:
                add_missing_columns(conn, tables, _SCHEMA)
            second.append("done")
        except BaseException as exc:  # asserted on below
            second.append(exc)

    with db.engine.begin() as first:
        add_missing_columns(first, tables, _SCHEMA)  # provisa_sources created, not committed
        other = threading.Thread(target=_second_start)
        other.start()
        other.join(1.5)
        assert other.is_alive()  # waiting for the first, not creating beside it
    other.join(30)
    assert second == ["done"], second
    assert _tables_by_schema(db, ["provisa_sources"]) == {(_SCHEMA, "provisa_sources")}


# --- a column added to a table that already exists -------------------------------------------
#
# CREATE TABLE IF NOT EXISTS does nothing on a table that is already there, so a column declared
# only in a CREATE never reaches a control plane created before it (cloud outage 2026-11-07,
# glossary_term_scans.source_table). The rule: the new column goes in the CREATE and gets its own
# ALTER TABLE ... ADD COLUMN IF NOT EXISTS beside it.
#
# _CREATE_ONLY freezes the columns each table may declare with no ALTER: the ones it was first
# created with. It holds what history shows and grows only by a new table's first columns; a column
# added to an existing table does NOT go here, it gets an ALTER.
_CREATE_ONLY = {
    "admin_audit_log": "id action actor_id subject_id detail created_at",
    "api_endpoints": "id source_id path method table_name columns ttl created_at",
    "api_sources": "id type base_url spec_url auth created_at",
    "calendars": (
        "name version base_system tz fiscal_anchor_month fiscal_anchor_day retail_anchor "
        "week_start holidays weekend created_at"
    ),
    "catalog_bindings": "id semantic_uri vendor_ref physical_key updated_at",
    "checker_latest_runs": "table_id scan_id scan_time outcome",
    "checker_targets": "table_id checked_table_id",
    "code_source_builds": (
        "id source_id code_hash arch registry_id status image_ref digest archive_path node error "
        "started_by started_at finished_at"
    ),
    "code_source_cache": "source_id key value expires_at",
    "code_source_llm_usage": (
        "id source_id month model input_tokens output_tokens cache_read_tokens cache_write_tokens "
        "cost_usd created_at"
    ),
    "code_source_vectors": "source_id item_id vector text item_metadata",
    "config_stamp": "kind stamp",
    "connectors": (
        'name git ref "commit" path build_command language image digest code_hash arch requires'
    ),
    "creation_requests": (
        "id request_type capability payload requested_by status rejection_reason resolved_by "
        "created_at resolved_at"
    ),
    "data_product_commands": "product_id command_name",
    "data_product_tables": "product_id table_id",
    "data_products": "id domain_id name",
    "dbt_manifests": "source_id definitions updated_at",
    "dbt_node_runs": "source_id unique_id status generated_at built_at",
    "domains": "id description",
    "email_send_authority_audit": (
        "id email_address user_id granted_by org_id action reason created_at"
    ),
    "entitlement_expiries": (
        "id rule_key principal role_id ends_at ends removed reason set_by valid_from recorded_at"
    ),
    "entitlement_pending": (
        "id email role_id kind identifiers expires_at outcome principal reason set_by valid_from "
        "recorded_at"
    ),
    "entitlements": (
        "id source principal role_id kind capability identifiers removed reason set_by valid_from "
        "recorded_at"
    ),
    "erd_layout_domains": "layout_name domain_id selected collapsed x y w h",
    "erd_layout_tables": "layout_name table_id selected x y",
    "erd_layouts": "name options updated_by updated_at",
    "event_status": (
        "event_id dependent_table claim_status processor_name heartbeat_at deadline completed_at"
    ),
    "events": "id source_table event_type payload created_at",
    "fake_adequacy_checks": "table_id column_name check_name checked_at status measure detail",
    "file_source_mtimes": "table_id source_mtime synced_at",
    "glossary_ontology_alignments": (
        "id import_id item_id term_id other_item_id rel_type item_is_source basis status "
        "use_definition decided_by decided_at tenant_id"
    ),
    "glossary_ontology_imports": (
        "id title file_name format domains summary imported_by imported_at applied_by applied_at "
        "tenant_id"
    ),
    "glossary_ontology_items": (
        "id import_id ordinal name kind definition iri payload status term_id tenant_id"
    ),
    "glossary_term_domains": "id term_id domain_id tenant_id",
    "glossary_term_drafts": (
        "id scan_id ordinal name category sense definition definition_kind proposal match_term_id "
        "match_kind payload status term_id decided_by decided_at tenant_id"
    ),
    "glossary_term_edges": "id from_term_id to_term_id rel_type tenant_id",
    "glossary_term_experts": "id term_id user_id kind tenant_id",
    "glossary_term_refs": "id term_id table_id column_name tenant_id",
    "glossary_term_scans": (
        "id document_id document_title document_source domains status error summary started_by "
        "started_at finished_at tenant_id"
    ),
    "glossary_term_vectors": "term_id embedding_model digest vector tenant_id",
    "glossary_terms": "id name definition deprecated tenant_id",
    "graph_sessions": "user_id generation stored tables graph roles assigned rights used_at",
    "idempotent_calls": "scope actor call_key response created_at",
    "judgment_bases": "id name description owner changed_at changed_by change_note",
    "judgment_findings": (
        "id object_type object_key judgment category finding observed first_seen last_seen "
        "resolved_at resolved_by resolution"
    ),
    "kafka_sources": "id bootstrap_servers schema_registry_url auth_type created_at",
    "kafka_topics": "id source_id topic table_name schema_source value_format columns created_at",
    "lineage_edges": (
        "id source_relation source_column target_relation target_column via via_kind "
        "source_table_id source_column_name target_table_id target_column_name"
    ),
    "live_query_state": "",
    "mail_sync_state": "source_id account scope kind token updated_at",
    "materialized_views": (
        "id source_tables target_catalog target_schema target_table refresh_interval enabled "
        "join_pattern custom_sql expose_in_sdl sdl_config created_at"
    ),
    "metrics": "name expression datatype description ai_context visible_to from_fact",
    "model_name_clashes": "surface name holder newcomer message",
    "model_names": (
        "object_kind object_id surface name physical_name label domain_id table_id is_alias "
        "warnings"
    ),
    "model_seed": "id seeded_at",
    "mv_build_state": (
        "mv_id region status last_refresh_at row_count last_error writer lease_until "
        "materialized_definition_version materialized_input_version snapshot_id"
    ),
    "mv_delta_ledger": (
        "id mv_id refresh_version definition_version trace_id change_type row_key old_hash "
        "new_hash old_values new_values created_at"
    ),
    "mv_refresh_log": "id mv_id status row_count duration_ms error created_at",
    "naming_rules": "id pattern replacement",
    "node_freshness_state": "node content_hash probe_token updated_at",
    "node_ids": "id composite_id label properties",
    "openlineage_run_datasets": (
        "id job_namespace job_name run_id event_time direction dataset_namespace dataset_name "
        "column_lineage"
    ),
    "org_regions": "id engine replicas views cache state record",
    "org_secrets": "key value_enc updated_at updated_by",
    "org_settings": "key value updated_at updated_by",
    "preserved_snapshots": "name reason location content_hash window_id sealed_at",
    "profiler_classifications": (
        "table_id physical_column kind status evidence run_id decided_by decided_at"
    ),
    "profiler_constraints": (
        "id table_id signature kind column_name other_column definition evidence share sampled "
        "status run_id decided_at"
    ),
    "profiler_sensitive_hints": (
        "table_id physical_column kind scan_id values_tested values_matched share evidence"
    ),
    "profiler_sensitive_scans": (
        "scan_id table_id status started_at finished_at sampled_rows scanned_columns requested_by "
        "error"
    ),
    "reference_document_domains": "id document_id domain_id tenant_id",
    "reference_document_tags": (
        "id tag_id base_tag_id object_type document_id section_heading section_page object_key "
        "reason expires_on purpose bases review_on decided_by decided_at evidence tenant_id"
    ),
    "reference_documents": (
        "id title source_kind source media_type content_sha256 status embedding_model added_by "
        "added_at refreshed_at replaced_at tenant_id"
    ),
    "reference_passages": "id document_id ordinal section page text vector tenant_id",
    "registered_tables": "id source_id domain_id schema_name table_name",
    "rel_ids": "id composite_id rel_type properties",
    "relationship_candidates": (
        "id source_table_id target_table_id source_column target_column cardinality confidence "
        "reasoning status scope rejection_reason created_at"
    ),
    "relationships": "id source_table_id target_table_id source_column target_column cardinality",
    "replica_build_log": (
        "id source_id schema_name table_name started_at finished_at outcome method rows_copied "
        "error_code error_message"
    ),
    "replica_state": "source_id schema_name table_name promoted promoted_at",
    "rls_rules": "id table_id role_id filter_expr",
    "role_llm_usage": "role_id grain bucket tokens usd",
    "roles": "id capabilities domain_access",
    "scheduled_triggers": (
        "id name cron kind url webhook_name args sql role enabled created_at updated_at"
    ),
    "schema_layout": "id digest",
    "session_temp_schemas": "store_schema session_id beat_at",
    "sources": "id type host port database username dialect",
    "stores": "id url",
    "synthetic_dataset_tables": "dataset_id table_id profile_env run_id scale",
    "synthetic_datasets": "id seed scale status store_schema error generated_at created_at",
    "synthetic_generations": "finished_at generated_rows seconds",
    "synthetic_report": (
        "id dataset_id table_name column_name measure source_value synthetic_value delta note"
    ),
    "synthetic_source_bindings": "source_id previous",
    "table_columns": "id table_id column_name visible_to",
    "table_meta_links": "source_table_id target_table_id",
    "table_write_grants": (
        "id table_id role_id operation columns rows_expr check_expr presets tenant_id"
    ),
    "tag_assignments": (
        "id tag_id object_type source_id table_id column_name relationship_id object_key"
    ),
    "tag_param_values": "tag_id value description",
    "tags": "id description applies_to is_system",
    "tracked_functions": (
        "id name source_id schema_name function_name returns arguments visible_to domain_id "
        "description created_at updated_at"
    ),
    "tracked_webhooks": (
        "id name url method timeout_ms returns inline_return_type arguments visible_to domain_id "
        "description created_at updated_at"
    ),
    "trigger_runs": "trigger_id ran_at outcome error fired_by",
    "user_attributes": "id user_id name value updated_at",
    "user_directory": "user_id display_name email updated_at",
    "user_idp_attributes": "user_id name value seen_at",
    "user_idp_roles": "user_id role_id domain_id seen_at",
    "user_role_assignments": "id user_id role_id domain_id created_at",
}
_NOT_A_COLUMN = {"PRIMARY", "UNIQUE", "CONSTRAINT", "FOREIGN", "CHECK", "EXCLUDE", "LIKE"}


def _schema_text() -> str:
    """schema.sql without comments and with every string literal emptied, so it can be scanned."""
    bare = re.sub(r"('(?:[^']|'')*')|--[^\n]*", lambda m: m.group(1) or "", _SCHEMA_SQL)
    return re.sub(r"'(?:[^']|'')*'", "''", bare)


def _created_columns() -> dict[str, list[str]]:
    """Every CREATE TABLE of schema.sql: the columns it declares, in order."""
    text = _schema_text()
    created: dict[str, list[str]] = {}
    for match in re.finditer(r"CREATE TABLE IF NOT EXISTS (\w+) \(", text):
        depth, start, parts = 1, match.end(), []
        position = match.end()
        while depth:
            char = text[position]
            depth += {"(": 1, ")": -1}.get(char, 0)
            if depth == 0 or (char == "," and depth == 1):
                parts.append(text[start:position])
                start = position + 1
            position += 1
        firsts = [part.split()[0] for part in parts if part.strip()]
        created[match.group(1)] = [f for f in firsts if f.upper() not in _NOT_A_COLUMN]
    return created


def _altered_columns() -> set[tuple[str, str]]:
    """(table, column) of every ADD COLUMN IF NOT EXISTS in schema.sql."""
    return {
        (table, column)
        for table, actions in re.findall(r"ALTER TABLE (\w+)([^;]*);", _schema_text())
        for column in re.findall(r"ADD COLUMN IF NOT EXISTS (\w+)", actions)
    }


def _columns_added_later() -> dict[str, list[str]]:
    """Per table, the columns the CREATE declares beyond the ones it was first created with."""
    return {
        table: added
        for table, columns in _created_columns().items()
        if (added := [c for c in columns if c not in _CREATE_ONLY[table].split()])
    }


async def test_every_column_added_to_an_existing_table_has_its_add_column():
    created = _created_columns()
    new_tables = sorted(set(created) - set(_CREATE_ONLY))
    assert not new_tables, f"add each new table's columns to _CREATE_ONLY: {new_tables}"
    altered = _altered_columns()
    missing = [
        f"{table}.{column}"
        for table, columns in _columns_added_later().items()
        for column in columns
        if (table, column) not in altered
    ]
    assert not missing, f"needs ALTER TABLE ... ADD COLUMN IF NOT EXISTS: {missing}"


async def test_schema_sql_alone_restores_every_column_added_to_an_existing_table(own_database):
    """A control plane created before these columns: the script by itself brings them back, with
    no metadata reconciliation behind it."""
    db = own_database
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    added = _columns_added_later()
    assert ("source_table" in added["glossary_term_scans"]) and len(added) >= 30

    with db.engine.begin() as conn:
        for table, columns in added.items():
            drops = ", ".join(f'DROP COLUMN "{column}" CASCADE' for column in columns)
            conn.exec_driver_sql(f'ALTER TABLE "{_SCHEMA}"."{table}" {drops}')
    regressed = {table: {c[0] for c in _column_shapes(db, _SCHEMA, table)} for table in added}
    assert not any(set(added[table]) & regressed[table] for table in added)

    async with db.acquire() as conn:
        await conn.execute(f'SET search_path TO "{_SCHEMA}"')
        await conn.execute(_SCHEMA_SQL)

    restored = {table: {c[0] for c in _column_shapes(db, _SCHEMA, table)} for table in added}
    still_missing = [
        f"{table}.{column}"
        for table, columns in added.items()
        for column in columns
        if column not in restored[table]
    ]
    assert not still_missing, still_missing


# --- a constraint added to a table that already exists ----------------------------------------
#
# The same gap for constraints: CREATE TABLE IF NOT EXISTS does nothing on a table that is already
# there, so a key or CHECK declared only in the CREATE never reaches a control plane created before
# it. A table's key that replaced an older one has to take the older one away too, or the older one
# goes on refusing the rows written now.

_CONSTRAINT_TABLES = (
    "api_endpoints",
    "catalog_bindings",
    "live_query_state",
    "relationships",
    "scheduled_triggers",
    "tags",
)
_NOT_NULL_ONCE_EMPTY = (
    ("catalog_bindings", "target"),
    ("live_query_state", "source"),
    ("live_query_state", "output_type"),
    ("profiler_constraints", "physical_column"),
)
# Each table as it was before the constraint (git log -p provisa/core/schema.sql).
_OLDER_CONSTRAINTS = """
ALTER TABLE catalog_bindings DROP CONSTRAINT catalog_bindings_target_semantic_uri_key;
ALTER TABLE catalog_bindings ALTER COLUMN target DROP NOT NULL;
ALTER TABLE catalog_bindings ADD COLUMN provider TEXT NOT NULL;
ALTER TABLE catalog_bindings ADD CONSTRAINT catalog_bindings_provider_semantic_uri_key
    UNIQUE (provider, semantic_uri);

ALTER TABLE live_query_state DROP CONSTRAINT live_query_state_pkey;
ALTER TABLE live_query_state ALTER COLUMN source DROP NOT NULL;
ALTER TABLE live_query_state ALTER COLUMN output_type DROP NOT NULL;
ALTER TABLE live_query_state ADD COLUMN query_id TEXT PRIMARY KEY;

ALTER TABLE profiler_constraints ALTER COLUMN physical_column DROP NOT NULL;

ALTER TABLE tags DROP CONSTRAINT tags_param_policy_check;

ALTER TABLE relationships DROP CONSTRAINT relationships_cardinality_check;
ALTER TABLE relationships ADD CONSTRAINT relationships_cardinality_check
    CHECK (cardinality IN ('many-to-one', 'one-to-many'));

ALTER TABLE api_endpoints DROP CONSTRAINT api_endpoints_source_table_key;
ALTER TABLE api_endpoints ADD CONSTRAINT api_endpoints_table_name_key UNIQUE (table_name);

ALTER TABLE scheduled_triggers DROP CONSTRAINT scheduled_triggers_when;
ALTER TABLE scheduled_triggers DROP CONSTRAINT scheduled_triggers_kind_fields;
ALTER TABLE scheduled_triggers ADD CONSTRAINT scheduled_triggers_kind_fields CHECK (
    (kind = 'sql' AND sql IS NOT NULL AND role IS NOT NULL)
    OR (kind = 'webhook' AND (url IS NOT NULL OR webhook_name IS NOT NULL))
);
ALTER TABLE scheduled_triggers ALTER COLUMN cron SET NOT NULL;
"""


def _constraints(db) -> dict[tuple[str, str], str]:
    """(table, constraint name) -> its definition, for the tables whose constraints changed."""
    with db.engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT t.relname, c.conname, pg_get_constraintdef(c.oid) "
                "FROM pg_constraint c "
                "JOIN pg_class t ON t.oid = c.conrelid "
                "JOIN pg_namespace n ON n.oid = t.relnamespace "
                "WHERE n.nspname = :schema AND t.relname = ANY(:tables)"
            ),
            {"schema": _SCHEMA, "tables": list(_CONSTRAINT_TABLES)},
        ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def _nullable(db, table: str, column: str) -> bool:
    shape = {c[0]: c[2] for c in _column_shapes(db, _SCHEMA, table)}
    return shape[column] == "YES"


async def _run_in_org_schema(db, sql: str) -> None:
    async with db.acquire() as conn:
        await conn.execute(f'SET search_path TO "{_SCHEMA}"')
        await conn.execute(sql)


async def test_schema_sql_alone_restores_every_constraint_added_to_an_existing_table(own_database):
    db = own_database
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    fresh = _constraints(db)
    await _run_in_org_schema(db, _OLDER_CONSTRAINTS)
    assert _constraints(db) != fresh

    await _run_in_org_schema(db, _SCHEMA_SQL)
    assert _constraints(db) == fresh
    assert [c for c in _NOT_NULL_ONCE_EMPTY if _nullable(db, *c)] == []
    assert _nullable(db, "scheduled_triggers", "cron")

    # A second start changes nothing.
    await _run_in_org_schema(db, _SCHEMA_SQL)
    assert _constraints(db) == fresh
    assert [c for c in _NOT_NULL_ONCE_EMPTY if _nullable(db, *c)] == []


# --- constraints REMOVED from a CREATE that survive on older planes ---------------------------
#
# The inverse of the gap above: a constraint removed from the CREATE TABLE stays on a control
# plane that was built before the removal. schema.sql must DROP it so it no longer blocks the
# writes the design now allows.

# SQL that reinstates each removed constraint on a fresh schema, reproducing the older plane.
_OLDER_REMOVED_CONSTRAINTS = """
-- rls_rules: uniqueness per target+role the design removed (a role may now have multiple rules
-- on the same target, one per rights set). Commit 78be70ea removed them.
ALTER TABLE rls_rules ADD CONSTRAINT rls_rules_table_id_role_id_key
    UNIQUE (table_id, role_id);
ALTER TABLE rls_rules ADD CONSTRAINT rls_rules_action_name_role_id_key
    UNIQUE (action_name, role_id);
ALTER TABLE rls_rules ADD CONSTRAINT rls_rules_tag_role_key
    UNIQUE (tag_id, role_id);
ALTER TABLE rls_rules ADD CONSTRAINT rls_rules_domain_role_key
    UNIQUE (domain_id, role_id);

-- mv_refresh_log / mv_delta_ledger: state-store tables; FK into the model removed in c43e33c3.
ALTER TABLE mv_refresh_log ADD CONSTRAINT mv_refresh_log_mv_id_fkey
    FOREIGN KEY (mv_id) REFERENCES materialized_views(id) ON DELETE CASCADE;
ALTER TABLE mv_delta_ledger ADD CONSTRAINT mv_delta_ledger_mv_id_fkey
    FOREIGN KEY (mv_id) REFERENCES materialized_views(id) ON DELETE CASCADE;

-- file_source_mtimes: state-store table; FK into the model removed in c43e33c3.
ALTER TABLE file_source_mtimes ADD CONSTRAINT file_source_mtimes_table_id_fkey
    FOREIGN KEY (table_id) REFERENCES registered_tables(id) ON DELETE CASCADE;

-- tag_assignments: uniqueness was on tag_id before base_tag_id was introduced (333860e7).
ALTER TABLE tag_assignments ADD CONSTRAINT tag_assignments_tag_id_object_key_key
    UNIQUE (tag_id, object_key);
"""

# Every constraint the script must drop, keyed by table.
_REMOVED_CONSTRAINT_NAMES: dict[str, set[str]] = {
    "rls_rules": {
        "rls_rules_table_id_role_id_key",
        "rls_rules_action_name_role_id_key",
        "rls_rules_tag_role_key",
        "rls_rules_domain_role_key",
    },
    "mv_refresh_log": {"mv_refresh_log_mv_id_fkey"},
    "mv_delta_ledger": {"mv_delta_ledger_mv_id_fkey"},
    "file_source_mtimes": {"file_source_mtimes_table_id_fkey"},
    "tag_assignments": {"tag_assignments_tag_id_object_key_key"},
}


def _constraint_names_for(db, schema: str, tables: list[str]) -> dict[str, set[str]]:
    """table_name -> set of its constraint names, for each of the given tables."""
    with db.engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT t.relname, c.conname "
                "FROM pg_constraint c "
                "JOIN pg_class t ON t.oid = c.conrelid "
                "JOIN pg_namespace n ON n.oid = t.relnamespace "
                "WHERE n.nspname = :schema AND t.relname = ANY(:tables)"
            ),
            {"schema": schema, "tables": tables},
        ).fetchall()
    result: dict[str, set[str]] = {}
    for table, name in rows:
        result.setdefault(table, set()).add(name)
    return result


async def test_schema_sql_drops_constraints_removed_from_the_create(own_database):
    """A control plane built from an older CREATE still carries constraints the design removed.
    schema.sql must DROP them so the rows written today are accepted — including a second rule
    the rls_rules design allows (same table+role, different rights).

    # REQ-065: V1 no-migration policy — schema.sql handles all schema evolution in place.
    """
    db = own_database
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)

    # Reinstate each old constraint so the plane looks like one built before the removal.
    await _run_in_org_schema(db, _OLDER_REMOVED_CONSTRAINTS)
    tables = list(_REMOVED_CONSTRAINT_NAMES)
    before = _constraint_names_for(db, _SCHEMA, tables)
    for table, names in _REMOVED_CONSTRAINT_NAMES.items():
        assert names <= before.get(table, set()), (
            f"{table}: expected {names!r} present before schema.sql, got {before.get(table)!r}"
        )

    # schema.sql running drops them.
    await _run_in_org_schema(db, _SCHEMA_SQL)
    after = _constraint_names_for(db, _SCHEMA, tables)
    still_present = {
        table: names & after.get(table, set())
        for table, names in _REMOVED_CONSTRAINT_NAMES.items()
        if names & after.get(table, set())
    }
    assert not still_present, f"constraints still present after schema.sql: {still_present}"

    # A second run changes nothing.
    await _run_in_org_schema(db, _SCHEMA_SQL)
    after2 = _constraint_names_for(db, _SCHEMA, tables)
    still_present2 = {
        table: names & after2.get(table, set())
        for table, names in _REMOVED_CONSTRAINT_NAMES.items()
        if names & after2.get(table, set())
    }
    assert not still_present2, f"still present after second run: {still_present2}"

    # The design allows multiple rules per (table_id, role_id); the old unique blocked a second.
    async with db.acquire() as conn:
        await conn.execute(f'SET search_path TO "{_SCHEMA}"')
        await conn.execute("INSERT INTO sources (id, type) VALUES ('src_rls', 'postgres')")
        await conn.execute(
            "INSERT INTO registered_tables (source_id, domain_id, schema_name, table_name) "
            "VALUES ('src_rls', '', 'public', 'orders')"
        )
        await conn.execute("INSERT INTO roles (id) VALUES ('analyst_rls')")
        tbl_id = (
            await conn.fetchrow("SELECT id FROM registered_tables WHERE table_name = 'orders'")
        )["id"]
        # First rule — always accepted.
        await conn.execute(
            "INSERT INTO rls_rules (table_id, role_id, filter_expr, rights) "
            "VALUES ($1, 'analyst_rls', decode('00', 'hex'), 'R')",
            tbl_id,
        )
        # Second rule on the SAME (table_id, role_id) — refused by the old unique key.
        await conn.execute(
            "INSERT INTO rls_rules (table_id, role_id, filter_expr, rights) "
            "VALUES ($1, 'analyst_rls', decode('00', 'hex'), 'U')",
            tbl_id,
        )


async def test_constraints_reach_a_table_holding_rows_that_have_no_key(own_database):
    """Rows from before the key column have NULL in it and no value can be derived for them. They
    stay, what they block waits, and the writes of today work beside them; once they are gone the
    next start finishes the job."""
    from provisa.core.repositories import catalog_binding
    from provisa.live import watermark

    db = own_database
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    fresh = _constraints(db)
    await _run_in_org_schema(db, _OLDER_CONSTRAINTS)
    await _run_in_org_schema(
        db,
        # no registered table to hang the older decision on: only its nullability is under test
        "ALTER TABLE profiler_constraints DROP CONSTRAINT profiler_constraints_table_id_fkey;"
        "INSERT INTO catalog_bindings (provider, semantic_uri, vendor_ref, physical_key) "
        "VALUES ('atlas', 'urn:old', 'guid', 'key');"
        "INSERT INTO live_query_state (query_id) VALUES ('old_query');"
        "INSERT INTO profiler_constraints (id, table_id, signature, kind, column_name, definition, "
        "evidence, sampled, status, run_id) "
        "VALUES ('old', 1, 'sig', 'not_null', 'c', 'd', 'e', FALSE, 'accepted', 'r');",
    )

    for _ in range(2):
        await _run_in_org_schema(db, _SCHEMA_SQL)

    assert [c for c in _NOT_NULL_ONCE_EMPTY if not _nullable(db, *c)] == []
    async with db.acquire() as conn:
        await conn.execute(f'SET search_path TO "{_SCHEMA}"')
        for sql in (
            "INSERT INTO catalog_bindings (target, semantic_uri, vendor_ref, physical_key) "
            "VALUES ('t', 'urn:new', 'guid', 'key') ON CONFLICT (target, semantic_uri) "
            "DO UPDATE SET vendor_ref = EXCLUDED.vendor_ref",
            "INSERT INTO live_query_state (source, output_type, last_watermark) "
            "VALUES ('s', 'o', '1') ON CONFLICT (source, output_type) "
            "DO UPDATE SET last_watermark = EXCLUDED.last_watermark",
        ):
            await conn.execute(sql)
            await conn.execute(sql)  # the second one is the conflict
        # and the code that writes them, on the rows that were already there
        await catalog_binding.upsert_bindings(conn, "t2", {"urn:x": ("v", "k")})
        assert await catalog_binding.load_bindings(conn, "t2") == {"urn:x": ("v", "k")}
        await watermark.set_watermark(conn, "s2", "o2", "9")
        assert await watermark.get_watermark(conn, "s2", "o2") == "9"
        kept = await conn.fetch("SELECT query_id FROM live_query_state WHERE query_id IS NOT NULL")
        assert [r["query_id"] for r in kept] == ["old_query"]
        old = await conn.fetch("SELECT provider FROM catalog_bindings WHERE target IS NULL")
        assert [r["provider"] for r in old] == ["atlas"]

    await _run_in_org_schema(
        db,
        "DELETE FROM catalog_bindings WHERE target IS NULL;"
        "DELETE FROM live_query_state WHERE source IS NULL OR output_type IS NULL;"
        "DELETE FROM profiler_constraints WHERE physical_column IS NULL;",
    )
    await _run_in_org_schema(db, _SCHEMA_SQL)
    assert _constraints(db) == fresh
    assert [c for c in _NOT_NULL_ONCE_EMPTY if _nullable(db, *c)] == []
