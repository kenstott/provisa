# Copyright (c) 2026 Kenneth Stott
# Canary: 3f62f688-50d6-4c1e-a2b9-d44a528be83b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1194: an engine other than Trino writes a statement's result to the results object store
itself, with the statement's bound values, and returns where it is and how many rows it holds.

Here: the statement each engine runs, what each backend declares it writes, and that a format
an engine does not write is refused by name. The writes themselves are proven against a real
object store in tests/integration/test_engine_result_sink.py."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation import result_sink
from provisa.federation.result_sink import ResultFormatNotWritten, ResultTarget

TARGET = ResultTarget(bucket="provisa-results", result_id="abc123")
MINIO = SimpleNamespace(
    bucket="provisa-results",
    endpoint_url="http://127.0.0.1:9000",
    access_key="key",
    secret_key="it's-secret",
    region="us-east-1",
)
AWS = SimpleNamespace(
    bucket="provisa-results", endpoint_url="", access_key="AK", secret_key="SK", region="eu-west-1"
)


def test_a_result_has_its_own_prefix_at_the_address_the_presigner_reads():
    assert TARGET.s3_prefix == "s3a://provisa-results/results/abc123"
    assert TARGET.object_url == "s3://provisa-results/results/abc123/data.parquet"
    first, second = result_sink.new_target(MINIO), result_sink.new_target(MINIO)
    assert first.bucket == "provisa-results" and first.result_id != second.result_id


def test_duckdb_copies_the_query_with_its_placeholders_kept():
    sql = result_sink.duckdb_copy("SELECT id FROM t WHERE id > ?", TARGET)
    assert sql == (
        "COPY (SELECT id FROM t WHERE id > ?) TO "
        "'s3://provisa-results/results/abc123/data.parquet' (FORMAT PARQUET)"
    )


def test_duckdbs_secret_is_scoped_to_the_results_prefix_and_addresses_a_private_store():
    secret = result_sink.duckdb_secret(MINIO)
    assert secret.startswith("CREATE OR REPLACE SECRET provisa_results (TYPE S3")
    assert "SCOPE 's3://provisa-results/results/'" in secret
    assert "ENDPOINT '127.0.0.1:9000'" in secret and "USE_SSL false" in secret
    assert "URL_STYLE 'path'" in secret
    assert "SECRET 'it''s-secret'" in secret  # quoted as a literal
    aws = result_sink.duckdb_secret(AWS)
    assert "ENDPOINT" not in aws and "REGION 'eu-west-1'" in aws


def test_clickhouse_inserts_into_its_s3_function_with_the_keys_as_parameters():
    # The keys are server-side parameters: the statement text, which ClickHouse keeps in its
    # query log and Provisa may log, holds no credential.
    sql, params = result_sink.clickhouse_insert("SELECT id FROM t", TARGET, MINIO)
    assert sql == (
        "INSERT INTO FUNCTION s3({provisa_results_url:String}, {provisa_results_key:String}, "
        "{provisa_results_secret:String}, 'Parquet') SELECT id FROM t"
    )
    assert params == {
        "provisa_results_url": (
            "http://127.0.0.1:9000/provisa-results/results/abc123/data.parquet"
        ),
        "provisa_results_key": "key",
        "provisa_results_secret": "it's-secret",
    }
    _aws_sql, aws = result_sink.clickhouse_insert("SELECT 1", TARGET, AWS)
    assert aws["provisa_results_url"].startswith(
        "https://s3.eu-west-1.amazonaws.com/provisa-results/results/abc123/"
    )
    count_sql, count_params = result_sink.clickhouse_count(TARGET, MINIO)
    assert count_sql == (
        "SELECT count() FROM s3({provisa_results_url:String}, {provisa_results_key:String}, "
        "{provisa_results_secret:String}, 'Parquet')"
    )
    assert count_params == params
    for statement in (sql, _aws_sql, count_sql):
        assert "key'" not in statement and "secret" not in statement.replace(
            "provisa_results_secret", ""
        )


def test_snowflake_unloads_through_its_storage_integration_never_with_credentials():
    config = SimpleNamespace(**vars(AWS), snowflake_storage_integration="provisa_results")
    sql = result_sink.snowflake_copy("SELECT id FROM t WHERE id > ?", TARGET, config)
    assert sql == (
        "COPY INTO 's3://provisa-results/results/abc123/' FROM (SELECT id FROM t WHERE id > ?) "
        'STORAGE_INTEGRATION = "provisa_results" '
        "FILE_FORMAT = (TYPE = PARQUET) HEADER = TRUE"
    )
    assert "SK" not in sql and "CREDENTIALS" not in sql


def test_snowflake_with_no_storage_integration_does_not_write_results_itself():
    config = SimpleNamespace(**vars(AWS), snowflake_storage_integration=None)
    with pytest.raises(result_sink.ResultStoreNotGranted, match="storage integration"):
        result_sink.snowflake_copy("SELECT 1", TARGET, config)


def test_databricks_overwrites_the_results_directory_and_counts_what_it_wrote():
    assert result_sink.databricks_insert("SELECT id FROM t", TARGET) == (
        "INSERT OVERWRITE DIRECTORY 's3://provisa-results/results/abc123/' USING PARQUET "
        "SELECT id FROM t"
    )
    assert result_sink.databricks_count(TARGET) == (
        "SELECT count(*) FROM parquet.`s3://provisa-results/results/abc123/`"
    )


def test_a_format_the_engine_does_not_write_is_refused_by_name():
    with pytest.raises(ResultFormatNotWritten, match="'duckdb' does not write 'orc'"):
        result_sink.require_format("duckdb", "orc", frozenset({"parquet"}))
    result_sink.require_format("duckdb", "PARQUET", frozenset({"parquet"}))


@pytest.mark.parametrize(
    ("module", "name", "formats"),
    [
        ("provisa.federation.duckdb_backend", "DuckDBBackend", {"parquet"}),
        ("provisa.federation.clickhouse_backend", "ClickHouseBackend", {"parquet"}),
        ("provisa.federation.snowflake_backend", "SnowflakeBackend", {"parquet"}),
        ("provisa.federation.databricks_backend", "DatabricksBackend", {"parquet"}),
        ("provisa.federation.backend", "TrinoBackend", {"parquet", "orc"}),
        ("provisa.federation.pg_backend", "PgBackend", set()),
    ],
)
def test_each_engine_declares_the_formats_it_writes_itself(module, name, formats):
    import importlib

    backend = getattr(importlib.import_module(module), name)
    assert backend.result_formats == frozenset(formats)
    # An engine that declares a format has its own write; one that declares none has not.
    from provisa.federation.backend import EngineBackend

    assert (backend.ctas_redirect is not EngineBackend.ctas_redirect) is bool(formats)


def test_the_tier_needs_an_engine_that_writes_the_format_and_is_connected():
    from provisa.federation.runtime import EngineRuntime

    def _runtime(formats, connected, granted=True):
        runtime = object.__new__(EngineRuntime)
        runtime._state = None
        runtime._backend = SimpleNamespace(
            result_formats=frozenset(formats),
            is_connected=lambda state: connected,
            writes_results_now=lambda: granted,
        )
        return runtime

    # An engine the deployment has not given its grant (Snowflake with no storage integration).
    assert _runtime({"parquet"}, True, granted=False).writes_result("parquet") is False

    assert _runtime({"parquet"}, True).writes_result("Parquet") is True
    assert _runtime({"parquet"}, True).writes_result("orc") is False
    assert _runtime({"parquet"}, False).writes_result("parquet") is False  # asleep
    assert _runtime(set(), True).writes_result("parquet") is False


def test_snowflake_writes_results_only_with_a_storage_integration_named(monkeypatch):
    from provisa.core import settings_registry
    from provisa.federation.snowflake_backend import SnowflakeBackend

    backend = object.__new__(SnowflakeBackend)
    named: dict[str, str | None] = {"redirect.snowflake_storage_integration": None}
    monkeypatch.setattr(settings_registry, "value", lambda key: named[key])
    assert backend.writes_results_now() is False
    named["redirect.snowflake_storage_integration"] = "provisa_results"
    assert backend.writes_results_now() is True
