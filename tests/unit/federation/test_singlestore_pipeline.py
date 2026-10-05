# Copyright (c) 2026 Kenneth Stott
# Canary: d4469e72-96a6-4817-bad2-befd7ca4d39c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-990: the SingleStore PIPELINE face renders the DDL the store runs to load an origin itself
(no row through Provisa), and refuses by name what it cannot land."""

from __future__ import annotations

import pytest

from provisa.federation.singlestore_pipeline import (
    LandOrigin,
    PipelineRefused,
    ICEBERG_CATALOG_HINTS,
    file_pipeline_ddl,
    iceberg_pipeline_ddl,
    kafka_pipeline_ddl,
    lands_by_pipeline,
    link_name,
    object_store,
    object_store_link_ddl,
    origin_of,
    pipeline_name,
    procedure_name,
    refuse_region_filter,
    s3_link_ddl,
    start,
    start_foreground,
    store_path,
    teardown,
)

_HINTS = {
    "access_key_id": "AKIA1",
    "secret_access_key": "s'cr\\et",
    "region": "us-east-1",
}


class _Source:
    def __init__(self, type_: str, path: str | None = None) -> None:
        self.type = type_
        self.path = path


def test_origin_of_reads_kafka_topic_and_format_from_the_table_and_files_from_the_source():
    table = {"live": {"strategy": "kafka", "kafka": {"topic": "orders", "format": "AVRO"}}}
    assert origin_of(_Source("kafka"), table) == LandOrigin("kafka", "orders", "avro")
    assert origin_of(_Source("kafka"), {}) == LandOrigin("kafka", "", "json")
    assert origin_of(_Source("parquet", "s3://b/k.parquet")) == LandOrigin(
        "parquet", "s3://b/k.parquet", "parquet"
    )


def test_out_of_scope_origins_keep_the_relay():
    for origin in (
        LandOrigin("kafka", "t", "protobuf"),
        LandOrigin("csv", "/local/file.csv", "csv"),
        LandOrigin("iceberg", "gs://b/t", "iceberg"),
        LandOrigin("delta_lake", "s3://b/t", "delta_lake"),
        LandOrigin("hive", "", "hive"),
        LandOrigin("postgresql", "", "postgresql"),
    ):
        assert lands_by_pipeline(origin) is False


def test_a_region_filtered_copy_is_refused_by_name():
    refuse_region_filter(None, table="t")  # no filter: nothing to refuse
    with pytest.raises(PipelineRefused, match="needs a region filter at load"):
        refuse_region_filter("region = 'eu'", table="t")


def test_names_are_stable_per_replica_and_distinct_per_kind():
    names = {pipeline_name("s", "t"), link_name("s", "t"), procedure_name("s", "t")}
    assert len(names) == 3
    assert pipeline_name("s", "t") == pipeline_name("s", "t") != pipeline_name("s", "u")
    assert all(len(n) <= 64 for n in names)


def test_s3_link_holds_the_credentials_escaped():
    ddl = s3_link_ddl("db", "ln", {**_HINTS, "endpoint": "https://r2.example"})
    assert ddl == (
        "CREATE OR REPLACE LINK `db`.`ln` AS S3 CREDENTIALS "
        """'{"aws_access_key_id":"AKIA1","aws_secret_access_key":"s\\'cr\\\\\\\\et"}' """
        """CONFIG '{"endpoint_url":"https://r2.example","region":"us-east-1"}'"""
    )


def test_s3_link_without_region_or_keys_is_refused_by_name():
    with pytest.raises(PipelineRefused, match="region"):
        s3_link_ddl("db", "ln", {"access_key_id": "a", "secret_access_key": "b"})


def test_parquet_pipeline_reads_by_field_name_through_the_link():
    ddl = file_pipeline_ddl(
        schema="db",
        pipeline="pl",
        link="ln",
        origin=LandOrigin("parquet", "s3://bucket/dir/k.parquet", "parquet"),
        into_table="build__t",
        columns=["id", "name"],
    )
    assert ddl == (
        "CREATE PIPELINE `db`.`pl` AS LOAD DATA LINK `db`.`ln` 'bucket/dir/k.parquet' "
        "INTO TABLE `db`.`build__t` FORMAT PARQUET (`id` <- `id`, `name` <- `name`)"
    )


def test_csv_pipeline_maps_the_header_by_position_and_skips_undeclared_columns():
    ddl = file_pipeline_ddl(
        schema="db",
        pipeline="pl",
        link="ln",
        origin=LandOrigin("csv", "s3://bucket/k.csv", "csv"),
        into_table="build__t",
        columns=["id", "name"],
        csv_header=["id", "extra", "name"],
    )
    assert ddl.endswith(
        "INTO TABLE `db`.`build__t` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
        "LINES TERMINATED BY '\\n' IGNORE 1 LINES (`id`, @skip, `name`)"
    )


def test_csv_pipeline_refuses_a_declared_column_missing_from_the_header():
    with pytest.raises(PipelineRefused, match="no column name in its header"):
        file_pipeline_ddl(
            schema="db",
            pipeline="pl",
            link="ln",
            origin=LandOrigin("csv", "s3://bucket/k.csv", "csv"),
            into_table="build__t",
            columns=["id", "name"],
            csv_header=["id"],
        )


_COLUMNS = [("id", "BIGINT"), ("name", "TEXT")]


def test_kafka_json_lands_through_a_procedure_that_upserts_and_deletes_by_key():
    proc, pipe = kafka_pipeline_ddl(
        schema="db",
        pipeline="pl",
        procedure="pp",
        bootstrap="broker:9092",
        origin=LandOrigin("kafka", "orders", "json"),
        table="t",
        columns=_COLUMNS,
        pk_columns=["id"],
        field_mapping={"customerName": "name"},
    )
    assert proc == (
        "CREATE OR REPLACE PROCEDURE `db`.`pp`(batch QUERY(op TEXT, `id` BIGINT, `name` TEXT)) AS "
        "BEGIN DELETE t FROM `db`.`t` t JOIN batch b ON t.`id` = b.`id` WHERE b.op = 'delete'; "
        "INSERT INTO `db`.`t` (`id`, `name`) SELECT `id`, `name` FROM batch WHERE op <> 'delete' "
        "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`); END"
    )
    assert pipe == (
        "CREATE PIPELINE `db`.`pl` AS LOAD DATA KAFKA 'broker:9092/orders' "
        "INTO PROCEDURE `db`.`pp` FORMAT JSON (op <- op DEFAULT 'insert', "
        "@f0 <- `id` DEFAULT NULL, @r0 <- row::`id` DEFAULT NULL, "
        "@f1 <- `customerName` DEFAULT NULL, @r1 <- row::`customerName` DEFAULT NULL) "
        "SET `id` = COALESCE(@r0, @f0), `name` = COALESCE(@r1, @f1)"
    )


def test_kafka_avro_lands_into_the_table_from_the_schema_registry():
    (pipe,) = kafka_pipeline_ddl(
        schema="db",
        pipeline="pl",
        procedure="pp",
        bootstrap="broker:9092",
        origin=LandOrigin("kafka", "orders", "avro"),
        table="t",
        columns=_COLUMNS,
        pk_columns=["id"],
        schema_registry="http://registry:8081",
    )
    assert pipe == (
        "CREATE PIPELINE `db`.`pl` AS LOAD DATA KAFKA 'broker:9092/orders' INTO TABLE `db`.`t` "
        "FORMAT AVRO SCHEMA REGISTRY 'http://registry:8081' (`id` <- %::`id`, `name` <- %::`name`) "
        "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"pk_columns": []}, "has no primary key"),
        ({"origin": LandOrigin("kafka", "orders", "avro")}, "needs a schema registry"),
        ({"origin": LandOrigin("kafka", "orders", "protobuf")}, "Kafka protobuf does not land"),
    ],
)
def test_kafka_pipeline_refusals_are_named(kwargs, message):
    args = {
        "schema": "db",
        "pipeline": "pl",
        "procedure": "pp",
        "bootstrap": "broker:9092",
        "origin": LandOrigin("kafka", "orders", "json"),
        "table": "t",
        "columns": _COLUMNS,
        "pk_columns": ["id"],
        **kwargs,
    }
    with pytest.raises(PipelineRefused, match=message):
        kafka_pipeline_ddl(**args)


def test_start_and_teardown_statements():
    assert start_foreground("db", "pl") == "START PIPELINE `db`.`pl` FOREGROUND"
    assert start("db", "pl") == "START PIPELINE `db`.`pl`"
    assert teardown("db", "pl") == ["DROP PIPELINE IF EXISTS `db`.`pl`"]
    assert teardown("db", "pl", link="ln", procedure="pp") == [
        "DROP PIPELINE IF EXISTS `db`.`pl`",
        "DROP PROCEDURE IF EXISTS `db`.`pp`",
        "DROP LINK `db`.`ln`",
    ]


@pytest.mark.parametrize(
    ("location", "store", "path"),
    [
        ("s3://bucket/dir/k.csv", "S3", "bucket/dir/k.csv"),
        ("gs://bucket/dir/k.csv", "GCS", "bucket/dir/k.csv"),
        ("azure://container/dir/k.csv", "AZURE", "container/dir/k.csv"),
        ("abfss://container@acct.dfs.core.windows.net/dir/k.csv", "AZURE", "container/dir/k.csv"),
        ("wasbs://container@acct.blob.core.windows.net/k.csv", "AZURE", "container/k.csv"),
    ],
)
def test_each_object_store_and_its_path_as_a_pipeline_names_it(location, store, path):
    assert object_store(location) == store
    assert store_path(location) == path


def test_a_local_or_other_transport_is_no_object_store():
    assert object_store("/data/k.csv") is None
    assert object_store("sftp://h/k.csv") is None


def test_gcs_link_holds_its_hmac_keys():
    hints = {"gcs_access_id": "GOOG1", "gcs_secret_key": "s3cr3t"}
    assert object_store_link_ddl("db", "ln", "gs://b/k.csv", hints) == (
        "CREATE OR REPLACE LINK `db`.`ln` AS GCS "
        """CREDENTIALS '{"access_id":"GOOG1","secret_key":"s3cr3t"}' CONFIG '{}'"""
    )


def test_azure_link_holds_its_account_key():
    hints = {"azure_account_name": "acct", "azure_account_key": "a2V5"}
    assert object_store_link_ddl("db", "ln", "azure://c/k.csv", hints) == (
        "CREATE OR REPLACE LINK `db`.`ln` AS AZURE "
        """CREDENTIALS '{"account_key":"a2V5","account_name":"acct"}' CONFIG '{}'"""
    )


def test_s3_link_is_the_s3_rendering():
    assert object_store_link_ddl("db", "ln", "s3://b/k.csv", _HINTS) == s3_link_ddl(
        "db", "ln", _HINTS
    )


@pytest.mark.parametrize(
    ("location", "hints", "missing"),
    [
        ("gs://b/k.csv", {"gcs_access_id": "GOOG1"}, "gcs_secret_key"),
        # The service-account JSON other engines read is not what a pipeline takes.
        ("gs://b/k.csv", {"credentials_path": "/sa.json"}, "gcs_access_id, gcs_secret_key"),
        ("azure://c/k.csv", {"azure_account_name": "acct"}, "azure_account_key"),
        ("s3://b/k.csv", {"access_key_id": "a", "secret_access_key": "b"}, "region"),
    ],
)
def test_a_missing_store_credential_is_refused_by_name(location, hints, missing):
    with pytest.raises(PipelineRefused, match=f"needs federation_hints {missing} on the source"):
        object_store_link_ddl("db", "ln", location, hints)


_S3 = {"access_key_id": "AKIA1", "secret_access_key": "s3cr3t", "region": "us-west-2"}


@pytest.mark.parametrize(
    ("catalog", "extra", "config"),
    [
        ("GLUE", {}, {}),
        ("REST", {"iceberg_catalog_uri": "http://c:8181"}, {"catalog.uri": "http://c:8181"}),
        (
            "JDBC",
            {
                "iceberg_catalog_name": "cat",
                "iceberg_catalog_warehouse": "s3://wh",
                "iceberg_catalog_uri": "jdbc:mysql://h:3306/default",
                "iceberg_catalog_user": "u",
                "iceberg_catalog_password": "p",
            },
            {
                "catalog_name": "cat",
                "catalog.warehouse": "s3://wh",
                "catalog.uri": "jdbc:mysql://h:3306/default",
                "catalog.jdbc.user": "u",
                "catalog.jdbc.password": "p",
            },
        ),
        (
            "SNOWFLAKE",
            {
                "iceberg_catalog_uri": "jdbc:snowflake://acct.snowflakecomputing.com",
                "iceberg_catalog_user": "u",
                "iceberg_catalog_password": "p",
                "iceberg_catalog_role": "r",
            },
            {
                "catalog.uri": "jdbc:snowflake://acct.snowflakecomputing.com",
                "catalog.jdbc.user": "u",
                "catalog.jdbc.password": "p",
                "catalog.jdbc.role": "r",
            },
        ),
    ],
)
def test_iceberg_pipeline_per_catalog_type(catalog, extra, config):
    import json

    hints = {**_S3, "iceberg_catalog_type": catalog.lower(), "iceberg_table_id": "db.t", **extra}
    ddl = iceberg_pipeline_ddl(
        schema="db", pipeline="pl", hints=hints, into_table="build__t", columns=["id", "name"]
    )
    head = "CREATE PIPELINE `db`.`pl` AS LOAD DATA S3 'db.t' CONFIG '"
    assert ddl.startswith(head)
    config_json, rest = ddl[len(head) :].split("' CREDENTIALS '", 1)
    assert json.loads(config_json) == {
        "region": "us-west-2",
        "catalog_type": catalog,
        "ingest_mode": "one_time",
        **config,
    }
    credentials, tail = rest.split("' INTO TABLE ", 1)
    assert json.loads(credentials) == {
        "aws_access_key_id": "AKIA1",
        "aws_secret_access_key": "s3cr3t",
    }
    assert tail == "`db`.`build__t` (`id` <- `id`, `name` <- `name`) FORMAT ICEBERG"


@pytest.mark.parametrize(
    ("hints", "message"),
    [
        ({**_S3, "iceberg_table_id": "db.t"}, "needs federation_hints iceberg_catalog_type"),
        (
            {**_S3, "iceberg_catalog_type": "HADOOP", "iceberg_table_id": "db.t"},
            "iceberg_catalog_type, one of GLUE, REST, JDBC, SNOWFLAKE",
        ),
        (
            {**_S3, "iceberg_catalog_type": "GLUE"},
            "GLUE pipeline needs federation_hints iceberg_table_id",
        ),
        (
            {**_S3, "iceberg_catalog_type": "SNOWFLAKE", "iceberg_table_id": "d.s.t"},
            "iceberg_catalog_uri, iceberg_catalog_user, iceberg_catalog_password, "
            "iceberg_catalog_role",
        ),
        (
            {
                "iceberg_catalog_type": "REST",
                "iceberg_table_id": "db.t",
                "iceberg_catalog_uri": "u",
            },
            "access_key_id, secret_access_key, region",
        ),
    ],
)
def test_iceberg_pipeline_refusals_name_the_missing_hints(hints, message):
    with pytest.raises(PipelineRefused, match=message):
        iceberg_pipeline_ddl(
            schema="db", pipeline="pl", hints=hints, into_table="b", columns=["id"]
        )


def test_the_four_catalog_types_are_the_ruled_set():
    assert list(ICEBERG_CATALOG_HINTS) == ["GLUE", "REST", "JDBC", "SNOWFLAKE"]
