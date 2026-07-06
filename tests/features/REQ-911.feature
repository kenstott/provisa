# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-911 — ClickHouse Federation Engine
  # ClickHouse is a selectable federation engine (PROVISA_ENGINE=clickhouse) alongside trino/pg/duckdb/sqlalchemy. It is a s…

  Scenario: REQ-911 default behaviour
    Given PROVISA_ENGINE=clickhouse with registered sources (postgresql, mongodb, s3://bucket/file.parquet)
    When the federation engine initializes
    Then postgresql source mounts as CREATE DATABASE ... ENGINE=PostgreSQL
    And mongodb source mounts as CREATE TABLE ... ENGINE=MongoDB with supplied columns
    And s3 parquet source mounts as CREATE TABLE ... ENGINE=S3 with inferred columns
    And all sources support predicate pushdown where available (postgresql, mysql, parquet)
    And query execution transpiles to ClickHouse dialect and routes through ClickHouseFederationRuntime
