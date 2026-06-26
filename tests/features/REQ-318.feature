# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-318 — OpenAPI Auto-Registration Connector
  # GET operation results are materialized as Parquet in a Trino Iceberg table on S3 (`results.api_cache`, `s3a://provisa-re…

  Scenario: REQ-318 default behaviour
    Given a GET operation result cached in Trino Iceberg on S3
    When the same query with identical args is issued within TTL
    Then results are served from Trino directly with zero upstream REST calls
