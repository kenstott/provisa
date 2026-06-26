# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-327 — gRPC Remote Schema Connector (REQ-322–329)
  # Query method results are materialized as Parquet in a Trino Iceberg table on S3 (`results.api_cache`, `s3a://provisa-res…

  Scenario: REQ-327 default behaviour
    Given a gRPC query method result cached in Trino Iceberg on S3
    When the same call is repeated within TTL
    Then results are served from Trino directly and the gRPC channel is reused without a new connection
