# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-327 — gRPC Remote Schema Connector (REQ-322–329)
  # [SUPERSEDED by REQ-845, 2026-10-03 -- the API cache is held in the engine's own store on every engine, not in a Trino Ic…

  Scenario: REQ-327 default behaviour
    Given a gRPC query method result cached in Trino Iceberg on S3
    When the same call is repeated within TTL
    Then results are served from Trino directly and the gRPC channel is reused without a new connection
