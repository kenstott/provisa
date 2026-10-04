# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-318 — OpenAPI Auto-Registration Connector
  # [SUPERSEDED by REQ-845, 2026-10-03 -- the API cache is held in the engine's own store on every engine, not in a Trino Ic…

  Scenario: REQ-318 default behaviour
    Given a GET operation result cached in Trino Iceberg on S3
    When the same query with identical args is issued within TTL
    Then results are served from Trino directly with zero upstream REST calls
