# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-623 — Hasura Migration Converters
  # v2 converter maps Hasura source `kind` to Provisa `SourceType`: `pg`/`postgres` -> `postgresql`, `mssql` -> `sqlserver`,…

  Scenario: REQ-623 default behaviour
    Given a Hasura v2 source config with kind, database_url, and pool_settings
    When the v2 converter runs
    Then SourceType is mapped correctly and connection URL is parsed into components with pool settings preserved
