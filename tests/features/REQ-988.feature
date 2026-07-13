# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-988 — Arrow Flight Transport
  # Snowflake federation engine must be promoted from a source-only connector (federated through Trino) to a first-class Pro…

  Scenario: REQ-988 default behaviour
    Given Snowflake promoted to a first-class federation engine peer to Trino/DuckDB/ClickHouse
    When it declares capabilities
    Then ROWS, ARROW, and ARROW_STREAM are advertised

    Given a single-source query targeting Snowflake
    When physical SQL is transpiled
    Then it targets the Snowflake dialect and routes directly with no Trino detour
