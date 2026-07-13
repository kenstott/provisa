# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-986 — Arrow Flight Transport
  # ClickHouse federation engine must expose Arrow-native transport through the Provisa Arrow Flight server to honor its dec…

  Scenario: REQ-986 default behaviour
    Given the ClickHouse federation engine advertising ARROW and ARROW_STREAM
    When a query executes through the Provisa Arrow Flight server via run_arrow
    Then columnar data is returned as an Arrow table without row materialization

    When the same query executes via run_arrow_stream
    Then it yields Arrow record batches lazily
