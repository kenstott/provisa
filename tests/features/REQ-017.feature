# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-017 — Registration & Governance
  # NoSQL/non-relational sources are exposed read-only through their native Trino connector (e.g. the MongoDB connector), dr…

  Scenario: REQ-017 default behaviour
    Given a registered NoSQL source with a native Trino connector
    When a consumer queries a table from that source
    Then the query is executed read-only through the Trino connector with no mutation path available
