# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-605 — Registration & Governance
  # When `root_table_ids` is set on a `SchemaInput`, tables whose IDs are absent from that set are excluded from root query…

  Scenario: REQ-605 default behaviour
    Given a SchemaInput with root_table_ids set excluding some tables
    When the SDL is generated
    Then excluded tables are present as named types but absent from root query fields
