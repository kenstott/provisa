# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1070 — Data Catalog Integration
  # Published metadata payload includes datasets/tables/columns, domains, stewards and ownership (REQ-609/020), approved rel…

  Scenario: REQ-1070 default behaviour
    Given a governed config with a stewarded domain, a table with described columns, an approved relationship, and a view derived from that table
    When a metadata snapshot is built for the org
    Then the snapshot publishes the source, domain, table and every column as addressable assets
    And the approved relationship carries its defining steward, version and review flag
    And the view's lineage is published per column with the transform that produces it
    And a domain with no designated steward is published as pending rather than omitted
