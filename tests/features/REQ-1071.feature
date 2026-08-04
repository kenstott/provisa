# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1071 — Data Catalog Integration
  # Governance-signal projection: enforcement facts Provisa already computes (which columns/tables are masked, RLS-restricte…

  Scenario: REQ-1071 default behaviour
    Given a governed config with a masked column, a column hidden from one role, and an RLS rule
    When a metadata snapshot is built for the org
    Then each restricted asset carries a governance tag naming the signal and the rule
    And every tag names the roles the restriction applies to and the roles exempt from it
    And no mask pattern or RLS predicate appears anywhere in the snapshot
