# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-020 — Registration & Governance
  # Relationships owned by defining steward, versioned, flagged for re-review on schema changes affecting join fields.

  Scenario: REQ-020 default behaviour
    Given a registered relationship between two tables
    When a schema change affects one of the join fields
    Then the relationship is flagged for re-review and the owning steward is notified
