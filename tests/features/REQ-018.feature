# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-018 — Registration & Governance
  # Trino FK metadata used to infer candidate intra-source relationships for steward confirmation/rejection.

  Scenario: REQ-018 default behaviour
    Given tables in a registered source with FK constraints visible via Trino metadata
    When a steward reviews relationship candidates
    Then intra-source FK relationships are presented as candidates for confirmation or rejection
