# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-905 — Query Execution
  # Cost-based promotion of VIRTUAL/SCAN sources to MATERIALIZED when the source connector cannot push down row-reducing ope…

  Scenario: REQ-905 default behaviour
    Given a federated query with row-reducing operators (filter/aggregate) over a VIRTUAL/SCAN source
    When the source connector cannot push down these operators
    And the estimated row count >= 1,000,000
    Then the source is promoted to MATERIALIZED
    And the engine store handles the reduction, amortized across the TTL
    And UNKNOWN cardinality does not trigger promotion.
