# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-049 — Output & Delivery
  # Normalized tabular: flattened to relational tables with FK relationships preserved, Parquet or CSV.

  Scenario: REQ-049 default behaviour
    Given a query with nested relationships
    When the output format is normalized tabular (Parquet or CSV)
    Then results are flattened to relational tables with FK relationships preserved
