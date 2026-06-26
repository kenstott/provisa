# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-478 — Compiler & Schema
  # Statistical row sampling is a user query feature, not governance. Root query fields accept an optional `sample: Float` a…

  Scenario: REQ-478 default behaviour
    Given a query with sample: 10.0
    When the compiler processes it
    Then it emits TABLESAMPLE BERNOULLI (10) on the base table and rejects values outside (0, 100]
