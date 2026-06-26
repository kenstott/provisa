# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-372 — File & Lake Sources
  # Iceberg and Delta Lake sources support time-travel queries via an optional `as_of` argument on root query fields (snapsh…

  Scenario: REQ-372 default behaviour
    Given an Iceberg source with an as_of argument supplied at query time
    When the compiler processes the query
    Then FOR TIMESTAMP AS OF / FOR VERSION AS OF syntax is emitted; non-capable sources with as_of are rejected
