# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-066 — SQLGlot Transpilation
  # Compiler emits PG-style SQL as canonical output; SQLGlot translates to Trino SQL or target RDBMS dialect.

  Scenario: REQ-066 default behaviour
    Given a compiled GraphQL query
    When the transpiler processes it
    Then PG-style SQL is emitted as canonical output and SQLGlot translates it to the target dialect
