# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-891 — Federation Engine Abstraction
  # Pgwire per-role authorization maps an authenticated account to Calcite/source schema+table visibility, enforced in BOTH…

  Scenario: REQ-891 default behaviour
    Given an authenticated role whose grant excludes table 'secret'
    When the role queries information_schema/pg_catalog through the catalog intercept
    Then 'secret' is filtered out of discovery results

    Given the same role issues a query referencing an object outside its grant
    When the query executes
    Then execution rejects the access rather than returning rows
