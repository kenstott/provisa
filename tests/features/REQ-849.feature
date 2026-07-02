# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-849 — Views (Governed Computed Datasets)
  # After SQL compilation, the query rewriter inspects FROM/JOIN clauses and transparently rewrites the query to read from a…

  Scenario: REQ-849 default behaviour
    Given a fresh, enabled MV covering a query's root table (and optionally some joins)
    When a query is compiled that references the underlying source tables without naming the MV
    Then the rewriter transparently redirects the FROM/JOIN to the MV target (full or partial), otherwise the query runs against the live source tables.
