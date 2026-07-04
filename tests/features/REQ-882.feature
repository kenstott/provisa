# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-882 — Materialization Store
  # Wire the aggregate MV rewrite path into the live query execution path. Populate AggregateMVCatalog from MVRegistry on st…

  Scenario: REQ-882 default behaviour
    Given an aggregate MV over "orders" pre-computing SUM(amount), registered with no filters, and the aggregate catalog populated from the MV registry
    When a query "SELECT SUM(amount) FROM orders WHERE region = 'us'" reaches the endpoint and the join-MV rewriter did not fire
    Then the query is rewritten to read the MV target table with region = 'us' re-applied, and its sources become the MV catalog
    And an MV pre-computed WITH status = 'active' is NOT used for a query that lacks that filter (subset-safety), so no rows are silently dropped
