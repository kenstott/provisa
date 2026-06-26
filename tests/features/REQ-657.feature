# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-657 — JSON:API Remote Schema Connector
  # JSON:API relationship expansion via `?include=` parameter integrates with Provisa JOINs — when a JOIN targets a relation…

  Scenario: REQ-657 default behaviour
    Given a JOIN query targeting a JSON:API relationship field
    When the compiler processes the query
    Then the corresponding include list is injected into the remote request to expand the traversal in a single call
