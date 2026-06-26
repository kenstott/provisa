# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-218 — Hasura v2 Parity: Medium-Complexity Features
  # Cursor-based pagination -- `first`, `after`, `last`, `before` args on root query fields. Returns `edges[{cursor, node}]`…

  Scenario: REQ-218 default behaviour
    Given a root query field with first/after/last/before cursor pagination args
    When the query executes
    Then edges[{cursor, node}] and pageInfo are returned with base64-encoded cursors
