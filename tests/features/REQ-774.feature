# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-774 — Cypher Query Frontend (Phase AU)
  # Duplicate JSON key prevention. When a table's id_column differs from 'id' but the table has a column named 'id', the JSO…

  Scenario: REQ-774 default behaviour
    Given a table with id_column='inquiry_id' but also a column named 'id'
    When the graph rewriter projects the node
    Then the JSON_OBJECT contains only one 'id' key corresponding to the id_column
