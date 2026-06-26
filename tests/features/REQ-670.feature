# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-670 — Cypher Mutations
  # Cypher write endpoints return the number of rows affected (rows inserted for CREATE, rows updated for SET, rows deleted…

  Scenario: REQ-670 default behaviour
    Given a successful Cypher CREATE statement executed via the write endpoint
    When the response is returned to the client
    Then the JSON body includes an affected_rows field with the count of inserted rows
