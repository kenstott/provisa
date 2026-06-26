# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-212 — Hasura v2 Parity: Low-Complexity Features
  # Upsert mutations -- `INSERT ... ON CONFLICT ... DO UPDATE`. New `upsert_<table>` mutation field. Conflict columns inferr…

  Scenario: REQ-212 default behaviour
    Given a GraphQL upsert_<table> mutation request
    When the compiler processes it
    Then INSERT ... ON CONFLICT ... DO UPDATE SQL is generated with conflict columns from primary key metadata
