# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-363 — Semantic Layer / Semantic Model
  # The SQLAlchemy dialect introspects table and column metadata via `POST /data/graphql` (GraphQL introspection query) inst…

  Scenario: REQ-363 default behaviour
    Given a SQLAlchemy client using the Provisa dialect with a specific role
    When get_table_names() or get_columns() is called
    Then the results are filtered through the governed GraphQL introspection endpoint and only
    permitted tables and columns are returned
