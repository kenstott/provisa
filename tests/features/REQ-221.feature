# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-221 — Hasura v2 Parity: Medium-Complexity Features
  # Enum table auto-detection -- introspect `pg_enum` at schema build time, generate GraphQL enum types for columns using Po…

  Scenario: REQ-221 default behaviour
    Given a PostgreSQL schema with user-defined enum types used as column types
    When the schema is built
    Then GraphQL enum types are generated and enum columns are mapped to GraphQL enum types instead of String
