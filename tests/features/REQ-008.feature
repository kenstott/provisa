# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-008 — Compiler & Schema
  # Schema generation pass runs at registration time; queries Trino INFORMATION_SCHEMA, applies per-role column visibility,…

  Scenario: REQ-008 default behaviour
    Given a table is registered
    When the schema generation pass runs
    Then it queries Trino INFORMATION_SCHEMA, applies per-role column visibility, incorporates relationships, and produces GraphQL SDL
