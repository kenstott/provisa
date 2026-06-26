# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-253 — Compiler & Schema
  # Naming convention changes (global, source, or table level) reflected immediately in GraphQL schema. Admin mutations trig…

  Scenario: REQ-253 default behaviour
    Given a naming convention change is applied via admin mutation
    When _rebuild_schemas() completes
    Then the in-memory GraphQL schema is regenerated and fresh introspection is returned on the next
    request
