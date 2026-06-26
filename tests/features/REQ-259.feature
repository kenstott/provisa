# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-259 — Compiler & Schema
  # Apollo Federation v2 subgraph support — when enabled, Provisa generates a Federation v2 compliant schema with @key direc…

  Scenario: REQ-259 default behaviour
    Given Apollo Federation v2 support is enabled
    When the schema is generated
    Then @key directives, _service, and _entities fields are present and entity resolution respects
    RLS and masking
