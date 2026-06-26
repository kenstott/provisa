# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-311 — GraphQL Remote Schema Connector (REQ-307–313)
  # Remote schema introspection is re-run on demand via an "Refresh Schema" admin mutation. Provisa does not poll for remote…

  Scenario: REQ-311 default behaviour
    Given a remote GraphQL schema that has changed upstream
    When a steward triggers the Refresh Schema admin mutation
    Then registrations are updated and existing RLS/masking rules are preserved
