# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-182 — Hasura Migration Converters
  # Hasura v2 metadata converter -- CLI tool that reads a Hasura v2 metadata export directory and emits valid Provisa YAML c…

  Scenario: REQ-182 default behaviour
    Given a Hasura v2 metadata export directory
    When the CLI converter is run against it
    Then valid Provisa YAML config is emitted covering tables, relationships, permissions, roles, and auth
