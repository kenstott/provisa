# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-183 — Hasura Migration Converters
  # Hasura DDN (v3) HML converter -- CLI tool that reads a DDN supergraph project and emits valid Provisa YAML config. Conve…

  Scenario: REQ-183 default behaviour
    Given a Hasura DDN supergraph project
    When the HML converter CLI tool is run
    Then valid Provisa YAML config is emitted covering ObjectTypes, Models, Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks
