# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-313 — GraphQL Remote Schema Connector (REQ-307–313)
  # Relationships between remote schema virtual tables and local registered tables are supported via the standard relationsh…

  Scenario: REQ-313 default behaviour
    Given a relationship defined between a remote schema virtual table and a local table
    When a joined query is executed
    Then the local side is resolved from cache/DB and the remote side via the cached remote call
