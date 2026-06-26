# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-625 — Hasura Migration Converters
  # When a Hasura v2 source `database_url` is an environment variable reference (`{"from_env": "VAR"}`) or cannot be parsed,…

  Scenario: REQ-625 default behaviour
    Given a Hasura v2 source with database_url as an env var reference or unparseable URL
    When the v2 converter runs
    Then placeholder connection values are substituted and operators are directed to use --source-overrides
