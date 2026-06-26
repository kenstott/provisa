# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-184 — Hasura Migration Converters
  # Shared boolean expression-to-SQL converter for Hasura filter expressions. Supports `_eq`, `_neq`, `_gt`, `_gte`, `_lt`,…

  Scenario: REQ-184 default behaviour
    Given a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not
    When the shared converter processes it
    Then valid SQL is produced with session variable references mapped to current_setting('provisa.<name>')
