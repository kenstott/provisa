# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-626 — Hasura Migration Converters
  # Roles are collected exclusively from permission entries (select, insert, update, delete permissions on tables, action pe…

  Scenario: REQ-626 default behaviour
    Given a Hasura project with roles that have no permission entries on any table or action
    When the v2 converter runs
    Then those roles are excluded from the output config
