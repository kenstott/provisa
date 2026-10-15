# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-403 — Compiler & Schema
  # [SUPERSEDED by REQ-403, 2026-10-15 -- the rule is resolved at runtime build, not by an inject_rls() step. Kept here for…

  Scenario: REQ-403 default behaviour
    Given a table with both table-specific and domain-level RLS rules
    When the role's query is governed
    Then table-specific rules take precedence over domain-level rules
