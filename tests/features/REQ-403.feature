# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-403 — Compiler & Schema
  # RLS injection via `inject_rls()` checks for table-specific rules first, then falls back to domain-level rules; table-lev…

  Scenario: REQ-403 default behaviour
    Given a table with both table-specific and domain-level RLS rules
    When inject_rls() runs
    Then table-specific rules take precedence over domain-level rules
