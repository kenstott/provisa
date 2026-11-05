# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-741 — Security
  # [SUPERSEDED by REQ-2142, 2026-10-23 -- a regex mask is the universal provisa_regex_replace call, not REGEXP_REPLACE. Kep…

  Scenario: REQ-741 default behaviour
    Given a masked column in queries against different source types
    When build_mask_expression generates the mask
    Then output is the universal provisa_regex_replace and ANSI DATE_TRUNC regardless of source dialect
