# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-741 — Security
  # Column masking output uses ANSI SQL dialects (REGEXP_REPLACE, DATE_TRUNC, SQL literals) independent of source type. Dial…

  Scenario: REQ-741 default behaviour
    Given a masked column in queries against different source types
    When build_mask_expression generates the mask
    Then output is ANSI REGEXP_REPLACE/DATE_TRUNC regardless of source dialect
