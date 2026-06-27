# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-743 — Security
  # Masking constant expressions must emit syntactically valid SQL for their type — numeric literals without quotes, string…

  Scenario: REQ-743 default behaviour
    Given various constant mask values (null, boolean, numeric, string with apostrophe)
    When build_mask_expression generates the SQL literal
    Then output is syntactically valid (NULL keyword, TRUE/FALSE, numeric unquoted, strings single-quoted with escaped apostrophes)
