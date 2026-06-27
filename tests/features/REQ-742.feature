# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-742 — Security
  # Masking respects column type constraints — NULL masks allowed only on nullable columns; regex and truncate masks validat…

  Scenario: REQ-742 default behaviour
    Given a masking rule configured with an incompatible type
    When config is loaded
    Then validation rejects the rule (e.g., regex on integer, truncate on varchar, NULL on NOT NULL)
