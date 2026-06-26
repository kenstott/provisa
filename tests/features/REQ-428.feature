# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-428 — Vector Search
  # Generated embedding columns must support scheduled incremental refresh — only rows where the source data has changed sin…

  Scenario: REQ-428 default behaviour
    Given a generated embedding column with changed source rows
    When the scheduled incremental refresh runs
    Then only changed rows are re-embedded; a model or schema change triggers a full rebuild
