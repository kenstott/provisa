# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-171 — Infrastructure
  # MinIO results bucket auto-created at startup.

  Scenario: REQ-171 default behaviour
    Given the Provisa stack starts for the first time
    When the startup sequence runs
    Then the MinIO results bucket is created automatically without manual intervention
