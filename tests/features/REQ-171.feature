# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-171 — Infrastructure
  # [SUPERSEDED by REQ-171 (amendment ENSURED BY THE FIRST REDIRECT), 2026-10-01 -- the bucket is not created at startup. Ke…

  Scenario: REQ-171 default behaviour
    Given the Provisa stack starts for the first time
    When the first redirect that needs the results bucket runs
    Then the MinIO results bucket is created automatically without manual intervention
