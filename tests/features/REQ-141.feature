# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-141 — Large Result Redirect & CTAS
  # S3 data cleanup scheduled after presigned URL TTL expires.

  Scenario: REQ-141 default behaviour
    Given redirect results have been written to S3 with a presigned URL
    When the presigned URL TTL expires
    Then a scheduled job removes the corresponding S3 data
