# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-139 — Large Result Redirect & CTAS
  # Non-native formats (JSON, NDJSON, CSV, Arrow IPC) serialized by Provisa and uploaded to S3 via boto3.

  Scenario: REQ-139 default behaviour
    Given a redirect format of JSON, NDJSON, CSV, or Arrow IPC
    When the query executes above the threshold
    Then Provisa serializes the result and uploads it to S3 via boto3
