# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-992 — Live Delivery Strategy
  # Poll signals without a watermark column execute a full-replace poll mode: LiveEngine._poll_replace re-scans the full res…

  Scenario: REQ-992 default behaviour
    Given a poll-signal table with no watermark column and 100 rows
    When the full table is re-scanned and the content hash matches the previous scan
    Then no snapshot is delivered to downstream subscribers

    When the content hash differs (row added, deleted, or modified)
    Then a replace snapshot is delivered
