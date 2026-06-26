# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-260 — Subscriptions
  # Polling-based subscription provider for sources without native CDC. A `watermark_column` (monotonic timestamp or sequenc…

  Scenario: REQ-260 default behaviour
    Given a table config declares a watermark_column and a source without native CDC
    When a poll subscription is created for that table
    Then new or updated rows since the last watermark are delivered to the subscriber on each poll interval
