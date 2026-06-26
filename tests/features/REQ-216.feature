# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-216 — Hasura v2 Parity: Low-Complexity Features
  # Scheduled triggers -- time-based execution of registered webhooks or internal functions. APScheduler in-process, cron ex…

  Scenario: REQ-216 default behaviour
    Given scheduled trigger config in provisa.yaml with a cron expression
    When the cron fires
    Then the configured webhook or internal function is executed via APScheduler
