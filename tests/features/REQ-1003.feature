# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1003 — Scheduled Execution
  # Extend the APScheduler-backed ScheduledTrigger (provisa/core/models.py:640) to support execution of a user-supplied SQL…

  Scenario: REQ-1003 default behaviour
    Given a ScheduledTrigger with a SQL statement and a cron expression
    When the cron fires at the scheduled time
    Then the SQL statement is executed against the federated engine
    And the result (table creation, inserts, etc.) persists in the target source
