# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1004 — Template Substitution
  # Scheduled SQL statements support run-time substitution of date/timestamp tokens (e.g., {{yyyymmdd}}, {{YYYY-MM-DD}}, {{i…

  Scenario: REQ-1004 default behaviour
    Given a ScheduledTrigger with scheduled CTAS statement: CREATE TABLE archive.orders_{{yyyymmdd}} AS SELECT * FROM live.orders
    When the cron fires on 2026-07-12
    Then the token {{yyyymmdd}} is substituted with 20260712 and the table archive.orders_20260712 is created

    When the cron fires again on 2026-07-13
    Then the token {{yyyymmdd}} is substituted with 20260713 and the table archive.orders_20260713 is created
    And both archive.orders_20260712 and archive.orders_20260713 exist in the model with distinct names
