# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-962 — Derived / MV Processor
  # Temporal window boundaries come from named, shared, versioned CALENDARS — not fixed intervals. A calendar is a registere…

  Scenario: REQ-962 default behaviour
    Given an MV declaring (fiscal-us calendar, quarterly grain)
    When fiscal Q1 closes
    Then the window [Q1-start, Q1-end) fires with id 2026-Q1, pegged as-of Q1-end
    And its expected set is the sealed daily sub-windows of Q1
    Given a daily-sales MV on a business-day grain
    When the day is a holiday in the calendar
    Then no window is created and the MV does not generate, with no alarm
    Given a daily-inventory MV on a calendar-day grain
    When the day is a holiday
    Then a window still opens and seals a zero/empty snapshot
