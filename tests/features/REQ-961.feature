# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-961 — Derived / MV Processor
  # A periodic MV's processing trigger is the CALENDAR boundary (REQ-962), bounded by the claim deadline (window.end + allow…

  Scenario: REQ-961 default behaviour
    Given a daily-sales MV with expected-events list [transactions] and a business-day calendar
    When the day is a business day and transactions are fresh-through-end-of-day
    Then at the deadline the window generates the day's partition, trusted
    When the day is a business day with genuinely no sales but transactions refreshed cleanly
    Then transactions is fresh-through-end-of-day with zero rows and a trustworthy zero is sealed
    When the day is a business day but transactions is not fresh-through-end-of-day at the deadline
    Then an outage is raised as warn/hold (not a silent skip)
    When the calendar marks the day a holiday
    Then no window exists, the MV does not generate, and no alarm is raised
