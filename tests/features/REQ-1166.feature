Feature: REQ-1166 — Repeating-calendar snapshot MV
  # A materialized view can carry a repeating calendar trigger that cuts a calendar-addressable
  # version at each boundary onto the append-only bitemporal log, so history is preserved and every
  # sealed period is readable as-of its boundary (calendar-addressed time travel).

  Scenario: A monthly snapshot seals an addressable version at each boundary
    Given a bitemporal snapshot MV on a monthly calendar
    When the January window closes with the month's data
    And the February window closes with changed data
    Then each closed window sealed a distinct version stamped at its boundary
    And reading as-of the January boundary returns January's data
    And reading as-of the February boundary returns February's data

  Scenario: An nth-weekday recurrence drives the snapshot boundary
    Given a bitemporal snapshot MV on a "3rd Wednesday of month" calendar
    When the third-Wednesday window closes
    Then a version is sealed addressed by that occurrence
