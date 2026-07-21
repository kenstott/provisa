# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1166 — Materialization Store
  # A materialized view can have a REPEATING CALENDAR TRIGGER attached that cuts a calendar-addressable version at each boun…

  Scenario: REQ-1166 default behaviour
    Given a materialized view with a repeating calendar trigger on a named boundary
    When each calendar boundary is reached and input freshness gates are satisfied
    Then a version is cut at that boundary with a stable window_id, and the version is addressable via that window_id in time-travel reads, regardless of whether the underlying storage is full snapshot or delta append
