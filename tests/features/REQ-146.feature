# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-146 — Arrow Flight
  # Falls back to materializing via Trino REST if Zaychik unavailable.

  Scenario: REQ-146 default behaviour
    Given the Zaychik proxy is unavailable
    When a Flight query is submitted
    Then the Flight server falls back to materializing results via Trino REST API
