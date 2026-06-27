# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-788 — File & Lake Sources
  # File connector sources accept a directory glob pattern to enumerate CSV files. Discovered files are introspected to extr…

  Scenario: REQ-788 default behaviour
    Given a directory containing multiple CSV files matching a glob pattern
    When a file connector source is registered with the directory glob pattern
    Then all matching files are discovered and enumerated as available tables
    And the table schema is extracted from CSV headers
