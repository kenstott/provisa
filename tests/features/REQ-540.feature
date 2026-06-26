# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-540 — GovData Sources
  # Sources of type `govdata` expose U.S. government open data partitioned by subject grouping. Configuring a govdata source…

  Scenario: REQ-540 default behaviour
    Given a govdata source configured with a subject grouping
    When the source is registered
    Then all schemas for that subject are automatically exposed as governed tables
