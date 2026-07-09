# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-953 — Replica Lifecycle
  # Source replicas are built at boot by posting replace events for each source, and refreshed by events based on their cach…

  Scenario: REQ-953 default behaviour
    Given a Provisa instance with source specs configured
    When the application boots
    Then register_runtime schedules a one-shot "events:boot" job that posts replace events for each source
    And supervisor.drain lands every source replica and fans to materialized views once, idempotently
    And register_runtime schedules refresh injectors for each source at its cache_ttl cadence
    And push sources are refreshed by their listeners
