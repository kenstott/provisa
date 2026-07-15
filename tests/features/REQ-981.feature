# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-981 — Live Data & Events
  # The event loop's content-hash output gate — an always-on no-op ripple suppressor for replace-shaped landings. After a so…

  Scenario: REQ-981 default behaviour
    Given a materialized source with change_signal=ttl and probe_type=none
    And its prior land stored a content hash H
    When the TTL poll fires and the re-fetched rows hash to H
    Then the node skips the store write
    And the node does not re-post its change event
    And no dependent MV recomputes
    Given the same source
    When the re-fetched rows hash to a value other than H
    Then the node lands the rows and persists the new hash
    And re-posts its change event to its dependents
