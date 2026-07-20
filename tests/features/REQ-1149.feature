# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1149 — Freshness / Change Detection
  # A new change_signal PUSH variant, `signal`, for a DATA-LESS external trigger (a Kafka control message on a topic, or an…

  Scenario: REQ-1149 default behaviour
    Given a source with change_signal=signal, load-protected under REQ-1141
    When a data-less trigger arrives (a Kafka control message or an HTTP webhook), carrying no rows
    Then the change-trigger token for that source bumps, marking its snapshot stale
    And the load-protected scheduler treats the bumped token as a changed probe and re-pulls the rows from the source of truth on its own schedule (deferred to the off-peak window)
    And with no trigger the token is unchanged, so the scheduler pulls nothing
