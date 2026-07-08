# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-926 — Change Subscriptions
  # The watermark column gates refresh mode and subscribability: watermark set → APPEND refresh (incremental, WHERE wm > cur…

  Scenario: REQ-926 default behaviour
    Given a source with a watermark column set
    When refresh is triggered
    Then the system executes APPEND refresh (incremental WHERE wm > cursor)
    And subscriptions to this source are permitted
    Given a source with no watermark column
    When refresh is triggered
    Then the system executes REPLACE refresh (full DELETE+INSERT)
    And subscriptions to this source are forbidden
