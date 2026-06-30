# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-812 — API & Integration
  # The `X-Provisa-Sink` request header on a subscription request redirects the subscription's change-event output to a Kafk…

  Scenario: REQ-812 default behaviour
    Given a subscription request with the header "X-Provisa-Sink" set to a Kafka target
    When the request is accepted
    Then the response status is 202 Accepted
    And subscription change events are delivered to the Kafka sink instead of an SSE stream
