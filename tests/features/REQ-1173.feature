# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1173 — Protocol Support
  # Webhooks (tracked_webhooks from the remote schema registry) are now governed, discoverable commands routable across all…

  Scenario: REQ-1173 default behaviour
    Given a tracked webhook registered with name "notify_customer" in the schema registry
    When a pgwire client executes SELECT notify_customer(customer_id)
    Then the webhook routes through the shared invoke_tracked_function executor
    And writable_by authorization is enforced (403 if denied)
    And the HTTP POST fires to the registered URL with the arguments
