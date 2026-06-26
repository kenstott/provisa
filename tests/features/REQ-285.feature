# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-285 — Live Query Engine (Unified Subscription & Sink Delivery)
  # Tables declare delivery mode in the subscription/sink config: `delivery: cdc` or `delivery: poll`. `cdc` is available fo…

  Scenario: REQ-285 default behaviour
    Given a Trino-federated source with delivery: cdc in its config
    When Provisa starts up
    Then config validation fails with an error indicating CDC is not supported for that source
