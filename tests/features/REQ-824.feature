# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-824 — Live Delivery Mechanisms
  # Source-level CDC transport configuration. Debezium/Kafka delta-transport (bootstrap_servers, topic_prefix, schema_regist…

  Scenario: REQ-824 default behaviour
    Given a MySQL source with a source-level cdc block (bootstrap_servers, topic_prefix)
    When a table from that source sets live.delivery=cdc
    Then validation passes and the runtime routes the subscription to the Debezium provider using the source's transport

    Given a MySQL source WITHOUT a cdc block
    When a table sets live.delivery=cdc
    Then config validation rejects it (no push mechanism)

    Given a warehouse source (e.g. snowflake)
    When a cdc block is set on it
    Then validation rejects the cdc block as unsupported for that source type
