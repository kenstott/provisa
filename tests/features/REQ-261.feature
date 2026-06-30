# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-261 — Subscriptions
  # Debezium CDC subscription provider for non-PG RDBMS sources (MySQL, MariaDB, SQL Server, Oracle). Debezium captures chan…

  Scenario: REQ-261 default behaviour
    Given a MySQL source is connected via Debezium and Kafka
    When a row is inserted, updated, or deleted in MySQL
    Then the change is captured by Debezium, published to Kafka, consumed by Provisa, and streamed
      as an SSE event to subscribers
