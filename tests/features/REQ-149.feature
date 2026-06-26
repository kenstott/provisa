# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-149 — Kafka Sources
  # Discriminator filter for multi-type topics — multiple table configs on the same physical topic, each filtered by a discr…

  Scenario: REQ-149 default behaviour
    Given multiple table configs registered against the same Kafka topic with different discriminator values
    When each table is queried
    Then only messages matching that table's discriminator field/value are returned
