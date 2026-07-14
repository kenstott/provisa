# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-954 — Replica Strategy
  # To materialize files/sharepoint/splunk sources, Provisa starts the connector's bundled Calcite pgwire server (pgwire-fil…

  Scenario: REQ-954 default behaviour
    Given a files/sharepoint/splunk source registered in federation config
    When the replica strategy selects _CONNECTOR_PGWIRE_REPLICA
    Then Provisa launches the source's bundled pgwire server and connects as generic PostgreSQL
    And SELECTs from the connector schema (files.*, sharepoint.*, splunk.* tables)
    And lands rows into the materialize store as the replica
