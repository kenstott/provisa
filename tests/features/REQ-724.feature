# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-724 — Splunk Connector
  # Splunk connector supports optional app and datamodel-filter properties from source.database and source.mapping, plus dis…

  Scenario: REQ-724 default behaviour
    Given a source with database=search_app and mapping.disable_ssl_validation=true
    When the connector properties are built
    Then props contains app=search_app and disable-ssl-validation=true
