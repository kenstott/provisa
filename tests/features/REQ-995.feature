# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-995 — Federation Engine Capabilities
  # Microsoft Fabric federation engine attaches Apache Iceberg data LIVE via OneLake Delta metadata virtualization (OPENROWS…

  Scenario: REQ-995 default behaviour
    Given an Iceberg table registered in OneLake on Microsoft Fabric
    When a live attachment is configured for Fabric engine with _FABRIC_ONLY_FORMAT constraint
    Then the table is queryable LIVE via OPENROWSET FORMAT='DELTA' without materialization

    Given the same Iceberg table targeted for Azure Synapse serverless
    When attachment is attempted without OneLake virtualization
    Then the table is materialized as a REPLICA instead of attached live
