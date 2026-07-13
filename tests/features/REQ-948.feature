# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-948 — Driver Management
  # Driver availability is per-connector data, not new machinery: Connector.runtime_deps entries are tagged by who provides…

  Scenario: REQ-948 default behaviour
    Given a connector runtime_deps entry tagged provider="bundled"
    When the source list is rendered
    Then the source is enabled because Provisa ships and relocates its driver

    Given a runtime_deps entry tagged provider="operator" whose driver is not installed
    When probe() reports it unavailable
    Then the source appears in the dropdown but disabled with its operator remediation
