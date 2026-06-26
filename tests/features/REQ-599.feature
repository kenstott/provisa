# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-599 — GraphQL Remote Schema Connector (REQ-307–313)
  # For all remote schema source types (GraphQL, gRPC, OpenAPI), required input parameters that are not already response fie…

  Scenario: REQ-599 default behaviour
    Given a remote schema source with required input parameters not in the response fields
    When the source is registered
    Then those parameters become _nf_-prefixed native filter columns with the appropriate native_filter_type
