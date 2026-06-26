# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-329 — gRPC Remote Schema Connector (REQ-322–329)
  # Proto schema refresh is triggered on demand via an admin mutation. On refresh, Provisa re-parses the proto, updates virt…

  Scenario: REQ-329 default behaviour
    Given a gRPC source whose proto has changed
    When a steward triggers the proto refresh admin mutation
    Then registrations are updated, proto import paths are reused, and RLS/masking rules are preserved
