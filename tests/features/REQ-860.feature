# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-860 — Freshness Gating
  # Freshness gating is now available at the Source level (provisa/core/models.py), not just on MV. A source can declare a f…

  Scenario: REQ-860 default behaviour
    Given a source S with freshness_gate=true declaring change_signal=ttl and cache_ttl When a query reads a table from S whose last residency landing is older than cache_ttl Then S is scheduled into the execution plan residency prep and refreshed before the read, while a source within its TTL is read with no refresh
