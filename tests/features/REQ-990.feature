# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-990 — Materialization Writes
  # Materialization-store writes MUST use the target store's columnar or bulk-COPY ingest path wherever available, instead o…

  Scenario: REQ-990 default behaviour
    Given a materialization target declaring bulk-COPY/columnar ingest support
    When a batch at or above the bulk threshold is landed
    Then the bulk/columnar ingest path is used, never row-by-row INSERT

    Given a target without bulk support or a tiny write
    When the batch is landed
    Then row INSERT is used, capability-gated and explicit, never a silent fallback
