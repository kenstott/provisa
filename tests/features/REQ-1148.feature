# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1148 — Freshness / Change Detection
  # A new freshness PROBE transport whose opaque token comes from a zero-byte SENTINEL marker rather than from scanning the…

  Scenario: REQ-1148 default behaviour
    Given a source with sentinel_path pointing at a zero-byte marker and change_signal=probe, probe_type=hash
    When the poll node evaluates and the marker's mtime/size (or ETag/Last-Modified) is unchanged
    Then the freshness token equals the stored baseline and no data is pulled
    And when the producer touches the marker the token changes, so the node re-pulls from the source
    And a missing/unreachable marker yields a None token, degrading the node to its TTL cadence
