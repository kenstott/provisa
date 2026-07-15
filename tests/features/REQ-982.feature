# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-982 — Live Data & Events
  # probe_type is the event loop's input-side change-detection axis, orthogonal to the change_signal cadence (ttl | probe |…

  Scenario: REQ-982 default behaviour
    Given a postgresql source table with change_signal=probe and probe_type=watermark and watermark_column=updated_at
    When the poll fires and MAX(updated_at) exceeds the stored cursor
    Then the node posts an append event and lands only the rows past the cursor
    Given an openapi source with change_signal=probe and probe_type=hash
    When the poll fires and the endpoint returns the same ETag as stored
    Then the node does not fetch, land, or re-post
    Given a csv file source configured with probe_type=watermark
    When the config is parsed
    Then validation rejects it because file sources do not support the watermark probe type
