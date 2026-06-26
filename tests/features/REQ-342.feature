# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-342 — Phase AT — WebSocket & RSS Sources (REQ-338–344)
  # `rss` is a supported source type. Polls an RSS 2.0 or Atom feed URL at a configurable interval (default: 300 s = 5 min).…

  Scenario: REQ-342 default behaviour
    Given an RSS source polling an Atom feed every 300 seconds
    When new items are published after the last-seen watermark
    Then only those items are emitted as ChangeEvents with operation="insert"
