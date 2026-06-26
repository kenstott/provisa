# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-563 — Installer & Packaging
  # The AppImage first-launch in `--non-interactive` mode installs a systemd unit (`/etc/systemd/system/provisa.service`) so…

  Scenario: REQ-563 default behaviour
    Given AppImage first-launch runs with --non-interactive
    When the first-launch sequence completes
    Then a systemd unit is installed for boot autostart and credentials are written to ~/.provisa/config.yaml
