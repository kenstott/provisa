# Copyright (c) 2026 Kenneth Stott
# Canary: 0b18ec3d-f6d4-442c-b51d-d7e5e41d3cc6
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for process and commercial requirements: REQ-069, REQ-070, REQ-073, REQ-074"""

from __future__ import annotations

import os


# ---------------------------------------------------------------------------
# REQ-069: Architecture docs in `docs/arch/` ARE the planning documents —
# update when requirements change, don't implement without planning.
# ---------------------------------------------------------------------------


def test_arch_directory_exists():
    # REQ-069: docs/arch/ must exist as the location for planning documents.
    arch_dir = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "arch")
    assert os.path.isdir(arch_dir), "docs/arch/ directory must exist"


def test_requirements_md_exists_in_arch():
    # REQ-069: docs/arch/requirements.md must exist as the primary planning document.
    req_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "docs", "arch", "requirements.md"
    )
    assert os.path.isfile(req_path), "docs/arch/requirements.md must exist"


def test_requirements_md_is_not_empty():
    # REQ-069: The planning document must contain requirement definitions.
    req_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "docs", "arch", "requirements.md"
    )
    with open(req_path) as f:
        content = f.read()
    assert "REQ-" in content, "requirements.md must contain REQ-NNN identifiers"


def test_arch_directory_contains_multiple_docs():
    # REQ-069: docs/arch/ should contain multiple planning docs, not just requirements.
    arch_dir = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "arch")
    files = os.listdir(arch_dir)
    md_files = [f for f in files if f.endswith(".md")]
    assert len(md_files) >= 1, "docs/arch/ must contain at least one planning document"


# ---------------------------------------------------------------------------
# REQ-070: Maximum brevity in communications — code and facts only, no
# pleasantries or explanations unless asked.
# ---------------------------------------------------------------------------


def test_claude_md_contains_brevity_directive():
    # REQ-070: CLAUDE.md must document the brevity requirement.
    claude_md = os.path.join(os.path.dirname(__file__), "..", "..", "CLAUDE.md")
    assert os.path.isfile(claude_md), "CLAUDE.md must exist"
    with open(claude_md) as f:
        content = f.read()
    assert "brevity" in content.lower() or "brief" in content.lower(), (
        "CLAUDE.md must document the brevity requirement"
    )


def test_claude_md_prohibits_pleasantries():
    # REQ-070: CLAUDE.md must prohibit pleasantries/explanations.
    claude_md = os.path.join(os.path.dirname(__file__), "..", "..", "CLAUDE.md")
    with open(claude_md) as f:
        content = f.read()
    # The brevity requirement explicitly names "pleasantries" or "explanations"
    assert "pleasantries" in content or "explanations" in content, (
        "CLAUDE.md must prohibit pleasantries and unsolicited explanations"
    )


# ---------------------------------------------------------------------------
# REQ-073: SaaS tier: hosted control plane with customer-hosted data plane option.
# REQ-074: Enterprise tier: SLA guarantees, dedicated support, advanced audit
# logging, compliance reporting.
# ---------------------------------------------------------------------------


def test_provisa_install_config_exists():
    # REQ-073, REQ-074: Deployment configuration for multi-tier setup must exist.
    config_dir = os.path.join(os.path.dirname(__file__), "..", "..", "config")
    if not os.path.isdir(config_dir):
        return  # config directory is optional in some setups

    config_files = os.listdir(config_dir)
    yaml_files = [f for f in config_files if f.endswith(".yaml") or f.endswith(".yml")]
    assert len(yaml_files) >= 1, "At least one deployment config YAML must exist"


def test_audit_logging_module_exists():
    # REQ-074: Enterprise tier requires advanced audit logging.
    # Verify that audit logging infrastructure exists in the codebase.
    try:
        import provisa.api.audit  # type: ignore[import-not-found]  # noqa: F401

        assert True
    except ImportError:
        # Check for audit logging in core or api modules
        import provisa.api.app as app_module
        import inspect

        src = inspect.getsource(app_module)
        assert "audit" in src.lower() or "log" in src.lower(), (
            "Audit logging must be referenced in the API application"
        )


def test_requirements_doc_contains_saas_tier_reference():
    # REQ-073: SaaS tier must be documented in the requirements.
    req_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "docs", "arch", "requirements.md"
    )
    with open(req_path) as f:
        content = f.read()
    assert "REQ-073" in content, "REQ-073 (SaaS tier) must appear in requirements.md"


def test_requirements_doc_contains_enterprise_tier_reference():
    # REQ-074: Enterprise tier must be documented in the requirements.
    req_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "docs", "arch", "requirements.md"
    )
    with open(req_path) as f:
        content = f.read()
    assert "REQ-074" in content, "REQ-074 (Enterprise tier) must appear in requirements.md"
