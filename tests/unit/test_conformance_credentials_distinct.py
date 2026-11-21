# Copyright (c) 2026 Kenneth Stott
# Canary: 8a4c2e7b-6d1f-4b3a-8e5c-0f9d7a6b3c21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A CONFORMANCE INSTANCE'S CREDENTIALS ARE NO NAME ITS MEASURE READS. A credential's value is
redacted in the error text a client is told (REQ-2087), and a measure compares that text with
PostgreSQL's and classifies a refusal by it. A password equal to the fixture's schema made every
error naming the schema differ (Conformance (functions) run 43926612814: 20 base calls skipped by
design became errors, and one exact refusal a wrong answer)."""

# Requirements: REQ-2087, REQ-2167

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "conformance"))

import function_support as support  # noqa: E402
import pgwire_catalog_types as catalog  # noqa: E402
import pgwire_function_probe as function_probe  # noqa: E402
from provisa.security.redaction import MIN_SECRET_LENGTH  # noqa: E402

WORKFLOWS = sorted((REPO / ".github" / "workflows").glob("conformance-*.yml"))
#: The names a measure's statements and PostgreSQL's errors hold.
NAMES = (
    catalog.FIXTURE_SCHEMA,
    function_probe.PROBE_SEQUENCE,
    function_probe.PROBE_ENUM,
    support.REPLICATION_SLOT,
    support.FIXTURE_DOMAIN,
)


def _environments(node: Any) -> list[dict[str, Any]]:
    """Every ``env`` mapping in a workflow: its own, each job's, each service's, each step's."""
    found: list[dict[str, Any]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "env" and isinstance(value, dict):
                found.append(value)
            found.extend(_environments(value))
    elif isinstance(node, list):
        for item in node:
            found.extend(_environments(item))
    return found


def _credentials(path: Path) -> dict[str, str]:
    return {
        name: str(value)
        for env in _environments(yaml.safe_load(path.read_text()))
        for name, value in env.items()
        if name.endswith(("PASSWORD", "SECRET")) and "${{" not in str(value)
    }


def test_every_conformance_workflow_is_read():
    assert len(WORKFLOWS) >= 7
    assert all(_credentials(path) for path in WORKFLOWS)


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_no_credential_is_text_of_a_name_the_measure_reads(path: Path):
    for variable, value in _credentials(path).items():
        assert len(value) >= MIN_SECRET_LENGTH, variable
        for name in NAMES:
            assert value not in name, f"{path.name}: {variable} is text of {name!r}"


def test_the_postgresql_service_takes_the_password_the_instance_is_given():
    for path in WORKFLOWS:
        held = _credentials(path)
        assert held["POSTGRES_PASSWORD"] == held["CONFORMANCE_PG_PASSWORD"], path.name
        # The PostgreSQL leg's engine is that server, addressed with the same password.
        for line in path.read_text().splitlines():
            if "engine-url: postgresql://" in line:
                assert f":{held['CONFORMANCE_PG_PASSWORD']}@" in line, path.name
