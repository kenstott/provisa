# Copyright (c) 2026 Kenneth Stott
# Canary: 66b06647-4516-4638-9a3e-2a50951d4d05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1368: the published metadata-export documentation page, checked against the code it
describes (provider registry, export config model) and the mkdocs navigation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.api.metadata_export import registered_providers
from provisa.core.models import MetadataExportConfig

_ROOT = Path(__file__).resolve().parents[2]
DOC = _ROOT / "docs" / "metadata-export.md"
NAV_SECTION = "Security & Governance"


class _NavLoader(yaml.SafeLoader):
    pass


# mkdocs.yml carries tags the safe loader rejects (!!python/name: for extensions); only the nav is
# read here, so unknown tags are tolerated without executing anything.
_NavLoader.add_multi_constructor("tag:yaml.org,2002:python/name", lambda *_: None)
_NavLoader.add_multi_constructor("!", lambda *_: None)


def _section_targets(node, section: str) -> list[str]:
    if isinstance(node, list):
        return [t for item in node for t in _section_targets(item, section)]
    if isinstance(node, dict):
        out: list[str] = []
        for key, value in node.items():
            if key == section and isinstance(value, list):
                out.extend(v for item in value if isinstance(item, dict) for v in item.values())
            out.extend(_section_targets(value, section))
        return out
    return []


@pytest.fixture(scope="module")
def text() -> str:
    return DOC.read_text()


def test_the_page_is_navigable_under_security_and_governance() -> None:
    mkdocs = yaml.load((_ROOT / "mkdocs.yml").read_text(), Loader=_NavLoader)
    assert "metadata-export.md" in _section_targets(mkdocs.get("nav", []), NAV_SECTION)
    assert "metadata-export.md" not in (mkdocs.get("exclude_docs") or "")


def test_the_page_states_publication_is_outbound_only(text: str) -> None:
    lowered = text.lower()
    assert "outbound only" in lowered
    assert "reads an external catalog back" in lowered


def test_the_page_names_every_registered_provider(text: str) -> None:
    for name in registered_providers():
        assert f"`{name}`" in text, name


def test_the_page_documents_every_export_config_setting(text: str) -> None:
    for name in MetadataExportConfig.model_fields:
        assert f"`{name}`" in text, name


def test_the_page_describes_the_event_driven_and_scheduled_sync_model(text: str) -> None:
    for claim in ("Change-driven", "Scheduled reconcile", "On demand"):
        assert claim in text, claim
