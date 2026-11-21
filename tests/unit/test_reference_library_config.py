# Copyright (c) 2026 Kenneth Stott
# Canary: 3b93cf3b-8a69-4bd2-8937-f23a44135ac9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2185 (ONE RETRIEVAL, NO SETTING): the reference library has one retrieval, so nothing
declares, stores or serves a retrieval setting."""

import inspect
from pathlib import Path

import pytest

from provisa.api.admin import reference_library_router as router
from provisa.core import models, org_settings
from provisa.reference_library import retrieve

ROOT = Path(__file__).resolve().parents[2]


def test_a_configuration_declares_no_library_retrieval():
    assert "reference_library" not in models.ProvisaConfig.model_fields
    assert not hasattr(models, "ReferenceLibraryConfig")


def test_an_org_holds_no_library_retrieval_setting():
    assert "reference_library" not in org_settings.ORG_OVERRIDABLE_KEYS


async def test_an_org_cannot_be_given_a_library_retrieval_setting():
    with pytest.raises(ValueError, match="not org-overridable: reference_library"):
        await org_settings.write_org_overrides(
            None,  # type: ignore[arg-type]  # refused before the store is reached
            {"reference_library": {"retrieval": "graph"}},
            updated_by="admin",
        )


def test_retrieval_takes_no_choice_of_retrieval():
    assert "retrieval" not in inspect.signature(retrieve.retrieve_with_neighbours).parameters
    for gone in ("PASSAGE", "GRAPH", "RETRIEVALS", "SETTING", "retrieval_setting"):
        assert not hasattr(retrieve, gone), gone


def test_the_admin_api_serves_no_library_settings():
    paths = {getattr(r, "path", "") for r in router.router.routes}
    assert "/admin/reference-library/settings" not in paths
    assert not hasattr(router, "get_settings") and not hasattr(router, "put_settings")


def test_no_demo_declares_a_library_retrieval():
    declared = [
        p.relative_to(ROOT).as_posix()
        for p in sorted((ROOT / "demo").rglob("*.yaml"))
        if "reference_library:" in p.read_text(encoding="utf-8")
    ]
    assert declared == []
