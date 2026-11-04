# Copyright (c) 2026 Kenneth Stott
# Canary: 3b93cf3b-8a69-4bd2-8937-f23a44135ac9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2185: a configuration declares the reference library's retrieval setting."""

import pytest
from pydantic import ValidationError

from provisa.core.models import ProvisaConfig, ReferenceLibraryConfig

_ROLES = [{"id": "admin", "description": "admin", "capabilities": [], "domain_access": ["*"]}]


def test_the_retrieval_setting_validates_and_defaults_to_passage():
    assert ReferenceLibraryConfig().retrieval == "passage"
    assert ReferenceLibraryConfig(retrieval="graph").retrieval == "graph"


def test_an_invalid_retrieval_is_a_validation_error():
    with pytest.raises(ValidationError):
        ReferenceLibraryConfig(retrieval="vector")


def test_provisa_config_declares_reference_library():
    config = ProvisaConfig.model_validate(
        {"roles": _ROLES, "reference_library": {"retrieval": "graph"}}
    )
    assert config.reference_library.retrieval == "graph"
    assert "reference_library" in config.model_fields_set
    assert (
        "reference_library" not in ProvisaConfig.model_validate({"roles": _ROLES}).model_fields_set
    )
    with pytest.raises(ValidationError):
        ProvisaConfig.model_validate({"roles": _ROLES, "reference_library": {"retrieval": "x"}})
