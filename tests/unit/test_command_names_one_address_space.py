# Copyright (c) 2026 Kenneth Stott
# Canary: 86d3a36b-4c0f-4428-ba54-45932c58b19b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1385: functions and webhooks are both commands, named in one address space. A config that
gives a function and a webhook the same name is refused when it loads, naming the command, instead
of failing later when the environment's files are written."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from provisa.core.models import ProvisaConfig

_FUNCTION = {
    "name": "add_pet",
    "source_id": "",
    "function_name": "add_pet",
    "returns": "",
    "domain_id": "pet-store",
}
_WEBHOOK = {
    "name": "add_pet",
    "url": "http://petstore.example/pet",
    "domain_id": "pet-store",
}


def _config(functions: list[dict], webhooks: list[dict]) -> dict:
    return {
        "sources": [],
        "tables": [],
        "domains": [{"id": "pet-store"}],
        "roles": [],
        "functions": functions,
        "webhooks": webhooks,
    }


def test_a_function_and_a_webhook_with_one_name_are_refused_naming_it():
    with pytest.raises(ValidationError, match="add_pet"):
        ProvisaConfig.model_validate(_config([_FUNCTION], [_WEBHOOK]))


def test_distinct_command_names_load():
    webhook = dict(_WEBHOOK, name="petstore_add_pet")
    ProvisaConfig.model_validate(_config([_FUNCTION], [webhook]))
