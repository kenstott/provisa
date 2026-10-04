# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e1b94-52d8-4f0a-9a6e-d18b2f4c7e05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""config/provisa.yaml is the config every in-process app in the test suite boots on
(tests/conftest.py sets it as the session's PROVISA_CONFIG). It declared an ``r2-orders`` source
whose credentials were ``${env:AWS_ACCESS_KEY_ID}`` / ``${env:AWS_SECRET_ACCESS_KEY}``, which only
the maintainer's .env supplies; on a CI runner without them every such boot died in
``parse_config_dict`` with ``KeyError: 'Environment variable not set: AWS_ACCESS_KEY_ID'``. The
suite's default config must parse with no external-provider credential in the environment: a test
that needs a cloud source registers its own."""

import os
from pathlib import Path

import pytest
import yaml

from provisa.core.config_loader import parse_config_dict
from tests.env_creds import _CRED_PREFIXES

pytestmark = pytest.mark.unit

_SUITE_DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "provisa.yaml"


def test_the_suite_default_config_parses_without_external_provider_credentials(monkeypatch):
    for name in [n for n in os.environ if n.startswith(_CRED_PREFIXES)]:
        monkeypatch.delenv(name)

    parse_config_dict(yaml.safe_load(_SUITE_DEFAULT_CONFIG.read_text()))
