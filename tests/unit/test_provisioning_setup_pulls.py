# Copyright (c) 2026 Kenneth Stott
# Canary: 0b43f969-a3dd-4d44-88f5-6e91bc51714a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The provisioning demo's setup step (setup_pulls.py) runs its consumer as the run compose does:
told its topics' suffix, the broker's namespace (PROVISIONING_KAFKA_NS). The bundle build runs the
step directly, where no compose file hands the consumer TOPIC_SUFFIX (Demo bundles run
39467273812: KeyError: 'TOPIC_SUFFIX')."""

import importlib.util
from pathlib import Path

import pytest

_SETUP = Path(__file__).resolve().parents[2] / "demo/named/provisioning/setup_pulls.py"


@pytest.fixture
def setup_pulls(monkeypatch):
    monkeypatch.setenv("PROVISA_URL", "http://127.0.0.1:1")
    spec = importlib.util.spec_from_file_location("provisioning_setup_pulls", _SETUP)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "_dataset_built", lambda client: True)
    monkeypatch.setattr(module, "_sql", lambda client, sql: [])
    return module


@pytest.mark.parametrize("namespace", ["", "_provisioning_1a2b3c4d"])
def test_the_consumer_is_told_its_topics_suffix(setup_pulls, monkeypatch, namespace):
    monkeypatch.setenv("PROVISIONING_KAFKA_NS", namespace)
    ran: list[dict] = []
    monkeypatch.setattr(
        setup_pulls.subprocess, "run", lambda cmd, env, check, timeout: ran.append(env)
    )
    setup_pulls.main()
    [env] = ran
    assert env["TOPIC_SUFFIX"] == namespace


def test_a_run_given_no_namespace_stops_naming_it(setup_pulls, monkeypatch):
    monkeypatch.delenv("PROVISIONING_KAFKA_NS", raising=False)
    monkeypatch.setattr(setup_pulls.subprocess, "run", lambda *a, **k: None)
    with pytest.raises(KeyError, match="PROVISIONING_KAFKA_NS"):
        setup_pulls.main()
