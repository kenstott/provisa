# Copyright (c) 2026 Kenneth Stott
# Canary: a3b4c5d6-e7f8-9a0b-c1d2-e3f4a5b6c7d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pytest configuration for BDD step definition files.

Applies the ``bdd`` marker to every pytest-bdd scenario collected from
``steps_*.py`` modules so that ``-m bdd`` selects them.
"""

import pytest


def pytest_collection_modifyitems(items: list) -> None:
    """Mark every item from a steps_*.py module with the ``bdd`` marker."""
    bdd_mark = pytest.mark.bdd
    for item in items:
        module = getattr(item, "module", None)
        if module is None:
            continue
        name = getattr(module, "__name__", "") or ""
        # Match tests.steps.steps_* or just steps_*
        if name.startswith("tests.steps.steps_") or name.startswith("steps_"):
            item.add_marker(bdd_mark, append=False)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "unbound: run with no org bound -- for the entrypoints that bind one themselves and for "
        "the refusal of work bound to none (REQ-1266)",
    )


@pytest.fixture(autouse=True)
def _deployment_org_bound(request: pytest.FixtureRequest):
    """Step scenarios run as work for the deployment's own org (REQ-1266), as the unit and
    integration harnesses do: a step that drives a core path directly (a mutation resolver, the hot
    table manager, the store) stands in for the boot or the request that binds it."""
    if request.node.get_closest_marker("unbound") is not None:
        yield
        return
    from tests.conftest import as_deployment_org

    with as_deployment_org():
        yield
        if "monkeypatch" in request.fixturenames:
            # A routed AppState attribute monkeypatch replaced is put back while the org is bound.
            request.getfixturevalue("monkeypatch").undo()
