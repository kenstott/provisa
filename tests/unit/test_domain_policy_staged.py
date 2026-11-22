# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e61d4-58a2-4f07-9c1e-7d20a4b8f6c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A runtime build's domain-policy window (domain_policy.staged, REQ-1266, REQ-2006).

A build sets the deployment's naming, reads its model under it, then sets the org's own
(app._build_org_runtime). The policy is keyed by the org, so two builds of one org's environments
at once (a sandbox's open) each read the other's in-between value, and the open built them one at
a time. Inside ``staged`` each build reads its own window, and the org's policy is written once, at
the window's normal end. A dry run's window (``trial``) never writes it."""

# Requirements: REQ-1266, REQ-2006, REQ-2222

from __future__ import annotations

import ast
import asyncio
import contextvars
import threading
from pathlib import Path

import pytest

from provisa.core import domain_policy
from provisa.core.request_context import current_org, reset_current_org, set_current_org

ORG = "acme"
DEPLOYMENT = (None, "default")
SEED = (False, "seeded")
OWN = (True, "shop")


@pytest.fixture(autouse=True)
def _scoped():
    domain_policy.set_scope_resolver(current_org.get)
    yield
    domain_policy.reset_all()
    domain_policy.set_scope_resolver(None)


def _org_policy(org: str = ORG) -> tuple[bool | None, str]:
    token = set_current_org(org)
    try:
        return domain_policy.snapshot()
    finally:
        reset_current_org(token)


def test_two_builds_of_one_org_each_read_their_own_window():
    """Dev's build sets the org's own naming while test's is still reading under the seed's:
    test's read is the seed's, and the org ends with its own."""
    meet = threading.Barrier(2, timeout=5)
    dev_done = threading.Event()
    read: dict[str, tuple[bool | None, str]] = {}

    def _build(env: str) -> None:
        token = set_current_org(ORG)
        try:
            with domain_policy.staged():
                domain_policy.configure(*SEED)
                meet.wait()
                if env == "test":
                    assert dev_done.wait(5)
                read[env] = domain_policy.snapshot()
                domain_policy.configure(*OWN)
            if env == "dev":
                dev_done.set()
        finally:
            reset_current_org(token)

    lines = [threading.Thread(target=_build, args=(env,)) for env in ("dev", "test")]
    for line in lines:
        line.start()
    for line in lines:
        line.join(10)

    assert read == {"dev": SEED, "test": SEED}
    assert _org_policy() == OWN


def test_the_orgs_requests_read_its_settled_policy_during_a_build():
    token = set_current_org(ORG)
    try:
        domain_policy.configure(*OWN)
    finally:
        reset_current_org(token)
    seen: list[tuple[bool | None, str]] = []

    token = set_current_org(ORG)
    try:
        with domain_policy.staged():
            domain_policy.configure(*SEED)
            # A request of the org, in a context of its own.
            contextvars.Context().run(lambda: seen.append(_org_policy()))
            assert domain_policy.snapshot() == SEED
    finally:
        reset_current_org(token)

    assert seen == [OWN]


def test_a_window_that_configures_nothing_writes_nothing():
    """The org keeps inheriting the deployment's policy, and follows it when it changes."""
    token = set_current_org(ORG)
    try:
        with domain_policy.staged():
            assert domain_policy.snapshot() == DEPLOYMENT
    finally:
        reset_current_org(token)
    unbound = current_org.set(None)  # the deployment's: no org bound
    try:
        domain_policy.configure(*SEED)
    finally:
        current_org.reset(unbound)

    assert _org_policy() == SEED


def test_a_failed_build_leaves_the_orgs_policy_as_it_was():
    token = set_current_org(ORG)
    try:
        domain_policy.configure(*OWN)
        with pytest.raises(RuntimeError), domain_policy.staged():
            domain_policy.configure(*SEED)
            raise RuntimeError("the build failed")
    finally:
        reset_current_org(token)

    assert _org_policy() == OWN


def test_a_dry_runs_window_never_writes_the_orgs_policy():
    token = set_current_org(ORG)
    try:
        with domain_policy.trial():
            domain_policy.configure(*SEED)
            assert domain_policy.snapshot() == SEED
    finally:
        reset_current_org(token)

    assert _org_policy() == DEPLOYMENT


def test_work_started_in_the_window_reads_it_and_then_the_orgs():
    """A task started inside the window carries a copy of the context: it reads the window's
    policy while the window is open, the one configured after it started included, and the org's
    once the window has closed, not the window's last value kept for good."""

    async def _main() -> list[tuple[bool | None, str]]:
        seen: list[tuple[bool | None, str]] = []
        step = asyncio.Event()
        closed = asyncio.Event()

        async def _work() -> None:
            await step.wait()
            seen.append(domain_policy.snapshot())
            await closed.wait()
            seen.append(domain_policy.snapshot())

        token = set_current_org(ORG)
        try:
            domain_policy.configure(*OWN)
            with domain_policy.trial():
                domain_policy.configure(*DEPLOYMENT)
                task = asyncio.create_task(_work())
                domain_policy.configure(*SEED)
                step.set()
                await asyncio.sleep(0)
                await asyncio.sleep(0)
            closed.set()
            await task
        finally:
            reset_current_org(token)
        return seen

    assert asyncio.run(_main()) == [SEED, OWN]


def test_another_orgs_work_inside_the_window_reads_its_own_policy():
    token = set_current_org("other")
    try:
        domain_policy.configure(*OWN)
    finally:
        reset_current_org(token)

    token = set_current_org(ORG)
    try:
        with domain_policy.staged():
            domain_policy.configure(*SEED)
            inner = set_current_org("other")
            try:
                assert domain_policy.snapshot() == OWN
            finally:
                reset_current_org(inner)
    finally:
        reset_current_org(token)

    assert _org_policy() == SEED
    assert _org_policy("other") == OWN


_APP = Path(__file__).resolve().parents[2] / "provisa/api/app.py"


def _build_function() -> ast.AsyncFunctionDef:
    for node in ast.walk(ast.parse(_APP.read_text())):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_build_org_runtime":
            return node
    raise AssertionError(f"_build_org_runtime not found in {_APP}")


def _called(call: ast.Call) -> str:
    func = call.func
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        return f"{func.value.id}.{func.attr}"
    return func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")


def _is_staged(item: ast.withitem) -> bool:
    call = item.context_expr
    return isinstance(call, ast.Call) and _called(call) == "domain_policy.staged"


def test_the_org_runtime_build_sets_and_reads_its_policy_in_its_own_window():
    """app._build_org_runtime configures the policy, reads its model and compiles its schemas
    inside one domain_policy.staged block: the two builds of a sandbox's environments run at once
    (demo_sandbox._environments) and take no turn."""
    build = _build_function()
    windows = [
        node
        for node in ast.walk(build)
        if isinstance(node, ast.With) and any(_is_staged(item) for item in node.items)
    ]
    assert len(windows) == 1, "the build has one domain-policy window"
    inside = {id(node) for node in ast.walk(windows[0])}
    calls = [node for node in ast.walk(build) if isinstance(node, ast.Call)]
    for name in (
        "domain_policy.configure",
        "store_config",
        "_rebuild_schemas",
        "_install_cloned_env_schema",
    ):
        named = [call for call in calls if _called(call) == name]
        assert named, f"_build_org_runtime no longer calls {name}"
        outside = [call.lineno for call in named if id(call) not in inside]
        assert not outside, f"{name} outside the build's window at app.py lines {outside}"
