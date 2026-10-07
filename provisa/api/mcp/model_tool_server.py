# Copyright (c) 2026 Kenneth Stott
# Canary: c4e09b7a-61d2-4f3e-8a5b-2f9d16e7c083
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The profiler, fakes, synthetic-dataset and config-export tools on the MCP server (REQ-1857:
every capability-gated tool Polly has is on the MCP server too).

Each wrapper resolves the caller's role the way every MCP tool does and hands
:mod:`provisa.api.mcp.model_tools` the request-shaped shim ``server._capability_request`` builds,
so the admin route behind the tool reads the caller's identity and org from it. Its description is
the one Polly reads (:data:`provisa.api.mcp.model_tool_specs.SPECS`).
"""

# Requirements: REQ-1934, REQ-1494, REQ-1939, REQ-1919, REQ-1857

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from provisa.api.mcp import model_tools as mt
from provisa.api.mcp.model_tool_specs import SPECS

_DESCRIPTIONS = {s["name"]: s["description"] for s in SPECS}


def register(
    tool: Callable[[Any], Any],
    role_of: Callable[[str | None], str],
    request_for: Callable[[str], Any],
    state: Any,
) -> None:
    """Register every model tool through ``tool`` (server._tool)."""

    def described(fn: Any) -> Any:
        fn.__doc__ = _DESCRIPTIONS[fn.__name__]
        return tool(fn)

    def ctx(role: str | None) -> tuple[Any, str, Any]:
        resolved = role_of(role)
        return state, resolved, request_for(resolved)

    @described
    async def find_table_id(domain: str, table: str, role: str | None = None) -> list[dict]:
        return await mt.find_table_id(*ctx(role), domain, table)

    @described
    async def list_profilers(role: str | None = None) -> list[dict]:
        return await mt.list_profilers(*ctx(role))

    @described
    async def set_table_profiler(
        table_id: int, profiler_source_id: str | None, role: str | None = None
    ) -> dict:
        return await mt.set_table_profiler(*ctx(role), table_id, profiler_source_id)

    @described
    async def run_profiler(source_id: str, role: str | None = None) -> list[dict]:
        return await mt.run_profiler(*ctx(role), source_id)

    @described
    async def run_table_profile(table_id: int, role: str | None = None) -> dict:
        return await mt.run_table_profile(*ctx(role), table_id)

    @described
    async def list_profile_runs(table_id: int, role: str | None = None) -> list[dict]:
        return await mt.list_profile_runs(*ctx(role), table_id)

    @described
    async def get_table_profile(
        table_id: int,
        run_id: str | None = None,
        kinds: list[str] | None = None,
        role: str | None = None,
    ) -> dict:
        return await mt.get_table_profile(*ctx(role), table_id, run_id, kinds)

    @described
    async def list_profile_constraints(table_id: int, role: str | None = None) -> dict:
        return await mt.list_profile_constraints(*ctx(role), table_id)

    @described
    async def decide_profile_constraint(
        table_id: int,
        kind: str,
        column: str,
        status: str,
        other_column: str | None = None,
        definition: dict | None = None,
        role: str | None = None,
    ) -> dict:
        return await mt.decide_profile_constraint(
            *ctx(role), table_id, kind, column, status, other_column, definition
        )

    @described
    async def forget_profile_constraint(
        table_id: int, constraint_id: str, role: str | None = None
    ) -> dict:
        return await mt.forget_profile_constraint(*ctx(role), table_id, constraint_id)

    @described
    async def export_profile_constraint(
        table_id: int,
        constraint_id: str,
        checker_table_id: int | None = None,
        role: str | None = None,
    ) -> dict:
        return await mt.export_profile_constraint(
            *ctx(role), table_id, constraint_id, checker_table_id
        )

    @described
    async def list_profile_checks(table_id: int, role: str | None = None) -> dict:
        return await mt.list_profile_checks(*ctx(role), table_id)

    @described
    async def create_drift_check(
        table_id: int, checker_table_id: int | None = None, role: str | None = None
    ) -> dict:
        return await mt.create_drift_check(*ctx(role), table_id, checker_table_id)

    @described
    async def create_expectation_check(
        table_id: int,
        expectations_table_id: int,
        checker_table_id: int | None = None,
        role: str | None = None,
    ) -> dict:
        return await mt.create_expectation_check(
            *ctx(role), table_id, expectations_table_id, checker_table_id
        )

    @described
    async def list_fake_kinds(role: str | None = None) -> dict:
        return await mt.list_fake_kinds(*ctx(role))

    @described
    async def get_table_fakes(table_id: int, role: str | None = None) -> dict:
        return await mt.get_table_fakes(*ctx(role), table_id)

    @described
    async def set_column_fake(
        table_id: int,
        column: str,
        fake: str | None = None,
        synthetic_rule: str | None = None,
        stable: bool | None = None,
        role: str | None = None,
    ) -> dict:
        return await mt.set_column_fake(
            *ctx(role), table_id, column, fake=fake, synthetic_rule=synthetic_rule, stable=stable
        )

    @described
    async def propose_fakes(table_id: int, role: str | None = None) -> dict:
        return await mt.propose_fakes(*ctx(role), table_id)

    @described
    async def list_synthetic_datasets(role: str | None = None) -> list[dict]:
        return await mt.list_synthetic_datasets(*ctx(role))

    @described
    async def list_synthetic_profile_runs(env: str, role: str | None = None) -> list[dict]:
        return await mt.list_synthetic_profile_runs(*ctx(role), env)

    @described
    async def define_synthetic_dataset(
        dataset_id: str,
        seed: int,
        scale: float,
        tables: list[dict],
        fanoutConditions: list[dict] | None = None,  # noqa: N803 -- the tool's argument names
        assertions: list[str] | None = None,
        privateEpsilon: float | None = None,  # noqa: N803
        closenessThreshold: float | None = None,  # noqa: N803
        closenessDraws: int | None = None,  # noqa: N803
        role: str | None = None,
    ) -> dict:
        return await mt.define_synthetic_dataset(
            *ctx(role),
            dataset_id,
            seed,
            scale,
            tables,
            fanoutConditions,
            assertions,
            privateEpsilon,
            closenessThreshold,
            closenessDraws,
        )

    @described
    async def generate_synthetic_dataset(dataset_id: str, role: str | None = None) -> dict:
        return await mt.generate_synthetic_dataset(*ctx(role), dataset_id)

    @described
    async def get_synthetic_report(dataset_id: str, role: str | None = None) -> list[dict]:
        return await mt.get_synthetic_report(*ctx(role), dataset_id)

    @described
    async def drop_synthetic_dataset(dataset_id: str, role: str | None = None) -> dict:
        return await mt.drop_synthetic_dataset(*ctx(role), dataset_id)

    @described
    async def get_environment_detail(env: str, role: str | None = None) -> dict:
        return await mt.get_environment_detail(*ctx(role), env)

    @described
    async def set_environment_data(
        env: str,
        dataMode: str | None = None,  # noqa: N803 -- the tool's argument names
        mutationHandling: str | None = None,  # noqa: N803
        confirmDiscard: bool = False,  # noqa: N803
        role: str | None = None,
    ) -> dict:
        return await mt.set_environment_data(
            *ctx(role), env, dataMode, mutationHandling, confirmDiscard
        )

    @described
    async def set_source_binding(
        env: str,
        sourceId: str,  # noqa: N803 -- the tool's argument names
        binding: str,
        connection: dict | None = None,
        role: str | None = None,
    ) -> dict:
        return await mt.set_source_binding(*ctx(role), env, sourceId, binding, connection)

    @described
    async def get_environment_synthetic_plan(env: str, role: str | None = None) -> dict:
        return await mt.get_environment_synthetic_plan(*ctx(role), env)

    @described
    async def generate_environment_model(
        env: str,
        runs: dict | None = None,
        seed: int = 0,
        scale: float = 1.0,
        confirmDiscard: bool = False,  # noqa: N803 -- the tool's argument names
        role: str | None = None,
    ) -> dict:
        return await mt.generate_environment_model(
            *ctx(role), env, runs, seed, scale, confirmDiscard
        )

    @described
    async def reset_environment_mutations(env: str, role: str | None = None) -> dict:
        return await mt.reset_environment_mutations(*ctx(role), env)

    @described
    async def export_model_config(role: str | None = None) -> dict:
        return await mt.export_model_config(*ctx(role))
