# Copyright (c) 2026 Kenneth Stott
# Canary: 2b9e6d14-3c70-4f8a-b1e5-a07d3e91c652
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a dbt source offers as commands, and where each one runs (REQ-2228).

Anything dbt can run is offered: each model, test, snapshot and seed, ``build``, ``source
freshness`` and each macro through ``run-operation``. Each is a mutation. Nothing runs one unless a
call, a trigger or a schedule does, and dbt goes on running on its own schedule.

Provisa holds no dbt logic. A command only says what to invoke, at wherever dbt already runs: dbt
Cloud's jobs API (an ``http`` command) or a ``dbt`` a command line reaches (a ``script`` command).
"""

# Requirements: REQ-2228

from __future__ import annotations

import shlex
from dataclasses import dataclass
from typing import Any

from provisa.dbt.manifest import DbtManifest

#: The one argument of a macro's command: the macro's arguments, given to ``--args`` as JSON.
MACRO_ARGUMENT = "args"

TARGET_SCRIPT = "script"
TARGET_CLOUD = "cloud"


class TargetRefused(ValueError):
    """A command's target is not stated well enough to run anything."""


@dataclass(frozen=True)
class DbtCommand:
    name: str
    comment: str | None
    #: What is given to ``dbt``: ``("run", "--select", "orders")``.
    invocation: tuple[str, ...]
    arguments: tuple[str, ...] = ()


def _selected(prefix: str, verb: str, name: str, comment: str | None) -> DbtCommand:
    return DbtCommand(f"{prefix}_{name}", comment, (verb, "--select", name))


def offered_commands(manifest: DbtManifest) -> list[DbtCommand]:
    """Every command ``manifest`` offers, a new manifest offering what it declares."""
    return [
        *(_selected("run", "run", m.name, m.description) for m in manifest.models),
        *(_selected("test", "test", t.name, t.description) for t in manifest.tests),
        *(_selected("snapshot", "snapshot", s.name, s.description) for s in manifest.snapshots),
        *(_selected("seed", "seed", s.name, s.description) for s in manifest.seeds),
        DbtCommand("build", f"Build the {manifest.project} project", ("build",)),
        DbtCommand(
            "source_freshness",
            f"Check the freshness of the {manifest.project} project's sources",
            ("source", "freshness"),
        ),
        *(
            DbtCommand(
                f"run_operation_{m.name}",
                m.description or f"Run the {m.package} macro {m.name}",
                ("run-operation", m.name),
                (MACRO_ARGUMENT,),
            )
            for m in manifest.macros
        ),
    ]


def command_named(manifest: DbtManifest, name: str) -> DbtCommand | None:
    return next((c for c in offered_commands(manifest) if c.name == name), None)


def script_prefix(command_line: str) -> list[str]:
    """The words a ``dbt`` is reached by: ``dbt``, or ``dbt --project-dir /srv/jaffle``."""
    words = shlex.split(command_line)
    if not words:
        raise TargetRefused("a dbt source run by a script names the command that runs dbt")
    return words


def script_binding(command: DbtCommand, command_line: str) -> dict[str, Any]:
    """A ``script`` command's binding: the argv, and dbt's plain-text output returned as it is."""
    binding: dict[str, Any] = {
        "argv": [*script_prefix(command_line), *command.invocation],
        "stdout": "text",
    }
    if command.arguments:
        binding["json_argument"] = {"flag": "--args", "argument": MACRO_ARGUMENT}
    return binding


def cloud_binding(
    command: DbtCommand, *, source_id: str, host: str, account_id: str, job_id: str
) -> dict[str, Any]:
    """An ``http`` command's binding: run the job with this command as its one step. Its token is
    the dbt source's credential, resolved when the command runs."""
    if not host or not account_id or not job_id:
        raise TargetRefused("a dbt source run by dbt Cloud names its host, account and job")
    return {
        "url": f"{host.rstrip('/')}/api/v2/accounts/{account_id}/jobs/{job_id}/run/",
        "method": "POST",
        "body": {
            "cause": f"Provisa: {command.name}",
            "steps_override": [shlex.join(("dbt", *command.invocation))],
        },
        "credential_source": source_id,
        "auth_scheme": "Token",
    }
