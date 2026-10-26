# Copyright (c) 2026 Kenneth Stott
# Canary: 6c2e8a17-4f93-4b5d-9e01-8a7d3c5b2f46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The commands Provisa ships (REQ-2168): the APOC set, declared in ``apoc_commands.yaml`` on the
command-binding mechanism, and the built-in implementations a ``builtin`` command names.

Unlike a ``python`` command, which runs a deployment's own code and is host code
(core/host_code.py), a built-in runs only what this registry names: code that ships with Provisa.
A binding naming anything else is refused. Each implementation is called with the call's
arguments, the app state and the acting role, and answers the command's rows.
"""

from __future__ import annotations

import importlib
from collections.abc import Awaitable, Callable
from functools import cache
from importlib import resources
from typing import Any

Implementation = Callable[[dict, Any, str | None], Awaitable[list[dict]]]

_REGISTRY: dict[str, str] = {
    "apoc.meta.data": "provisa.bolt.apoc_meta:meta_data",
    "apoc.meta.schema": "provisa.bolt.apoc_meta:meta_schema",
    "apoc.schema.nodes": "provisa.bolt.apoc_meta:schema_nodes",
}


class BuiltinRefused(ValueError):
    """A ``builtin`` command whose binding names no built-in."""


def check(command: str, binding: dict) -> None:
    """A ``builtin`` command's binding names a built-in Provisa ships."""
    spec = binding.get("callable")
    if spec not in _REGISTRY:
        raise BuiltinRefused(
            f"command {command!r}: a builtin command's binding.callable names one of the "
            f"commands Provisa ships ({', '.join(sorted(_REGISTRY))}), not {spec!r}"
        )


def implementation(spec: str) -> Implementation:
    """The built-in ``spec`` names."""
    check(spec, {"callable": spec})
    module, attr = _REGISTRY[spec].split(":", 1)
    return getattr(importlib.import_module(module), attr)


@cache
def _declared() -> tuple[dict, ...]:
    import yaml

    from provisa.core.models import Function

    text = resources.files("provisa.core").joinpath("apoc_commands.yaml").read_text()
    return tuple(Function.model_validate(f).model_dump() for f in yaml.safe_load(text)["functions"])


def declared() -> list[dict]:
    """The commands Provisa ships, each as a registered command's row reads."""
    return [
        {
            **f,
            "language_bindings": {
                k: {kk: vv for kk, vv in v.items() if vv not in (None, False)}
                for k, v in f["language_bindings"].items()
            },
        }
        for f in _declared()
    ]
