# Copyright (c) 2026 Kenneth Stott
# Canary: da23482f-3df7-4b40-bb35-887131fe774d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What this process holds for an org, forgotten when the org is deleted (REQ-1300, REQ-2006).

A map kept by an org, its schemas or its users is told when the org goes (``destroy_org``, for
an admin's delete and a demo sandbox's reap alike): every live-demo visitor is an org of its own,
so one never told grows by a visitor's entries per visitor for the process's life. A module
registers what drops its entries (:func:`on_forget`); nothing here imports those modules, which
register at their own import -- one never imported holds nothing to forget.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import overload

_hooks: list[Callable[[str], None]] = []
# How many entries each registered module holds, by its hook's name: what a leak test reads before
# and after orgs come and go (REQ-2149), the same set the hooks clear.
_sizes: dict[str, Callable[[], int]] = {}

Hook = Callable[[str], None]


@overload
def on_forget(hook: Hook, *, size: Callable[[], int] | None = None) -> Hook: ...
@overload
def on_forget(
    hook: None = None, *, size: Callable[[], int] | None = None
) -> Callable[[Hook], Hook]: ...
def on_forget(
    hook: Hook | None = None, *, size: Callable[[], int] | None = None
) -> Hook | Callable[[Hook], Hook]:
    """Register ``hook(org_id)``, which drops what its module holds for ``org_id``; ``size``, how
    many entries the module holds for every org together."""

    def register(fn: Hook) -> Hook:
        _hooks.append(fn)
        if size is not None:
            _sizes[f"{fn.__module__}.{fn.__name__}"] = size
        return fn

    return register(hook) if hook is not None else register


def sizes() -> dict[str, int]:
    """Each registered module's entry count, by its hook's name."""
    return {name: size() for name, size in _sizes.items()}


def forget_org(org_id: str) -> None:
    """Drop what every registered module holds for ``org_id``."""
    for hook in _hooks:
        hook(org_id)


def schema_of_org(org_id: str, schema: str) -> bool:
    """Whether ``schema`` is one of ``org_id``'s (``org_<id>`` and every ``org_<id>_...``
    environment, region and store schema; environments.org_schema). An org whose id extends this
    one past an underscore is matched too: what is forgotten is only a cache, refilled on its next
    use, never wrong."""
    scope = f"org_{org_id}"
    return schema == scope or schema.startswith(f"{scope}_")
