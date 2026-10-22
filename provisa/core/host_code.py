# Copyright (c) 2026 Kenneth Stott
# Canary: 8a6ccc0d-8d72-43af-8d54-4cfc944a541b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.


"""Host code: the command kinds that run code on the server's host (REQ-2134).

A ``python`` command calls a ``module:attr`` in the server's own process; a ``script`` command
runs a subprocess on its host. Either runs with the server's full access -- its memory,
credentials, files and network -- and its model entry holds only a reference to the code, which
does not travel with the model when it is promoted. They are allowed only where the operator
turns ``commands.allow_host_code`` on; off, as it is by default, registering, updating or calling
one is refused, here, by the one check. Code resolvers are the governed alternative.
"""

# Requirements: REQ-2134

from __future__ import annotations

from provisa.core import settings_registry

#: The command kinds that run code on the server's host.
HOST_CODE_KINDS = frozenset({"python", "script"})


class HostCodeRefused(ValueError):
    """A host-code command registered, updated or called while host code is off."""

    code = "functions.host_code_refused"

    def __init__(self, name: str, impl_kind: str) -> None:
        super().__init__(
            f"command {name!r} is of kind {impl_kind!r}, which runs code on the server's host "
            "with the server's own access; host code is off on this deployment "
            "(commands.allow_host_code). Use a code resolver instead."
        )
        self.params = {"name": name, "impl_kind": impl_kind}


def refuse_host_code(name: str, impl_kind: str) -> None:
    """Refuse a command of a host-code kind unless the deployment allows host code."""
    if impl_kind in HOST_CODE_KINDS and not settings_registry.value("commands.allow_host_code"):
        raise HostCodeRefused(name, impl_kind)
