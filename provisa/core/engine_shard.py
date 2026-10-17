# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which provisioned engine shard the bound org's catalogs are created on (REQ-2073).

A shard's coordinator shares no directory with the control plane, and a running one cannot be given
a directory per catalog. A source whose catalog needs one (a Kafka source with no schema registry,
any Redis source) is therefore refused when it is saved for an org whose engine is a shard, and one
stored before that rule gets no catalog at boot. The API layer installs the resolver
(provisa.api.app), since the org's engine lane is its runtime's.
"""

# Requirements: REQ-2073

from __future__ import annotations

from collections.abc import Callable

_resolver: Callable[[], str | None] | None = None


def set_resolver(resolver: Callable[[], str | None]) -> None:
    global _resolver
    _resolver = resolver


def bound_org_shard() -> str | None:
    """The shard the bound org's engine catalogs are created on; None where its engine is not a
    shard the deployment provisions."""
    if _resolver is None:
        raise RuntimeError(
            "No engine-shard resolver is installed, so a source cannot be checked against the "
            "org's engine. The API layer installs it at import (provisa.api.app)."
        )
    return _resolver()


class EngineShardRefused(ValueError):
    """A source the bound org's engine shard cannot serve, refused when it is saved."""

    code = ""

    def __init__(self, message: str, source_id: str, shard: str) -> None:
        super().__init__(message)
        self.params = {"source": source_id, "shard": shard}
