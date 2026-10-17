# Copyright (c) 2026 Kenneth Stott
# Canary: 3b077df4-2094-4cb7-8877-4e5989f00974
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where an engine's catalog finds a source's bundled pgwire server, without importing it.

``trino_connectors`` is read by the model and the compiler (``core.models`` consults the Trino
registry for a type's connector name), and the import contracts keep those out of the executor's
import chain, which ``pgwire_replica`` is in. So the server's module registers its endpoint
resolver here when it is imported, and a Trino catalog over a pgwire server asks here.
"""

from __future__ import annotations

from typing import Any, Callable

#: ``source -> (host an engine reaches the server by, its pgwire port)``; starts the source's
#: server if it is not running. Set by ``pgwire_replica`` at import.
_resolver: Callable[[Any], tuple[str, int]] | None = None


def register(resolver: Callable[[Any], tuple[str, int]]) -> None:
    global _resolver
    _resolver = resolver


def engine_endpoint(source: Any) -> tuple[str, int]:
    """The (host, port) an engine's catalog dials for ``source``'s pgwire server."""
    if _resolver is None:
        raise RuntimeError(
            f"the pgwire server of source {source.id!r} cannot be located: "
            "provisa.federation.pgwire_replica has not been imported in this process"
        )
    return _resolver(source)


def sign_in_password(source: Any) -> str:
    """The password every connection to ``source``'s pgwire server signs in with. The govdata
    server signs a connection in with the source's AskAmerica API key and meters what runs on it
    under that key (the connector's ASKAM-010); a connection without it is refused. The server of
    every other type checks no credentials."""
    stype = source.type
    if (stype.value if hasattr(stype, "value") else str(stype)) == "govdata":
        from provisa.federation.askamerica import api_key

        return api_key(source)
    return ""
