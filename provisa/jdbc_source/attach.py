# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0e8a3c-91b4-4f27-8c6a-2e7f4b19d063
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A JDBC source's live attach: as the built-in source type its URL names (REQ-2191).

The maintainer, 2026-11-02: a JDBC source's operator is declaring that the source cannot use the
standard connector but is close to it. So a SQLAlchemy URL's scheme names the built-in source type
of the same database (:data:`NAMED_TYPES`), and with live attach turned on for the active engine
the source attaches exactly as that built-in type does there, through that type's own attach
connector, given the host, port, database and user the URL names (:func:`attach_view`).

A source has ONE effective attach type on an engine (:func:`attach_type`): the type its URL names
when attach is turned on for that engine and the engine has an attach connector for the type, else
none. A jdbc: URL (a customer's driver) never attaches; nor does a scheme naming no built-in type.
"""

# Requirements: REQ-2191

from __future__ import annotations

from typing import Any

from provisa.jdbc_source import SOURCE_TYPE
from provisa.jdbc_source.settings import NAMED_TYPES, named_type

__all__ = ["NAMED_TYPES", "attach_type", "attach_view", "named_type", "reach_type", "reach_types"]


def _type_of(source: Any) -> str:
    kind = source.type
    return str(getattr(kind, "value", kind))


def _federation_engine(engine: Any) -> Any:
    """The bare FederationEngine of ``engine``, which may be its runtime wrapper."""
    return engine if getattr(engine, "connectors", None) is not None else engine.engine


def attach_type(source: Any, engine: Any) -> str | None:
    """The type ``source`` attaches as on ``engine``: for a JDBC source with live attach turned
    on there, the built-in type its URL names, when the engine has an attach connector for it;
    None for any other source, and on no engine (``engine`` None)."""
    from provisa.federation.strategy import engine_attaches
    from provisa.jdbc_source.settings import ATTACH, parse

    if engine is None or _type_of(source) != SOURCE_TYPE:
        return None
    settings = parse(getattr(source, "federation_hints", None) or {})
    named = named_type(settings.url)
    if named is None or not settings.on(_federation_engine(engine).name, ATTACH):
        return None
    return named if engine_attaches(engine, named) else None


def reach_type(source: Any, engine: Any) -> str:
    """The source type ``engine``'s reach of ``source`` is judged by
    (``strategy.engine_attaches``): its effective attach type, else its own type."""
    return attach_type(source, engine) or _type_of(source)


def reach_types(
    source_types: dict[str, str], hints: dict[str, dict[str, str]], engine: Any
) -> dict[str, str]:
    """:func:`reach_type` of each source a runtime's per-source maps hold, by id: its type
    (``source_types``) and its settings (``hints``)."""
    from types import SimpleNamespace

    return {
        sid: reach_type(
            SimpleNamespace(id=sid, type=stype, federation_hints=hints.get(sid, {})), engine
        )
        for sid, stype in source_types.items()
    }


def attach_view(source: Any, named: str) -> Any:
    """``source`` as the built-in type ``named`` is attached: that type, with the host, port,
    database and user its URL names (a file database's path, as written), and its own password.
    ``source`` keeps everything else (its id, catalog, table and settings)."""
    from types import SimpleNamespace

    from sqlalchemy.engine import make_url

    from provisa.core.models import SourceType
    from provisa.jdbc_source.url import FILE_BACKENDS

    url = make_url(source.federation_hints["url"])
    connection: dict[str, Any] = {"type": SourceType(named)}
    if url.get_backend_name() in FILE_BACKENDS:
        connection["path"] = url.database or ""
    else:
        connection.update(host=url.host, port=url.port, database=url.database)
        if url.username:
            connection["username"] = url.username
    if hasattr(source, "model_copy"):
        return source.model_copy(update=connection)
    return SimpleNamespace(**{**vars(source), **connection})
