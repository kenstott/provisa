# Copyright (c) 2026 Kenneth Stott
# Canary: 744772fc-e9a1-4668-9390-f4bcdce4f886
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's data surface: one object, built whole and installed by one assignment.

A role is served through five things built from the same inputs — its GraphQL schema, the
compilation context its queries are lowered with, its row filters, its REST path map and its
proto. They were five maps, written one after another, and a request (on its own thread) read
them one after another: in between, a query was validated against the NEW schema and compiled
against the OLD context, and a just-registered table was an unhandled "Unknown root query
field" (#213).

``RoleSurface`` holds the five. A build makes one, complete, and installs it with a single
assignment into the runtime's ``surfaces``; a request takes it once (:func:`role_surface`) and
reads every part from that one object, so no request sees parts of two builds.

``surfaces`` is never changed in place: installing or dropping a role's surface makes a new
mapping and puts it in the runtime's ``surfaces`` (:func:`install_surface`). Whoever holds the
old mapping holds what was true then.

``state.schemas`` / ``contexts`` / ``rls_contexts`` / ``table_path_maps`` / ``proto_files`` remain
— one part across all roles — for code that needs only that part: each is a view of the
``surfaces`` mapping current when it was taken (:class:`SurfacePart`), reading a role's part in
one lookup. Product code never writes through one: the build is the one writer
(tests/unit/test_role_surface.py holds both rules).

A build installs each role's surface LAZILY (:class:`LazySurface`): the role is known at once, and
its parts are built the first time anything reads them, once, then held. A rebuild's cost is then
the model-wide parts alone, not every role's schema, context and proto (REQ-1882); the role a
request acts as is built when it is first asked for. A role whose build fails holds that failure:
it is logged as an ERROR naming the role, and every read of the role raises it
(:class:`RoleSurfaceUnbuilt`) -- never "no schema".
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterator, MutableMapping
from dataclasses import dataclass, replace
from typing import Any

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RoleSurface:
    """Everything one role is served through, from one build."""

    schema: Any = None  # graphql.GraphQLSchema
    context: Any = None  # compiler.sql_gen.CompilationContext
    rls: Any = None  # compiler.rls.RLSContext
    table_path_map: Any = None  # {gql field name -> {schema_name, table_name, domain_id}}
    proto: Any = None  # the role's .proto text


class RoleSurfaceUnbuilt(RuntimeError):
    """A role's data surface could not be built; every read of the role raises this."""

    def __init__(self, role_id: str, cause: BaseException) -> None:
        self.role_id = role_id
        super().__init__(f"the data surface of role {role_id!r} could not be built: {cause}")


class LazySurface:
    """A role's surface, built the first time it is read and held after: one build, however
    many requests ask at once. A failed build is held too, and raised to every reader; a build a
    request's deadline cut short is not held, and the next reader builds again."""

    __slots__ = ("_build", "_failure", "_lock", "_surface", "role_id")

    def __init__(self, role_id: str, build: Callable[[], RoleSurface]) -> None:
        self.role_id = role_id
        self._build = build
        self._lock = threading.Lock()
        self._surface: RoleSurface | None = None
        self._failure: RoleSurfaceUnbuilt | None = None

    @property
    def built(self) -> bool:
        return self._surface is not None or self._failure is not None

    def resolve(self) -> RoleSurface:
        from provisa.core.request_deadline import DeadlinePassed, RequestTimedOut

        with self._lock:
            if self._surface is not None:
                return self._surface
            if self._failure is not None:
                raise self._failure
            try:
                self._surface = self._build()
            except (DeadlinePassed, RequestTimedOut):
                raise  # the reader's time ran out, not the role's build: the next one builds
            except Exception as exc:
                self._failure = RoleSurfaceUnbuilt(self.role_id, exc)
                log.error("%s", self._failure, exc_info=exc)
                raise self._failure from exc
            return self._surface


def resolved(held: RoleSurface | LazySurface | None) -> RoleSurface | None:
    """What a ``surfaces`` entry serves: a lazy one is built (or raises its failure) here."""
    return held.resolve() if isinstance(held, LazySurface) else held


def built_surface(held: RoleSurface | LazySurface) -> RoleSurface:
    """A registered role's surface (an entry that is there): a lazy one is built here."""
    return held.resolve() if isinstance(held, LazySurface) else held


def role_surface(state: Any, role_id: str) -> RoleSurface | None:
    """The surface ``role_id`` is served through now, or None when it has none (a role with no
    data plane, or one this runtime does not know). Taken ONCE per request: every part a request
    reads comes from the object this returns. A role not built yet is built here; one whose build
    failed raises :class:`RoleSurfaceUnbuilt`."""
    return resolved(state.surfaces.get(role_id))


# Installs are rare (a build, a meta-role's first use) and come from more than one lock's
# holders; this serializes the read-copy-replace below so none is lost.
_INSTALL = threading.Lock()


def install_surface(holder: Any, role_id: str, surface: RoleSurface | LazySurface | None) -> None:
    """Make ``surface`` the role's surface on ``holder`` (an org runtime) — or, given None, leave
    the role with none. ``holder.surfaces`` is replaced by a new mapping, never changed in
    place: one assignment, and a mapping taken earlier still shows what it showed."""
    with _INSTALL:
        surfaces = dict(holder.surfaces)
        if surface is None:
            surfaces.pop(role_id, None)
        else:
            surfaces[role_id] = surface
        holder.surfaces = surfaces


class SurfacePart(MutableMapping):
    """One part of every role's surface, as role id -> part: a view of the ``surfaces`` mapping
    that was current when the view was taken.

    Reading a role's part is one lookup. A role whose surface has no such part is not in the
    view. Because ``surfaces`` is replaced and never changed in place, a view taken before a
    change still shows what was there — it is what a test's teardown puts back.

    A write installs, for that role, a surface that differs in this part alone
    (:func:`install_surface`) — for a test that sets a state up part by part; the build installs
    whole surfaces and never comes here.
    """

    __slots__ = ("_holder", "_held", "_part")

    def __init__(self, holder: Any, part: str) -> None:
        self._holder = holder
        self._held: dict[str, RoleSurface] = holder.surfaces
        self._part = part

    def __getitem__(self, role_id: str) -> Any:
        value = getattr(built_surface(self._held[role_id]), self._part)
        if value is None:
            raise KeyError(role_id)
        return value

    def __setitem__(self, role_id: str, value: Any) -> None:
        held = resolved(self._holder.surfaces.get(role_id)) or RoleSurface()
        install_surface(self._holder, role_id, replace(held, **{self._part: value}))
        self._held = self._holder.surfaces

    def __delitem__(self, role_id: str) -> None:
        self[role_id]  # KeyError when the role has no such part
        remaining = replace(built_surface(self._holder.surfaces[role_id]), **{self._part: None})
        install_surface(self._holder, role_id, None if remaining == RoleSurface() else remaining)
        self._held = self._holder.surfaces

    def __iter__(self) -> Iterator[str]:
        # A role not built yet is listed without building it: a built surface holds every part.
        return iter(
            [
                r
                for r, held in self._held.items()
                if isinstance(held, LazySurface) or getattr(held, self._part) is not None
            ]
        )

    def __contains__(self, role_id: object) -> bool:
        # Membership names the role without building it.
        held = self._held.get(role_id) if isinstance(role_id, str) else None
        if held is None:
            return False
        return isinstance(held, LazySurface) or getattr(held, self._part) is not None

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def __repr__(self) -> str:
        return f"SurfacePart({self._part}, {dict(self)!r})"


def set_part(holder: Any, part: str, values: Any) -> None:
    """Make ``part`` across all roles exactly ``values`` (a test replacing a whole map, or its
    teardown putting an earlier one back).

    Given a view of this part (what a teardown puts back), the ``surfaces`` mapping the view
    shows is made current again whole: a role not built then is put back unbuilt. Reading the
    view's parts would build every such role, and one whose build fails would fail the teardown
    -- leaving whatever it was to undo after it (the bound org among them) undone."""
    if isinstance(values, SurfacePart) and values._part == part:
        for role_id in [r for r in holder.surfaces if r not in values._held]:
            install_surface(holder, role_id, None)
        for role_id, held in values._held.items():
            install_surface(holder, role_id, held)
        return
    wanted = dict(values)
    view = SurfacePart(holder, part)
    for role_id in list(view):
        if role_id not in wanted:
            del view[role_id]
    for role_id, value in wanted.items():
        view[role_id] = value
