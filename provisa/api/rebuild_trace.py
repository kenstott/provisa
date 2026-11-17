# Copyright (c) 2026 Kenneth Stott
# Canary: b4d830f7-074b-44bd-9526-b98043555444
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What each schema build of an org runtime did: its phases and their times, what caused it, the
stamps it was loaded at, and the counts its cost follows (REQ-1914).

It measures and changes nothing. ``app._rebuild_schemas`` opens one :class:`BuildRecord` per call,
``app._rebuild_schemas_impl`` marks each phase as it begins (:meth:`BuildRecord.phase`), and the
finished record is logged in one line and handed to whoever is capturing (:func:`capture`): a test
counts builds and phases, never seconds.

A build's record ends when its caller is answered: the compiled model is published. What nobody
waits on runs after that, in the runtime's after-answer pass (``build_after_answer``), whose own
record (:class:`AfterAnswer`) hangs on the build's (:attr:`BuildRecord.after`): the phases it ran
(:data:`AFTER_PHASES`), the stamps of the build it ran for, and what became of it.

It lives beside the build it measures (``provisa.api``) and imports nothing of the product, so the
build, the compile (``app_loaders``) and the watcher (``model_reload``) can all report to it.

The phase names are listed here and nowhere else. A phase's time is taken around the phase itself
(``time.perf_counter`` at its first and last statement), never from a log line's timestamp: a
runner stamps a line when its pipe flushes.
"""

# Requirements: REQ-1914

from __future__ import annotations

import contextlib
import functools
import inspect
import logging
import sys
import threading
import time
from collections.abc import Callable, Generator
from contextvars import ContextVar
from dataclasses import dataclass, field
from types import CodeType

log = logging.getLogger(__name__)

# A build's phases, in the order it runs them. Every build that reaches its end has entered each
# of them once, in this order; a phase whose condition does not hold for the build (the process
# configuration of another org's build, the names table of an environment's) is entered and does
# nothing.
EFFECTIVE_NAMES = "effective_names"  # relationships' effective names stored (before the stamp)
STAMP_READ = "stamp_read"  # the model and build stamps this build is loaded at
STORE_CONFIG = "store_config"  # the default org's prod build: the process configuration
REMOTE_SOURCES = "remote_sources"  # naming, encryption, remote GraphQL and gRPC sources loaded
TABLES = "tables"  # every table and column, drafts, relationships, the domain tree
NOTIFY_TRIGGERS = "notify_triggers"  # LISTEN/NOTIFY trigger DDL per registered postgresql table
TAGS = "tags"  # watermarks, naming rules, cache TTLs, domains, tags attached to every row
SOURCES = "sources"  # every source row, catalogs, scope, binding; reconcile of changed sources
ROLES = "roles"  # roles, inheritance chains, owner grant, grants folded over every table
VIEWS = "views"  # ops views, user views, class tables, replica routes, code-source pin
LANDED_RECONCILE = "landed_reconcile"  # landed tables converged in the materialize store (DDL)
INTROSPECT = "introspect"  # column metadata from stored types, API sources
GOVERNANCE = "governance"  # row rules, masks, kept, attributes, entitlements, rules by right
WRITE_CAPABILITY = (
    "write_capability"  # metrics, grant expansion, field numbers, write ops per table
)
COMPILE = "compile"  # the whole-model context, names, path map, lazy role surfaces, wire proto
FIELD_NUMBERS = "field_numbers"  # the wire field numbers the compile allocated, stored
PUBLISH = "publish"  # graph sessions swept, tables/relationships/metrics published, generation,
# stamps; prod: the names the clash policy derived frozen as aliases
FINALIZE = "finalize"  # tables served, live engine, every view's SQL lowered, MV registry
MEASURE = "measure"  # measured fakes read from data, masks published again
CONVERGE = "converge"  # replica convergence started (detached)

# The after-answer pass's phases, in the order it runs them: what a build derives that no request
# is governed by. A pass a newer build superseded stops before the next of them.
DERIVED_WRITES = "derived_writes"  # lineage edges, checker targets written
NAMES_TABLE = "names_table"  # prod: the stored names table written
ANNOUNCE = "announce"  # offers re-derived, catalogs told
LIFECYCLE = "lifecycle"  # push listeners, ingest engines, poll and row-materialize jobs
VIEW_REBUILDS = "view_rebuilds"  # stored views whose kept rule inputs the rules changed, built
WARM_UP = "warm_up"  # every role's surface built

PHASES: tuple[str, ...] = (
    EFFECTIVE_NAMES,
    STAMP_READ,
    STORE_CONFIG,
    REMOTE_SOURCES,
    TABLES,
    NOTIFY_TRIGGERS,
    TAGS,
    SOURCES,
    ROLES,
    VIEWS,
    LANDED_RECONCILE,
    INTROSPECT,
    GOVERNANCE,
    WRITE_CAPABILITY,
    COMPILE,
    FIELD_NUMBERS,
    PUBLISH,
    FINALIZE,
    MEASURE,
    CONVERGE,
)

AFTER_PHASES: tuple[str, ...] = (
    DERIVED_WRITES,
    NAMES_TABLE,
    ANNOUNCE,
    LIFECYCLE,
    VIEW_REBUILDS,
    WARM_UP,
)

# The causes that are not a writer: every other cause is the calling function, by name.
BOOT = "boot"  # the process's own build of the deployment org at startup
ORG_BUILD = "org build"  # an org runtime (or an environment's) built on first use
WATCHER = "watcher"  # the config watcher: a stored build stamp ahead of the runtime's
SETTINGS_WATCHER = "watcher: org settings"  # the config watcher: an org's naming setting changed

# What became of a call for a build.
WAITING = "waiting"  # ended before its turn came (cancelled, or the wait refused): no build
BUILT = "built"
CURRENT = "current"  # the watcher's reload found the runtime built at the stored stamp: no build
FAILED = "failed"

# What became of an after-answer pass.
PENDING = "pending"  # its build is answered; it has not ended
RAN = "ran"  # every phase, for the stamps of its build
SUPERSEDED = "superseded"  # a newer build was published: it stopped before ``stopped_before``


# The functions that only pass a call for a build on: their caller is the cause, not they.
_passes_on: set[CodeType] = set()


def passes_on[F: Callable[..., object]](function: F) -> F:
    """``function`` only hands a call for a build to the build: the cause recorded is its caller.
    It must be a plain function, so that its caller's frame is under it when the build is called."""
    if inspect.iscoroutinefunction(function):
        raise TypeError(
            f"{function.__qualname__} is a coroutine function: its caller's frame is "
            "gone by the time it runs"
        )
    _passes_on.add(function.__code__)
    return function


def calling_function() -> str:
    """The function that called the caller of this, as ``module:qualified name``, looking past
    any function that only passes the call on (:func:`passes_on`).

    Exact because it is read from the interpreter's own frame at the moment of the call, in a
    plain function: the caller's frame is on the stack whether or not the coroutine it gets back
    is awaited there, wrapped in a task or run on another thread."""
    frame = sys._getframe(2)
    while frame.f_code in _passes_on:
        assert frame.f_back is not None, "a function is called from a frame"
        frame = frame.f_back
    return f"{frame.f_globals['__name__']}:{frame.f_code.co_qualname}"


def caused[**P, R](build: Callable[P, R]) -> Callable[P, R]:
    """``build`` (the one entry to a schema build), told its cause on every call: the ``cause`` it
    is given, or the function calling it. The 89 writers pass nothing and are named exactly: the
    frame is read when the call is made, before any coroutine runs."""

    @functools.wraps(build)
    def call(*args: P.args, **kwargs: P.kwargs) -> R:
        if kwargs.get("cause") is None:
            kwargs["cause"] = calling_function()
        return build(*args, **kwargs)

    # Still a coroutine function to whoever asks: it answers the coroutine ``build`` makes.
    return inspect.markcoroutinefunction(call)


@dataclass
class Warmed:
    """The role surfaces built by a build's after-answer pass (``warm_role_surfaces``): work the
    build's own time does not include, that the next request and the next write compete with."""

    roles: int  # surfaces it built (or found refused), of the roles the build registered
    busy_s: float  # the time it held the loop building them, pauses left out
    superseded: bool  # a newer build replaced its inputs before it reached every role


@dataclass
class AfterAnswer:
    """The after-answer pass of one build: what ran once the build's caller had its answer."""

    runtime: str  # the runtime it is the pass of, as the runtime is registered
    org_id: str
    env: str
    cause: str  # of the build it ran for
    stamps: dict[str, int]  # the stamps of the build it ran for
    answered_s: float = 0.0  # what the build's caller waited: its turn and the build
    outcome: str = PENDING
    waited_s: float = 0.0  # from the answer to the pass's first phase
    took_s: float = 0.0  # its phases, from the first to its end
    phases: list[tuple[str, float]] = field(default_factory=list)  # ran, with its seconds
    failed_in: str | None = None  # the phase that raised
    error: str | None = None  # what it raised
    stopped_before: str | None = None  # superseded: the first phase it did not run

    @property
    def phase_names(self) -> list[str]:
        return [name for name, _ in self.phases]

    def line(self) -> str:
        """The pass in one log line, beside its build's: names, stamps and seconds."""
        parts = [
            f"after-answer pass of {self.org_id}: answered after {self.answered_s:.1f}s, "
            f"waited {self.waited_s:.1f}s, took {self.took_s:.1f}s",
            f"env={self.env} cause={self.cause} outcome={self.outcome}"
            + (f" failed_in={self.failed_in}" if self.failed_in else "")
            + (f" stopped_before={self.stopped_before}" if self.stopped_before else ""),
            "for stamps " + " ".join(f"{kind} {at}" for kind, at in self.stamps.items()),
        ]
        if self.phases:
            parts.append("phases " + " ".join(f"{n}={s:.2f}" for n, s in self.phases))
        return "; ".join(parts)


@dataclass
class BuildRecord:
    """One call for a build of one org runtime, to its caller's answer."""

    org_id: str
    env: str
    cause: str
    outcome: str = WAITING
    waited_s: float = 0.0  # for the runtime's turn (its rebuild lock)
    took_s: float = 0.0  # the build itself, once it had its turn
    phases: list[tuple[str, float]] = field(default_factory=list)  # entered, with its seconds
    failed_in: str | None = None  # the phase a failed build stopped in
    loaded_stamps: dict[str, int | None] = field(default_factory=dict)  # the runtime's, before
    read_stamps: dict[str, int] = field(default_factory=dict)  # this build is loaded at
    counts: dict[str, int] = field(default_factory=dict)
    warmed: Warmed | None = None  # set when the warm-up of this build's pass has ended
    after: AfterAnswer | None = None  # what ran after the answer; None when nothing was published
    _open: tuple[str, float] | None = field(default=None, repr=False)

    def phase(self, name: str) -> None:
        """``name`` begins here, and the phase before it has ended."""
        if name not in PHASES:
            raise ValueError(
                f"{name!r} is not a phase of the build before its answer (rebuild_trace.PHASES)"
            )
        now = time.perf_counter()
        self._close(now)
        self._open = (name, now)

    def count(self, **counts: int) -> None:
        self.counts.update(counts)

    def stamps(self, *, loaded: dict[str, int | None], read: dict[str, int]) -> None:
        self.loaded_stamps = dict(loaded)
        self.read_stamps = dict(read)

    @property
    def phase_names(self) -> list[str]:
        return [name for name, _ in self.phases]

    def _close(self, now: float) -> None:
        if self._open is not None:
            name, began = self._open
            self.phases.append((name, now - began))
            self._open = None

    def _end(self, outcome: str) -> None:
        self.outcome = outcome
        if outcome == FAILED and self._open is not None:
            self.failed_in = self._open[0]
        self._close(time.perf_counter())

    def line(self) -> str:
        """The record in one log line: names, counts, stamps and seconds, nothing of the model."""
        parts = [
            f"schema rebuild of {self.org_id}: waited {self.waited_s:.1f}s for its turn, "
            f"took {self.took_s:.1f}s",
            f"env={self.env} cause={self.cause} outcome={self.outcome}"
            + (f" failed_in={self.failed_in}" if self.failed_in else ""),
        ]
        if self.counts:
            parts.append(" ".join(f"{k}={v}" for k, v in self.counts.items()))
        if self.read_stamps:
            parts.append(
                "stamps "
                + " ".join(
                    f"{kind} {self.loaded_stamps.get(kind)}->{read}"
                    for kind, read in self.read_stamps.items()
                )
            )
        if self.phases:
            parts.append("phases " + " ".join(f"{n}={s:.2f}" for n, s in self.phases))
        return "; ".join(parts)


_building: ContextVar[BuildRecord | None] = ContextVar(
    "provisa_rebuild_trace_building", default=None
)


@contextlib.contextmanager
def building(record: BuildRecord) -> Generator[BuildRecord]:
    """``record`` is the build running in this context, from here to the block's end. It ends
    built, or failed in the phase that raised; the failure is the caller's as before."""
    token = _building.set(record)
    began = time.perf_counter()
    try:
        yield record
    except BaseException:
        record._end(FAILED)
        raise
    else:
        record._end(BUILT)
    finally:
        record.took_s = time.perf_counter() - began
        _building.reset(token)


def this_build() -> BuildRecord:
    """The build running in this context. The model build runs only inside one."""
    record = _building.get()
    if record is None:
        raise LookupError(
            "no schema build is being recorded here: the model build runs only "
            "inside app._rebuild_schemas"
        )
    return record


def build_if_any() -> BuildRecord | None:
    """The build running in this context, for a step that also runs outside one (the per-role
    compile is called by tests and the cloned-environment install on its own)."""
    return _building.get()


class Recorded:
    """The calls for a build made while a :func:`capture` was open, in the order they ended."""

    def __init__(self) -> None:
        self.calls: list[BuildRecord] = []
        self.passes: list[AfterAnswer] = []  # the after-answer passes, in the order they ended

    @property
    def builds(self) -> list[BuildRecord]:
        """The calls that ran a build, to its end or to a failure: not a reload that found the
        runtime current, nor a call that ended before its turn."""
        return [r for r in self.calls if r.outcome in (BUILT, FAILED)]

    @property
    def causes(self) -> list[str]:
        return [r.cause for r in self.builds]

    def clear(self) -> None:
        self.calls.clear()
        self.passes.clear()


_capturing: list[Recorded] = []
_capturing_guard = threading.Lock()


@contextlib.contextmanager
def capture() -> Generator[Recorded]:
    """Every call for a build that ends in this process while the block is open, whatever thread
    or loop it ran on."""
    recorded = Recorded()
    with _capturing_guard:
        _capturing.append(recorded)
    try:
        yield recorded
    finally:
        with _capturing_guard:
            _capturing.remove(recorded)


def finished(record: BuildRecord, *, slow_s: float) -> None:
    """A call for a build has ended: hand the record to every open capture, and log a build in
    one line. A slow build is logged at WARNING, the level a hosted tier logs at, so its operator
    sees what a slow save spent and on what; any other at INFO."""
    with _capturing_guard:
        open_captures = list(_capturing)
    for recorded in open_captures:
        recorded.calls.append(record)
    if record.outcome not in (BUILT, FAILED):
        return
    slow = record.waited_s + record.took_s >= slow_s
    log.log(logging.WARNING if slow else logging.INFO, "%s", record.line())


def pass_ended(after: AfterAnswer, *, slow_s: float) -> None:
    """An after-answer pass has ended: hand its record to every open capture and log it in one
    line. One that failed is logged by whoever caught the failure, with its traceback."""
    with _capturing_guard:
        open_captures = list(_capturing)
    for recorded in open_captures:
        recorded.passes.append(after)
    if after.outcome == FAILED:
        return
    log.log(logging.WARNING if after.took_s >= slow_s else logging.INFO, "%s", after.line())


def warm_up_ended(
    record: BuildRecord | None, org_id: str, warmed: Warmed, *, slow_s: float
) -> None:
    """The warm-up of a build's pass has ended. Its time is in no build's ``took``."""
    if record is not None:
        record.warmed = warmed
    log.log(
        logging.WARNING if warmed.busy_s >= slow_s else logging.INFO,
        "role surface warm-up of %s%s: %d role(s) in %.1fs%s",
        org_id,
        f" after the build by {record.cause}" if record is not None else "",
        warmed.roles,
        warmed.busy_s,
        ", superseded by a newer build" if warmed.superseded else "",
    )
