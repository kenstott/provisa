# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4b6e12-7a3c-4f85-b0e7-2c8a5d1f6e39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An organization's Python preflight run outside the server, in a held organization (REQ-2133).

On a deployment of many organizations a preflight an organization wrote is another tenant's code:
it never runs in the server process. Each run starts one process under the code source's sandbox
(:func:`provisa.codesource.sandbox.sandboxed_once`) running :mod:`provisa.mv.preflight_runner`,
which is handed the check's inputs a batch at a time as Arrow over the process's pipes and
returns the verdict and the warnings. ``ctx`` crosses as a copy of its fields; the warnings come
back as data and are applied here.

The process reads Provisa's own Python (its home and its packages), the runner's directory and
the services client; it writes nothing; its network is the egress proxy with an empty allow-list,
which answers Provisa's services for a check that imports them and refuses every other host. Its
environment is PATH, the proxy's variables and, for such a check, ``PROVISA_SERVICES``.

A process that cannot be started refuses the run by name (:class:`PreflightConfinedUnavailable`):
the check is never run in the server instead.
"""

# Requirements: REQ-2133, REQ-1165
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import pickle  # nosec B403 - written here for the confined process; never read here
import subprocess  # nosec B404 - argv-list invocation only, never shell=True
import sys
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

from provisa.events.processor import PreprocessError
from provisa.mv import preflight_runner as wire
from provisa.mv.preflight import Decision, PreflightContractError, Verdict

log = logging.getLogger(__name__)

RUNNER = Path(wire.__file__).resolve()

#: What the process says of itself on standard error, kept this far back for a refusal.
_STDERR_TAIL = 2000


class PreflightConfinedUnavailable(PreprocessError):
    """The confined process a held organization's preflight runs in could not be started."""

    code = "preflight.confined_unavailable"

    def __init__(self, subject: str, reason: str) -> None:
        self.params = {"subject": subject, "reason": reason}
        super().__init__(
            f"The preflight of {subject} was not run: in this organization it runs only in a "
            f"confined process, and one could not be started ({reason})."
        )


class PreflightConfinedFailed(PreprocessError):
    """The confined process ended before it returned a verdict."""

    code = "preflight.confined_failed"

    def __init__(self, subject: str, reason: str) -> None:
        self.params = {"subject": subject, "reason": reason}
        super().__init__(
            f"The preflight of {subject} returned no verdict: its confined process ended first "
            f"({reason})."
        )


class PreflightRaised(Exception):
    """The check raised in its process: ``raised`` names the exception's type, and the message is
    the exception's own, as it reads when the check runs in the server."""

    def __init__(self, raised: str, message: str) -> None:
        super().__init__(message)
        self.raised = raised


def readable(runner: Path = RUNNER) -> list[Path]:
    """The directories the process may read beside the system's own: the runner's, the services
    client's, and Provisa's Python (its environment and the interpreter's home)."""
    return [
        runner.parent,
        RUNNER.parent,
        wire.SERVICES_CLIENT.parent,
        Path(sys.prefix),
        Path(sys.base_prefix),
    ]


def command(
    *, runner: Path = RUNNER, services: str | None = None
) -> tuple[list[str], dict[str, str]]:
    """The confined process's argv and environment. ``services`` is the value of
    ``PROVISA_SERVICES`` for a check that imports them."""
    from provisa.codesource.sandbox import sandboxed_once
    from provisa.codesource.services import SERVICES_ENV
    from provisa.codesource.worker import private_environment

    python = Path(sys.executable)
    extra = {SERVICES_ENV: services} if services is not None else {}
    return sandboxed_once(
        [str(python), "-I", str(runner)],
        executable=python,
        read=readable(runner),
        env=private_environment(PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1", **extra),
        egress=(),
    )


def _ctx_fields(ctx: Any) -> dict[str, Any]:
    if not dataclasses.is_dataclass(ctx) or isinstance(ctx, type):
        raise TypeError(
            f"a confined preflight is given its ctx as fields, and {type(ctx).__name__} has none"
        )
    return {f.name: getattr(ctx, f.name) for f in dataclasses.fields(ctx)}


def _encoded(batch: Any) -> bytes:
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _batches(stream: Any) -> Iterator[Any]:
    """``stream`` as Arrow batches: its own when it is the engine's (never through row dicts),
    else its rows grouped."""
    from provisa.processors.arrow import BatchRows, record_batches

    if isinstance(stream, BatchRows):
        return stream.batches()
    return record_batches(stream)


def _exchange(
    argv: Sequence[str], env: dict[str, str], subject: str, run: dict[str, Any], streams: Any
) -> dict[str, Any]:
    """Start the process, serve its reads of ``streams`` and return its answer (its ``kind``
    added). Blocking: run on a thread."""
    feeds = {node: _batches(stream) for node, stream in streams.items()}
    with tempfile.TemporaryFile() as stderr:

        def said() -> str:
            stderr.seek(0)
            return stderr.read().decode(errors="replace").strip()[-_STDERR_TAIL:]

        try:
            # The interpreter Provisa runs on and Provisa's own runner: a fixed argv, no shell.
            proc = subprocess.Popen(  # nosec B603
                list(argv), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr, env=env
            )
        except OSError as exc:
            raise PreflightConfinedUnavailable(subject, str(exc)) from exc
        assert proc.stdin is not None and proc.stdout is not None
        answered = False
        try:
            wire.write_frame(proc.stdin, wire.RUN, pickle.dumps(run))
            while True:
                kind, payload = wire.read_frame(proc.stdout)
                answered = True
                if kind in (wire.RESULT, wire.FAILED):
                    return {"kind": kind, **json.loads(payload)}
                assert kind == wire.NEXT, kind
                batch = next(feeds[payload.decode()], None)
                if batch is None:
                    wire.write_frame(proc.stdin, wire.END)
                else:
                    wire.write_frame(proc.stdin, wire.BATCH, _encoded(batch))
        except (EOFError, BrokenPipeError) as exc:
            code = proc.wait()
            reason = said() or f"it exited with status {code}"
            if answered:
                raise PreflightConfinedFailed(subject, reason) from exc
            # It ended before its first frame: the sandbox refused it, or it could not start.
            raise PreflightConfinedUnavailable(subject, reason) from exc
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait()
            proc.stdin.close()
            proc.stdout.close()


def _services_token(subject: str, product_ids: Sequence[str]) -> str:
    """A grant for the view's check, as the in-process run issues one (REQ-2133): dq answers as
    the role every build runs as."""
    from provisa.api.app import state
    from provisa.codesource.services import ServiceGrant, issue
    from provisa.core import region_admin
    from provisa.core.request_context import require_current_org

    return issue(
        ServiceGrant(
            org_id=require_current_org(),
            source_id=subject,
            admin_db=state.admin_db,
            role=region_admin.ROLE,
            product_ids=tuple(product_ids),
        )
    )


async def run_confined(
    source: str,
    streams: Any,
    ctx: Any,
    *,
    subject: str,
    product_ids: Sequence[str] = (),
    runner: Path = RUNNER,
) -> Verdict:
    """The verdict of the check ``source`` over ``streams``, run in a confined process of its own.
    The warnings it took are added to ``ctx``; a check that raised raises here
    (:class:`PreflightRaised`, or the contract error it broke)."""
    from provisa.codesource.linux_sandbox import SandboxUnavailable
    from provisa.codesource.services import address, revoke
    from provisa.mv.preprocess import uses_services

    whole: Iterable[str] = [n for n, s in streams.items() if isinstance(s, (list, tuple))]
    run = {
        "source": source,
        "ctx": _ctx_fields(ctx),
        "nodes": list(streams),
        "whole": list(whole),
    }
    token = _services_token(subject, product_ids) if uses_services(source) else None
    try:
        try:
            argv, env = command(
                runner=runner, services=address(token) if token is not None else None
            )
        except SandboxUnavailable as exc:
            raise PreflightConfinedUnavailable(subject, str(exc)) from exc
        answer = await asyncio.to_thread(_exchange, argv, env, subject, run, streams)
    finally:
        if token is not None:
            revoke(token)
    ctx.warn(answer["warns"])
    log.info("preflight of %s ran confined in process %s", subject, answer["pid"])
    if answer["kind"] == wire.FAILED:
        if answer["contract"]:
            raise PreflightContractError(answer["message"])
        raise PreflightRaised(answer["type"], answer["message"])
    return Verdict(Decision(answer["decision"]), answer["reason"])
