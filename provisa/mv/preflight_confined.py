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
it never runs in the server process. It runs as a preflight call (:meth:`Worker.preflight`) to
the organization's preflight worker: one sandboxed host per organization per Provisa process,
started at that organization's first check and kept, so a run pays no process start. The check's
inputs go to it a batch at a time as Arrow, only as the check reads them; ``ctx`` crosses as a
copy of its fields; the verdict and the warnings come back as data and are applied here.

The worker is Python's own code-source host (``provisa/python/host.py``) under the code source's
sandbox, run by the interpreter Provisa itself runs on, with ``provisa/mv/preflight_host`` as
its code. A check is text, not an installed source, so it needs no install and no pinned
runtime: the interpreter and the pyarrow the server already has serve it, and its rows are read
by the same pyarrow that wrote them.

The worker reads Provisa's Python (its home and its packages), ``provisa/python`` (the host and
the services client) and ``provisa/mv`` (the purity check); it writes nothing; its network is the
egress proxy with an empty allow-list, which answers Provisa's services and refuses every other
host. Its environment is PATH, Python's switches, the proxy's variables and ``PROVISA_SERVICES``,
under a grant for its organization that answers as the role every build runs as.

A worker that cannot be started refuses the run by name (:class:`PreflightConfinedUnavailable`):
the check is never run in the server instead.
"""

# Requirements: REQ-2133, REQ-1165
from __future__ import annotations

import asyncio
import dataclasses
import logging
import sys
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from provisa.events.processor import PreprocessError
from provisa.mv.preflight import Decision, PreflightContractError, Verdict

if TYPE_CHECKING:
    from provisa.codesource.worker import HostCommand, Worker

log = logging.getLogger(__name__)

#: The worker's code: the directory of its entry module, and the module.
CODE = Path(__file__).resolve().parent / "preflight_host"
ENTRY = "preflight_entry"

#: What the pool knows the worker as, in each organization.
WORKER = "preflight"


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


def readable(code: Path = CODE) -> list[Path]:
    """The directories the worker may read beside the system's own: Python's host and the services
    client, its code, the purity check's, and Provisa's Python (the interpreter's home, its
    environment -- whose ``pyvenv.cfg`` the interpreter reads as it starts -- and the packages
    pyarrow is among)."""
    import pyarrow

    from provisa.python.source import HOST

    return [
        HOST.parent,
        code,
        CODE.parent,
        Path(sys.base_prefix),
        Path(sys.prefix),
        Path(pyarrow.__file__).resolve().parent.parent,
    ]


def command(org: str, *, code: Path = CODE) -> "HostCommand":
    """How ``org``'s preflight worker is started: Python's host on :data:`ENTRY`, sandboxed, with
    a services grant for the organization (each call is answered for the view it checks)."""
    import pyarrow

    from provisa.api.app import state
    from provisa.codesource.sandbox import sandboxed
    from provisa.codesource.services import ServiceGrant
    from provisa.codesource.worker import private_environment
    from provisa.core import region_admin
    from provisa.python.source import HOST

    python = Path(sys.executable)
    host_lib = Path(pyarrow.__file__).resolve().parent.parent
    confined = sandboxed(
        lambda address: [str(python), "-I", str(HOST), address, str(code), ENTRY],
        executable=python,
        read=readable(code),
        env=private_environment(
            PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1", PROVISA_HOST_LIB=str(host_lib)
        ),
        cwd=code,
        egress=(),
        table_reads="pgwire",  # REQ-2226: the minted-token pgwire port, as before Flight
    )
    grant = ServiceGrant(
        org_id=org, source_id=WORKER, admin_db=state.admin_db, role=region_admin.ROLE
    )
    return dataclasses.replace(confined, services=grant)


def content_hash() -> str:
    """What the worker's hello must name: the protocol's own preflight host."""
    from provisa.codesource.worker import PROTOCOL

    return f"{WORKER}-{PROTOCOL}"


def worker(org: str, subject: str, *, code: Path = CODE) -> "Worker":
    """``org``'s preflight worker, started at its first use; refused by name when it cannot be."""
    from provisa.codesource.linux_sandbox import SandboxUnavailable
    from provisa.codesource.worker import WorkerFailed, pool

    try:
        return pool.get(
            f"{WORKER}:{code}" if code != CODE else WORKER,
            content_hash(),
            lambda: command(org, code=code),
            org=org,
        )
    except SandboxUnavailable as exc:
        raise PreflightConfinedUnavailable(subject, str(exc)) from exc
    except OSError as exc:
        raise PreflightConfinedUnavailable(subject, str(exc)) from exc
    except WorkerFailed as exc:
        raise PreflightConfinedUnavailable(subject, str(exc)) from exc


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


def _ask(
    org: str,
    source: str,
    fields: dict[str, Any],
    streams: Any,
    subject: str,
    product_ids: Sequence[str],
    code: Path,
) -> dict[str, Any]:
    """The worker's answer to one run of the check. Blocking: run on a thread."""
    from provisa.codesource.worker import CodeFailed, WorkerFailed

    serving = worker(org, subject, code=code)
    try:
        return serving.preflight(
            source,
            fields,
            {name: map(_encoded, _batches(stream)) for name, stream in streams.items()},
            whole=[n for n, s in streams.items() if isinstance(s, (list, tuple))],
            subject=subject,
            product_ids=product_ids,
        )
    except (WorkerFailed, CodeFailed) as exc:
        # The worker ended under the call, or its host failed outside the check.
        raise PreflightConfinedFailed(subject, str(exc)) from exc


async def run_confined(
    source: str,
    streams: Any,
    ctx: Any,
    *,
    subject: str,
    product_ids: Sequence[str] = (),
    code: Path = CODE,
) -> Verdict:
    """The verdict of the check ``source`` over ``streams``, run in the bound organization's
    preflight worker. The warnings it took are added to ``ctx``; a check that raised raises here
    (:class:`PreflightRaised`, or the contract error it broke)."""
    from provisa.core.request_context import require_current_org

    answer = await asyncio.to_thread(
        _ask, require_current_org(), source, _ctx_fields(ctx), streams, subject, product_ids, code
    )
    ctx.warn(answer["warns"])
    log.info("preflight of %s ran confined in process %s", subject, answer["pid"])
    if "raised" in answer:
        if answer["contract"]:
            raise PreflightContractError(answer["message"])
        raise PreflightRaised(answer["raised"], answer["message"])
    return Verdict(Decision(answer["decision"]), answer["reason"])
