# Copyright (c) 2026 Kenneth Stott
# Canary: 9d41c7b0-5e28-4a63-b1f7-26c8ad30fe91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's half of the organization's CI (REQ-1527).

WHY THERE IS NO RUNNER HERE
    Provisa does not run CI. The organization's review policy, its secrets and its runners already
    live in the git host it uses, and a pipeline built inside Provisa would be a second, weaker one
    beside the one they trust -- and absent entirely from the airgapped distribution of REQ-294,
    which is where a gate is most likely to be mandatory. So this module is two things and nothing
    else: the projection PUSHED to the org's own remote, and a status POSTED when it lands or fails
    to land. The check that reads them is theirs.

WHY THE ANNOUNCEMENT CANNOT FAIL THE CHANGE
    Same rule as the commit it reports (REQ-1524): a model edit that succeeded is not undone
    because a remote was unreachable or a webhook returned 500. A failed push or post is logged and
    leaves the environment exactly as the commit left it -- drifted if the projection did not land,
    clean if it did. The org learns of a stalled mirror from the absence of pushes, which is what
    the sha in the status line is for.

WHY THE REMOTE IS A REFERENCE AND NOT A CREDENTIAL
    A push URL usually carries a token. The stored value is resolved through the secrets provider
    (REQ-125) at push time, so the org stores ``https://${env:GIT_TOKEN}@github.com/acme/model.git``
    and the token itself never enters the control plane -- the same rule REQ-1525 enforces on the
    model's own carried fields.

WHY NOTHING IS ANNOUNCED WHEN NOTHING CHANGED
    ``write_through`` returns ``None`` for an unchanged model as well as for a failed projection,
    and the two are told apart by the drift flag. An unchanged model is not an event: pushing and
    posting on every mutation that touched no carried class would make the status stream noise and
    the pipeline fire on nothing.
"""

# Requirements: REQ-1527, REQ-1524, REQ-125, REQ-294

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select, update

from provisa.core.schema_admin import orgs

if TYPE_CHECKING:
    from provisa.core.db import Database

log = logging.getLogger(__name__)

#: How long an announcement may take before it is abandoned. A model edit is waiting on this, so
#: the budget is small: a remote or a receiver that cannot answer in this time is treated as one
#: that did not answer, and the next projection announces again.
TIMEOUT_S = 10.0


@dataclass(frozen=True)
class RepoIntegration:
    """Where an org's projection is mirrored and where its status is reported.

    Both are optional and independent. A deployment with neither -- the airgapped one, and every
    org that has not asked for this -- still commits, merges and approves exactly as before: this
    whole module is skipped when both are ``None``.
    """

    remote: str | None = None
    status_webhook: str | None = None

    @property
    def configured(self) -> bool:
        return self.remote is not None or self.status_webhook is not None


async def read_integration(admin_db: "Database", org_id: str) -> RepoIntegration:
    """The org's repository integration as stored. Unconfigured is a ``RepoIntegration`` of Nones."""
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(orgs.c.repo_remote, orgs.c.repo_status_webhook).where(orgs.c.id == org_id)
        )
        row = result.fetchone()
    if row is None:
        raise KeyError(f"no such organization: {org_id}")
    return RepoIntegration(remote=row[0], status_webhook=row[1])


async def write_integration(
    admin_db: "Database", org_id: str, *, remote: str | None, status_webhook: str | None
) -> RepoIntegration:
    """Set both halves at once. ``None`` clears one, which is how an org stops mirroring."""
    async with admin_db.acquire() as conn:
        await conn.execute_core(
            update(orgs)
            .where(orgs.c.id == org_id)
            .values(repo_remote=remote, repo_status_webhook=status_webhook)
        )
    return RepoIntegration(remote=remote, status_webhook=status_webhook)


def push(org_id: str, env: str, remote: str) -> None:
    """Push ``env``'s branch to ``remote``. Raises -- the caller decides what a failure means.

    The refspec names the one branch that changed rather than every branch, so a push reports the
    environment that was edited and cannot carry another environment's history along with it.
    """
    from dulwich import porcelain

    from provisa.core.env_repo import _branch_ref, repo_path
    from provisa.core.secrets import resolve_secrets

    ref = _branch_ref(env)
    with _discard() as (binary, _text):
        porcelain.push(
            str(repo_path(org_id)),
            resolve_secrets(remote),
            [ref + b":" + ref],
            errstream=binary,
            outstream=binary,
        )


def delete_remote_branch(org_id: str, env: str, remote: str) -> None:
    """Delete ``env``'s branch ON THE REMOTE. Raises -- the caller decides what a failure means.

    The empty refspec is git's own way of saying it: nothing pushed onto a ref removes it. This is
    asked for explicitly and never implied by a local deletion, because the remote is where the
    work survives a lost volume (REQ-1546) and an org that deletes a local branch has usually said
    the opposite of "lose the copy that is somewhere else".
    """
    from dulwich import porcelain

    from provisa.core.env_repo import _branch_ref, repo_path
    from provisa.core.secrets import resolve_secrets

    with _discard() as (binary, _text):
        porcelain.push(
            str(repo_path(org_id)),
            resolve_secrets(remote),
            [b":" + _branch_ref(env)],
            errstream=binary,
            outstream=binary,
        )


def fetch(org_id: str, remote: str) -> dict[str, str]:
    """Bring ``remote``'s branches into this repository as remote-tracking refs (REQ-1541).

    THE OTHER HALF OF THE MIRROR. The push sends what an environment holds; this brings back what
    the org's git host holds after review happened there -- the merged branch a deploy is then
    pointed at. It is an ACT, never a poll: a fetch happens because somebody asked for one, so what
    an operator sees is the repository as of a moment they chose rather than as of whenever a timer
    last ran.

    Raises on an unreachable remote or an unresolvable secret reference; the caller decides what a
    failure means. Nothing here touches an environment, so a failed fetch changes nothing at all.
    """
    from dulwich import porcelain

    from provisa.core.env_repo import ensure_repo, repo_path, track_remote
    from provisa.core.secrets import resolve_secrets

    # An org may fetch before it has ever edited a model, and there would then be nothing on disk
    # to fetch INTO. Creating it here holds the same invariant the write-through holds.
    ensure_repo(org_id)
    with _discard() as (binary, text):
        result = porcelain.fetch(
            str(repo_path(org_id)),
            resolve_secrets(remote),
            errstream=binary,
            outstream=text,
        )
    return track_remote(org_id, dict(result.refs))


@contextlib.contextmanager
def _discard():
    """dulwich writes progress to these; nothing here reads it, and it is not the server's log.

    Real file objects on the null device rather than a stub with ``write``/``flush``: dulwich
    declares full ``BinaryIO``/``TextIO`` streams and hands them to code that may use more of the
    interface than two methods, and the null device discards without accumulating.
    """
    with open(os.devnull, "wb") as binary, open(os.devnull, "w") as text:
        yield binary, text


async def post_status(url: str, payload: dict) -> None:
    """POST one status. Raises on a transport failure or a non-2xx -- the caller logs it."""
    import httpx

    async with httpx.AsyncClient(timeout=TIMEOUT_S) as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()


async def announce(
    admin_db: "Database", org_id: str, env: str, sha: str | None, drifted: bool
) -> None:
    """Mirror the branch and report the projection's outcome. Never raises (REQ-1524).

    Called with what the commit produced: a sha means it landed, ``drifted`` means it did not. The
    push is attempted first, so the status a pipeline receives describes a remote that already
    holds the sha it names.
    """
    try:
        integration = await read_integration(admin_db, org_id)
    except Exception:  # noqa: BLE001 — REQ-1527: reporting a change never fails the change
        log.exception("could not read the repository integration for %s", org_id)
        return
    if not integration.configured:
        return
    pushed = False
    if integration.remote is not None and sha is not None:
        try:
            # REQ-1546: dulwich's push is blocking and dials somebody else's git host. Off the loop
            # it goes: a slow remote must not hold the request that made the edit.
            await asyncio.to_thread(push, org_id, env, integration.remote)
            pushed = True
        except Exception:  # noqa: BLE001 — REQ-1527: an unreachable remote is not a failed edit
            log.exception("could not push %s/%s to the configured remote", org_id, env)
    if integration.status_webhook is None:
        return
    try:
        await post_status(
            integration.status_webhook,
            {
                "org": org_id,
                "environment": env,
                "sha": sha,
                "drifted": drifted,
                "pushed": pushed,
            },
        )
    except Exception:  # noqa: BLE001 — REQ-1527: a receiver that is down is not a failed edit
        log.exception("could not post the projection status for %s/%s", org_id, env)
