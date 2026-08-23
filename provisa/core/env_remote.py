# Copyright (c) 2026 Kenneth Stott
# Canary: 4f2ab61c-93de-4d70-8c1a-5b7e0d2914af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Does the remote an org just configured actually exist, and may Provisa create it (REQ-1537).

WHY THIS EXISTS AT ALL
    ``write_integration`` accepts any string, and the first thing that finds out whether the string
    names a repository is a push -- which runs inside a model edit, cannot fail that edit
    (REQ-1527), and therefore reports a missing repository as a log line nobody reads. The org
    learns its mirror never worked from the absence of commits on a remote they believed was wired
    up. Probing at CONFIGURE time moves that answer to the moment somebody is looking at it.

WHY CREATION IS OFFERED AND NEVER PERFORMED
    Creating a repository is a write to somebody else's account -- a namespace Provisa was handed a
    token for, not one it owns. A missing repository is also the signature of a TYPO, and silently
    creating ``acme/modle`` would leave an org mirroring into a repository nobody will ever look at
    while the real one stays empty. So probing is the automatic half and creation is the answered-
    question half: this module never calls ``create`` on its own, and the router exposes it only
    behind an explicit request that names the remote the operator was shown.

WHAT "EXISTS" MEANS FOR EACH KIND
    A local path is a bare repository on this filesystem -- the airgapped deployment's mirror
    (REQ-294), where there is no API and creation is ``init_bare``. A hosted URL is probed by
    asking the git host itself for its refs, because that is exactly what the push will do: a URL
    that answers ``ls_remote`` is one a push can reach, and no amount of API-level existence makes
    up for a credential that cannot fetch it. Creation is API work, so it is offered only for the
    hosts whose API this module speaks -- GitHub and GitLab. Everything else is probed and
    reported, never created: an operator with a Gitea or a Bitbucket gets told the repository is
    missing and creates it where they already have the rights to.

WHY THE TOKEN IS READ FROM THE REMOTE AND NOWHERE ELSE
    The remote is stored as a reference (``https://${env:GIT_TOKEN}@github.com/acme/model.git``)
    and resolved through the secrets provider at the moment of use (REQ-125, REQ-1525) -- here, as
    at push. There is no second credential setting to keep in sync, and the token this module sends
    to an API is by construction the one the push would send to the same host.
"""

# Requirements: REQ-1537, REQ-1527, REQ-125, REQ-1525, REQ-294

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

log = logging.getLogger(__name__)

#: A probe runs while an operator waits on a form, so it gets the announcement budget of REQ-1527
#: rather than a push's patience.
TIMEOUT_S = 10.0

PATH = "path"
GITHUB = "github"
GITLAB = "gitlab"
OTHER = "other"

#: Which host each API belongs to. Self-hosted GitHub Enterprise and GitLab instances answer at
#: their own domains and are NOT assumed here: guessing that ``git.acme.internal`` speaks the GitLab
#: API would send an org's token to an endpoint chosen by pattern-matching a hostname.
_API_HOSTS = {
    "github.com": GITHUB,
    "www.github.com": GITHUB,
    "gitlab.com": GITLAB,
    "www.gitlab.com": GITLAB,
}


class RemoteError(Exception):
    """The remote could not be probed or created, with a reason meant for an operator to read."""


@dataclass(frozen=True)
class RemoteProbe:
    """What was found at the configured remote.

    *kind* is what the remote IS, so the caller can say "bare repository" or "GitHub repository"
    rather than "remote". *creatable* is whether THIS module could make it -- false for a host
    whose API it does not speak, and false for a hosted URL that carries no credential, because an
    unauthenticated create is a 401 dressed up as an offer.
    """

    exists: bool
    kind: str
    creatable: bool
    target: str
    detail: str

    def as_dict(self) -> dict:
        return {
            "exists": self.exists,
            "kind": self.kind,
            "creatable": self.creatable,
            "target": self.target,
            "detail": self.detail,
        }


def _is_path(remote: str) -> bool:
    """A local filesystem mirror, written either as a plain path or as ``file://``."""
    return remote.startswith("file://") or "://" not in remote


def _local_path(remote: str) -> Path:
    if remote.startswith("file://"):
        return Path(urlsplit(remote).path)
    return Path(remote).expanduser()


def _split_hosted(resolved: str) -> tuple[str, str, str, str]:
    """``(host, owner, repo, token)`` for a hosted URL, with the token as stored in its userinfo."""
    parts = urlsplit(resolved)
    if not parts.hostname:
        raise RemoteError(f"remote has no host: {_redact(resolved)}")
    segments = [s for s in parts.path.split("/") if s]
    if len(segments) < 2:
        raise RemoteError(
            f"remote names no repository -- expected <owner>/<name>: {_redact(resolved)}"
        )
    owner = "/".join(segments[:-1])
    repo = segments[-1].removesuffix(".git")
    # Either half of the userinfo may be the token: GitHub takes ``<token>@`` or
    # ``<user>:<token>@``, and GitLab's own docs write ``oauth2:<token>@``. The password wins when
    # both are present, which is the form every host documents.
    token = parts.password or parts.username or ""
    return parts.hostname, owner, repo, token


def _redact(url: str) -> str:
    """The URL with its userinfo removed -- what may be logged or returned to a browser."""
    parts = urlsplit(url)
    if not parts.hostname:
        return url
    netloc = parts.hostname + (f":{parts.port}" if parts.port else "")
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def probe(remote: str) -> RemoteProbe:
    """Does *remote* name a repository that exists? Never writes anything.

    *remote* is the STORED value, secret references included; it is resolved here exactly as a push
    would resolve it, and nothing derived from the resolved form is returned to the caller.
    """
    from provisa.core.secrets import resolve_secrets

    if _is_path(remote):
        path = _local_path(remote)
        # A bare repository keeps its refs at the top level; a working clone keeps them under
        # ``.git``. Both are pushable, and both are what an operator may have pointed at.
        exists = (path / "HEAD").exists() or (path / ".git" / "HEAD").exists()
        return RemoteProbe(
            exists=exists,
            kind=PATH,
            creatable=True,
            target=str(path),
            detail=(f"{path} is a git repository" if exists else f"{path} holds no git repository"),
        )

    resolved = resolve_secrets(remote)
    host, owner, repo, token = _split_hosted(resolved)
    api = _API_HOSTS.get(host, OTHER)
    shown = _redact(resolved)
    exists = _refs_reachable(resolved)
    return RemoteProbe(
        exists=exists,
        kind=api,
        # A missing repository on a host this module cannot call is still worth REPORTING; it just
        # cannot be offered. Same for a URL with no credential: creating needs one.
        creatable=(not exists) and api in (GITHUB, GITLAB) and bool(token),
        target=f"{owner}/{repo}",
        detail=(
            f"{shown} answered as a repository"
            if exists
            else f"{shown} did not answer as a repository"
        ),
    )


def _refs_reachable(resolved: str) -> bool:
    """Can a push reach this URL? Asked by fetching its refs, which is what a push does first.

    A failure here is genuinely ambiguous -- absent repository, wrong credential, host down -- and
    this function deliberately does not try to tell those apart: every one of them means the mirror
    is not working, which is the thing the operator is being shown. The exception's text is logged
    so the distinction is recoverable from the server log.
    """
    from dulwich import porcelain

    try:
        porcelain.ls_remote(resolved)
    except Exception as exc:
        log.info("remote %s did not answer ls-remote: %s", _redact(resolved), exc)
        return False
    return True


def create(remote: str, *, private: bool = True) -> RemoteProbe:
    """Create the repository *remote* names, and return the probe that now finds it.

    Called only from the endpoint an operator answered "yes" on (REQ-1537). Raises rather than
    reporting a partial success: an org that was told the repository was created and finds nothing
    there is worse off than one that was told the creation failed.
    """
    from provisa.core.secrets import resolve_secrets

    if _is_path(remote):
        from dulwich.repo import Repo

        path = _local_path(remote)
        if (path / "HEAD").exists() or (path / ".git" / "HEAD").exists():
            raise RemoteError(f"{path} already holds a git repository")
        path.mkdir(parents=True, exist_ok=True)
        Repo.init_bare(str(path))
        log.info("created bare repository at %s", path)
        return probe(remote)

    resolved = resolve_secrets(remote)
    host, owner, repo, token = _split_hosted(resolved)
    api = _API_HOSTS.get(host, OTHER)
    if api == OTHER:
        raise RemoteError(
            f"Provisa can create repositories on GitHub and GitLab; {host} is neither. "
            f"Create {owner}/{repo} on {host} and configure the remote again."
        )
    if not token:
        raise RemoteError(
            f"the remote for {owner}/{repo} carries no credential, and creating a repository "
            f"needs one -- store it as a secret reference in the URL (REQ-125)"
        )
    if api == GITHUB:
        _create_github(owner, repo, token, private)
    else:
        _create_gitlab(owner, repo, token, private)
    return probe(remote)


def _create_github(owner: str, repo: str, token: str, private: bool) -> None:
    """POST the repository into *owner*, whether that is the token's own account or an org.

    Which endpoint creates it is not a guess: ``/user/repos`` makes a repository owned by the
    authenticated account and ``/orgs/{org}/repos`` one owned by an organization, and sending the
    wrong one either creates the repository at the wrong address or 404s. The token is asked who it
    is first, and the answer decides.
    """
    import httpx

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    with httpx.Client(timeout=TIMEOUT_S) as client:
        me = client.get("https://api.github.com/user", headers=headers)
        if me.status_code != 200:
            raise RemoteError(
                f"GitHub rejected the remote's credential ({me.status_code}); it cannot create "
                f"{owner}/{repo}"
            )
        login = me.json()["login"]
        url = (
            "https://api.github.com/user/repos"
            if login == owner
            else f"https://api.github.com/orgs/{owner}/repos"
        )
        made = client.post(url, headers=headers, json={"name": repo, "private": private})
    if made.status_code not in (200, 201):
        raise RemoteError(
            f"GitHub refused to create {owner}/{repo} ({made.status_code}): {made.text[:200]}"
        )
    log.info("created GitHub repository %s/%s", owner, repo)


def _create_gitlab(owner: str, repo: str, token: str, private: bool) -> None:
    """POST the project into the group *owner*, or into the token's own namespace.

    GitLab creates into a NAMESPACE ID rather than a path, so the namespace is looked up by the
    path in the URL. A path that resolves to nothing is reported as such: creating the project in
    the token's personal namespace instead would put the org's mirror somewhere nobody agreed to.
    """
    import httpx

    headers = {"PRIVATE-TOKEN": token}
    with httpx.Client(timeout=TIMEOUT_S) as client:
        found = client.get(
            f"https://gitlab.com/api/v4/namespaces/{quote(owner, safe='')}", headers=headers
        )
        if found.status_code != 200:
            raise RemoteError(
                f"GitLab could not resolve the namespace {owner!r} with the remote's credential "
                f"({found.status_code}); it cannot create {owner}/{repo}"
            )
        namespace_id = found.json()["id"]
        made = client.post(
            "https://gitlab.com/api/v4/projects",
            headers=headers,
            json={
                "name": repo,
                "path": repo,
                "namespace_id": namespace_id,
                "visibility": "private" if private else "public",
            },
        )
    if made.status_code not in (200, 201):
        raise RemoteError(
            f"GitLab refused to create {owner}/{repo} ({made.status_code}): {made.text[:200]}"
        )
    log.info("created GitLab project %s/%s", owner, repo)


def open_pull_request(remote: str, *, head: str, base: str, title: str, body: str) -> dict:
    """Ask the git host to review *head* into *base*, and return where the review now lives.

    WHY THIS IS THE SAME ACT AS AN INTERNAL MERGE REQUEST (REQ-1551). A protected environment
    proposes rather than merges (REQ-1504), and the proposal is a row somebody in the org decides.
    When the org's model ALSO lives on a git host whose branch is governed by pull requests, that
    row is the wrong place for the conversation: the reviewers are already on the host, the branch
    protection is enforced there, and a merge Provisa performed locally would arrive at the remote
    as a push the host is configured to refuse. So the request is opened WHERE THE RULE LIVES, and
    what Provisa holds is the link to it.

    The branch must already be pushed (REQ-1546): a pull request names two refs on the host, and a
    host cannot review a branch it has never received. The caller pushes first and this raises if
    the host says the head is unknown, rather than pushing on its own -- a review request that
    silently published work is a different act from the one the operator asked for.
    """
    from provisa.core.secrets import resolve_secrets

    if _is_path(remote):
        raise RemoteError(
            "a bare repository has no pull requests; review the merge in Provisa instead (REQ-1504)"
        )
    resolved = resolve_secrets(remote)
    host, owner, repo, token = _split_hosted(resolved)
    api = _API_HOSTS.get(host, OTHER)
    if api == OTHER:
        raise RemoteError(
            f"Provisa opens pull requests on GitHub and GitLab; {host} is neither. Open the "
            f"request for {head} -> {base} on {host} yourself, or review the merge in Provisa."
        )
    if not token:
        raise RemoteError(
            f"the remote for {owner}/{repo} carries no credential, and opening a pull request "
            f"needs one -- store it as a secret reference in the URL (REQ-125)"
        )
    if api == GITHUB:
        return _pr_github(owner, repo, token, head=head, base=base, title=title, body=body)
    return _pr_gitlab(owner, repo, token, head=head, base=base, title=title, body=body)


def _pr_github(
    owner: str, repo: str, token: str, *, head: str, base: str, title: str, body: str
) -> dict:
    """POST the pull request, and return the EXISTING one when the host says there already is one.

    GitHub answers a duplicate with 422, and re-asking for a review of a branch that is already
    under review is a reasonable thing for somebody to do -- they lost the link. The open request
    for the same pair is found and returned, so the answer is the same either way: here is where
    this review is happening.
    """
    import httpx

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
    with httpx.Client(timeout=TIMEOUT_S) as client:
        made = client.post(
            url, headers=headers, json={"title": title, "body": body, "head": head, "base": base}
        )
        if made.status_code == 422:
            open_ones = client.get(
                url, headers=headers, params={"state": "open", "head": f"{owner}:{head}"}
            )
            existing = open_ones.json() if open_ones.status_code == 200 else []
            if existing:
                return {
                    "url": existing[0]["html_url"],
                    "number": existing[0]["number"],
                    "new": False,
                }
        if made.status_code not in (200, 201):
            raise RemoteError(
                f"GitHub refused a pull request for {head} -> {base} in {owner}/{repo} "
                f"({made.status_code}): {made.text[:200]}"
            )
        opened = made.json()
    log.info("opened GitHub pull request %s", opened["html_url"])
    return {"url": opened["html_url"], "number": opened["number"], "new": True}


def _pr_gitlab(
    owner: str, repo: str, token: str, *, head: str, base: str, title: str, body: str
) -> dict:
    """POST the merge request against the project, addressed by its URL-encoded path."""
    import httpx

    project = quote(f"{owner}/{repo}", safe="")
    headers = {"PRIVATE-TOKEN": token}
    url = f"https://gitlab.com/api/v4/projects/{project}/merge_requests"
    with httpx.Client(timeout=TIMEOUT_S) as client:
        made = client.post(
            url,
            headers=headers,
            json={
                "source_branch": head,
                "target_branch": base,
                "title": title,
                "description": body,
            },
        )
        if made.status_code == 409:
            open_ones = client.get(
                url,
                headers=headers,
                params={"state": "opened", "source_branch": head, "target_branch": base},
            )
            existing = open_ones.json() if open_ones.status_code == 200 else []
            if existing:
                return {"url": existing[0]["web_url"], "number": existing[0]["iid"], "new": False}
        if made.status_code not in (200, 201):
            raise RemoteError(
                f"GitLab refused a merge request for {head} -> {base} in {owner}/{repo} "
                f"({made.status_code}): {made.text[:200]}"
            )
        opened = made.json()
    log.info("opened GitLab merge request %s", opened["web_url"])
    return {"url": opened["web_url"], "number": opened["iid"], "new": True}
