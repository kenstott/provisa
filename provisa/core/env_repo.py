# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2a91c4-58fb-4e13-a70c-3f1d8b6e5240
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The org's one bare repository, and the write-through that projects an environment into it.

ONE REPOSITORY PER ORGANIZATION, ONE BRANCH PER ENVIRONMENT. A repository is exactly the set of
refs that can branch from one another and merge back into one another, and that set is the ORG: an
environment is always created from another environment of the same org or a sha of one, and a
branch or merge across orgs is never a legal operation. Two environments in separate repositories
share no object graph, so a pull request between them could not be expressed at all. A single
deployment-wide repository namespaced per org is refused for the opposite reason -- git objects are
content-addressed and shared across every ref in a store, so one store holding every tenant would
let an org fetch another org's model objects by hash. Per-org draws the isolation boundary exactly
where REQ-1488 already draws it, and where git already draws one.

THE REPOSITORY IS A PROJECTION, NEVER AN AUTHORITY. The model lives in the control plane and every
query reads it from there. So a commit that fails MUST NOT fail the change it observes: a model
edit that succeeded is not undone by a disk that would not take a picture of it. The environment is
marked DRIFTED instead, and drift is repairable by construction -- :func:`rebuild` re-serializes
every carried class from the control plane and commits the result, which is correct however far
behind the tree had fallen. That is the whole repair; there is no incremental catch-up to get wrong.

COMMITTING IS NOT A VERB. Provisa's git surface is BRANCH, BROWSE and LOAD. Every change commits
itself as it happens, which is what makes undo free rather than a feature: undoing is loading an
earlier sha, and it works for any change ever made rather than for the ones somebody remembered to
mark. The cost is a history of generated messages, and a person who wants a legible one squashes it
afterwards with ordinary git.

EVERYTHING ELSE IS ORDINARY GIT. Pushing, fetching, merging, rebasing and CI are done against this
repository by whoever wants to do them; Provisa neither performs them nor polices them. It is a
normal repository on disk, so normal tools work on it. A branch may therefore move underneath an
environment, and that is not an error state tracked here -- what protects the model is REQ-1496: a
tree is not a model until a load validates it and applies it whole or not at all.

WHY DULWICH. It is pure Python, so REQ-294's airgap distribution carries no compiler and the host
needs no git binary. GitPython shells out, so an image without git silently loses history; pygit2
needs libgit2 built.
"""

# Requirements: REQ-294, REQ-1487, REQ-1488, REQ-1489, REQ-1496, REQ-1502, REQ-1505, REQ-1524,
# REQ-1526, REQ-1543, REQ-1555

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dulwich.graph import find_merge_base
from dulwich.objects import Blob, Commit, ObjectID, ShaFile, Tree
from dulwich.refs import Ref
from dulwich.repo import Repo

from provisa.core.env_files import dump
from provisa.core.env_project import project

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

#: The mode every file in the tree carries. One mode, because the tree holds serialized model and
#: nothing executable, and a mode that varied would be a diff nobody asked for.
FILE_MODE = 0o100644

#: Who a commit is attributed to when the act had no acting user. REQ-1487 gives an org its ``prod``
#: environment at creation, before there is a member to attribute anything to, and REQ-1524 requires
#: that environment to hold a repository from its first moment rather than from its first edit. So
#: this is the author of exactly those commits -- provisioning and rebuild-with-no-actor -- and never
#: a stand-in for an actor the caller failed to pass.
SYSTEM_AUTHOR = "provisa <provisa@localhost>"


class RepositoryError(Exception):
    """The repository cannot answer the question asked of it."""


def repo_root() -> Path:
    """Where the deployment keeps its bare repositories.

    The embedded default is the only one that can always work: REQ-294 makes the distribution
    airgap-capable, so an external forge cannot be a dependency of editing a model. It follows the
    same data-dir convention as the rest of the embedded stack.
    """
    root = os.environ.get("PROVISA_REPO_DIR")
    if root:
        return Path(root)
    return Path(os.environ.get("PROVISA_DATA_DIR") or (Path.home() / ".provisa")) / "repos"


def repo_path(org_id: str) -> Path:
    """The org's bare repository. ``.git`` because a bare repository is conventionally named so."""
    return repo_root() / f"{org_id}.git"


def ensure_repo(org_id: str) -> Repo:
    """The org's repository, created if it does not exist yet.

    Idempotent because it is called from org creation AND from every write-through: REQ-1524 admits
    no state in which an environment holds a model and has no repository to project it into, and
    the cheapest way to hold that invariant is for the projection to be able to create the store it
    writes into rather than to assume somebody else did.
    """
    path = repo_path(org_id)
    if (path / "objects").is_dir():
        return Repo(str(path))
    path.mkdir(parents=True, exist_ok=True)
    return Repo.init_bare(str(path))


def _branch_ref(env: str) -> Ref:
    return Ref(f"refs/heads/{env}".encode())


#: Where a FETCH puts what the remote had (REQ-1541). Remote-tracking refs are a separate namespace
#: from ``refs/heads`` on purpose: an environment's branch is written by the write-through and by
#: nothing else, so a fetch that landed on ``refs/heads`` would let the remote reposition an
#: environment behind the control plane's back. Under ``refs/remotes/origin`` it is only a tree
#: somebody may CHOOSE to deploy.
REMOTE_PREFIX = b"refs/remotes/origin/"


def remote_ref(branch: str) -> Ref:
    return Ref(REMOTE_PREFIX + branch.encode())


def _build_tree(repo: Repo, files: dict[str, str]) -> ObjectID:
    """Write ``path -> text`` as git trees and return the root tree's sha.

    Built recursively because a git tree names only its immediate children: ``sales/tables/Order``
    is three objects, and flattening it into one tree with slashes in the names would produce a
    repository no git client could read.
    """
    root: dict[str, Any] = {}
    for path, text in files.items():
        node = root
        *directories, name = path.split("/")
        for directory in directories:
            node = node.setdefault(directory, {})
            if not isinstance(node, dict):
                raise RepositoryError(f"{path!r} names a directory that is also a file")
        node[name] = text

    def write(node: dict[str, Any]) -> ObjectID:
        tree = Tree()
        for name, value in sorted(node.items()):
            if isinstance(value, dict):
                tree.add(name.encode(), 0o040000, write(value))
            else:
                blob = Blob.from_string(value.encode())
                repo.object_store.add_object(blob)
                tree.add(name.encode(), FILE_MODE, blob.id)
        repo.object_store.add_object(tree)
        return tree.id

    return write(root)


def _author(actor: str | None) -> bytes:
    """The commit's author line. REQ-1524: the acting user, so REQ-1505's history needs no ledger."""
    if actor is None:
        return SYSTEM_AUTHOR.encode()
    return f"{actor} <{actor}>".encode()


def has_branch(repo: Repo, env: str) -> bool:
    """Whether ``env``'s line has been started at all.

    REQ-1543: an environment whose branch has no ref has no history, so the next model edit would
    become the first commit of the line and be unundoable for that reason alone. Callers use this to
    write the baseline the environment should have been given when it was created -- HOWEVER it was
    created, which is the point: the guarantee belongs to the environment, not to one code path that
    happens to make one.
    """
    return repo.refs.read_ref(_branch_ref(env)) is not None


def start_branch(repo: Repo, env: str, from_env: str) -> str | None:
    """Point ``env``'s branch at whatever ``from_env`` is standing on. ``None`` if it has nothing.

    REQ-1543: an environment created from another one HOLDS THAT ONE'S MODEL, so its history begins
    where the source's stands rather than at a root commit of its own. Two things follow, and both
    are the reason this exists: the first edit made in the new environment is a child with a parent
    to step back to, and a later merge between the two branches has a merge base -- a root commit
    would have made the branches unrelated histories that no comparison could line up.

    Refuses to move a branch that already exists: a ref under ``refs/heads`` is written by the
    write-through and by the creation of the environment it belongs to, and repointing one would
    move an environment's history out from under it.
    """
    ref = _branch_ref(env)
    if repo.refs.read_ref(ref) is not None:
        raise RepositoryError(f"{env!r} already has a branch; it cannot be started again")
    head = repo.refs.read_ref(_branch_ref(from_env))
    if head is None:
        return None
    repo.refs[ref] = ObjectID(head)
    return head.decode()


def commit_files(
    repo: Repo, env: str, files: dict[str, str], message: str, actor: str | None
) -> str | None:
    """Commit ``files`` as the whole content of ``env``'s branch. ``None`` when nothing changed.

    The commit replaces the tree rather than patching it, because the caller hands over a complete
    projection: a file that vanished from the model has to vanish from the tree, and an incremental
    write could only remove it by first working out what used to be there.

    An unchanged tree makes NO commit. A change that the projection does not express -- editing a
    binding, which REQ-1489 excludes -- would otherwise write an empty commit to every environment
    every time, and a history of them is a history nobody can read.
    """
    tree_id = _build_tree(repo, files)
    ref = _branch_ref(env)
    parents: list[ObjectID] = []
    head = repo.refs.read_ref(ref)
    if head is not None:
        parent = _object(repo, ObjectID(head), Commit)
        if parent.tree == tree_id:
            return None
        parents = [ObjectID(head)]

    commit = Commit()
    commit.tree = tree_id
    commit.parents = parents
    commit.author = commit.committer = _author(actor)
    commit.commit_time = commit.author_time = int(time.time())
    commit.commit_timezone = commit.author_timezone = 0
    commit.encoding = b"UTF-8"
    commit.message = message.encode()
    repo.object_store.add_object(commit)
    repo.refs[ref] = commit.id
    return commit.id.decode()


async def write_through(
    conn: "Connection",
    admin_db: "Database",
    org_id: str,
    env: str,
    schema: str,
    message: str,
    actor: str | None,
) -> str | None:
    """Project ``schema`` into ``env``'s branch and commit it. Never raises.

    THE RETURN IS THE SHA, and ``None`` means either nothing changed or the projection did not land
    -- the caller cannot act differently on those two anyway, because neither is a reason to undo the
    model change it just made. What a failure DOES do is mark the environment drifted, which is the
    state :func:`rebuild` repairs and the UI reports.

    Blanket ``except Exception`` is the requirement, not a swallowed error: REQ-1524 says a failed
    commit must not fail the change it observes, and a projection can fail for reasons that have
    nothing to do with the model -- a full disk, a repository someone deleted, a permission change.
    Narrowing it would let one of those reach the caller and undo an edit that succeeded.
    """
    from provisa.core.env_ci import announce
    from provisa.core.env_store import set_drifted, set_position

    try:
        files = dump(await project(conn, schema))
        sha = commit_files(ensure_repo(org_id), env, files, message, actor)
    except Exception:  # noqa: BLE001 — REQ-1524: the projection never fails the change it observes
        import logging

        logging.getLogger(__name__).exception(
            "projection of %s/%s did not land; marking the environment drifted", org_id, env
        )
        await set_drifted(admin_db, org_id, env, True)
        await announce(admin_db, org_id, env, None, True)
        return None
    await set_drifted(admin_db, org_id, env, False)
    if sha is not None:
        # REQ-1543: the model now equals THIS commit, so this is where an undo starts from. The
        # redo cursor is cleared in the same write: an edit made after an undo is the environment
        # choosing a different future, and the one it stepped back from is no longer ahead of it.
        await set_position(admin_db, org_id, env, deployed_sha=sha, redo_sha=None)
        # REQ-1527: an unchanged model is not an event. ``sha is None`` here means the tree matched
        # what the branch already held, so there is nothing for a pipeline to run against and
        # nothing to mirror -- announcing it would make the status stream noise.
        await announce(admin_db, org_id, env, sha, False)
    return sha


async def rebuild(
    conn: "Connection",
    admin_db: "Database",
    org_id: str,
    env: str,
    schema: str,
    actor: str | None = None,
) -> str | None:
    """Re-serialize every carried class and commit the result, clearing drift.

    The same act as a write-through, which is the point: the repair is not a second mechanism that
    can itself fall behind. Whatever the tree held, the next commit holds the whole model.
    """
    return await write_through(conn, admin_db, org_id, env, schema, f"rebuild {env}", actor)


def branches(org_id: str) -> list[str]:
    """Every branch in the org's repository, sorted. BROWSE lists refs and never paths."""
    repo = ensure_repo(org_id)
    return sorted(
        ref[len(b"refs/heads/") :].decode()
        for ref in repo.refs.keys()
        if ref.startswith(b"refs/heads/")
    )


def remote_branches(org_id: str) -> dict[str, str]:
    """What the last FETCH found on the org's remote: branch name -> sha (REQ-1541).

    Empty when nothing has been fetched, which is a different answer from "the remote has no
    branches" and is why this reads refs rather than the network: a list that dialled out would make
    every page load an act against somebody else's git host, with somebody else's token.
    """
    repo = ensure_repo(org_id)
    return {
        ref[len(REMOTE_PREFIX) :].decode(): sha.decode()
        for ref, sha in repo.refs.as_dict().items()
        if ref.startswith(REMOTE_PREFIX)
    }


def track_remote(org_id: str, refs: dict[Ref, ObjectID | None]) -> dict[str, str]:
    """Write ``refs`` -- a fetch result -- as this repository's remote-tracking branches.

    PRUNES: a tracking ref the fetch did not name is deleted, because the branch it named is gone
    from the remote and a stale ref would offer a deploy of a tree the org has already retired.

    Provisa imports the refs itself rather than letting dulwich do it, because dulwich imports them
    only for a remote configured in the repository, and configuring one would write the resolved
    push URL -- token included -- into a config file on disk. REQ-125 says the credential is
    resolved at the moment of use and never stored, and this is that rule surviving contact with a
    convenience.
    """
    repo = ensure_repo(org_id)
    fetched: dict[str, str] = {}
    for ref, sha in refs.items():
        if not ref.startswith(b"refs/heads/"):
            continue
        if sha is None:
            # dulwich's ref maps carry None for a name that resolves to no object. A fetch result
            # naming a branch the remote could not resolve is a broken answer, not a branch, and
            # tracking it would offer a deploy of a tree that does not exist.
            raise RepositoryError(f"the remote named {ref.decode()!r} with no sha")
        fetched[ref[len(b"refs/heads/") :].decode()] = sha.decode()
    for name, sha in fetched.items():
        repo.refs[remote_ref(name)] = ObjectID(sha.encode())
    for stale in set(remote_branches(org_id)) - set(fetched):
        del repo.refs[remote_ref(stale)]
    return fetched


def delete_branch(org_id: str, env: str) -> bool:
    """Drop ``env``'s branch. True when there was one, False when there was not (REQ-1542).

    THE REF GOES, THE HISTORY DOES NOT. Deleting a ref deletes a name; the commits stay in the
    object store and stay reachable by sha, which is what makes retiring a merged branch a safe
    default rather than a destruction. Somebody who kept a sha can still browse it and still deploy
    it, exactly as they could before the name went away.

    Called when a merge retires the environment it came from: a branch is the history of exactly one
    environment and is never shared with another (REQ-1554), so removing the environment and leaving
    the branch would leave a ref that no environment writes and no environment reads.
    """
    repo = ensure_repo(org_id)
    ref = _branch_ref(env)
    if repo.refs.read_ref(ref) is None:
        return False
    del repo.refs[ref]
    return True


def _object[T: ShaFile](repo: Repo, sha: ObjectID, kind: type[T]) -> T:
    """``sha``'s object, as the kind the caller resolved it as.

    A ref resolves to a commit and a tree entry declares its own mode, so the store answering with
    another kind means the projection on disk disagrees with itself. That is a repository error --
    the same answer BROWSE gives for a sha this node has never seen -- and never an object the
    caller carries on with.
    """
    obj = repo.get_object(sha)
    if not isinstance(obj, kind):
        raise RepositoryError(
            f"{sha.decode()} is a {obj.type_name.decode()}, not a {kind.type_name.decode()}"
        )
    return obj


def _resolve(repo: Repo, ref: str) -> ObjectID:
    """A branch name, a remote-tracking name, or a sha, as a commit sha. Refuses anything else.

    The load sources are a ref in this repository and a set of files the operator brings; there is
    no third one, and in particular no server filesystem for a tenant to name a path out of.

    ``origin/main`` resolves because that is the whole point of REQ-1541: the review happened on the
    org's git host, the merge landed there, and the deploy that follows names the tree the host now
    holds. It resolves only as far as the last FETCH brought it -- a remote-tracking ref is a
    picture, and Provisa never reaches out to refresh one while resolving it.
    """
    branch = repo.refs.read_ref(_branch_ref(ref))
    if branch is not None:
        return ObjectID(branch)
    if ref.startswith("origin/"):
        tracked = repo.refs.read_ref(remote_ref(ref[len("origin/") :]))
        if tracked is not None:
            return ObjectID(tracked)
    candidate = ObjectID(ref.encode())
    if len(candidate) == 40 and candidate in repo.object_store:
        return candidate
    raise RepositoryError(f"{ref!r} is neither a branch nor a commit in this repository")


def resolve_sha(org_id: str, ref: str) -> str:
    """The commit ``ref`` names right now.

    A LOAD pins this before it plans and applies the sha rather than the ref (REQ-1496): a branch
    that moves between the report and the write would make the report describe a commit nobody
    applied, and an approval name a tree nobody read.
    """
    return _resolve(ensure_repo(org_id), ref).decode()


def history(org_id: str, ref: str, limit: int = 100) -> list[dict[str, Any]]:
    """``ref``'s commits, newest first: what BROWSE shows and what LOAD picks from."""
    repo = ensure_repo(org_id)
    out: list[dict[str, Any]] = []
    sha: ObjectID | None = _resolve(repo, ref)
    while sha is not None and len(out) < limit:
        commit = _object(repo, sha, Commit)
        out.append(
            {
                "sha": sha.decode(),
                "author": commit.author.decode(),
                "message": commit.message.decode(),
                "committed_at": commit.commit_time,
            }
        )
        sha = commit.parents[0] if commit.parents else None
    return out


def tip(org_id: str, env: str) -> str | None:
    """The commit ``env``'s branch points at, or ``None`` when nothing has been committed to it."""
    repo = ensure_repo(org_id)
    ref = repo.refs.as_dict().get(_branch_ref(env))
    return ref.decode() if ref is not None else None


def distance(org_id: str, sha: str, base: str) -> int | None:
    """How many commits ``sha`` is past ``base``, or ``None`` when ``base`` is not behind it.

    ``0`` means they are the same commit, and ``None`` means the two are on lines that diverged --
    a different answer entirely, and the one that says a branch and its remote cannot be reconciled
    by moving one of them forward.
    """
    walk: str | None = sha
    seen = 0
    while walk is not None:
        if walk == base:
            return seen
        walk = parent_of(org_id, walk)
        seen += 1
    return None


def sync_state(org_id: str, env: str) -> dict[str, Any]:
    """WHETHER THIS BRANCH HOLDS WORK THE REMOTE DOES NOT (REQ-1546).

    Computed from refs rather than stored, and never from the network: the answer is about what
    this repository knows, so a page that renders it does not dial another organization's git host.
    ``behind`` is therefore as of the last FETCH, which is the same honesty ``remote_branches`` has.

    ``diverged`` is the case neither count can describe -- both lines have commits the other does
    not -- and it is reported rather than resolved, because resolving it means choosing whose work
    to move and that is not a decision a status call makes.
    """
    local = tip(org_id, env)
    remote = remote_branches(org_id).get(env)
    if local is None or remote is None:
        # One side has no branch at all, so there is no line to count along. The counts stay None --
        # unknown, not zero -- and ``unsynced`` still says the thing a person needs to act on.
        return {
            "local": local,
            "remote": remote,
            "ahead": None,
            "behind": None,
            "diverged": False,
            "unsynced": local != remote,
        }
    ahead = distance(org_id, local, remote)
    behind = distance(org_id, remote, local)
    return {
        "local": local,
        "remote": remote,
        "ahead": ahead,
        "behind": behind,
        "diverged": ahead is None and behind is None,
        "unsynced": local != remote,
    }


def merge_base(org_id: str, a: str, b: str) -> str | None:
    """THE LAST COMMIT BOTH LINES HELD — the third input a conflict is measured against (REQ-1555).

    Comparing two models tells you they differ. It cannot tell you WHO changed what, and that is the
    whole of the question a merge has to answer: a row the source carries and the target does not
    hold identically is either work the source did, which the merge is for, or work the TARGET did,
    which the merge is about to overwrite. Only a common ancestor separates the two.

    ``None`` when the lines share no ancestor at all — two environments each rooted by their own
    baseline rather than branched from one another. That is not "no conflicts": it is the statement
    that the question cannot be asked here, and callers report it as such rather than as a clean
    merge.
    """
    repo = ensure_repo(org_id)
    bases = find_merge_base(repo, [ObjectID(a.encode()), ObjectID(b.encode())])
    # Several bases means a criss-cross history, which the write-through cannot produce: a commit
    # it writes has one parent, and the only multi-parent commits here are merges recorded against
    # their target's line first. The first is taken for the same reason ``parent_of`` takes it.
    return bases[0].decode() if bases else None


def parent_of(org_id: str, sha: str) -> str | None:
    """The commit before ``sha`` on its own line, or ``None`` at the beginning of the branch.

    REQ-1543: this is the whole of what an undo has to know. The FIRST parent is taken because that
    is the line the environment travelled: a commit written by the write-through has exactly one,
    and one written by a merge has its target's history first.

    A sha this repository does not hold is a ``RepositoryError`` like every other browse verb, not a
    dulwich ``KeyError``: the repository is a PROJECTION, so the control plane naming a commit this
    node's object store has never seen is a state the callers have to be able to answer, and the
    BROWSE contract is where they already answer it.
    """
    repo = ensure_repo(org_id)
    commit = _object(repo, _resolve(repo, sha), Commit)
    return commit.parents[0].decode() if commit.parents else None


def step_toward(org_id: str, sha: str, top: str) -> str | None:
    """The next commit FORWARD from ``sha`` along the line that ends at ``top``.

    REQ-1543: a redo cannot be derived from ``sha`` alone -- history is append-only and the branch
    keeps growing, so a commit may have several descendants and "the child" is not an answer.
    Walking BACK from the position the undo departed from is: exactly one commit on that line has
    ``sha`` as its parent, and it is the one step forward the redo is asking for. ``None`` means
    ``top`` is not on ``sha``'s line at all -- the run of undos was abandoned by a later edit -- and
    the caller clears the cursor rather than guessing a direction.
    """
    walk: str | None = top
    while walk is not None:
        parent = parent_of(org_id, walk)
        if parent == sha:
            return walk
        walk = parent
    return None


def files_at(org_id: str, ref: str) -> dict[str, str]:
    """The tree at ``ref``, as the same ``path -> text`` mapping the projection produced."""
    repo = ensure_repo(org_id)
    commit = _object(repo, _resolve(repo, ref), Commit)
    files: dict[str, str] = {}

    def walk(tree_id: ObjectID, prefix: str) -> None:
        for entry in _object(repo, tree_id, Tree).items():
            name = f"{prefix}{entry.path.decode()}"
            if entry.mode == 0o040000:
                walk(ObjectID(entry.sha), f"{name}/")
            else:
                files[name] = _object(repo, ObjectID(entry.sha), Blob).data.decode()

    walk(commit.tree, "")
    return files


def branch(org_id: str, ref: str, env: str) -> str:
    """Point ``env``'s branch at ``ref`` and return the sha it now names.

    Creating an environment from a sha IS this: branching that sha, and then loading its tree into
    the new environment's schema (REQ-1496). Refuses to move a branch that already exists, because
    an environment's branch is written by the write-through and nothing else may reposition it
    under a running environment.
    """
    repo = ensure_repo(org_id)
    if repo.refs.read_ref(_branch_ref(env)) is not None:
        raise RepositoryError(f"branch {env!r} already exists in this repository")
    sha = _resolve(repo, ref)
    repo.refs[_branch_ref(env)] = sha
    return sha.decode()
