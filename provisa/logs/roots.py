# Copyright (c) 2026 Kenneth Stott
# Canary: 2340865d-b409-407c-885e-0cec0dce646c
#
# This source code is licensed under the Business Source License 1.1
"""Where a Logs source may read (REQ-1996). The maintainer's ruling: an operator setting
``logs.allowed_roots``, by default ``["/var/log"]``, lists the directories a glob may resolve
into; a glob, or a file it matches, that resolves outside them after following symlinks is
refused by name. A held organization (``sandbox_egress.held``: a sandbox org on any install, every
org but the deployment's own on a server of many) reads no log file on the server's own disk
outside its own folder (REQ-1997, through ``source_uploads.require_own_disk``).

Two reads of log files outside a held org's folder are allowed (REQ-2049), each still only inside
the allowed roots: a demo bundle's seeding run (the bundle build's throwaway sandbox tier), and a
sandbox-tier demo org -- a demo's template or a sandbox of it -- reading its own demo's files,
inside that demo's directory and nowhere else: a demo's logs are its own source, read live from
its bundle."""

from __future__ import annotations

import os
import re

from provisa.core import settings_registry

_WILDCARD = re.compile(r"[*?\[]")


class LogPathRefused(PermissionError):
    """A path a Logs source may not read; the message names it and why."""


def allowed_roots() -> list[str]:
    """The directories a glob may resolve into, as the operator listed them, each resolved."""
    return [
        os.path.realpath(os.path.expanduser(root))
        for root in settings_registry.value("logs.allowed_roots")
    ]


def _inside(path: str, roots: list[str]) -> bool:
    return any(path == root or path.startswith(root.rstrip(os.sep) + os.sep) for root in roots)


def _own_demos(path: str) -> bool:
    """Whether ``path`` is inside the directory of the demo the bound org is (a sandbox-tier
    template or sandbox), once its symlinks are followed."""
    from provisa.core.demo_bundles import own_demo_directory
    from provisa.core.request_context import current_org

    directory = own_demo_directory(current_org.get())
    if directory is None:
        return False
    return _inside(os.path.realpath(os.path.expanduser(path)), [os.path.realpath(directory)])


def _refuse_if_held(path: str) -> None:
    """Refuse ``path`` for a held org when it is outside the org's folder (REQ-1997), unless it is
    a seeding run's or the org's own demo's (REQ-2049)."""
    from provisa.core.demo_bundles import seeding_run
    from provisa.core.source_uploads import LocalPathRefused, require_own_disk

    if seeding_run() or _own_demos(path):
        return
    try:
        require_own_disk(path)
    except LocalPathRefused as exc:
        raise LogPathRefused(
            f"{path}: this organization reads no log file on the server's own disk outside its "
            "own folder"
        ) from exc


def checked_glob(glob: str) -> str:
    """``glob`` with ``~`` expanded, when the directory it starts from resolves inside an
    allowed root; refused by name otherwise, and for a held org outside its own folder."""
    _refuse_if_held(glob)
    expanded = os.path.expanduser(glob)
    if not os.path.isabs(expanded):
        raise LogPathRefused(f"{glob}: name the files by their full path")
    parts = expanded.split(os.sep)
    fixed = next((i for i, part in enumerate(parts) if _WILDCARD.search(part)), len(parts))
    base = os.sep.join(parts[:fixed]) or os.sep
    real = os.path.realpath(base)
    roots = allowed_roots()
    if not _inside(real, roots):
        raise LogPathRefused(
            f"{glob} resolves to {real}, outside the directories log files may be read from: "
            f"{', '.join(roots) or 'none are listed'}"
        )
    return expanded


def checked_file(path: str) -> str:
    """``path`` resolved, when it is inside an allowed root once its symlinks are followed (and,
    for a held org, inside its own folder); refused by name otherwise."""
    _refuse_if_held(path)
    real = os.path.realpath(path)
    roots = allowed_roots()
    if not _inside(real, roots):
        raise LogPathRefused(
            f"{path} resolves to {real}, outside the directories log files may be read from: "
            f"{', '.join(roots) or 'none are listed'}"
        )
    return real
