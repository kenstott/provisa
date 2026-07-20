# Copyright (c) 2026 Kenneth Stott
# Canary: 4b3cacd2-d885-4345-82e1-3a2c1c0df4c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""MkDocs build hooks.

Auto-link every ``REQ-NNN`` mention across the docs to its anchored section on
the Requirements page (`arch/requirements.md`, one `{#REQ-NNN}` anchor per
requirement). Only ids that actually exist as anchors are linked, so there are
no dangling links; the Requirements page itself is skipped (the generator
already cross-links it) and matches inside code spans / fenced blocks are left
untouched so command samples like `provisa start` stay literal.
"""

from __future__ import annotations

import posixpath
import re

# The Requirements page, relative to the docs dir. Its anchors are the link
# targets; a page at any depth links to it via a computed relative path.
REQUIREMENTS_SRC = "arch/requirements.md"

_ANCHOR_RE = re.compile(r"\{#(REQ-\d+)\}")
# A bare REQ-NNN mention: not already part of a link label (`[`), an anchor
# (`#`), or a longer token, and not immediately closing a label (`]`).
_MENTION_RE = re.compile(r"(?<![\[\w#-])(REQ-\d+)(?![\]\w-])")
# Fenced code blocks (``` … ```) and inline code (` … `) — never rewritten.
_CODE_RE = re.compile(r"```.*?```|``.*?``|`[^`]*`", re.DOTALL)

_KNOWN: set[str] = set()


def on_config(config, **_kwargs):
    """Load the set of real requirement ids from the generated page."""
    _KNOWN.clear()
    req_path = posixpath.join(config["docs_dir"], REQUIREMENTS_SRC)
    with open(req_path, encoding="utf-8") as fh:
        _KNOWN.update(_ANCHOR_RE.findall(fh.read()))
    return config


def _linkify_text(text: str, rel_target: str) -> str:
    def repl(m: re.Match[str]) -> str:
        req = m.group(1)
        if req not in _KNOWN:
            return req
        return f"[{req}]({rel_target}#{req})"

    return _MENTION_RE.sub(repl, text)


def on_page_markdown(markdown: str, *, page, **_kwargs) -> str:
    src = page.file.src_path.replace("\\", "/")
    if src == REQUIREMENTS_SRC or not _KNOWN:
        return markdown

    rel_target = posixpath.relpath(REQUIREMENTS_SRC, posixpath.dirname(src))

    # Walk the source, skipping code spans/blocks so only prose is rewritten.
    out: list[str] = []
    last = 0
    for m in _CODE_RE.finditer(markdown):
        out.append(_linkify_text(markdown[last : m.start()], rel_target))
        out.append(m.group(0))
        last = m.end()
    out.append(_linkify_text(markdown[last:], rel_target))
    return "".join(out)
