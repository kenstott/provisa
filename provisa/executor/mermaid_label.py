# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6c1d2a-8e47-4b9a-9c55-2b7e1a04d913
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Mermaid label and node-id escaping for the server-built query charts.

Every chart Provisa draws (the GraphQL and SQL stats DAGs, the Analyze plan tree) puts engine
plan text, source ids, field names and route reasons inside Mermaid labels. Mermaid reads a
label as a quoted string, so a label is written in that form and any character with meaning
to the flowchart grammar or to Mermaid's entity and HTML handling is replaced by its numeric
entity code (``#NN;``), which Mermaid decodes back to the character when it draws the label.
"""

from __future__ import annotations

import re

# Longest label text drawn in a node or on an edge; the full text stays in the plan list beside
# the diagram.
MAX_LABEL = 80

# Characters written as an entity code. The replacement runs once per character, so the ``#``
# of an inserted code is never escaped again. Backslash: ``\n`` inside a label is a line
# break to Mermaid, so plan text holding a literal backslash would otherwise break lines.
_ENTITY = {ch: f"#{ord(ch)};" for ch in '#"<>[]{}|;`\\&'}

_WHITESPACE = re.compile(r"\s+")
_ID_UNSAFE = re.compile(r"[^A-Za-z0-9_]")


def label(text: str, limit: int = MAX_LABEL) -> str:
    """``text`` as the body of a quoted Mermaid label: one line, at most ``limit`` characters.

    Text over the limit is cut and ends in an ellipsis. Cutting happens before escaping, so an
    entity code is never split.
    """
    flat = _WHITESPACE.sub(" ", text).strip()
    if not flat:
        # Mermaid rejects an empty quoted label (a plan line with no text, an unnamed operator);
        # it is drawn as one non-breaking space.
        return "#160;"
    if len(flat) > limit:
        flat = flat[: limit - 1].rstrip() + "…"
    return "".join(_ENTITY.get(ch, ch) for ch in flat)


def node_id(text: str) -> str:
    """A Mermaid node id for ``text``: ``n_`` plus the text with every non-word character as ``_``.

    The prefix keeps a source or field named like a flowchart keyword (``end``, ``graph``,
    ``subgraph``) from being read as one.
    """
    return "n_" + _ID_UNSAFE.sub("_", text)
