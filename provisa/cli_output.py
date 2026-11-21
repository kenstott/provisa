# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2f4a17-8c3b-4e60-9f12-b7a5c1e03d94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One output option and one filter for every command (REQ-2222).

``--output json|yaml|table`` and ``--filter PATH`` apply to whatever a command prints. A command
writes its document as it always has; this reads the document back and shapes it, so no command
carries code for either. Without either option a command's output is left exactly as it was.

The filter is a path: ``a.b``, ``a[0]``, ``a[]`` for each item, ``a[key=value]`` for the items
that match. What is not there is ``null``. A refusal is never filtered: a filter is written for
the answer, and it must not swallow the code that says there was none.
"""

# Requirements: REQ-2222

from __future__ import annotations

import argparse
import contextlib
import io
import json
import math
import re
import sys
from dataclasses import dataclass
from typing import Any

import yaml

FORMATS = ("json", "yaml", "table")
NOT_STRUCTURED = 4

_STEP = re.compile(
    r"\.?(?P<name>[A-Za-z_][\w-]*)"
    r"|\[(?P<index>-?\d+)\]"
    r"|(?P<each>\[\])"
    r"|\[(?P<key>[A-Za-z_][\w-]*)=(?P<value>[^\]]*)\]"
)


class FilterError(ValueError):
    """A filter that is not a path."""


def parse_filter(text: str) -> list[re.Match[str]]:
    """The steps of ``text``, refused here so that a bad filter stops a command before it runs."""
    steps: list[re.Match[str]] = []
    position = 0
    while position < len(text):
        step = _STEP.match(text, position)
        if step is None:
            raise FilterError(f"--filter: cannot read {text!r} at position {position}")
        steps.append(step)
        position = step.end()
    if not steps:
        raise FilterError("--filter: the path is empty")
    return steps


def _matches(value: Any, wanted: str) -> bool:
    if isinstance(value, bool) or value is None:
        return json.dumps(value) == wanted
    if isinstance(value, (int, float)):
        try:
            return math.isclose(float(value), float(wanted), rel_tol=1e-14)
        except ValueError:
            return False
    return str(value) == wanted


def apply_filter(document: Any, steps: list[re.Match[str]]) -> Any:
    if not steps:
        return document
    step, rest = steps[0], steps[1:]
    if step["name"] is not None:
        return apply_filter(document.get(step["name"]), rest) if _is_map(document) else None
    if not isinstance(document, list):
        return None
    if step["index"] is not None:
        index = int(step["index"])
        return (
            apply_filter(document[index], rest) if -len(document) <= index < len(document) else None
        )
    if step["each"] is not None:
        return [apply_filter(item, rest) for item in document]
    chosen = [i for i in document if _is_map(i) and _matches(i.get(step["key"]), step["value"])]
    return [apply_filter(item, rest) for item in chosen]


def _is_map(value: Any) -> bool:
    return isinstance(value, dict)


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _rows(rows: list[list[str]]) -> list[str]:
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    return ["  ".join(c.ljust(w) for c, w in zip(row, widths)).rstrip() for row in rows]


def _table(document: Any) -> list[str]:
    if isinstance(document, dict):
        return _rows([[str(k), _cell(v)] for k, v in document.items()]) if document else []
    if not isinstance(document, list):
        return [_cell(document)]
    if not document:
        return []
    if not all(_is_map(i) for i in document):
        return [_cell(i) for i in document]
    columns = list(dict.fromkeys(k for item in document for k in item))
    return _rows([columns, *([_cell(item.get(c)) for c in columns] for item in document)])


def render(document: Any, output: str) -> None:
    if output == "json":
        print(json.dumps(document, sort_keys=True, default=str))
    elif output == "yaml":
        sys.stdout.write(yaml.safe_dump(document, sort_keys=False))
    else:
        for line in _table(document):
            print(line)


def _is_refusal(document: Any) -> bool:
    if not _is_map(document):
        return False
    return (
        "refused" in document
        or document.get("status") in ("refused", "failed")
        or {"code", "detail"} <= document.keys()
    )


def _read_document(text: str) -> Any:
    """The document a command printed: JSON, or the YAML an export is. Anything else is prose."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(text) from exc
    if not isinstance(parsed, (dict, list)):
        raise ValueError(text)
    return parsed


@dataclass(frozen=True)
class Shape:
    """What the command line asked of the output."""

    output: str
    steps: list[re.Match[str]] | None

    @classmethod
    def of(cls, args: argparse.Namespace) -> "Shape | None":
        output, text = getattr(args, "output", None), getattr(args, "filter", None)
        if output is None and text is None:
            return None
        return cls(output or "json", None if text is None else parse_filter(text))


def run_shaped(args: argparse.Namespace, shape: Shape) -> int:
    """Run the command, then print what it printed as ``shape`` asks. Its exit code stands."""
    args.json = True
    raw = io.BytesIO()
    captured = io.TextIOWrapper(raw, encoding="utf-8", write_through=True)
    try:
        with contextlib.redirect_stdout(captured):
            code = args.func(args)
    except BaseException:
        sys.stdout.write(raw.getvalue().decode(errors="replace"))
        raise
    text = raw.getvalue().decode(errors="replace")
    if not text.strip():
        return code
    try:
        document = _read_document(text)
    except ValueError:
        print(f"provisa {args.command}: no structured output to shape:\n{text}", file=sys.stderr)
        return NOT_STRUCTURED
    if shape.steps is not None and not _is_refusal(document):
        document = apply_filter(document, shape.steps)
    render(document, shape.output)
    return code
