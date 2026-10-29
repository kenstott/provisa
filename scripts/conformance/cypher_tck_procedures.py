# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2b9e41-3c86-4a5f-9e17-b04c6a8f2d93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A TCK test procedure as a command of the fixture model (REQ-2167, amended 2026-10-27;
REQ-2001), for scripts/conformance/cypher_tck.py.

A scenario's ``there exists a procedure test.my.proc(name :: STRING?) :: (out :: INTEGER?)``
with its table of rows is setup, so it is adapted, not excluded: Provisa never creates a
procedure through a query surface, and a Cypher CALL invokes a command (REQ-872, REQ-2168). The
procedure becomes a statement command (REQ-2001) of the fixture model: the procedure's arguments
are its arguments, its outputs its columns, and its statement answers the rows of the scenario's
table whose inputs are the call's arguments, in the table's order. Its Cypher binding is the
procedure's name. It is in the domain ``tck_empty`` and assigned to every role, so both the HTTP
runner (``org_admin``, which reaches every domain) and a Bolt session (``tck_empty``) call it.

The TCK declares one name (``test.my.proc``) with a different signature in different scenarios,
and two commands are never bound to one name. So the model holds one procedure at a time: each is
declared by putting the fixture model with its command (``PUT /admin/config``, as the conformance
instance loads it) before the scenarios that declare it run. A model put adds and updates and
removes nothing (REQ-1919), so the command keeps its name from one declaration of the procedure
to the next and is replaced; the runner runs the procedure scenarios after every other one, so a
scenario calling an unknown procedure meets none.

A procedure with no outputs (VOID) is a VOID command (REQ-2001, amended 2026-10-28): it declares
no output columns, so a call runs it and answers an empty result with no columns; its statement
reads each of its arguments and nothing else."""

# Requirements: REQ-2167, REQ-2001

from __future__ import annotations

import base64
import json
import re
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

#: The domain a procedure's command is in: the TCK session role's own, which holds no table.
DOMAIN = "tck_empty"

_SIGNATURE = re.compile(r"^([A-Za-z_][\w.]*)\s*\((.*?)\)\s*::\s*\((.*?)\)$")
_FIELD = re.compile(r"^([A-Za-z_]\w*)\s*::\s*([A-Z]+)\??$")

#: A TCK type: the IR type of its command argument and the SQL type its values are cast to.
_TYPES = {
    "STRING": ("text", "TEXT"),
    "INTEGER": ("bigint", "BIGINT"),
    "FLOAT": ("double", "DOUBLE PRECISION"),
    "NUMBER": ("double", "DOUBLE PRECISION"),
    "BOOLEAN": ("boolean", "BOOLEAN"),
}


class NotACommand(ValueError):
    """A TCK procedure no command expresses, said naming why."""


@dataclass(frozen=True)
class Procedure:
    name: str
    inputs: tuple[tuple[str, str], ...]  # (name, TCK type)
    outputs: tuple[tuple[str, str], ...]
    rows: tuple[tuple[Any, ...], ...]  # inputs then outputs, in the table's order


def _fields(text: str, signature: str) -> tuple[tuple[str, str], ...]:
    out = []
    for part in (p.strip() for p in text.split(",") if p.strip()):
        m = _FIELD.match(part)
        if m is None or m.group(2) not in _TYPES:
            raise NotACommand(
                f"the procedure {signature!r} declares {part!r}, of no type read here"
            )
        out.append((m.group(1), m.group(2)))
    return tuple(out)


def parse(text: str, table: list[list[str]] | None, read: Any) -> Procedure:
    """The procedure a ``there exists a procedure`` step declares: ``text`` is the step's text
    after those words, ``table`` its rows (a header of inputs then outputs), ``read`` reads a TCK
    value."""
    m = _SIGNATURE.match(text.strip())
    if m is None:
        raise NotACommand(f"the procedure {text!r} has no signature read here")
    inputs, outputs = _fields(m.group(2), text), _fields(m.group(3), text)
    header, *body = table or [[]]
    names = [n for n, _ in inputs + outputs]
    if [c for c in header if c] != names:
        raise NotACommand(f"the procedure {m.group(1)}'s table is {header}, its signature {names}")
    rows = tuple(tuple(read(c) for c in row) for row in body) if names else ()
    for value in (v for row in rows for v in row):
        if not (value is None or isinstance(value, (bool, int, float, str))):
            raise NotACommand(f"the procedure {m.group(1)}'s value {value!r} is no scalar")
    return Procedure(m.group(1), inputs, outputs, rows)


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + value.replace("'", "''") + "'"


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def statement(proc: Procedure) -> str:
    """The SQL of the procedure's command: the table's rows, numbered in order, those whose inputs
    are the call's arguments (null matching null), as its outputs."""
    columns = proc.inputs + proc.outputs
    rows = proc.rows or (tuple(None for _ in columns),)
    first = 1 if proc.rows else 0  # a table with no rows: one row of nulls, numbered out
    selects = [
        "SELECT "
        + ", ".join(
            [f'{n} AS "_n"']
            + [
                f"CAST({_literal(v)} AS {_TYPES[t][1]}) AS {_quoted(c)}"
                for (c, t), v in zip(columns, row, strict=True)
            ]
        )
        for n, row in enumerate(rows, start=first)
    ]
    where = ['"_n" > 0'] + [
        f"{_quoted(c)} IS NOT DISTINCT FROM CAST({{{{{c}}}}} AS {_TYPES[t][1]})"
        for c, t in proc.inputs
    ]
    return (
        f"SELECT {', '.join(_quoted(c) for c, _ in proc.outputs)} "
        f'FROM ({" UNION ALL ".join(selects)}) AS "t" '
        f'WHERE {" AND ".join(where)} ORDER BY "_n"'
    )


def void_statement(proc: Procedure) -> str:
    """The SQL of a VOID procedure's command: one row of its arguments, which its call does not
    answer (every declared argument is used, REQ-2001)."""
    items = ['1 AS "_n"'] + [
        f"CAST({{{{{c}}}}} AS {_TYPES[t][1]}) AS {_quoted(c)}" for c, t in proc.inputs
    ]
    return f"SELECT {', '.join(items)}"


def command(proc: Procedure) -> dict:
    """The procedure as a statement command of the fixture model, bound in Cypher to its name;
    a VOID procedure as a VOID command, declaring no output columns."""
    name = "tck_" + proc.name.replace(".", "_")
    void = {"output_columns": []} if not proc.outputs else {}
    return {
        "name": name,
        "function_name": name,
        "impl_kind": "statement",
        "kind": "query",
        "domain_id": DOMAIN,
        "description": f"The openCypher TCK's test procedure {proc.name}",
        "source_id": "",
        "returns": "",
        # Every role: org_admin over HTTP, tck_empty in a Bolt session.
        "visible_to": [],
        "arguments": [{"name": c, "type": _TYPES[t][0]} for c, t in proc.inputs],
        "language_bindings": {"cypher": {"name": proc.name}},
        "binding": {"sql": statement(proc) if proc.outputs else void_statement(proc)},
        **void,
    }


# --- declaring a procedure ---------------------------------------------------------------------

#: The instance, its fixture model as it booted, and the break-glass account that puts a model
#: (main sets them); the procedure the model holds now.
_MODEL: dict[str, Any] = {}


def configure(base: str, model: Path, username: str, password: str) -> None:
    import yaml

    _MODEL.update(
        base=base,
        model=yaml.safe_load(model.read_text()),
        authorization="Basic " + base64.b64encode(f"{username}:{password}".encode()).decode(),
        held=None,
    )


def configured() -> bool:
    return bool(_MODEL)


def _put(body: str) -> dict:
    req = urllib.request.Request(
        f"{_MODEL['base']}/admin/config",
        data=body.encode(),
        headers={"Content-Type": "application/yaml", "Authorization": _MODEL["authorization"]},
        method="PUT",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read() or b"null")


def declare(proc: Procedure) -> None:
    """The fixture model holding ``proc``'s command, put unless it holds it already."""
    import yaml

    if not configured():
        raise NotACommand(
            "no fixture model to declare its procedure in (CONFORMANCE_MODEL is not set)"
        )
    if _MODEL["held"] == proc:
        return
    entry = command(proc)
    answer = _put(yaml.safe_dump({**_MODEL["model"], "functions": [entry]}, sort_keys=False))
    if not (isinstance(answer, dict) and answer.get("success")):
        raise ValueError(f"the model with {proc.name} was not applied: {answer}")
    _MODEL["held"] = proc
