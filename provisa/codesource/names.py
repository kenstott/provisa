# Copyright (c) 2026 Kenneth Stott
# Canary: 1e26b50c-9cb5-48ab-b318-425ef8df26e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The name tables of a code source (REQ-2100): for each ``@table`` and ``@stream`` function,
every column and parameter as the code declares it, beside each form it takes elsewhere.

- ``name``: as declared, the only name the host uses with the code (its pushdown, the values it
  passes, its residual, its Arrow batches);
- ``sql``: what Provisa's catalog calls it, by Provisa's own live mapper
  (:func:`provisa.compiler.naming.apply_sql_name`), never a second implementation;
- ``quoted`` and ``folded``: its pgwire forms, the exact quoted identifier and the case-folded one
  an unquoted identifier arrives as;
- ``type``: its IR type, as its catalog column is typed.

Each table also carries its function's row ``schema``, the Arrow schema a call of it gives
(provisa/codesource/arrow_rows.py), for a front end that calls the function itself (pgwire).

A parameter is a column too, ``_nf_{name}``, as every source's parameters are. The tables are made
at Build image and at dev-mode discovery, baked into the image and reported in its hello; at
runtime they are only looked up. Two names one wire form cannot tell apart are refused here, so an
ambiguous image is never built.
"""

# Requirements: REQ-2100
from __future__ import annotations

from typing import Any

from provisa.codesource.manifest import (
    LOADER,
    Anonymous,
    Manifest,
    ManifestRefused,
    Named,
    Shape,
)
from provisa.core.parameters import NF_PREFIX


def _forms(name: str, shape: Shape) -> dict[str, str]:
    from provisa.codesource.discovery import data_type
    from provisa.codesource.manifest import ListOf
    from provisa.compiler.naming import apply_sql_name

    return {
        "name": name,
        "sql": apply_sql_name(name),
        "quoted": '"' + name.replace('"', '""') + '"',
        "folded": name.lower(),
        "type": "list" if isinstance(shape, ListOf) else data_type(shape),
    }


def name_tables(manifest: Manifest) -> dict[str, dict[str, Any]]:
    """Each row-returning function's name table, by function; refused with every collision."""
    from provisa.codesource.arrow_rows import row_schema, schema_text

    problems: list[str] = []
    tables: dict[str, dict[str, Any]] = {}
    for export in manifest.exports:
        if export.kind not in ("table", "stream") or export.preview:
            continue
        row = export.returns
        fields = (
            manifest.types[row.name].fields
            if isinstance(row, Named)
            else row.fields
            if isinstance(row, Anonymous)
            else ()
        )
        columns = [_forms(f.name, f.shape) for f in fields]
        parameters = [
            {**_forms(f"{NF_PREFIX}{p.name}", p.shape), "parameter": p.name}
            for p in export.parameters
            # REQ-2100: a join's keys reach the loader; a statement gives it no argument.
            if not (export.loader and p.name == LOADER)
        ]
        for form in ("folded", "sql"):
            seen: dict[str, str] = {}
            for entry in columns + parameters:
                other = seen.setdefault(entry[form], entry["name"])
                if other != entry["name"]:
                    problems.append(
                        f"{export.name}: {other} and {entry['name']} are both {entry[form]} "
                        + ("on pgwire" if form == "folded" else "in the catalog")
                    )
        tables[export.name] = {
            "schema": schema_text(row_schema(manifest, export)),
            "columns": columns,
            "parameters": parameters,
            "pushdown": {
                p.name: {"column": p.pushes.column, "op": p.pushes.op}
                for p in export.parameters
                if p.pushes is not None
            },
        }
    if problems:
        raise ManifestRefused(problems)
    return tables
