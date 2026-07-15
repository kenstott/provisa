# Copyright (c) 2026 Kenneth Stott
# Canary: 6b4d2e8f-1a3c-4b9d-8e70-2c5f9a0d3e61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL @encrypted directive marking for client-side decryption (REQ-692).

Columns declared ``encrypted: true`` in provisa.yaml surface in the GraphQL SDL with
an ``@encrypted`` directive. The backend still returns only the ciphertext envelope;
the Provisa GraphQL client wrapper reads this directive to learn which fields to
decrypt locally. This module collects the encrypted field names from config and
injects the directive into the printed SDL (graphql-core's ``print_schema`` does not
render custom field directives, so the annotation is a text post-process).
"""

from __future__ import annotations

import re
from typing import Any

ENCRYPTED_DIRECTIVE_SDL = "directive @encrypted on FIELD_DEFINITION"


def collect_encrypted_fields(config: Any) -> set[str]:
    """Return GraphQL field names of every column flagged ``encrypted`` in the config."""
    fields: set[str] = set()
    for table in getattr(config, "tables", None) or []:
        for col in getattr(table, "columns", None) or []:
            if getattr(col, "encrypted", False):
                fields.add(col.alias or col.name)
    return fields


def annotate_encrypted_sdl(sdl: str, encrypted_fields: set[str]) -> str:
    """Inject the ``@encrypted`` directive definition and mark flagged field lines.

    A field line ``  name: Type`` whose ``name`` is in ``encrypted_fields`` gets
    ``@encrypted`` appended. Idempotent — already-marked lines are left alone.
    """
    if not encrypted_fields:
        return sdl
    line_re = re.compile(r"^(\s+)([A-Za-z_]\w*)(\([^)]*\))?:\s*([\[\]\w!]+)\s*$")
    out_lines: list[str] = []
    for line in sdl.splitlines():
        m = line_re.match(line)
        if m and m.group(2) in encrypted_fields:
            out_lines.append(f"{line} @encrypted")
        else:
            out_lines.append(line)
    body = "\n".join(out_lines)
    if "directive @encrypted" in sdl:
        return body
    return f"{ENCRYPTED_DIRECTIVE_SDL}\n\n{body}"
