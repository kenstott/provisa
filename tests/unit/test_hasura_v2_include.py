# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-417: the Hasura v2 parser resolves the ``!include`` directive real exports use.

``hasura metadata export`` splits each table/function into its own file and references them from an
index (``databases/<db>/tables/tables.yaml``) as ``!include public_x.yaml`` — either the bare YAML
tag or a quoted string. Without resolution the parser sees opaque strings and crashes on ``.get()``.
These tests reproduce a REAL export's directory shape (verified against m-rgba/hasura-django-starter)
so the fix can't regress; both include spellings must load their per-table file.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.parser import parse_metadata_dir


def _write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def _real_export(tmp_path: Path, *, index_text: str) -> Path:
    """A metadata dir shaped like a real `hasura metadata export` (databases/ layout + per-table
    files behind an !include index). ``index_text`` supplies the tables.yaml index spelling."""
    md = tmp_path / "metadata"
    _write(md / "version.yaml", "version: 3\n")
    _write(
        md / "databases" / "databases.yaml",
        yaml.dump(
            [
                {
                    "name": "default",
                    "kind": "postgres",
                    "configuration": {"connection_info": {"database_url": {"from_env": "DB_URL"}}},
                    "tables": "!include default/tables/tables.yaml",
                }
            ]
        ),
    )
    tbls = md / "databases" / "default" / "tables"
    _write(tbls / "tables.yaml", index_text)
    _write(
        tbls / "public_customers.yaml",
        yaml.dump(
            {
                "table": {"schema": "public", "name": "customers"},
                "select_permissions": [
                    {"role": "user", "permission": {"columns": ["id", "name"], "filter": {}}}
                ],
            }
        ),
    )
    _write(
        tbls / "public_orders.yaml",
        yaml.dump(
            {
                "table": {"schema": "public", "name": "orders"},
                "object_relationships": [
                    {"name": "customer", "using": {"foreign_key_constraint_on": "customer_id"}}
                ],
                "select_permissions": [
                    {"role": "user", "permission": {"columns": ["id", "customer_id"], "filter": {}}}
                ],
            }
        ),
    )
    return md


_QUOTED_INDEX = '- "!include public_orders.yaml"\n- "!include public_customers.yaml"\n'
_BARE_TAG_INDEX = "- !include public_orders.yaml\n- !include public_customers.yaml\n"


def test_include_index_quoted_string_form_resolves(tmp_path: Path) -> None:
    md = _real_export(tmp_path, index_text=_QUOTED_INDEX)
    metadata = parse_metadata_dir(md)
    tables = [t for s in metadata.sources for t in s.tables]
    assert {t.name for t in tables} == {"orders", "customers"}
    orders = next(t for t in tables if t.name == "orders")
    # the relationship + permission inside the included file survived resolution
    assert [r.name for r in orders.object_relationships] == ["customer"]
    assert orders.select_permissions and orders.select_permissions[0].role == "user"


def test_include_index_bare_tag_form_resolves(tmp_path: Path) -> None:
    # yaml.safe_load alone would raise on the unknown `!include` tag; _HasuraLoader tolerates it.
    md = _real_export(tmp_path, index_text=_BARE_TAG_INDEX)
    metadata = parse_metadata_dir(md)
    assert {t.name for s in metadata.sources for t in s.tables} == {"orders", "customers"}


def test_included_tables_flow_through_to_provisa_config(tmp_path: Path) -> None:
    # End-to-end: parse the real-shaped export and convert — the included tables must reach the
    # Provisa config (a source + its tables), not vanish at the include boundary.
    md = _real_export(tmp_path, index_text=_QUOTED_INDEX)
    metadata = parse_metadata_dir(md)
    cfg = convert_metadata(metadata)  # -> ProvisaConfig
    table_names = {t.table_name for t in cfg.tables}
    assert {"orders", "customers"} <= table_names
