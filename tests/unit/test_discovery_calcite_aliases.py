# Copyright (c) 2026 Kenneth Stott
# Canary: 5c9e2a74-81d6-4b30-a7f5-3e0b6d1c8942
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The constraint queries run on a Calcite pgwire server (a files, Kaggle, Splunk or SharePoint
source): Calcite's SQL parser reserves REF, so a table aliased ``ref`` failed registration with
'Encountered "ref"' and the table never listed."""

from __future__ import annotations

import re

from provisa.discovery import declared_keys, fk_introspect

# Words Calcite's parser reserves that a query written for PostgreSQL could name a table with.
_CALCITE_RESERVED = ("ref", "value", "rank", "row", "type", "year", "month", "day")


def test_fk_queries_alias_no_calcite_reserved_word() -> None:
    for sql in (fk_introspect._PG_OUTBOUND, fk_introspect._PG_INBOUND):
        table_aliases = {
            m.lower() for m in re.findall(r"information_schema\.\w+\s+AS\s+(\w+)", sql, re.I)
        }
        assert table_aliases == {"tc", "kcu", "rc", "pk"}
        assert not table_aliases & set(_CALCITE_RESERVED)


def test_declared_foreign_key_query_aliases_no_calcite_reserved_word() -> None:
    aliases = {
        m.lower()
        for m in re.findall(
            r"information_schema\.\w+\s+AS\s+(\w+)", declared_keys._INFO_SCHEMA_FOREIGN, re.I
        )
    }
    assert aliases == {"rc", "kcu", "pk"}
    assert not aliases & set(_CALCITE_RESERVED)
