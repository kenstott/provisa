# Copyright (c) 2026 Kenneth Stott
# Canary: be1b6b37-14a9-4f5d-a59f-90cc4754ef70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The PostgreSQL driver reads a range or multirange as the text PostgreSQL wrote of it, the form a
range is held in everywhere (REQ-2171, RANGES ARE EMULATED ON EVERY ENGINE; REQ-2167).

psycopg's own loaders read one as its Range object, whose str() is no PostgreSQL text: table_to_xml
wrote an unbounded int4range of a PostgreSQL source as '(None, 4)', where PostgreSQL 17.11 writes
'(,4)'."""

# Requirements: REQ-2171, REQ-2167

from __future__ import annotations

import psycopg
import pytest
from psycopg.adapt import AdaptersMap, Transformer
from psycopg.pq import Format

from provisa.executor.drivers.postgresql import _configure


class _Conn:
    """What _configure sets up on a connection: its adapters and its prepared-statement cache."""

    def __init__(self) -> None:
        self.adapters = AdaptersMap(psycopg.adapters)
        self.connection = None
        self.prepared_max = 0


#: (type, PostgreSQL 17.11's text of a value of it)
TEXTS = [
    ("int4range", "(,4)"),
    ("int4range", "[1,5)"),
    ("int8range", "empty"),
    ("numrange", "(,3]"),
    ("daterange", "[2024-01-01,2024-02-01)"),
    ("tsrange", '(,"2000-01-01 00:00:00"]'),
    ("tstzrange", '["2024-01-01 00:00:00+00","2024-02-01 00:00:00+00")'),
    ("int4multirange", "{[1,5),[7,9)}"),
    ("int8multirange", "{(,4)}"),
    ("nummultirange", "{[1.5,2.5)}"),
    ("datemultirange", "{}"),
    ("tsmultirange", '{["2024-01-01 00:00:00","2024-02-01 00:00:00")}'),
    ("tstzmultirange", '{(,"2000-01-01 00:00:00+00"]}'),
]


@pytest.mark.parametrize("name,text", TEXTS)
def test_a_range_is_read_as_the_text_postgresql_wrote(name: str, text: str):
    conn = _Conn()
    _configure(conn)  # type: ignore[arg-type]
    loader = Transformer(conn).get_loader(psycopg.adapters.types[name].oid, Format.TEXT)  # type: ignore[arg-type]
    assert loader.load(text.encode()) == text
