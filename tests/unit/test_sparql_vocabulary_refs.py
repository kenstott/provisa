# Copyright (c) 2026 Kenneth Stott
# Canary: 1627e725-188a-401a-8fea-e8fa080e34f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What SPARQL's vocabulary reads of the glossary (REQ-2135, REQ-2141): the class term of each
grounded table and the terms of each column, of live terms only -- a term no consuming surface may
offer (undefined, retired) names nothing in SPARQL either."""

# Requirements: REQ-2135, REQ-2141, REQ-1387

import pytest

from provisa.core.repositories import glossary as glossary_repo
from tests.unit.test_glossary_class_grounding import _surface


@pytest.mark.asyncio
async def test_live_terms_ground_classes_and_columns(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        async with db.acquire() as conn:
            await glossary_repo.ground_table(conn, ids["party"], ids["customers"])
            note = await glossary_repo.get_term_by_ref(conn, ids["customers"], "note")
            assert note is not None

            # Neither is defined yet: proposals, offered nowhere.
            assert await glossary_repo.vocabulary_refs(conn) == ({}, {})

            await glossary_repo.set_definition(conn, ids["party"], "Someone we deal with.")
            await glossary_repo.set_definition(conn, note["id"], "A free-text remark.")
            table_terms, column_terms = await glossary_repo.vocabulary_refs(conn)
            assert table_terms == {ids["customers"]: "party"}
            assert column_terms[(ids["customers"], "note")] == [note["name"]]

            # Retired, party's table goes to its successor, which is not defined yet (REQ-2144).
            client = await glossary_repo.create_abstract_term(conn, "client", domains={"sales"})
            await glossary_repo.retire_term(
                conn, ids["party"], reason="renamed", successor=client, by=None
            )
            table_terms, _ = await glossary_repo.vocabulary_refs(conn)
            assert table_terms == {}
