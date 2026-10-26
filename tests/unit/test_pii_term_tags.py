# Copyright (c) 2026 Kenneth Stott
# Canary: 6feef399-21b3-4838-97ca-94ef58436677
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2145: pii is on columns, and on a Property term, whose every column carries it.

Only a defined, in-service term (live or deprecated) carries a tag and passes it on; a draft or
retired term governs nothing. Each column's tag says where it comes from. A column marked pii by
hand says why. A column that is pii through its term cannot be unmarked on its own. A tagged term
is curated work, and its definition cannot be cleared while it carries a tag. What a column carries
through its term is what hides it.
"""

# Requirements: REQ-2145

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import update

from provisa.api.admin import glossary_router, schema_mutation
from provisa.api.admin.types import TagAssignmentInput
from provisa.core.models import SYSTEM_TAGS, TagAssignment
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import glossary_terms
from provisa.core.term_tags import TermTagRefused, effective_column_tags, refuse_term_tag
from provisa.security.sensitive import sensitive_columns
from tests.unit.test_pii_term_moves import _define, _model, _move, _table_id, _tbl

_STATUSES = {
    # status: (definition, deprecated, retired)
    "draft": ("", False, False),
    "live": ("Where a person lives", False, False),
    "deprecated": ("Where a person lives", True, False),
    "retired": ("Where a person lives", False, True),
}


async def _set_status(conn, term_id: int, status: str) -> None:
    definition, deprecated, retired = _STATUSES[status]
    await conn.execute_core(
        update(glossary_terms)
        .where(glossary_terms.c.id == term_id)
        .values(definition=definition, deprecated=deprecated, retired=retired)
    )


async def _pii_of(conn, table_id: int, column: str) -> list[dict]:
    tags = (await effective_column_tags(conn, {table_id})).get((table_id, column), [])
    return [t for t in tags if t["base_tag_id"] == "pii"]


# --- inheritance ---------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "carried"),
    [("draft", False), ("live", True), ("deprecated", True), ("retired", False)],
)
async def test_only_a_defined_term_in_service_passes_its_tag_on(
    control_plane, monkeypatch, status, carried
):
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            await _set_status(conn, ids["address"], status)
            for table in ("customers", "accounts"):
                assert bool(await _pii_of(conn, ids[table], "home_address")) is carried
                # sensitive (REQ-1943) alike
                assert ("home_address" in await sensitive_columns(conn, ids[table])) is carried


@pytest.mark.asyncio
async def test_each_columns_pii_says_where_it_comes_from(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            await tag_repo.assign(
                conn,
                TagAssignment(
                    tag_id="pii",
                    object_type="column",
                    table_id=ids["notes"],
                    column_name="body",
                    reason="notes quote people",
                ),
            )
            assert await _pii_of(conn, ids["customers"], "home_address") == [
                {
                    "tag_id": "pii",
                    "base_tag_id": "pii",
                    "via": "term",
                    "term": "home address",
                    "term_id": ids["address"],
                }
            ]
            assert await _pii_of(conn, ids["notes"], "body") == [
                {
                    "tag_id": "pii",
                    "base_tag_id": "pii",
                    "via": "column",
                    "reason": "notes quote people",
                }
            ]


@pytest.mark.asyncio
async def test_a_column_that_comes_to_mean_the_term_carries_its_tag(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            await table_repo.upsert(conn, _tbl("suppliers", "home_address", keyed=True))
            suppliers = await _table_id(conn, "suppliers")
            assert [t["term"] for t in await _pii_of(conn, suppliers, "home_address")] == [
                "home address"
            ]


@pytest.mark.asyncio
async def test_moving_a_column_off_the_term_removes_the_tag_it_carried(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (db, ids):
        await _move(ids, confirm_tag_loss=["pii"])
        async with db.acquire() as conn:
            assert await _pii_of(conn, ids["customers"], "home_address") == []
            assert await _pii_of(conn, ids["accounts"], "home_address") != []


# --- what a term may carry -----------------------------------------------------------------------


def test_pii_goes_on_columns_and_terms_and_a_column_marked_by_hand_says_why():
    (pii,) = [t for t in SYSTEM_TAGS if t.id == "pii"]
    assert pii.applies_to == ["column", "term"]
    assert pii.reason_policy == "required"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["draft", "retired"])
async def test_a_term_not_defined_and_in_service_is_refused(control_plane, monkeypatch, status):
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            await _set_status(conn, ids["body"], status)
            with pytest.raises(TermTagRefused) as refused:
                await refuse_term_tag(conn, ids["body"], "pii")
    assert refused.value.code == "tag.term_not_live"
    assert str(refused.value) == "Define 'body' to mark it"


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["class", "concept"])
async def test_a_term_that_is_not_a_property_is_refused(control_plane, monkeypatch, role):
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            term = await glossary_repo.create_abstract_term(
                conn, "party", definition="A person or company", domains={"sales"}
            )
            if role == "class":
                await glossary_repo.ground_table(conn, term, ids["customers"])
            with pytest.raises(TermTagRefused) as refused:
                await refuse_term_tag(conn, term, "pii")
    assert refused.value.code == "tag.term_not_property"
    assert refused.value.params["role"] == role


@pytest.mark.asyncio
async def test_only_pii_goes_on_a_term(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            for tag in ("natural_language", "entity:customer", "technical"):
                with pytest.raises(TermTagRefused) as refused:
                    await refuse_term_tag(conn, ids["body"], tag)
                assert refused.value.code == "tag.not_on_terms"


@pytest.mark.asyncio
async def test_a_tag_stays_on_a_term_that_stops_being_a_property_and_is_reported(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            for table in ("customers", "accounts"):
                await glossary_repo.move_ref(conn, ids[table], "home_address", ids["body"])
            assert await glossary_repo.term_tags(conn, ids["address"]) == [
                {"tag_id": "pii", "reason": "a home address"}
            ]
        monkeypatch.setattr(glossary_router, "_require_glossary_read", lambda request: None)
        # REQ-2146: reported as a finding of the term (the role-conflicts report is folded in)
        found = {e["term_id"]: e for e in await glossary_router.list_findings(_request())}
        [finding] = [f for f in found[ids["address"]]["findings"] if f["kind"] == "tag_wrong_kind"]
        assert found[ids["address"]]["term"] == "home address"
        assert finding["params"] == {"tags": "pii", "role": "concept"}


# --- curation ------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_tagged_term_is_curated_work(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            graph = await glossary_repo._load_graph(conn)
            node = graph.terms[ids["address"]]
            assert node["tagged"] is True
            assert glossary_repo._is_curated(graph, {**node, "definition": None}) is True
            assert graph.terms[ids["body"]]["tagged"] is False


@pytest.mark.asyncio
async def test_the_definition_of_a_tagged_term_cannot_be_cleared(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            for cleared in (None, "  "):
                with pytest.raises(glossary_repo.TermIsTagged, match="Unmark pii first"):
                    await glossary_repo.set_definition(conn, ids["address"], cleared)
            assert await glossary_repo.set_definition(conn, ids["address"], "Home, as given")
            with pytest.raises(glossary_repo.TermIsTagged):
                await glossary_repo.upsert_declared_term(
                    conn, "home address", definition=None, domains={"sales"}
                )


# --- the assign and unassign mutations -----------------------------------------------------------


def _request() -> Any:
    return types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
            active_org_id="acme",
        )
    )


@pytest.fixture
def mutations(monkeypatch):
    """The tag mutations over the case's model, the caller's rights granted: what is asserted is
    what the tag and the term allow."""

    def _granted(db):
        async def _pool():
            return db

        async def _none(*args, **kwargs):
            return None

        monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
        monkeypatch.setattr(schema_mutation, "_require_tag_editor", lambda info: None)
        monkeypatch.setattr(schema_mutation, "_require_tag_target_domains", _none)
        monkeypatch.setattr(schema_mutation, "_require_tag_assignment_right", _none)
        monkeypatch.setattr(schema_mutation, "_require_term_tagger", _none)
        monkeypatch.setattr(schema_mutation, "_refresh_config_tags", _none)
        return schema_mutation.Mutation(), types.SimpleNamespace(context={"request": _request()})

    return _granted


@pytest.mark.asyncio
async def test_a_column_marked_by_hand_says_why_and_hears_how_many_columns_its_term_covers(
    control_plane, monkeypatch, mutations
):
    async with _model(control_plane, monkeypatch) as (db, ids):
        mutation, info = mutations(db)
        on_body = {"object_type": "column", "table_id": ids["notes"], "column_name": "body"}
        refused = await mutation.assign_tag(info, TagAssignmentInput(tag_id="pii", **on_body))
        assert (refused.success, refused.code) == (False, "schema.tag_reason_required")
        on_address = {
            "object_type": "column",
            "table_id": ids["customers"],
            "column_name": "home_address",
        }
        done = await mutation.assign_tag(
            info, TagAssignmentInput(tag_id="pii", reason="a home address", **on_address)
        )
        assert done.success
        assert done.params["covers"] == 1  # accounts.home_address means it too


@pytest.mark.asyncio
async def test_also_term_tags_the_columns_term_and_refuses_a_key(
    control_plane, monkeypatch, mutations
):
    async with _model(control_plane, monkeypatch) as (db, ids):
        mutation, info = mutations(db)
        done = await mutation.assign_tag(
            info,
            TagAssignmentInput(
                tag_id="pii",
                object_type="column",
                table_id=ids["notes"],
                column_name="body",
                reason="notes quote people",
                also_term=True,
            ),
        )
        assert done.success
        assert done.params["objectKey"] == f"term:{ids['body']}"
        assert done.params["covers"] == 1
        async with db.acquire() as conn:
            assert [t["via"] for t in await _pii_of(conn, ids["notes"], "body")] == ["term"]
        key = await mutation.assign_tag(
            info,
            TagAssignmentInput(
                tag_id="pii",
                object_type="column",
                table_id=ids["customers"],
                column_name="id",
                reason="r",
                also_term=True,
            ),
        )
        assert (key.success, key.code) == (False, "tag.key_column")


@pytest.mark.asyncio
async def test_a_draft_term_is_refused_on_the_mutation(control_plane, monkeypatch, mutations):
    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            await _define(conn, ids["body"], "")
        mutation, info = mutations(db)
        refused = await mutation.assign_tag(
            info,
            TagAssignmentInput(tag_id="pii", object_type="term", term_id=ids["body"], reason="r"),
        )
        assert (refused.success, refused.code) == (False, "tag.term_not_live")
        assert refused.params == {"term": "body"}


@pytest.mark.asyncio
async def test_a_column_pii_through_its_term_cannot_be_unmarked_alone(
    control_plane, monkeypatch, mutations
):
    async with _model(control_plane, monkeypatch) as (db, ids):
        mutation, info = mutations(db)
        refused = await mutation.unassign_tag(
            info,
            TagAssignmentInput(
                tag_id="pii",
                object_type="column",
                table_id=ids["customers"],
                column_name="home_address",
            ),
        )
        assert (refused.success, refused.code) == (False, "tag.inherited_from_term")
        assert refused.params == {"tag": "pii", "column": "home_address", "term": "home address"}
        assert refused.message == (
            "This column is PII because it means home address. Unmark that meaning, or move the "
            "column if it means something else."
        )
        # Unmarking the meaning is how: the term's tag comes off, and with it every column's.
        done = await mutation.unassign_tag(
            info, TagAssignmentInput(tag_id="pii", object_type="term", term_id=ids["address"])
        )
        assert done.success
        async with db.acquire() as conn:
            assert await _pii_of(conn, ids["customers"], "home_address") == []


# --- what hides it -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_column_pii_through_its_term_is_masked_and_held_to_its_fake(
    control_plane, monkeypatch
):
    """The profile masks a pii column (provisa.profiler.governance), and Test (fake) refuses while
    a sensitive column has no fake (provisa.core.env_data): both read the term's tag."""
    from provisa.core.env_data import uncovered_sensitive
    from provisa.profiler.governance import ColumnRule
    from provisa.profiler.run import column_tags

    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            tags = await column_tags(conn, ids["customers"])
            assert "pii" in tags["home_address"]
            rule = ColumnRule(
                physical="home_address",
                exposed="home_address",
                visible_to=frozenset({"analyst"}),
                masks=(),
                pii="pii" in tags["home_address"],
            )
            assert rule.restricted_for(frozenset({"analyst"})) is True
            assert set(await uncovered_sensitive(conn, "org_default")) == {
                "customers.home_address",
                "accounts.home_address",
            }


# --- an environment's tree -----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_terms_tag_rides_in_its_file_and_comes_back_on_the_term(control_plane, monkeypatch):
    from provisa.core.env_deploy import _decompose, _existing_ids, _roles_after
    from provisa.core.env_project import project

    async with _model(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            tree = await project(conn, "org_default")
            existing = await _existing_ids(conn, "org_default")
            # The roles the tree holds, as a deploy reads them (REQ-2155: they name their parents).
            roles = await _roles_after(conn, "org_default", tree)
        term_path = next(p for p, b in tree.items() if b.get("name") == "home address")
        assert tree[term_path]["tags"] == [
            {"tag_id": "pii", "base_tag_id": "pii", "reason": "a home address"}
        ]
        rows = _decompose(tree, existing, roles)
        assert [r for r in rows["tag_assignments"] if r.get("object_type") == "term"] == [
            {
                "tag_id": "pii",
                "base_tag_id": "pii",
                "reason": "a home address",
                "object_type": "term",
                "term_id": ids["address"],
                "object_key": f"term:{ids['address']}",
            }
        ]
