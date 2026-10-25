# Copyright (c) 2026 Kenneth Stott
# Canary: 3825e728-b69d-48f5-afeb-b1545069a1be
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A single-source statement on a Qdrant source, run in Qdrant (REQ-2000): what is expressible
there and what is the engine's, each read and write lowered to Qdrant's API, and SQL's answer
kept. Reads and writes run against qdrant-client's in-process Qdrant, so a filter's meaning is
Qdrant's own."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import sqlglot
from qdrant_client import QdrantClient, models

from provisa.core.statement_warnings import collecting
from provisa.federation.mail_write import WriteRefused
from provisa.qdrant.direct import NotExpressible, QdrantDriver, analyse, condition, _columns
from provisa.qdrant.fetch import QdrantConnection

_TABLE = {
    "id": 9,
    "source_id": "vec",
    "table_name": "docs",
    "query_template": None,
    "columns": [
        {"column_name": "id", "data_type": "bigint", "path": "@id", "native_filter_type": None},
        {"column_name": "title", "data_type": "text", "path": None, "native_filter_type": None},
        {"column_name": "lang", "data_type": "text", "path": None, "native_filter_type": None},
        {
            "column_name": "pages",
            "data_type": "integer",
            "path": "meta.pages",
            "native_filter_type": None,
        },
        {"column_name": "score", "data_type": "double", "path": None, "native_filter_type": None},
        {
            "column_name": "body_vec",
            "data_type": "json",
            "path": "@vector.text",
            "native_filter_type": None,
            "embedding": True,
            "embedding_model": "stub",
        },
    ],
}

_COSINE = lambda using: "Cosine"  # noqa: E731


def _plan(sql: str, distance=_COSINE):
    return analyse(sqlglot.parse_one(sql, read="postgres"), _TABLE, distance)


_SIM = "cosine_similarity(\"body_vec\", '[1,0]', 'stub', 2)"


@pytest.mark.parametrize(
    ("sql", "kind"),
    [
        ('SELECT "id", "title" FROM "default"."docs" WHERE "lang" = \'en\' LIMIT 5', "scroll"),
        ('SELECT "id" FROM "default"."docs"', "scroll"),
        ('SELECT "title" FROM "default"."docs" WHERE "id" IN (1, 2)', "retrieve"),
        ('SELECT "title" FROM "default"."docs" WHERE "id" = $1', "retrieve"),
        (f'SELECT "id", {_SIM} AS s FROM "default"."docs" ORDER BY {_SIM} DESC LIMIT 3', "query"),
        (
            f'SELECT "id" FROM "default"."docs" WHERE "pages" >= 2 AND NOT "lang" IS NULL '
            f"ORDER BY {_SIM} DESC LIMIT 3",
            "query",
        ),
    ],
)
def test_a_statement_qdrant_states_exactly_is_one_api_call(sql, kind):
    assert _plan(sql).kind == kind


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        ('SELECT "lang", COUNT(*) FROM "default"."docs" GROUP BY "lang"', "GROUP BY"),
        ('SELECT COUNT(*) FROM "default"."docs"', "aggregates"),
        ('SELECT DISTINCT "lang" FROM "default"."docs"', "DISTINCT"),
        ('SELECT "id" FROM "default"."docs" d JOIN "s"."t" t ON t."id" = d."id"', "joins"),
        ('SELECT "id" FROM "default"."docs" LIMIT 5 OFFSET 5', "OFFSET"),
        ('SELECT "id" FROM "default"."docs" ORDER BY "title"', "most similar first"),
        ('SELECT "id" FROM "default"."docs" WHERE "title" LIKE \'a%\'', "no Qdrant filter form"),
        ('SELECT "id" FROM "default"."docs" WHERE "pages" = "id"', "not a value"),
        ('SELECT \'***\' AS "title" FROM "default"."docs"', "computed"),
        (
            'SELECT CASE WHEN TRUE THEN NULL ELSE "title" END AS "title" FROM "default"."docs"',
            "computed",
        ),
        (f'SELECT "id" FROM "default"."docs" ORDER BY {_SIM} DESC', "LIMIT"),
        ('SELECT "id" FROM "default"."docs" WHERE "lang" = NULL', "NULL"),
        ('SELECT "id" FROM "default"."docs" WHERE "title" > \'a\'', "no Qdrant range"),
    ],
)
def test_what_qdrant_does_not_state_exactly_is_named(sql, reason):
    with pytest.raises(NotExpressible, match=reason):
        _plan(sql)


def test_a_similarity_over_a_vector_not_scored_by_cosine_is_the_engines():
    with pytest.raises(NotExpressible, match="scored by Dot, not cosine"):
        _plan(
            f'SELECT "id" FROM "default"."docs" ORDER BY {_SIM} DESC LIMIT 3',
            distance=lambda using: "Dot",
        )


def test_a_search_table_is_never_run_directly():
    with pytest.raises(NotExpressible, match="search table"):
        analyse(
            sqlglot.parse_one('SELECT "id" FROM "default"."docs"', read="postgres"),
            {**_TABLE, "query_template": "{}"},
            _COSINE,
        )


def _where(sql: str):
    tree = sqlglot.parse_one(f'SELECT "id" FROM "default"."docs" WHERE {sql}', read="postgres")
    return condition(tree.args["where"].this, _columns(_TABLE))


def test_a_value_unequal_is_one_the_point_holds():
    """SQL's ``<>`` is false for a row with no value: so is Qdrant's filter, by excluding it."""
    assert _where("\"lang\" <> 'en'") == {
        "must_not": [{"key": "lang", "match": {"value": "en"}}, {"is_empty": {"key": "lang"}}]
    }


def test_ranges_ids_and_nulls_have_their_qdrant_forms():
    assert _where('"pages" > 3') == {"key": "meta.pages", "range": {"gt": 3}}
    assert _where('3 < "pages"') == {"key": "meta.pages", "range": {"gt": 3}}
    assert _where('"id" = 4') == {"has_id": [4]}
    assert _where('"lang" IS NULL') == {"must": [{"is_empty": {"key": "lang"}}]}
    assert _where("\"lang\" IN ('en', 'de')") == {"key": "lang", "match": {"any": ["en", "de"]}}
    assert _where("TRUE") is None


# -- reads and writes against Qdrant ------------------------------------------------------------


@pytest.fixture
def qdrant(monkeypatch):
    """An in-process Qdrant holding ``docs`` (a cosine vector ``text``), the driver over it, and
    the collection's registered table."""
    from provisa.api import app as app_mod

    client = QdrantClient(":memory:")
    client.create_collection(
        "docs",
        vectors_config={"text": models.VectorParams(size=2, distance=models.Distance.COSINE)},
    )
    client.upsert(
        "docs",
        points=[
            models.PointStruct(
                id=1,
                vector={"text": [1, 0]},
                payload={"title": "a", "lang": "en", "meta": {"pages": 2}},
            ),
            models.PointStruct(
                id=2,
                vector={"text": [0.9, 0.1]},
                payload={"title": "b", "lang": "de", "meta": {"pages": 5}},
            ),
            models.PointStruct(
                id=3, vector={"text": [0, 1]}, payload={"title": "c", "meta": {"pages": 9}}
            ),
        ],
    )
    monkeypatch.setattr(QdrantConnection, "client", lambda self: client)
    monkeypatch.setattr(app_mod.state, "tables", [_TABLE], raising=False)
    driver = QdrantDriver()
    driver.bind_source("vec")
    driver._conn = QdrantConnection(url="http://q", api_key=None)
    return driver, client


async def test_a_value_unequal_skips_points_that_hold_none(qdrant):
    driver, _ = qdrant
    result = await driver.execute('SELECT "id" FROM "default"."docs" WHERE "lang" <> \'en\'', None)
    assert result.rows == [(2,)]  # point 3 holds no lang: SQL's <> is not true for it


async def test_a_filtered_similarity_search_reads_the_nearest_matching_points(qdrant):
    driver, _ = qdrant
    with collecting() as said:
        result = await driver.execute(
            f'SELECT "id", "pages", {_SIM} AS s FROM "default"."docs" WHERE "pages" >= 3 '
            f"ORDER BY {_SIM} DESC LIMIT 1",
            None,
        )
    assert [r[:2] for r in result.rows] == [(2, 5)]  # point 1 is nearer but has 2 pages
    assert result.rows[0][2] == pytest.approx(0.9938, abs=1e-3)
    assert [w.code for w in said] == ["statement.approximate_index"]


async def test_a_key_read_is_one_retrieve(qdrant):
    driver, _ = qdrant
    result = await driver.execute('SELECT "title" FROM "default"."docs" WHERE "id" = $1', [3])
    assert result.rows == [("c",)]


async def test_an_insert_writes_new_points_and_refuses_an_id_held(qdrant):
    driver, client = qdrant
    model = type("M", (), {"id": "stub", "dimensions": 2})()
    with patch("provisa.qdrant.table.registered_model", AsyncMock(return_value=model)):
        done = await driver.execute(
            'INSERT INTO "default"."docs" ("id", "title", "body_vec") VALUES ($1, $2, $3)',
            [7, "g", "[0.5, 0.5]"],
        )
        assert done.rowcount == 1
        assert client.retrieve("docs", ids=[7], with_vectors=True)[0].vector == {
            "text": pytest.approx([0.7071, 0.7071], abs=1e-3)
        }
        with pytest.raises(WriteRefused, match=r"already holds point\(s\) \[1\]"):
            await driver.execute(
                'INSERT INTO "default"."docs" ("id", "title") VALUES (1, \'x\')', None
            )


async def test_a_text_written_to_a_vector_is_embedded_by_the_columns_model(qdrant):
    driver, client = qdrant
    model = type("M", (), {"id": "stub", "dimensions": 2})()
    with (
        patch("provisa.qdrant.table.registered_model", AsyncMock(return_value=model)),
        patch(
            "provisa.vector.query.vectorize_text", AsyncMock(return_value=[0.0, 1.0])
        ) as embedded,
    ):
        await driver.execute(
            'INSERT INTO "default"."docs" ("id", "body_vec") VALUES (8, \'refund policy\')', None
        )
    embedded.assert_awaited_once()
    assert client.retrieve("docs", ids=[8], with_vectors=True)[0].vector == {
        "text": pytest.approx([0.0, 1.0])
    }


async def test_an_upsert_replaces_the_columns_it_sets_and_keeps_the_rest(qdrant):
    driver, client = qdrant
    await driver.execute(
        'INSERT INTO "default"."docs" ("id", "title") VALUES (1, \'renamed\'), (9, \'new\') '
        'ON CONFLICT ("id") DO UPDATE SET "title" = EXCLUDED."title"',
        None,
    )
    kept = client.retrieve("docs", ids=[1])[0].payload
    assert kept == {"title": "renamed", "lang": "en", "meta": {"pages": 2}}
    assert client.retrieve("docs", ids=[9])[0].payload == {"title": "new"}


@pytest.mark.parametrize(
    ("clause", "reason"),
    [
        ('ON CONFLICT ("id") DO NOTHING', "DO UPDATE SET only"),
        ('ON CONFLICT ("title") DO UPDATE SET "title" = EXCLUDED."title"', "DO UPDATE SET only"),
        ('ON CONFLICT ("id") DO UPDATE SET "title" = \'x\'', "EXCLUDED"),
    ],
)
async def test_another_conflict_clause_is_refused_by_name(qdrant, clause, reason):
    driver, _ = qdrant
    with pytest.raises(WriteRefused, match=reason):
        await driver.execute(
            f'INSERT INTO "default"."docs" ("id", "title") VALUES (1, \'x\') {clause}', None
        )


async def test_an_update_and_a_delete_act_on_the_points_their_filter_selects(qdrant):
    driver, client = qdrant
    updated = await driver.execute(
        'UPDATE "default"."docs" SET "lang" = \'fr\' WHERE "pages" >= 5', None
    )
    assert updated.rowcount == 2
    assert {p.id: p.payload.get("lang") for p in client.scroll("docs")[0]} == {
        1: "en",
        2: "fr",
        3: "fr",
    }
    deleted = await driver.execute('DELETE FROM "default"."docs" WHERE "lang" = $1', ["fr"])
    assert deleted.rowcount == 2
    assert [p.id for p in client.scroll("docs")[0]] == [1]


async def test_a_write_whose_condition_has_no_filter_form_is_refused(qdrant):
    driver, _ = qdrant
    with pytest.raises(WriteRefused, match="does not translate"):
        await driver.execute('DELETE FROM "default"."docs" WHERE "title" LIKE \'a%\'', None)


async def test_the_route_check_names_why_a_read_is_the_engines(qdrant):
    driver, _ = qdrant
    assert await driver.reason('SELECT "id" FROM "default"."docs" LIMIT 2', _TABLE) is None
    said = await driver.reason('SELECT COUNT(*) FROM "default"."docs"', _TABLE)
    assert said == "the statement aggregates"


def test_a_qdrant_read_is_planned_by_its_proxy_and_qdrant_runs_its_similarity():
    import inspect

    from provisa.pgwire import _pipeline
    from provisa.transpiler.dialect_capabilities import CAPABILITIES, SIMILARITY

    assert "proxy.plan(" in inspect.getsource(_pipeline._proxy_plan)
    assert SIMILARITY in CAPABILITIES["qdrant"]


# --- a statement the engine answers: the collection's part, read live (REQ-2000) ---


def _part(sql: str) -> str:
    from provisa.federation.live_only import cut

    return cut(sql, _TABLE).sql


_COLUMNS = '"id", "title", "lang", "pages", "score", "body_vec"'


def test_a_join_reads_the_rows_its_conditions_on_the_collection_alone_select():
    sql = (
        'SELECT d."title", o."n" FROM "default"."docs" AS d JOIN "pg"."orders" AS o '
        'ON o."doc_id" = d."id" AND d."pages" >= 2 WHERE d."lang" = \'en\' AND o."n" > 1'
    )
    assert _part(sql) == (f'SELECT {_COLUMNS} FROM "docs" WHERE "lang" = \'en\' AND "pages" >= 2')


def test_a_masked_read_of_the_collection_alone_keeps_its_conditions():
    sql = (
        'SELECT CASE WHEN FALSE THEN "title" ELSE NULL END AS "title" FROM "default"."docs" '
        'WHERE "lang" = \'en\' AND "id" IN (1, 2)'
    )
    assert _part(sql) == f'SELECT {_COLUMNS} FROM "docs" WHERE "lang" = \'en\' AND "id" IN (1, 2)'


def test_a_top_k_read_alone_in_its_select_is_the_parts_top_k():
    sql = (
        f'SELECT x."id", o."n" FROM (SELECT d."id", {_SIM} AS s FROM "default"."docs" AS d '
        f'WHERE d."pages" >= 3 ORDER BY {_SIM} DESC LIMIT 1) AS x JOIN "pg"."orders" AS o '
        'ON o."doc_id" = x."id"'
    )
    assert _part(sql) == (
        f'SELECT {_COLUMNS} FROM "docs" WHERE "pages" >= 3 '
        f"ORDER BY {_SIM.replace('cosine_similarity', 'COSINE_SIMILARITY')} DESC LIMIT 1"
    )


def test_a_join_ordering_by_similarity_has_no_top_k_of_the_collection():
    sql = (
        f'SELECT d."id" FROM "default"."docs" AS d JOIN "pg"."orders" AS o ON o."doc_id" = d."id" '
        f"ORDER BY {_SIM} DESC LIMIT 1"
    )
    from provisa.federation.live_only import LiveReadRefused

    with pytest.raises(LiveReadRefused, match="no condition, key or top-k"):
        _part(sql)


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT d."id", o."n" FROM "default"."docs" AS d JOIN "pg"."orders" AS o '
        'ON o."doc_id" = d."id"',
        # A condition over a parameter is the engine's: it binds it.
        'SELECT d."id" FROM "default"."docs" AS d JOIN "pg"."orders" AS o ON o."doc_id" = d."id" '
        'WHERE d."lang" = $1',
        # The padded side of an outer join: a WHERE condition on it is the engine's.
        'SELECT o."n" FROM "pg"."orders" AS o LEFT JOIN "default"."docs" AS d '
        'ON o."doc_id" = d."id" WHERE d."lang" = \'en\'',
    ],
)
def test_a_part_that_would_read_the_whole_collection_is_refused_by_name(sql):
    from provisa.federation.live_only import LiveReadRefused

    with pytest.raises(LiveReadRefused, match="'docs' is read live from its source: .* whole"):
        _part(sql)


def test_the_padded_side_of_an_outer_join_reads_its_join_conditions():
    sql = (
        'SELECT o."n" FROM "pg"."orders" AS o LEFT JOIN "default"."docs" AS d '
        'ON o."doc_id" = d."id" AND d."lang" = \'en\''
    )
    assert _part(sql) == f'SELECT {_COLUMNS} FROM "docs" WHERE "lang" = \'en\''


def _state(driver):
    from types import SimpleNamespace

    return SimpleNamespace(source_pools=SimpleNamespace(get=lambda source_id: driver))


async def test_a_part_is_read_live_as_rows_for_the_engine(qdrant):
    from provisa.federation.live_only import cut, read

    driver, _ = qdrant
    with collecting() as said:
        answer = await read(
            _state(driver),
            cut(
                f'SELECT d."id" FROM (SELECT d."id" FROM "default"."docs" AS d '
                f'WHERE d."pages" >= 3 ORDER BY {_SIM} DESC LIMIT 1) AS d',
                _TABLE,
            ),
        )
    assert [(r["id"], r["title"], r["pages"]) for r in answer.rows] == [(2, "b", 5)]
    assert [n for n, _t in answer.columns] == [
        "id",
        "title",
        "lang",
        "pages",
        "score",
        "body_vec",
    ]
    assert [w.code for w in said] == ["statement.approximate_index"]


async def test_a_part_qdrant_cannot_state_is_refused_naming_the_table(qdrant):
    from provisa.federation.live_only import LiveReadRefused, cut, read

    driver, _ = qdrant
    part = cut(
        'SELECT d."id" FROM "default"."docs" AS d JOIN "pg"."orders" AS o ON o."doc_id" = d."id" '
        "WHERE d.\"title\" LIKE 'a%'",
        _TABLE,
    )
    with pytest.raises(LiveReadRefused, match="'docs' is read live .*no Qdrant filter form"):
        await read(_state(driver), part)


def test_the_engine_route_reads_live_only_tables_live():
    import inspect

    from provisa.pgwire import _pipeline

    assert "_read_live_parts(" in inspect.getsource(_pipeline._optimize_and_route)


async def test_an_insert_in_its_physical_form_reads_the_aliased_targets_columns(qdrant):
    # the physical rewrite aliases the target; sqlglot then keeps the column list on the alias
    driver, client = qdrant
    done = await driver.execute(
        'INSERT INTO "default"."docs" AS "docs" ("id", "title") VALUES ($1, $2)', [8, "h"]
    )
    assert done.rowcount == 1
    assert client.retrieve("docs", ids=[8])[0].payload == {"title": "h"}
    with pytest.raises(WriteRefused, match="names its columns"):
        await driver.execute('INSERT INTO "default"."docs" VALUES (9, \'i\')', None)


async def test_a_search_table_is_the_engines_without_asking_qdrant(qdrant):
    # a search table's name is no collection: asking Qdrant for its vectors answers 404
    driver, client = qdrant
    search = {**_TABLE, "table_name": "docs_search", "query_template": "{}"}
    asked: list[str] = []
    real = driver.distance_of
    driver.distance_of = lambda collection: asked.append(collection) or real(collection)
    said = await driver.reason('SELECT "id" FROM "default"."docs_search"', search)
    assert said == "a search table is read under its arguments"
    assert asked == []
