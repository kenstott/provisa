import pytest
from tests.unit.ephemeral_harness import Session

CASES = [
    (
        "dir-1",
        "CREATE (a {k:1}), (b {k:2}) CREATE (a)-[:T]->(b)",
        "MATCH (x)<--(y) RETURN x.k, y.k",
    ),
    ("dir-2", "CREATE (a {k:1}), (b {k:2}) CREATE (a)-[:T]->(b)", "MATCH (x)--(y) RETURN x.k, y.k"),
    (
        "dir-3",
        "CREATE (a {k:1}), (b {k:2}), (c {k:3}) CREATE (a)-[:T]->(b), (b)-[:T]->(c)",
        "MATCH (x)-->(y)-->(z) RETURN x.k, y.k, z.k",
    ),
    (
        "ok-1",
        "CREATE (a), (b), (c) CREATE (a)-[:T]->(b), (b)-[:T]->(c)",
        "MATCH (a)-->(b) RETURN b",
    ),
    ("ok-2", "CREATE (a:A)-[:LOOP]->(a)", "MATCH (n)-[r]->(m) RETURN n, r"),
    (
        "ok-3",
        "CREATE (a), (b), (c) CREATE (a)-[:T]->(b), (b)-[:T]->(c)",
        "MATCH (a)-[:T]->(b) RETURN b",
    ),
    (
        "m3-10",
        "CREATE (a), (b), (c) CREATE (a)-[:T]->(b), (b)-[:T]->(c)",
        "MATCH (a)-->(b), (b)-->(b) RETURN b",
    ),
    ("m3-12", "CREATE (a:A)-[:LOOP]->(a)", "MATCH (n)-[r]-(n) RETURN n, r"),
    ("m3-14", "CREATE (a:A)-[:LOOP]->(a)", "MATCH (n)-[r]->(n) RETURN n, r"),
    (
        "m3-19",
        "CREATE (a {name: 'A'}), (b {name: 'B'}), (x1 {name: 'x1'}), (x2 {name: 'x2'}) CREATE (a)-[:KNOWS]->(x1), (a)-[:KNOWS]->(x2), (b)-[:KNOWS]->(x1), (b)-[:KNOWS]->(x2)",
        "MATCH (a {name: 'A'}), (b {name: 'B'}) MATCH (a)-->(x)<-->(b) RETURN x.name",
    ),
    (
        "m3-23",
        "CREATE (a:A), (b:B), (c:C) CREATE (a)-[:T]->(b), (a)-[:T]->(c)",
        "MATCH (a)-->(b) MATCH (c)-->(d) RETURN labels(a), labels(b), labels(c), labels(d)",
    ),
]


@pytest.mark.parametrize("name,setup,q", CASES)
def test_repro(monkeypatch, name, setup, q):
    s = Session(monkeypatch, {})
    s.run(setup)
    try:
        print(name, "SQL:", s.sql(q))
        print(name, "ROWS:", s.run(q))
    except Exception as e:
        print(name, "ERR:", type(e).__name__, str(e)[:600])


def test_sql25(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (s:Single), (a:A {num: 42}), (b:B {num: 46}), (c:C) CREATE (s)-[:REL]->(a), (s)-[:REL]->(b), (a)-[:REL]->(c), (b)-[:LOOP]->(b)"
    )
    for q in [
        "MATCH (a) WHERE NOT (a:B) OPTIONAL MATCH (a)-[r]->(a) RETURN r",
        "MATCH (a) WHERE NOT (a:B) RETURN a",
        "MATCH (a) OPTIONAL MATCH (a)-[r]->(a) RETURN a, r",
    ]:
        try:
            print("\nSQL25:", s.sql(q))
            print("ROWS25:", s.run(q))
        except Exception as e:
            print("ERR25", e)


def test_with(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE ()-[:T1 {id: 0}]->(:X), ()-[:T2 {id: 1}]->(:X), ()-[:T2 {id: 2}]->()")
    for q in [
        "MATCH (a)-[r]->(b) WITH a, r RETURN r.id",
        "MATCH (a)-[r]->(b) WITH a, b RETURN b",
        "MATCH (a)-[r]->(b:X) WITH a, r, b RETURN r",
        "MATCH (a)-[r]->(b:X) WITH a, r, b MATCH (a)-[r]->(b) RETURN r AS rel ORDER BY rel.id",
    ]:
        try:
            print("\nSQLW:", s.sql(q))
            print("ROWSW:", s.run(q))
        except Exception as e:
            print("ERRW", type(e).__name__, str(e)[:300])
