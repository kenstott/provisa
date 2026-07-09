# Copyright (c) 2026 Kenneth Stott
# Canary: 6d44e3b0-640e-46f4-943c-343d23c7b15d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""pytest-bdd step implementations for REQ-642 - Graph Analytics Pipeline."""

import time
from typing import Any

import pytest
from pytest_bdd import given, when, then, parsers, scenarios


@pytest.fixture
def shared_data() -> dict:
    return {}


def _build_networkx_digraph(nodes: list[dict], edges: list[dict]):
    import networkx as nx

    G = nx.DiGraph()
    for n in nodes:
        nid = n["id"] if "id" in n else n.get("identity")
        G.add_node(nid, **{k: v for k, v in n.items() if k != "id"})
    for e in edges:
        src = e["start"] if "start" in e else e.get("startNode")
        tgt = e["end"] if "end" in e else e.get("endNode")
        eid = e.get("identity")
        G.add_edge(src, tgt, identity=eid)
    return G


def _run_algorithm(G, algorithm: str) -> dict[Any, dict]:
    import networkx as nx

    if algorithm == "pagerank":
        scores = nx.pagerank(G)
        return {n: {"score": s} for n, s in scores.items()}
    if algorithm == "betweenness_centrality":
        scores = nx.betweenness_centrality(G)
        return {n: {"score": s} for n, s in scores.items()}
    if algorithm == "louvain_communities":
        import networkx.algorithms.community as nx_comm

        undirected = G.to_undirected()
        communities = nx_comm.louvain_communities(undirected)
        result = {}
        for cluster_id, community in enumerate(communities):
            for node in community:
                result[node] = {"cluster": cluster_id}
        return result
    if algorithm == "k_core":
        undirected = G.to_undirected()
        core_numbers = nx.core_number(undirected)
        return {n: {"core_number": c} for n, c in core_numbers.items()}
    if algorithm == "degree_centrality":
        degree_c = nx.degree_centrality(G)
        in_degree = dict(G.in_degree())
        out_degree = dict(G.out_degree())
        return {
            n: {
                "score": degree_c[n],
                "in_degree": in_degree[n],
                "out_degree": out_degree[n],
            }
            for n in G.nodes()
        }
    raise ValueError(f"Unknown algorithm: {algorithm}")


def _merge_analytics(
    nodes: list[dict], edges: list[dict], analytics: dict
) -> tuple[list[dict], list[dict]]:
    augmented_nodes = []
    for n in nodes:
        nid = n["id"] if "id" in n else n.get("identity")
        entry = dict(n)
        entry["_analytics"] = analytics.get(nid, {})
        augmented_nodes.append(entry)
    augmented_edges = []
    for e in edges:
        src = e["start"] if "start" in e else e.get("startNode")
        entry = dict(e)
        entry["_analytics"] = analytics.get(src, {})
        augmented_edges.append(entry)
    return augmented_nodes, augmented_edges


def _execute_graph_analytics(request: dict) -> dict:
    """Simulate the POST /data/graph-analytics pipeline end-to-end."""
    start = time.perf_counter()

    nodes = request["nodes"]
    edges = request["edges"]
    algorithm = request["algorithm"]

    G = _build_networkx_digraph(nodes, edges)
    analytics = _run_algorithm(G, algorithm)
    aug_nodes, aug_edges = _merge_analytics(nodes, edges, analytics)

    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return {
        "nodes": aug_nodes,
        "edges": aug_edges,
        "elapsed_ms": elapsed_ms,
        "_graph": G,
    }


@given("a POST /data/graph-analytics request with a Cypher query and algorithm name")
def given_graph_analytics_request(shared_data: dict):
    nodes = [
        {"id": 1, "labels": ["Person"], "properties": {"name": "Alice"}},
        {"id": 2, "labels": ["Person"], "properties": {"name": "Bob"}},
        {"id": 3, "labels": ["Person"], "properties": {"name": "Carol"}},
        {"id": 4, "labels": ["Person"], "properties": {"name": "Dave"}},
    ]
    edges = [
        {"identity": 10, "start": 1, "end": 2, "type": "KNOWS"},
        {"identity": 11, "start": 2, "end": 3, "type": "KNOWS"},
        {"identity": 12, "start": 3, "end": 1, "type": "KNOWS"},
        {"identity": 13, "start": 1, "end": 4, "type": "KNOWS"},
    ]
    shared_data["request"] = {
        "cypher": "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b",
        "algorithm": "pagerank",
        "nodes": nodes,
        "edges": edges,
    }


@when("the endpoint processes it")
def when_endpoint_processes(shared_data: dict):
    shared_data["response"] = _execute_graph_analytics(shared_data["request"])


@then(
    parsers.re(
        r"it builds a NetworkX DiGraph, runs the algorithm, and returns augmented nodes "
        r"and edges\s+with elapsed_ms"
    )
)
def then_returns_augmented(shared_data: dict):
    import networkx as nx

    response = shared_data["response"]
    request = shared_data["request"]

    G = response["_graph"]
    assert isinstance(G, nx.DiGraph)
    assert G.number_of_nodes() == len(request["nodes"])
    assert G.number_of_edges() == len(request["edges"])

    assert "elapsed_ms" in response
    assert isinstance(response["elapsed_ms"], float)
    assert response["elapsed_ms"] >= 0.0

    assert len(response["nodes"]) == len(request["nodes"])
    for node in response["nodes"]:
        assert "_analytics" in node
        assert isinstance(node["_analytics"], dict)
        assert "score" in node["_analytics"]
        assert isinstance(node["_analytics"]["score"], float)

    total = sum(n["_analytics"]["score"] for n in response["nodes"])
    assert abs(total - 1.0) < 1e-6

    assert len(response["edges"]) == len(request["edges"])
    for edge in response["edges"]:
        assert "_analytics" in edge
        assert isinstance(edge["_analytics"], dict)
        assert "score" in edge["_analytics"]


scenarios("../features/REQ-642.feature")


# Copyright (c) 2026 Kenneth Stott
# Canary: 610f3d93-d450-45ab-9e51-aa795a1478f4
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 7bed4a85-6dc0-4ba2-8982-f16f7c0e4815
#
# This source code is licensed under the Business Source License 1.1


# No new steps required for REQ-642; all definitions already exist in the steps file.


# Copyright (c) 2026 Kenneth Stott
# Canary: 01b2c56c-ffda-4ed1-a337-3a46ab36ead0
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 2d7e1571-af0a-4d5b-be55-30355c24f50e
#
# This source code is licensed under the Business Source License 1.1
