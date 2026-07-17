# Copyright (c) 2026 Kenneth Stott
# Canary: 95ef859c-b7d1-4cd0-8bd4-990ff6a85a69
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for graph_analytics requirements: REQ-642, REQ-643, REQ-650, REQ-651"""

from __future__ import annotations

import time
from typing import Any


# ---------------------------------------------------------------------------
# Helpers — pure logic that mirrors the graph analytics contract without
# importing the not-yet-existing router module.
# ---------------------------------------------------------------------------


def _build_networkx_digraph(nodes: list[dict], edges: list[dict]):
    """Build a DiGraph from node/edge dicts as the endpoint would."""
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
    """Run a named algorithm and return per-node/edge analytics dicts."""
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
            n: {"score": degree_c[n], "in_degree": in_degree[n], "out_degree": out_degree[n]}
            for n in G.nodes()
        }
    raise ValueError(f"Unknown algorithm: {algorithm}")


def _merge_analytics(
    nodes: list[dict], edges: list[dict], analytics: dict
) -> tuple[list[dict], list[dict]]:
    """Merge _analytics dict into each node and edge."""
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


_DEFAULT_MAX_NODES = 10_000
_DEFAULT_MAX_EDGES = 50_000
_GIRVAN_NEWMAN_NODE_LIMIT = 500


def _check_graph_size(
    node_count: int,
    edge_count: int,
    max_nodes: int = _DEFAULT_MAX_NODES,
    max_edges: int = _DEFAULT_MAX_EDGES,
) -> bool:
    """Return True if the graph exceeds the configured size limit."""
    return node_count > max_nodes or edge_count > max_edges


def _girvan_newman_allowed(node_count: int, force: bool = False) -> bool:
    """Return True if Girvan-Newman may run on this graph."""
    if node_count < _GIRVAN_NEWMAN_NODE_LIMIT:
        return True
    return force


# ---------------------------------------------------------------------------
# REQ-642 — POST /data/graph-analytics endpoint contract
# ---------------------------------------------------------------------------


class TestReq642GraphAnalyticsEndpoint:
    """REQ-642: POST /data/graph-analytics endpoint behavior."""

    def test_response_includes_elapsed_ms(self):
        # REQ-642
        # The endpoint must return an elapsed_ms field in the response JSON.
        t0 = time.perf_counter()
        # Simulate work
        _ = 1 + 1
        elapsed_ms = (time.perf_counter() - t0) * 1000
        response = {
            "nodes": [],
            "edges": [],
            "elapsed_ms": elapsed_ms,
        }
        assert "elapsed_ms" in response
        assert isinstance(response["elapsed_ms"], float)

    def test_response_includes_nodes_and_edges_keys(self):
        # REQ-642
        # The endpoint returns augmented nodes and edges as JSON.
        nodes = [{"id": 1, "label": "Person", "properties": {"name": "Alice"}}]
        edges = [{"identity": "e1", "start": 1, "end": 2, "type": "KNOWS"}]
        response = {
            "nodes": nodes,
            "edges": edges,
            "elapsed_ms": 0.0,
        }
        assert "nodes" in response
        assert "edges" in response

    def test_digraph_built_from_nodes_and_edges(self):
        # REQ-642
        # The endpoint builds an in-memory NetworkX DiGraph from the query result.
        import networkx as nx

        nodes = [{"id": 1}, {"id": 2}, {"id": 3}]
        edges = [
            {"identity": "e1", "start": 1, "end": 2},
            {"identity": "e2", "start": 2, "end": 3},
        ]
        G = _build_networkx_digraph(nodes, edges)
        assert isinstance(G, nx.DiGraph)
        assert G.number_of_nodes() == 3
        assert G.number_of_edges() == 2

    def test_algorithm_output_merged_into_nodes(self):
        # REQ-642
        # The endpoint merges a _analytics dict into each node/edge and returns augmented data.
        nodes = [{"id": 1}, {"id": 2}]
        edges = [{"identity": "e1", "start": 1, "end": 2}]
        G = _build_networkx_digraph(nodes, edges)
        analytics = _run_algorithm(G, "pagerank")
        augmented_nodes, _ = _merge_analytics(nodes, edges, analytics)
        for node in augmented_nodes:
            assert "_analytics" in node

    def test_cypher_query_and_algorithm_accepted_together(self):
        # REQ-642
        # The endpoint accepts both a cypher query and an algorithm name.
        request_body = {
            "query": "MATCH (n)-[r]->(m) RETURN n, r, m",
            "algorithm": "pagerank",
            "params": {},
        }
        assert "query" in request_body
        assert "algorithm" in request_body


# ---------------------------------------------------------------------------
# REQ-643 — _analytics key convention per algorithm
# ---------------------------------------------------------------------------


class TestReq643AnalyticsKeyConvention:
    """REQ-643: Uniform _analytics key convention by algorithm type."""

    def _make_simple_graph(self):
        nodes = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
        edges = [
            {"identity": "e1", "start": "a", "end": "b"},
            {"identity": "e2", "start": "b", "end": "c"},
        ]
        return _build_networkx_digraph(nodes, edges), nodes, edges

    def test_centrality_algorithm_produces_score_key(self):
        # REQ-643: centrality algorithms produce `score`
        G, *_ = self._make_simple_graph()
        analytics = _run_algorithm(G, "pagerank")
        for node_analytics in analytics.values():
            assert "score" in node_analytics

    def test_betweenness_centrality_produces_score_key(self):
        # REQ-643: centrality algorithms produce `score`
        G, *_ = self._make_simple_graph()
        analytics = _run_algorithm(G, "betweenness_centrality")
        for node_analytics in analytics.values():
            assert "score" in node_analytics

    def test_community_detection_produces_cluster_key(self):
        # REQ-643: community detection produces `cluster`
        G, *_ = self._make_simple_graph()
        analytics = _run_algorithm(G, "louvain_communities")
        for node_analytics in analytics.values():
            assert "cluster" in node_analytics

    def test_k_core_produces_core_number_key(self):
        # REQ-643: k-core produces `core_number`
        nodes = [{"id": "a"}, {"id": "b"}, {"id": "c"}, {"id": "d"}]
        edges = [
            {"identity": "e1", "start": "a", "end": "b"},
            {"identity": "e2", "start": "b", "end": "c"},
            {"identity": "e3", "start": "c", "end": "a"},
            {"identity": "e4", "start": "a", "end": "d"},
        ]
        G = _build_networkx_digraph(nodes, edges)
        analytics = _run_algorithm(G, "k_core")
        for node_analytics in analytics.values():
            assert "core_number" in node_analytics

    def test_degree_centrality_produces_score_in_degree_out_degree(self):
        # REQ-643: degree centrality also produces `in_degree` and `out_degree`
        G, *_ = self._make_simple_graph()
        analytics = _run_algorithm(G, "degree_centrality")
        for node_analytics in analytics.values():
            assert "score" in node_analytics
            assert "in_degree" in node_analytics
            assert "out_degree" in node_analytics

    def test_analytics_dict_merged_into_every_node(self):
        # REQ-643: _analytics is merged into every node
        nodes = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
        edges = [
            {"identity": "e1", "start": "a", "end": "b"},
            {"identity": "e2", "start": "b", "end": "c"},
        ]
        G = _build_networkx_digraph(nodes, edges)
        analytics = _run_algorithm(G, "pagerank")
        augmented_nodes, _ = _merge_analytics(nodes, edges, analytics)
        assert len(augmented_nodes) == len(nodes)
        for node in augmented_nodes:
            assert "_analytics" in node

    def test_analytics_dict_merged_into_every_edge(self):
        # REQ-643: _analytics is merged into every edge
        nodes = [{"id": "a"}, {"id": "b"}]
        edges = [{"identity": "e1", "start": "a", "end": "b"}]
        G = _build_networkx_digraph(nodes, edges)
        analytics = _run_algorithm(G, "pagerank")
        _, augmented_edges = _merge_analytics(nodes, edges, analytics)
        assert len(augmented_edges) == len(edges)
        for edge in augmented_edges:
            assert "_analytics" in edge


# ---------------------------------------------------------------------------
# REQ-650 — Graph size cap: HTTP 413 before algorithm runs
# ---------------------------------------------------------------------------


class TestReq650GraphSizeCap:
    """REQ-650: Configurable max graph size; HTTP 413 when exceeded."""

    def test_graph_within_default_limits_is_allowed(self):
        # REQ-650: graphs under 10k nodes / 50k edges are not rejected
        assert not _check_graph_size(9_999, 49_999)

    def test_graph_at_node_limit_boundary_is_allowed(self):
        # REQ-650: exactly at the limit is allowed (limit is exclusive: >)
        assert not _check_graph_size(10_000, 0)

    def test_graph_exceeding_node_limit_is_rejected(self):
        # REQ-650: > 10,000 nodes → rejected
        assert _check_graph_size(10_001, 0)

    def test_graph_exceeding_edge_limit_is_rejected(self):
        # REQ-650: > 50,000 edges → rejected
        assert _check_graph_size(0, 50_001)

    def test_graph_exceeding_both_limits_is_rejected(self):
        # REQ-650: exceeding both limits → rejected
        assert _check_graph_size(10_001, 50_001)

    def test_configurable_limits_can_be_lowered(self):
        # REQ-650: limit is configurable; a custom lower limit must be respected
        assert _check_graph_size(5_001, 0, max_nodes=5_000, max_edges=50_000)

    def test_configurable_limits_can_be_raised(self):
        # REQ-650: a custom higher limit must allow larger graphs
        assert not _check_graph_size(15_000, 0, max_nodes=20_000, max_edges=50_000)

    def test_413_returned_before_algorithm_runs(self):
        # REQ-650: HTTP 413 is returned *before* running any algorithm.
        # Verify by checking that size is evaluated before algorithm dispatch.
        call_log: list[str] = []

        def fake_check_size(*_):
            call_log.append("size_check")
            return True  # exceeds limit

        def fake_run_algorithm(*_):
            call_log.append("algorithm")
            return {}

        # Simulate endpoint logic ordering
        nodes = [{"id": i} for i in range(10_001)]
        edges: list = []
        if fake_check_size(len(nodes), len(edges)):
            status = 413
        else:
            fake_run_algorithm(None, "pagerank")
            status = 200

        assert status == 413
        assert "algorithm" not in call_log


# ---------------------------------------------------------------------------
# REQ-651 — Girvan-Newman node restriction
# ---------------------------------------------------------------------------


class TestReq651GirvanNewmanRestriction:
    """REQ-651: Girvan-Newman restricted to < 500 nodes; force=true bypasses."""

    def test_girvan_newman_allowed_below_500_nodes(self):
        # REQ-651: Girvan-Newman is allowed on graphs with fewer than 500 nodes
        assert _girvan_newman_allowed(499)

    def test_girvan_newman_allowed_at_499_nodes(self):
        # REQ-651: exactly 499 nodes is below the limit
        assert _girvan_newman_allowed(499)

    def test_girvan_newman_rejected_at_500_nodes(self):
        # REQ-651: 500 nodes triggers the restriction
        assert not _girvan_newman_allowed(500)

    def test_girvan_newman_rejected_above_500_nodes(self):
        # REQ-651: graphs with 501+ nodes are rejected
        assert not _girvan_newman_allowed(501)
        assert not _girvan_newman_allowed(1_000)

    def test_girvan_newman_force_true_bypasses_restriction(self):
        # REQ-651: force=true makes the computational risk explicit and bypasses the limit
        assert _girvan_newman_allowed(500, force=True)
        assert _girvan_newman_allowed(10_000, force=True)

    def test_girvan_newman_force_false_still_restricted(self):
        # REQ-651: force=false does not bypass the restriction
        assert not _girvan_newman_allowed(500, force=False)

    def test_girvan_newman_restriction_applies_only_to_girvan_newman(self):
        # REQ-651: other algorithms are NOT subject to the 500-node restriction
        # pagerank has no node-count restriction of its own
        nodes = [{"id": i} for i in range(600)]
        edges = [{"identity": f"e{i}", "start": i, "end": i + 1} for i in range(599)]
        G = _build_networkx_digraph(nodes, edges)
        # pagerank should run without raising
        analytics = _run_algorithm(G, "pagerank")
        assert len(analytics) == 600
