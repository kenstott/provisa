# Copyright (c) 2026 Kenneth Stott
# Canary: e5395602-9100-410a-a4d5-a267fc787a3e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Louvain community detection on the schema relationship graph."""

import networkx as nx
from networkx.algorithms.community import louvain_communities


def compute_clusters(
    table_ids: list[int],
    edges: list[tuple[int, int]],
) -> dict[int, tuple[int, int, int]]:
    """Return {table_id: (l1, l2, l3)} using Louvain at three resolutions.

    Isolated nodes (no relationships) are each assigned their own singleton cluster.
    """
    G: nx.Graph = nx.Graph()
    G.add_nodes_from(table_ids)
    G.add_edges_from(edges)

    def _assign(communities: list[set[int]]) -> dict[int, int]:
        return {node: i for i, comm in enumerate(communities) for node in comm}

    # resolution < 1 → fewer, larger communities; > 1 → more, smaller communities
    l1 = _assign(louvain_communities(G, resolution=0.5, seed=42))
    l2 = _assign(louvain_communities(G, resolution=1.0, seed=42))
    l3 = _assign(louvain_communities(G, resolution=2.0, seed=42))

    return {tid: (l1[tid], l2[tid], l3[tid]) for tid in table_ids}
