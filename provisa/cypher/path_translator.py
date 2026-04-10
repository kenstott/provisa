# Copyright (c) 2026 Kenneth Stott
# Canary: 1a7e3c9f-4b2d-4e8a-9c5f-7b1d3e6a8c2f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Translate shortestPath / allShortestPaths to WITH RECURSIVE CTEs.

Generates:
  - An adjacency CTE from the registered join columns
  - A recursive CTE that iterates hops up to the depth limit
  - A final SELECT that retrieves shortest/all-shortest path rows
  - Injects _path_id, _depth, _direction columns for the assembler

Rejects unbounded [*] (enforced at parse time, also checked here).
"""

from __future__ import annotations

from provisa.cypher.parser import CypherParseError, PathFunction, RelPattern
from provisa.cypher.label_map import CypherLabelMap, RelationshipMapping


class PathTranslateError(Exception):
    pass


def path_to_recursive_sql(
    path_func: PathFunction,
    label_map: CypherLabelMap,
    start_var: str,
    end_var: str,
    path_var: str,
    max_depth: int,
) -> str:
    """Generate a WITH RECURSIVE SQL fragment for a path function.

    Returns a SQL string (not a SQLGlot AST) that can be prepended to the
    main query as a CTE. The caller wraps this into the full query.

    Columns injected:
      _path_id  — unique path identifier
      _depth    — hop count from start node
      _direction — edge direction indicator
    """
    pattern = path_func.pattern
    nodes = pattern.nodes
    rels = pattern.rels

    if not rels:
        raise PathTranslateError("Path function requires at least one relationship pattern")

    rel = rels[0]

    if rel.variable_length:
        if rel.max_hops is None:
            raise CypherParseError(
                "Unbounded variable-length pattern [*] is not allowed. Specify a depth limit."
            )
        effective_max = min(rel.max_hops, max_depth)
    else:
        effective_max = max_depth

    min_hops = rel.min_hops if rel.min_hops is not None else 1

    # Find relationship mapping
    rel_mapping: RelationshipMapping | None = None
    src_label = nodes[0].labels[0] if nodes[0].labels else None
    tgt_label = nodes[-1].labels[0] if nodes[-1].labels else None

    if rel.types:
        rel_type = rel.types[0].upper()
        rel_mapping = label_map.relationships.get(rel_type)
    elif src_label and tgt_label:
        candidates = label_map.relationships_for(src_label, tgt_label)
        if candidates:
            rel_mapping = candidates[0]

    if rel_mapping is None:
        raise PathTranslateError(
            f"No registered relationship found between {src_label!r} and {tgt_label!r}"
        )

    src_meta = label_map.nodes.get(src_label) if src_label else None
    tgt_meta = label_map.nodes.get(tgt_label) if tgt_label else None

    if not src_meta or not tgt_meta:
        raise PathTranslateError(f"Unknown node labels: {src_label!r}, {tgt_label!r}")

    src_full = f'"{src_meta.catalog_name}"."{src_meta.schema_name}"."{src_meta.table_name}"'
    tgt_full = f'"{tgt_meta.catalog_name}"."{tgt_meta.schema_name}"."{tgt_meta.table_name}"'
    src_id_col = rel_mapping.join_source_column
    tgt_id_col = rel_mapping.join_target_column
    src_pk = src_meta.id_column
    tgt_pk = tgt_meta.id_column

    is_shortest = path_func.func_name == "shortestpath"

    sql = f"""
WITH RECURSIVE _cypher_path AS (
  -- Base case: direct edges (depth 1)
  SELECT
    CAST(src."{src_pk}" AS VARCHAR) AS _start_id,
    CAST(tgt."{tgt_pk}" AS VARCHAR) AS _end_id,
    CAST(src."{src_pk}" AS VARCHAR) AS _path_id,
    1 AS _depth,
    'forward' AS _direction,
    CAST(src."{src_pk}" AS VARCHAR) AS _visited
  FROM {src_full} src
  JOIN {tgt_full} tgt ON src."{src_id_col}" = tgt."{tgt_id_col}"
  WHERE _depth <= {effective_max}

  UNION ALL

  -- Recursive case: extend paths
  SELECT
    p._start_id,
    CAST(tgt."{tgt_pk}" AS VARCHAR) AS _end_id,
    p._path_id,
    p._depth + 1 AS _depth,
    'forward' AS _direction,
    p._visited || ',' || CAST(tgt."{tgt_pk}" AS VARCHAR) AS _visited
  FROM _cypher_path p
  JOIN {tgt_full} tgt ON p._end_id = CAST(tgt."{tgt_id_col}" AS VARCHAR)
  WHERE p._depth < {effective_max}
    AND p._visited NOT LIKE '%,' || CAST(tgt."{tgt_pk}" AS VARCHAR) || ',%'
    AND p._visited NOT LIKE CAST(tgt."{tgt_pk}" AS VARCHAR) || ',%'
    AND p._visited NOT LIKE '%,' || CAST(tgt."{tgt_pk}" AS VARCHAR)
    AND p._visited != CAST(tgt."{tgt_pk}" AS VARCHAR)
)
"""
    if is_shortest:
        sql += f"""
, _shortest AS (
  SELECT
    _start_id,
    _end_id,
    _path_id,
    _depth,
    _direction,
    ROW_NUMBER() OVER (PARTITION BY _start_id, _end_id ORDER BY _depth ASC) AS _rank
  FROM _cypher_path
  WHERE _depth >= {min_hops}
)
"""
        final_alias = "_shortest"
        filter_clause = "WHERE _rank = 1"
    else:
        # allShortestPaths — all paths with minimum depth
        sql += f"""
, _min_depths AS (
  SELECT _start_id, _end_id, MIN(_depth) AS _min_depth
  FROM _cypher_path
  WHERE _depth >= {min_hops}
  GROUP BY _start_id, _end_id
)
, _shortest AS (
  SELECT p._start_id, p._end_id, p._path_id, p._depth, p._direction
  FROM _cypher_path p
  JOIN _min_depths m ON p._start_id = m._start_id AND p._end_id = m._end_id
    AND p._depth = m._min_depth
)
"""
        final_alias = "_shortest"
        filter_clause = ""

    return sql.strip(), final_alias, filter_clause
