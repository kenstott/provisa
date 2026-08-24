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

from provisa.cypher.parser import CypherParseError, PathFunction
from provisa.cypher.label_map import CypherLabelMap, RelationshipMapping

# Requirements: REQ-345, REQ-346, REQ-348, REQ-351


class PathTranslateError(Exception):
    pass


def path_to_recursive_sql(  # REQ-345, REQ-346, REQ-348, REQ-351
    path_func: PathFunction,
    label_map: CypherLabelMap,
    _start_var: str,
    _end_var: str,
    _path_var: str,
    max_depth: int,
) -> tuple[str, str, str]:
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

    src_full = f'"{src_meta.catalog_name}"."{src_meta.schema_name}"."{src_meta.sql_table_name}"'
    tgt_full = f'"{tgt_meta.catalog_name}"."{tgt_meta.schema_name}"."{tgt_meta.sql_table_name}"'
    tgt_id_col = rel_mapping.join_target_column
    src_pk = src_meta.id_column
    tgt_pk = tgt_meta.id_column

    if rel_mapping.source_constant is not None:
        escaped = str(rel_mapping.source_constant).replace("'", "''")
        src_join_expr = f"'{escaped}'"
    else:
        src_join_expr = f'src."{rel_mapping.join_source_column}"'

    # REQ-1586: a junction-backed edge reaches its target through the associative table, so
    # each depth step here is src -> junction -> tgt. The discriminator rides the junction hop,
    # so a discriminated junction contributes only its own edges to the path search.
    via = rel_mapping.via
    if via is not None:
        via_full = f'"{via.catalog_name}"."{via.schema_name}"."{via.table_name}"'
        via_disc = (
            f" AND _via.\"{via.type_column}\" = '{via.type_value}'" if via.type_column else ""
        )
        # A variable-length path carries one scalar end id per step, so a composite junction end
        # has nothing to pair against on the recursive hop. That is a limit of the path search,
        # not something to approximate by matching only the first column.
        if len(via.source_columns) != 1 or len(via.target_columns) != 1:
            raise PathTranslateError(
                f"variable-length path over {rel_mapping.rel_type!r} is not supported: its "
                "junction maps a composite key, which a path step has no end id to pair with"
            )
        base_join = (
            f'  JOIN {via_full} _via ON {src_join_expr} = _via."{via.source_columns[0]}"{via_disc}\n'
            f'  JOIN {tgt_full} tgt ON _via."{via.target_columns[0]}" = tgt."{tgt_id_col}"'
        )
        step_join = (
            f'  JOIN {via_full} _via ON p._end_id = CAST(_via."{via.source_columns[0]}" AS VARCHAR)'
            f"{via_disc}\n"
            f'  JOIN {tgt_full} tgt ON _via."{via.target_columns[0]}" = tgt."{tgt_id_col}"'
        )
    else:
        base_join = f'  JOIN {tgt_full} tgt ON {src_join_expr} = tgt."{tgt_id_col}"'
        step_join = f'  JOIN {tgt_full} tgt ON p._end_id = CAST(tgt."{tgt_id_col}" AS VARCHAR)'

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
{base_join}
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
{step_join}
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
