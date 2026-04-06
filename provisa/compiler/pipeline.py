# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9044ab-eee0-4cee-a262-d177b37f1691
# (run scripts/canary_stamp.py on this file after creating it)

"""Stage 1 + governance pipeline for the GraphQL path (REQ-262, REQ-263).

Assembles compile_graphql → RLS → masking → MV rewrite → Kafka → sampling.
"""
from __future__ import annotations

from graphql import DocumentNode

from provisa.compiler.mask_inject import MaskingRules, inject_masking
from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.sampling import apply_sampling, get_sample_size
from provisa.compiler.sql_gen import CompiledQuery, CompilationContext
from provisa.compiler.stage1 import compile_graphql
from provisa.mv.rewriter import rewrite_if_mv_match
from provisa.security.rights import Capability, has_capability


def run_pipeline(
    document: DocumentNode,
    ctx: CompilationContext,
    variables: dict | None,
    rls: RLSContext,
    masking_rules: MaskingRules,
    role_id: str,
    role,
    fresh_mvs,
    *,
    use_catalog: bool = False,
    view_sql_map: dict | None = None,
    kafka_table_configs=None,
    source_types: dict | None = None,
) -> list[CompiledQuery]:
    """Run Stage 1 + governance for a GraphQL query.

    Pipeline: compile_graphql → inject_rls → inject_masking
              → rewrite_if_mv_match → inject_kafka_filters → apply_sampling
    """
    compiled_list = compile_graphql(document, ctx, variables, use_catalog=use_catalog)
    result: list[CompiledQuery] = []

    for compiled in compiled_list:
        if view_sql_map:
            from provisa.compiler.view_expand import expand_views
            compiled = expand_views(compiled, view_sql_map)

        compiled = inject_rls(compiled, ctx, rls)
        compiled = inject_masking(compiled, ctx, masking_rules, role_id)
        compiled = rewrite_if_mv_match(compiled, fresh_mvs)

        if kafka_table_configs and source_types:
            from provisa.kafka.window import inject_kafka_filters
            compiled = inject_kafka_filters(compiled, ctx, source_types, kafka_table_configs)

        sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True
        if sampling:
            compiled = apply_sampling(compiled, get_sample_size())

        result.append(compiled)

    return result
