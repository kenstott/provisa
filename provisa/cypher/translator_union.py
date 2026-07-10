# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2a8e4f-9b7d-4f3a-8c1e-2d5b7f9a3c6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""All-relationships and domain UNION-building mixin for the Cypher→SQL translator.

Methods operate on _Translator state via self; mixed into _Translator, never
instantiated alone.
"""

from __future__ import annotations


import sqlglot.expressions as exp

from provisa.cypher.translator_helpers import (
    _const_literal,
)

from provisa.cypher.translator_types import CypherTranslateError


class _UnionMixin:  # mixin for _Translator
    def _build_all_rels_union(
        self,
        src_var: str | None,
        rel_var: str | None,
        tgt_var: str | None,
        src_domain: str | None = None,
        tgt_domain: str | None = None,
    ) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build UNION ALL subquery over all relationship types for fully-unlabeled patterns.

        src_domain / tgt_domain constrain the union to only include rels whose
        source / target node belongs to the named domain (PascalCase domain label).
        Pass None or "__all__" to leave that side unconstrained.
        """
        src_col = src_var or "n"
        rel_col = rel_var or "r"
        tgt_col = tgt_var or "m"
        alias = "_all_rels"
        self._all_rels_alias = alias
        self._all_rels_src_col = src_col
        self._all_rels_rel_col = rel_col
        self._all_rels_tgt_col = tgt_col

        src_type_set = (
            set(self._lm.domains.get(src_domain, []))
            if src_domain and src_domain != "__all__"
            else None
        )
        tgt_type_set = (
            set(self._lm.domains.get(tgt_domain, []))
            if tgt_domain and tgt_domain != "__all__"
            else None
        )

        branches: list[exp.Select] = []
        for rm in self._lm.relationships.values():
            src_nm = self._lm.nodes.get(rm.source_label)
            tgt_nm = self._lm.nodes.get(rm.target_label)
            if src_nm is None or tgt_nm is None:
                continue
            if src_type_set is not None and rm.source_label not in src_type_set:
                continue
            if tgt_type_set is not None and rm.target_label not in tgt_type_set:
                continue
            # Skip synthetic constant-join rels (e.g. HAS_TABLE to meta tables).
            # These are not real FK traversals and would pull in unrelated domain nodes.
            if rm.source_constant is not None:
                continue

            sa = f"_s_{rm.rel_type.lower()[:20]}"
            ta = f"_t_{rm.rel_type.lower()[:20]}"

            src_id_col = exp.Column(
                this=exp.Identifier(this=src_nm.id_column, quoted=True),
                table=exp.Identifier(this=sa),
            )
            tgt_id_col = exp.Column(
                this=exp.Identifier(this=tgt_nm.id_column, quoted=True),
                table=exp.Identifier(this=ta),
            )
            src_compound_id = exp.DPipe(
                this=exp.DPipe(
                    this=exp.Literal.string(src_nm.label),
                    expression=exp.Literal.string("|"),
                ),
                expression=exp.Cast(this=src_id_col, to=exp.DataType.build("VARCHAR")),
            )
            src_props_exprs: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]
            for prop_name, col_name in src_nm.properties.items():
                src_props_exprs.extend(
                    [
                        exp.Literal.string(prop_name),
                        exp.Column(
                            this=exp.Identifier(this=col_name, quoted=True),
                            table=exp.Identifier(this=sa),
                        ),
                    ]
                )
            src_json = exp.Anonymous(
                this="JSON_OBJECT",
                expressions=[
                    exp.Literal.string("id"),
                    src_compound_id,
                    exp.Literal.string("label"),
                    exp.Literal.string(src_nm.label),
                    exp.Literal.string("tableLabel"),
                    exp.Literal.string(src_nm.table_label),
                    exp.Literal.string("properties"),
                    exp.Anonymous(this="JSON_OBJECT", expressions=src_props_exprs),
                ],
            )
            tgt_compound_id = exp.DPipe(
                this=exp.DPipe(
                    this=exp.Literal.string(tgt_nm.label),
                    expression=exp.Literal.string("|"),
                ),
                expression=exp.Cast(this=tgt_id_col, to=exp.DataType.build("VARCHAR")),
            )
            tgt_props_exprs: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]
            for prop_name, col_name in tgt_nm.properties.items():
                tgt_props_exprs.extend(
                    [
                        exp.Literal.string(prop_name),
                        exp.Column(
                            this=exp.Identifier(this=col_name, quoted=True),
                            table=exp.Identifier(this=ta),
                        ),
                    ]
                )
            tgt_json = exp.Anonymous(
                this="JSON_OBJECT",
                expressions=[
                    exp.Literal.string("id"),
                    tgt_compound_id,
                    exp.Literal.string("label"),
                    exp.Literal.string(tgt_nm.label),
                    exp.Literal.string("tableLabel"),
                    exp.Literal.string(tgt_nm.table_label),
                    exp.Literal.string("properties"),
                    exp.Anonymous(this="JSON_OBJECT", expressions=tgt_props_exprs),
                ],
            )
            edge_id = exp.DPipe(
                this=exp.DPipe(
                    this=exp.Cast(this=src_id_col, to=exp.DataType.build("VARCHAR")),
                    expression=exp.Literal.string("-"),
                ),
                expression=exp.Cast(this=tgt_id_col, to=exp.DataType.build("VARCHAR")),
            )
            edge_json = exp.JSONObject(
                expressions=[
                    exp.JSONKeyValue(this=exp.Literal.string("id"), expression=edge_id),
                    exp.JSONKeyValue(
                        this=exp.Literal.string("type"), expression=exp.Literal.string(rm.rel_type)
                    ),
                    exp.JSONKeyValue(this=exp.Literal.string("startNode"), expression=src_json),
                    exp.JSONKeyValue(this=exp.Literal.string("endNode"), expression=tgt_json),
                ]
            )

            branch = (
                exp.select(
                    exp.alias_(src_json, src_col),
                    exp.alias_(edge_json, rel_col),
                    exp.alias_(tgt_json, tgt_col),
                )
                .from_(
                    exp.alias_(
                        exp.Table(
                            this=exp.Identifier(this=src_nm.sql_table_name, quoted=True),
                            db=exp.Identifier(this=src_nm.schema_name, quoted=True),
                            catalog=exp.Identifier(this=src_nm.catalog_name, quoted=True),
                        ),
                        alias=sa,
                    )
                )
                .join(
                    exp.alias_(
                        exp.Table(
                            this=exp.Identifier(this=tgt_nm.sql_table_name, quoted=True),
                            db=exp.Identifier(this=tgt_nm.schema_name, quoted=True),
                            catalog=exp.Identifier(this=tgt_nm.catalog_name, quoted=True),
                        ),
                        alias=ta,
                    ),
                    on=exp.EQ(
                        this=(
                            _const_literal(rm.source_constant)
                            if rm.source_constant is not None
                            else (
                                exp.maybe_parse(
                                    rm.source_expr.replace("{alias}", sa),
                                    dialect="postgres",
                                )
                                if rm.source_expr is not None
                                else exp.Column(
                                    this=exp.Identifier(this=rm.join_source_column, quoted=True),
                                    table=exp.Identifier(this=sa),
                                )
                            )
                        ),
                        expression=(
                            exp.maybe_parse(
                                rm.target_expr.replace("{alias}", ta),
                                dialect="postgres",
                            )
                            if rm.target_expr is not None
                            else exp.Column(
                                this=exp.Identifier(this=rm.join_target_column, quoted=True),
                                table=exp.Identifier(this=ta),
                            )
                        ),
                    ),
                    join_type="INNER",
                )
            )
            branches.append(branch)

        if not branches:
            raise CypherTranslateError("No relationship types found in schema")

        union: exp.Expression = branches[0]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for b in branches[1:]:
            union = exp.Union(this=union, expression=b, distinct=False)
        return exp.alias_(exp.Subquery(this=union), alias=alias)  # pyright: ignore[reportReturnType]

    def _build_domain_union(self, var: str, domain_name: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build UNION ALL subquery over all types in a domain."""
        type_labels = (
            list(self._lm.nodes.keys())
            if domain_name == "__all__"
            else self._lm.domains[domain_name]
        )
        props = self._collect_var_props(var)

        branches: list[exp.Select] = []
        had_resolvable = False
        for label in type_labels:
            nm = self._lm.nodes.get(label)
            if nm is None:
                continue
            if nm.native_filter_columns:
                continue
            had_resolvable = True
            # Metadata-based prune: skip branches where no required property exists.
            # Those branches can only contribute NULL rows, which a WHERE filter would discard.
            # This prevents full-table scans across all entity types on the engine.
            if props and not any(nm.properties.get(p) for p in props):
                continue
            select_items: list[exp.Expr] = [
                exp.alias_(exp.Literal.string(nm.label), alias="__label"),
                exp.alias_(
                    exp.DPipe(
                        this=exp.DPipe(
                            this=exp.Literal.string(nm.label),
                            expression=exp.Literal.string("|"),
                        ),
                        expression=exp.Cast(
                            this=exp.Column(this=exp.Identifier(this=nm.id_column, quoted=True)),
                            to=exp.DataType(this=exp.DataType.Type.VARCHAR),
                        ),
                    ),
                    alias="__id",
                ),
            ]
            for prop in props:
                phys_col = nm.physical_properties.get(prop)
                if phys_col:
                    select_items.append(
                        exp.alias_(
                            exp.Cast(
                                this=exp.Column(this=exp.Identifier(this=phys_col, quoted=True)),
                                to=exp.DataType(this=exp.DataType.Type.VARCHAR),
                            ),
                            alias=exp.Identifier(this=prop, quoted=True),
                        )
                    )
                else:
                    select_items.append(
                        exp.alias_(exp.null(), alias=exp.Identifier(this=prop, quoted=True))
                    )
            branch = exp.select(*select_items).from_(
                exp.alias_(
                    exp.Table(
                        this=exp.Identifier(this=nm.sql_table_name, quoted=True),
                        db=exp.Identifier(this=nm.schema_name, quoted=True),
                        catalog=exp.Identifier(this=nm.catalog_name, quoted=True),
                    ),
                    alias=f"_{nm.type_name.lower()}",
                )
            )
            branches.append(branch)

        if not branches:
            if not had_resolvable:
                raise CypherTranslateError(f"Domain {domain_name!r} has no resolvable types")
            # All types exist but none have the required properties — return zero rows without
            # scanning any tables (metadata resolved this at translation time).
            zero_items: list[exp.Expr] = [
                exp.alias_(exp.null(), alias="__label"),
                exp.alias_(exp.null(), alias="__id"),
            ] + [exp.alias_(exp.null(), alias=exp.Identifier(this=p, quoted=True)) for p in props]
            zero_row = exp.select(*zero_items).where(exp.false())
            return exp.alias_(exp.Subquery(this=zero_row), alias=var)  # pyright: ignore[reportReturnType]

        union: exp.Expression = branches[0]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for branch in branches[1:]:
            union = exp.Union(this=union, expression=branch, distinct=False)
        return exp.alias_(exp.Subquery(this=union), alias=var)  # pyright: ignore[reportReturnType]
