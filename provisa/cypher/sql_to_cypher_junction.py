# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A junction a statement reads beside a node's row, as the relationships it holds (REQ-1586,
REQ-2136): ``FROM junction j WHERE j.fk = t.pk AND j.type IN (...)`` is the hop
``(t)-[:A|B]->(far)``. Used for a relationship's condition (EXISTS) and a to-many field (LATERAL)
over a junction, which is no node of the graph."""

from __future__ import annotations

import re
from dataclasses import dataclass

import sqlglot.expressions as exp

from provisa.cypher.sql_to_cypher_agg import UntranslatableSubquery
from provisa.cypher.sql_to_cypher_helpers import _resolve_label, _sql_to_cypher_expr
from provisa.gql.label_map import CypherLabelMap, RelationshipMapping


@dataclass
class JunctionHop:
    """One junction's rows beside a node: the relationships its condition keeps, which way round,
    each junction column's role, and the conjuncts left once the hop holds them."""

    rels: list[RelationshipMapping]
    near_is_source: bool
    near_label: str
    far_label: str
    near_key: str
    far_key: str
    #: junction column -> ("near" | "far" | "rel" | "type", the property it is)
    columns: dict[str, tuple[str, str]]
    rest: list[exp.Expression]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    #: "near" | "far" -> that end node's column -> property
    props: dict[str, dict[str, str]]

    def end_props(self, role: str) -> dict[str, str]:
        return self.props[role]

    def arrow(self, rel_var: str = "") -> str:
        types = "|".join(r.rel_type for r in self.rels)
        return f"-[{rel_var}:{types}]->" if self.near_is_source else f"<-[{rel_var}:{types}]-"

    def column(self, name: str, near: str, rel: str, far: str, *, optional: bool) -> str:
        """The junction column ``name`` as Cypher, read off the hop's three variables. Beside an
        OPTIONAL hop every column is null where no relationship is, the near key included."""
        if name not in self.columns:
            raise UntranslatableSubquery(f"junction column {name}")
        role, prop = self.columns[name]
        if role == "far":
            return f"{far}.{prop}"
        if role == "rel":
            return f"{rel}.{prop}"
        if role == "near":
            if optional:
                return f"CASE WHEN {far}.{self.far_key} IS NULL THEN NULL ELSE {near}.{prop} END"
            return f"{near}.{prop}"
        stored = {r.rel_type: _stored(r) for r in self.rels}
        if all(v == t for t, v in stored.items()):
            return f"type({rel})"
        whens = " ".join(f"WHEN '{t}' THEN '{v}'" for t, v in stored.items())
        return f"CASE type({rel}) {whens} END"


def _stored(rel: RelationshipMapping) -> str:
    assert rel.via is not None
    return rel.via.type_value if rel.via.type_value is not None else rel.rel_type


def junction_type_name(
    table: exp.Table,
    ctx: object,  # object-ok: circular-import boundary -- CompilationContext lives in provisa.compiler
    domain_to_sql_name,
) -> str | None:
    """The type name of the registered table ``table`` names, or None. Semantic SQL names a table
    by its field or by its own name (before a meta view's alias), as the naming convention has it."""
    for table_meta in ctx.tables.values():  # type: ignore[attr-defined]
        field_key = (
            table_meta.field_name.split("__", 1)[1]
            if "__" in table_meta.field_name
            else table_meta.field_name
        )
        names = {field_key, table_meta.original_table_name or table_meta.table_name}
        if table.name in names and (
            not table.db or domain_to_sql_name(table_meta.domain_id) == table.db
        ):
            return table_meta.type_name
    return None


def junction_rels(label_map: CypherLabelMap, type_name: str) -> list[RelationshipMapping]:
    """Every relationship the role's graph declares through the junction ``type_name``."""
    found: list[RelationshipMapping] = []
    every = [
        *label_map.relationships.values(),
        *(r for rs in label_map.aliases.values() for r in rs),
    ]
    for rel in every:
        if rel.via is not None and rel.via.type_name == type_name and rel not in found:
            found.append(rel)
    return found


def _value(node: exp.Expression, params: list | None) -> str | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if isinstance(node, exp.Literal) and node.is_string:
        return node.this
    if isinstance(node, exp.Parameter) and params:
        value = params[int(node.name) - 1]
        return value if isinstance(value, str) else None
    return None


def junction_hop(
    table: exp.Table,
    where: exp.Expression | None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    near_alias: str,
    near_label: str,
    ctx: object,  # object-ok: circular-import boundary -- CompilationContext lives in provisa.compiler
    label_map: CypherLabelMap,
    domain_to_sql_name,
    params: list | None,
) -> JunctionHop | None:
    """``table``, read under ``where`` beside the row of ``near_alias`` (a node of ``near_label``),
    as the relationships it holds; None when ``table`` is no junction of the graph. A junction
    whose condition does not keep only types the graph declares, or that is not tied to the near
    row by one of its keys, has no Cypher form (UntranslatableSubquery): a row of another type is
    no relationship, and a shorter answer is a wrong one."""
    from provisa.compiler.sql_types import key_list

    type_name = junction_type_name(table, ctx, domain_to_sql_name)
    rels = junction_rels(label_map, type_name) if type_name is not None else []
    if not rels:
        return None
    alias = table.alias or table.name
    conjuncts = _conjuncts(where.this) if where is not None else []

    def _own(col: exp.Expression) -> bool:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        return isinstance(col, exp.Column) and col.table in ("", alias)

    first = rels[0]
    assert first.via is not None
    near_is_source: bool | None = None
    correlation = None
    for c in conjuncts:
        if not (isinstance(c, exp.EQ) and isinstance(c.this, exp.Column)):
            continue
        if not isinstance(c.expression, exp.Column):
            continue
        own, outer = (c.this, c.expression) if _own(c.this) else (c.expression, c.this)
        if not _own(own) or outer.table != near_alias:
            continue
        if (own.name,) == tuple(
            first.via.source_columns
        ) and outer.name == first.join_source_column:
            near_is_source, correlation = True, c
        elif (own.name,) == tuple(
            first.via.target_columns
        ) and outer.name == first.join_target_column:
            near_is_source, correlation = False, c
        if correlation is not None:
            break
    if near_is_source is None:
        raise UntranslatableSubquery(f"junction {alias} tied to no key of its row")
    rest = [c for c in conjuncts if c is not correlation]

    type_columns = {r.via.type_column for r in rels if r.via is not None}
    if type_columns != {None}:
        (type_column,) = type_columns if len(type_columns) == 1 else (None,)
        if type_column is None:
            raise UntranslatableSubquery(f"junction {alias} with several type columns")
        kept: set[str] | None = None
        for c in rest:
            target = c.this
            if not (_own(target) and target.name == type_column):
                continue
            items = [c.expression] if isinstance(c, exp.EQ) else list(c.expressions or [])
            if not isinstance(c, (exp.EQ, exp.In)) or not items:
                continue
            values = [_value(e, params) for e in items]
            if None in values:
                continue
            kept = set(values)  # type: ignore[arg-type]
            rest = [r for r in rest if r is not c]
            break
        declared = {_stored(r): r for r in rels}
        if kept is None or not kept <= set(declared):
            raise UntranslatableSubquery(f"junction {alias} not kept to declared types")
        rels = [declared[v] for v in sorted(kept)]
    elif len(rels) > 1:
        raise UntranslatableSubquery(f"junction {alias} of several types with no type column")

    near_nm = label_map.nodes.get(first.source_label if near_is_source else first.target_label)
    far_nm = label_map.nodes.get(first.target_label if near_is_source else first.source_label)
    if near_nm is None or far_nm is None or label_map.display_label(near_nm) != near_label:
        raise UntranslatableSubquery(f"junction {alias} beside no node of {near_label}")
    columns: dict[str, tuple[str, str]] = {}
    via = first.via
    sides = (
        ("near", near_nm, via.source_columns, key_list(first.join_source_column)),
        ("far", far_nm, via.target_columns, key_list(first.join_target_column)),
    )
    if not near_is_source:
        sides = (
            ("near", near_nm, via.target_columns, key_list(first.join_target_column)),
            ("far", far_nm, via.source_columns, key_list(first.join_source_column)),
        )
    keys: dict[str, str] = {}
    props: dict[str, dict[str, str]] = {}
    for role, nm, junction_cols, node_keys in sides:
        by_column = {col: prop for prop, col in nm.properties.items()}
        # A node's key column is readable as its property even where it is not listed as one.
        props[role] = {**{k: k for k in key_list(nm.id_column)}, **by_column}
        for junction_col, key in zip(junction_cols, node_keys, strict=True):
            columns[junction_col] = (role, by_column.get(key, key))
            keys[role] = by_column.get(key, key)
    for prop, col in via.attributes.items():
        columns[col] = ("rel", prop)
    if via.type_column is not None:
        columns[via.type_column] = ("type", "")
    return JunctionHop(
        rels=rels,
        near_is_source=near_is_source,
        near_label=near_label,
        far_label=label_map.display_label(far_nm),
        near_key=keys["near"],
        far_key=keys["far"],
        columns=columns,
        rest=rest,
        props=props,
    )


def _conjuncts(node: exp.Expression) -> list[exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if isinstance(node, exp.And):
        return [*_conjuncts(node.this), *_conjuncts(node.expression)]
    if isinstance(node, exp.Paren):
        return _conjuncts(node.this)
    return [node]


def junction_fields(tree: exp.Select, hop_of) -> list[tuple[str, JunctionHop]] | None:
    """Take out of ``tree``'s joins each LATERAL over a junction beside the root's row -- a to-many
    field whose rows are the junction's -- as ``(its alias, its hop)``, ``hop_of(table, where)``
    reading each. None when one has no Cypher form: a condition beyond its key and types, or a
    limit on its rows."""
    kept: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    found: list[tuple[str, JunctionHop]] = []
    for join in tree.args.get("joins") or []:
        node = join.this
        sub = node.this if isinstance(node, exp.Lateral) else node
        inner = sub.this if isinstance(sub, exp.Subquery) else None
        alias = node.alias if isinstance(node, (exp.Lateral, exp.Subquery)) else None
        from_ = inner.args.get("from_") if isinstance(inner, exp.Select) else None
        if alias and from_ is not None and isinstance(from_.this, exp.Table):
            try:
                hop = hop_of(from_.this, inner.args.get("where"))  # type: ignore[union-attr]
            except UntranslatableSubquery:
                return None
            if hop is not None:
                if hop.rest or inner.args.get("limit") or inner.args.get("joins"):  # type: ignore[union-attr]
                    return None
                found.append((alias, hop))
                continue
        kept.append(join)
    if found:
        tree.set("joins", kept or None)
    return found


def junction_selections(
    select_exprs: list[exp.Expression],  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    fields: list[tuple[str, JunctionHop]],
    near: str,
    label_of,
) -> tuple[list[exp.Expr], dict[str, str]] | None:
    """``select_exprs`` with each selection of a junction field's row made a named placeholder,
    and the Cypher each placeholder stands for, read off the hop beside ``near``: a junction
    column the hop's variables, and a subquery of the node at one of its ends -- tied to it by
    that end's key -- that node. None when a selection of the field is neither."""
    variables = {alias: (f"jr{i}", f"j{i}", hop) for i, (alias, hop) in enumerate(fields)}
    out: list[exp.Expr] = []
    texts: dict[str, str] = {}
    # A placeholder's name must not be another selection's: the RETURN reads it by name.
    taken = {
        e.alias_or_name
        for e in select_exprs
        if not any(c.table in variables for c in e.find_all(exp.Column))
    }
    for e in select_exprs:
        body = e.this if isinstance(e, exp.Alias) else e
        if isinstance(body, exp.Column) and body.table in variables:
            rel, far, hop = variables[body.table]
            text = hop.column(body.name, near, rel, far, optional=True)
        elif isinstance(body, exp.Subquery) and _reads(body, variables):
            text = _end_subquery(body, variables, near, label_of)
            if text is None:
                return None
        elif any(c.table in variables for c in e.find_all(exp.Column)):
            return None
        else:
            out.append(e)
            continue
        name = e.alias_or_name
        if name in texts or name in taken:
            n = 2
            while f"{name}_{n}" in taken or f"{name}_{n}" in texts:
                n += 1
            name = f"{name}_{n}"
        texts[name] = text
        out.append(exp.alias_(exp.column(f"__junction_{len(texts)}"), name))
    return out, texts


def _reads(node: exp.Expression, variables: dict) -> bool:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return any(c.table in variables for c in node.find_all(exp.Column))


def _end_subquery(sub: exp.Subquery, variables: dict, near: str, label_of) -> str | None:
    """``(SELECT ARRAY_AGG(t.c) FROM end_table t WHERE t.key = j.end_col)``: the node at that end
    of the junction row, as a list of its one value -- null where no relationship is."""
    select = sub.this
    if not isinstance(select, exp.Select) or select.args.get("joins"):
        return None
    from_ = select.args.get("from_")
    table = from_.this if from_ is not None else None
    exprs = select.expressions
    if (
        not isinstance(table, exp.Table)
        or len(exprs) != 1
        or not isinstance(exprs[0], exp.ArrayAgg)
    ):
        return None
    own = table.alias or table.name
    where = select.args.get("where")
    parts = _conjuncts(where.this) if where is not None else []
    if len(parts) != 1 or not isinstance(parts[0], exp.EQ):
        return None
    a, b = parts[0].this, parts[0].expression
    if not (isinstance(a, exp.Column) and isinstance(b, exp.Column)):
        return None
    mine, theirs = (a, b) if a.table == own else (b, a)
    if mine.table != own or theirs.table not in variables:
        return None
    rel, far, hop = variables[theirs.table]
    role, key_prop = hop.columns.get(theirs.name, ("", ""))
    if role not in ("near", "far"):
        return None
    if label_of(table) != (hop.far_label if role == "far" else hop.near_label):
        return None
    value = exprs[0].this
    if not (isinstance(value, exp.Column) and value.table == own):
        return None
    var = far if role == "far" else near
    props = hop.end_props(role)
    if mine.name not in props or props[mine.name] != key_prop:
        return None
    prop = props.get(value.name)
    if prop is None:
        return None
    return f"CASE WHEN {far}.{hop.far_key} IS NULL THEN NULL ELSE [{var}.{prop}] END"


def _cypher_in(node: exp.In, render) -> str:
    """``x IN (a, b)`` as Cypher's ``x IN [a, b]``, each part rendered by ``render``."""
    if node.args.get("query") is not None or not node.expressions:
        raise UntranslatableSubquery(f"IN over {node.sql()}")
    items = ", ".join(_sql_to_cypher_expr(render(e)) for e in node.expressions)
    return f"{_sql_to_cypher_expr(render(node.this))} IN [{items}]"


def _junction_columns(
    columns: dict[str, tuple[str, str]], type_column: str | None, type_value: str, table_alias: str
):
    """A transform reading a junction's columns as one relationship type's: each key column its
    end node's key, the discriminator the type's value, every other column an attribute of r."""

    def _cypher(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if isinstance(node, exp.Column) and (not node.table or node.table == table_alias):
            if type_column is not None and node.name == type_column:
                return exp.Literal.string(type_value)
            if node.name not in columns:
                raise UntranslatableSubquery(f"junction column {node.name}")
            variable, prop = columns[node.name]
            return exp.column(prop, table=variable)
        return node

    return _cypher


def _discriminator_values(
    where: exp.Expression | None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    column: str,
    table_alias: str,
) -> tuple[set[str], exp.Expression] | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """The discriminator values a junction root's own condition keeps, from a top-level
    ``column = 'v'`` or ``column IN ('v', ...)``, with that conjunct; None when its condition
    keeps no such set."""
    if where is None:
        return None
    for c in _conjuncts(where.this):
        target = c.this
        if not (
            isinstance(target, exp.Column)
            and target.name == column
            and (not target.table or target.table == table_alias)
        ):
            continue
        if (
            isinstance(c, exp.EQ)
            and isinstance(c.expression, exp.Literal)
            and c.expression.is_string
        ):
            return {c.expression.this}, c
        if (
            isinstance(c, exp.In)
            and c.expressions
            and all(isinstance(e, exp.Literal) and e.is_string for e in c.expressions)
        ):
            return {e.this for e in c.expressions}, c
    return None


def _junction_root_cypher(
    tree: exp.Select,
    from_tbl: exp.Table,
    ctx: object,  # object-ok: circular-import boundary -- CompilationContext lives in provisa.compiler
    label_map: CypherLabelMap,
    domain_to_sql_name,
    override_limit: int | None,
) -> str | None:
    """A root over a junction table (REQ-1586) as the relationships it holds (REQ-2136): one
    ``MATCH (a)-[r:TYPE]->(b)`` per relationship type the junction carries, put together with
    UNION ALL. Its two foreign keys are the end nodes' keys, its discriminator the type's own
    value, and every other column an attribute of ``r``. None when the table is no junction, when
    its condition does not keep only types the role's graph declares (a row of another type is
    no relationship), or when an ORDER BY or LIMIT would have to apply across several types."""
    from provisa.compiler.sql_types import key_list

    type_name = junction_type_name(from_tbl, ctx, domain_to_sql_name)
    if type_name is None:
        return None
    every = [
        *label_map.relationships.values(),
        *(r for rs in label_map.aliases.values() for r in rs),
    ]
    rels: list[RelationshipMapping] = []
    for rel in every:
        if rel.via is not None and rel.via.type_name == type_name and rel not in rels:
            rels.append(rel)
    if not rels:
        return None
    table_alias = from_tbl.alias or from_tbl.name
    type_columns = {rel.via.type_column for rel in rels if rel.via is not None}
    if type_columns != {None}:
        # A junction carrying several types is every relationship only for the rows whose type
        # is one the role's graph declares: a row of another type is a row of the SQL answer and
        # of no relationship. So the root is its relationships only when its own condition keeps
        # the declared types alone; otherwise it has no Cypher form, never a shorter answer.
        type_column = next(iter(type_columns))
        if len(type_columns) != 1 or type_column is None:
            return None
        found = _discriminator_values(tree.args.get("where"), type_column, table_alias)
        if found is None:
            return None
        kept, pinned = found
        # Each branch is of one kept type, so the condition that kept it holds there already.
        rest = [c for c in _conjuncts(tree.args["where"].this) if c is not pinned]
        where_rest = None
        for c in rest:
            where_rest = c if where_rest is None else exp.And(this=where_rest, expression=c)
        tree = tree.copy()
        tree.set("where", exp.Where(this=where_rest) if where_rest is not None else None)
        declared = {
            (rel.via.type_value if rel.via.type_value is not None else rel.rel_type): rel
            for rel in rels
            if rel.via is not None
        }
        if not kept <= set(declared):
            return None
        rels = [declared[v] for v in sorted(kept)]
    if len(rels) > 1 and (tree.args.get("order") or tree.args.get("limit") or override_limit):
        return None
    select_exprs = tree.args.get("expressions") or []
    branches = []
    for rel in rels:
        via = rel.via
        assert via is not None
        src = label_map.nodes.get(rel.source_label)
        tgt = label_map.nodes.get(rel.target_label)
        if src is None or tgt is None:
            return None
        columns: dict[str, tuple[str, str]] = {}
        for end, nm, junction_cols, keys in (
            ("a", src, via.source_columns, key_list(rel.join_source_column)),
            ("b", tgt, via.target_columns, key_list(rel.join_target_column)),
        ):
            by_column = {col: prop for prop, col in nm.properties.items()}
            for junction_col, key in zip(junction_cols, keys, strict=True):
                columns[junction_col] = (end, by_column.get(key, key))
        for prop, col in via.attributes.items():
            columns[col] = ("r", prop)
        type_value = via.type_value if via.type_value is not None else rel.rel_type

        _cypher = _junction_columns(columns, via.type_column, type_value, table_alias)

        try:
            items = []
            for e in select_exprs:
                col = e.this if isinstance(e, exp.Alias) else e
                name = e.alias_or_name
                items.append(
                    f"{_sql_to_cypher_expr(col.copy().transform(_cypher).sql(dialect='postgres'))}"
                    f" AS {name}"
                )
            where = tree.args.get("where")
            condition = (
                _sql_to_cypher_expr(where.this.copy().transform(_cypher).sql(dialect="postgres"))
                if where is not None
                else None
            )
        except UntranslatableSubquery:
            return None
        lines = [
            f"MATCH (a:{label_map.display_label(src)})-[r:{rel.rel_type}]->"
            f"(b:{label_map.display_label(tgt)})"
        ]
        if condition is not None:
            lines.append(f"WHERE {condition}")
        lines.append(f"RETURN {', '.join(items)}")
        branches.append("\n".join(lines))
    statement = "\nUNION ALL\n".join(branches)
    if len(rels) == 1:
        order = tree.args.get("order")
        if order is not None:
            keys = ", ".join(
                f"{o.this.alias_or_name}{' DESC' if o.args.get('desc') else ''}"
                for o in order.expressions
            )
            statement += f"\nORDER BY {keys}"
        limit = override_limit
        if limit is None and tree.args.get("limit") is not None:
            limit = int(tree.args["limit"].expression.sql())
        if limit is not None:
            statement += f"\nLIMIT {limit}"
    return statement


def junction_exists(
    tbl: exp.Table,
    hop: JunctionHop,
    outer: str,
    scope: dict[str, tuple[str, str]],
    domain_to_label: dict[tuple[str, str], str],
    join_to_rel: dict[tuple[str, str, str], RelationshipMapping | None],
    src_tgt_to_rel: dict[tuple[str, str], str | None],
    prop_map_for_label,
    junction_of,
    exists_pattern,
) -> str:
    """A condition over a junction beside the row of ``outer``: ``EXISTS { MATCH (o)-[r:A|B]->
    (x:Far) WHERE ... }``. A condition the junction's own columns state reads them off ``o``,
    ``r`` and ``x``; one over the far node's table, tied to it by its key, is a condition on
    ``x`` itself."""
    alias = tbl.alias or tbl.name
    outer_var, _outer_label = scope[outer]
    rel_var, far_var = f"r{len(scope)}", f"x{len(scope)}"
    far_scope = {**scope}
    nested: dict[str, str] = {}

    def _far_conditions(node: exp.Exists) -> list[exp.Expression] | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        sub = node.this.this if isinstance(node.this, exp.Subquery) else node.this
        if not isinstance(sub, exp.Select) or sub.args.get("joins"):
            return None
        from_node = sub.args.get("from_")
        t2 = from_node.this if from_node is not None else None
        if not isinstance(t2, exp.Table) or _resolve_label(t2, domain_to_label) != hop.far_label:
            return None
        t2_alias = t2.alias or t2.name
        where = sub.args.get("where")
        parts = _conjuncts(where.this) if where is not None else []
        far_cols = {c for c, (role, _p) in hop.columns.items() if role == "far"}
        tie = next(
            (
                c
                for c in parts
                if isinstance(c, exp.EQ)
                and {c.this.table if isinstance(c.this, exp.Column) else None,
                     c.expression.table if isinstance(c.expression, exp.Column) else None}
                == {t2_alias, alias}
                and {c.this.name, c.expression.name} & far_cols
            ),
            None,
        )  # fmt: skip
        if tie is None:
            return None
        far_scope[t2_alias] = (far_var, hop.far_label)
        return [c for c in parts if c is not tie]

    def _cypher(n: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if isinstance(n, exp.Exists):
            key = f"__exists_{far_var}_{len(nested)}"
            folded = _far_conditions(n)
            if folded is not None:
                texts = [_text(c) for c in folded]
                nested[key] = " AND ".join(t for t in texts if t.strip("() ").lower() != "true")
                nested[key] = f"({nested[key]})" if nested[key] else "true"
            else:
                nested[key] = exists_pattern(
                    n,
                    far_scope,
                    domain_to_label,
                    join_to_rel,
                    src_tgt_to_rel,
                    prop_map_for_label,
                    junction_of,
                )
            return exp.column(key)
        if isinstance(n, exp.In):
            key = f"__in_{far_var}_{len(nested)}"
            nested[key] = _cypher_in(
                n, lambda e: e.copy().transform(_cypher).sql(dialect="postgres")
            )
            return exp.column(key)
        if isinstance(n, exp.Column) and n.table in ("", alias):
            key = f"__col_{far_var}_{len(nested)}"
            nested[key] = hop.column(n.name, outer_var, rel_var, far_var, optional=False)
            return exp.column(key)
        if isinstance(n, exp.Column) and n.table in far_scope:
            v, lbl = far_scope[n.table]
            return exp.column(prop_map_for_label(lbl).get(n.name, n.name), table=v)
        return n

    def _text(c: exp.Expression) -> str:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        return _sql_to_cypher_expr(c.copy().transform(_cypher).sql(dialect="postgres"))

    conditions = [t for t in (_text(c) for c in hop.rest) if t.strip("() ").lower() != "true"]
    body = f"MATCH ({outer_var}){hop.arrow(rel_var)}({far_var}:{hop.far_label})"
    if conditions:
        body += " WHERE " + " AND ".join(conditions)
    for key, pattern in reversed(list(nested.items())):
        body = re.sub(rf"\b{key}\b", lambda _m, p=pattern: p, body)
    return "EXISTS { " + body + " }"
