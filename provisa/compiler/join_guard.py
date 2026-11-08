# Copyright (c) 2026 Kenneth Stott
# Canary: b4432b97-dda6-46bf-8ed8-e3fde9b57b27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Relationships govern how tables are related, however it is written (REQ-603).

Two registered tables are RELATED whenever rows of one are matched or filtered against rows of
the other by comparing their columns: a join of any spelling (ON, USING, NATURAL, a comma or a
CROSS JOIN with the comparison in WHERE), through a CTE or a derived table, ``x IN`` / ``NOT IN
(SELECT ...)``, EXISTS / NOT EXISTS, a correlated scalar subquery, LATERAL. Every such pairing of
columns must be a registered relationship between those two tables on those columns, in either
direction. Matching by anything else -- an inequality, an expression, no condition at all -- is
refused.

Not relating, so needing no relationship: the branches of a UNION / INTERSECT / EXCEPT, and a
subquery that refers to nothing outside itself (a column compared with another table's
aggregate).

The rule is stated over table INSTANCES -- each time a statement reads a registered table, so a
self-join needs a registered self-relationship like any other pair -- and every column reference
is traced to the instance it comes from, through CTE, derived-table and subquery boundaries. A
column that is an expression over a base column is not that column: an equality on it is not a
column equality. The statement checked is the ONE semantic statement: every view it reads is
replaced by its SQL first (provisa.mv.view_read), so every join any view makes is held to the
reader's relationships. A materialized view the reader sees whole keeps its reference through
governance and is answered from its stored rows at optimization; its SQL is checked all the same.

A view's SQL is marked where it is expanded for this check (``view_expand.VIEW_MARK``), and the
marked derived table is also an instance of the view's own registered table, combined with every
instance its SQL reads. A column the view's SQL gives as a constant, computes as an expression, or
takes from a value computed below it (a derived table's aggregate) is a column of that instance:
an equality on it relates the other side to the view along the view's registered relationships,
as an equality on any other column of it does.
"""

# Requirements: REQ-603

from __future__ import annotations

import functools
from dataclasses import dataclass, replace
from typing import Any

import sqlglot.expressions as exp
from sqlglot.optimizer.scope import Scope, build_scope

from provisa.compiler import walk_checks


@dataclass(frozen=True)
class _Base:
    """A column of one instance of a registered table."""

    instance: int  # id() of the exp.Table node reading it
    table_id: int
    column: str
    #: The same column as each marked view it is read through gives it, outermost last: a column
    #: a view passes through is that view's column too, related along the view's relationships as
    #: well as its table's.
    views: tuple["_Base", ...] = ()

    def as_read(self) -> tuple["_Base", ...]:
        """The column, and the same column of each view it is read through."""
        return (self, *self.views)


#: Why two tables are related outside the registered relationships.
UNREGISTERED = "unregistered"  # a column equality that is no registered relationship
NOT_EQUALITY = "not_equality"  # matched by something other than a column equality
NO_CONDITION = "no_condition"  # combined with nothing relating them
NEVER_TRUE = "never_true"  # joined on a condition that is never true: no relationship named

#: The alias a condition's side is written under when it is compared with a registered one.
_ANY = "__a"


def form_of(side: exp.Expr) -> str:
    """``side`` -- one side of a condition, every column of it of ONE table -- in the form a
    registered relationship's own side is compared in: the same expression whatever alias the
    statement reads the table under."""
    shaped = side.copy()
    for column in [shaped] if isinstance(shaped, exp.Column) else shaped.find_all(exp.Column):
        column.set("table", exp.to_identifier(_ANY))
        column.set("db", None)
        column.set("catalog", None)
    return shaped.sql(dialect="postgres", identify=True)


@functools.lru_cache(maxsize=8192)
def registered_form(template: str) -> str:
    """A registered relationship's side, written with ``{alias}`` for its table, as
    :func:`form_of` writes a statement's.

    Kept by its text: the form is that text's alone, so a relationship saved with another side is
    another text and its own form, and none can be served stale. Every statement's guard asks
    for every registered relationship's (``sql_validator.computed_joins``), each a parse."""
    import sqlglot

    return form_of(sqlglot.parse_one(template.replace("{alias}", _ANY), read="postgres"))


@dataclass(frozen=True)
class Unrelated:
    """Two tables a statement relates outside the registered relationships, and how
    (``reason``); the columns of the equality relating them, for an UNREGISTERED one."""

    left_table: int
    right_table: int
    reason: str
    left_column: str | None = None
    right_column: str | None = None
    #: For a condition the statement writes: the condition as written, and each side as written
    #: (left with ``left_table``), so a refusal names it as the statement does -- a side read
    #: through a derived table or a CTE is that side, not the base column it resolves to.
    written: str | None = None
    left_written: str | None = None
    right_written: str | None = None


def _selects(scope: Scope) -> list[exp.Expr]:
    """What ``scope``'s query projects."""
    query: Any = scope.expression
    # A LATERAL wraps its query; a table function (UNNEST, a row generator) projects no column
    # of any table.
    while not isinstance(query, exp.Query):
        query = query.args.get("this")
        if not isinstance(query, exp.Expr):
            return []
    return query.selects


def _bodies(scope: Scope) -> list[Scope]:
    """``scope`` and every branch of it, at any depth: a UNION of more than two branches is a
    tree of two-branch unions."""
    found = [scope]
    for branch in scope.set_operation_scopes:
        found += _bodies(branch)
    return found


class _Layout:
    """The columns of ``scope`` and of each of its branches, read once: a statement reading a
    union of many tables asks of every column of it (a Cypher ``MATCH (n)`` over every label, its
    star expanded), and reading the branches again for each made the check cubic in the labels."""

    def __init__(self, scope: Scope) -> None:
        self.leaves = [b for b in _bodies(scope) if not b.set_operation_scopes]
        first = _selects(self.leaves[0])
        self.first_star = any(s.is_star for s in first)
        self.at: dict[str, int] = {}
        for i, select in enumerate(first):
            self.at.setdefault(select.alias_or_name, i)
        # Each branch: its selects, whether one is a star, and its selects by the name each gives.
        self.branches: list[tuple[list[exp.Expr], bool, dict[str, list[exp.Expr]]]] = []
        # Each branch's stars and its selects by name, in the order it gives them.
        self.picked: list[tuple[list[exp.Expr], dict[str, list[exp.Expr]]]] = []
        for branch in self.leaves:
            selects = _selects(branch)
            named: dict[str, list[exp.Expr]] = {}
            stars: list[exp.Expr] = []
            for select in selects:
                named.setdefault(select.alias_or_name, []).append(select)
                if select.is_star:
                    stars.append(select)
            self.branches.append((selects, bool(stars), named))
            self.picked.append((stars, named))

    def gives(self, name: str) -> bool:
        """Whether the scope gives a column ``name``: its first branch names it, or reads a star."""
        return self.first_star or name in self.at

    def reading(self, index: int, name: str) -> list[exp.Expr]:
        """The selects of branch ``index`` that can give ``name``: its stars and those naming it,
        in the order the branch lists them."""
        stars, named = self.picked[index]
        found = [*stars, *named.get(name, [])]
        if stars and named.get(name):
            order = {id(s): i for i, s in enumerate(self.branches[index][0])}
            found.sort(key=lambda s: order[id(s)])
        return found

    def by_position(self, name: str) -> list[tuple[Scope, str]]:
        """Each branch with the name its own column gives the column ``name``: in a UNION /
        INTERSECT / EXCEPT the first branch names the columns and every branch fills them by
        position (``SELECT winners AS name ... UNION ALL SELECT team ...``). A scope with no set
        operation is itself, with ``name``."""
        at = self.at.get(name)
        if at is None or self.first_star:
            return [(b, name) for b in self.leaves]
        out: list[tuple[Scope, str]] = []
        for branch, (selects, star, _named) in zip(self.leaves, self.branches, strict=True):
            if at < len(selects) and not star:
                out.append((branch, selects[at].alias_or_name))
            else:
                out.append((branch, name))
        return out

    def named(self, index: int, name: str) -> list[exp.Expr]:
        """The selects of branch ``index`` giving the name ``name``."""
        return self.branches[index][2].get(name, [])


_ONE, _ANY_OF, _ALL_OF = "one", "any_of", "all_of"


@dataclass(frozen=True, eq=False)
class _Choice:
    """The sets of instances a scope may combine, unexpanded: ONE set (``instances``), each set of
    ANY OF ``parts`` (a union's branches), or one set of each of ALL OF ``parts`` combined (the
    sources of one FROM). ``reach``: every instance any of its sets holds."""

    kind: str
    instances: frozenset[int]
    parts: tuple["_Choice", ...]
    reach: frozenset[int]


def project(
    choice: _Choice, among: frozenset[int] | set[int] | None, memo: dict | None = None
) -> list[frozenset[int]]:
    """Each set ``choice`` may combine, cut to the instances ``among`` (all of each, for None) --
    as a list of the sets expanded and then cut would hold them, each once, in the order it first
    comes. Each combination is cut and made distinct as it is built, so cut to a few instances it
    never holds more than the sets of those few."""
    memo = {} if memo is None else memo
    key = id(choice)
    if key in memo:
        return memo[key]
    found: list[frozenset[int]]
    if among is not None and not choice.reach & among:
        found = [frozenset()]
    elif choice.kind == _ONE:
        found = [choice.instances if among is None else choice.instances & among]
    elif choice.kind == _ANY_OF:
        found = list(dict.fromkeys(s for p in choice.parts for s in project(p, among, memo)))
    else:
        found = [frozenset()]
        for part in choice.parts:
            sets = project(part, among, memo)
            found = list(dict.fromkeys(a | b for a in found for b in sets))
    memo[key] = found
    return found


@dataclass(frozen=True)
class _Piece:
    """One connected piece of a set of instances, as what lies beyond the choice it is drawn
    from sees it: all that decides whether, combined with more, it is connected, and how a
    refusal names it."""

    least: int  # its least instance: the one a refusal names the piece by
    named: bool  # ``least`` has a never-true partner among what it is combined with
    seen: frozenset[int]  # its instances something beyond can be, or be related to
    reaches: frozenset[int]  # instances beyond it it is related to
    partners: frozenset[int]  # ``least``'s never-true partners beyond it, while not ``named``


def _kept(pieces: list[_Piece]) -> tuple[_Piece, ...]:
    """``pieces`` by least instance, all that can decide a refusal kept: a piece nothing beyond
    can be or be related to is final, and of those only the two least can be the two a refusal
    names -- the first piece of a set and the least apart from it. Kept whole, each disconnected
    choice was its own pieces, and a refused statement multiplied them as before."""
    ordered = sorted(pieces, key=lambda p: p.least)
    final = [p for p in ordered if not p.seen][:2]
    return tuple(p for p in ordered if p.seen or any(p is f for f in final))


class _Pieces:
    """Each set a group combines, as the connected pieces the registered pairings make of it.

    A group's sets are a product of its unions' branches: reading the same unions again and again
    (a view reading a view of a UNION, read three times), far too many to list (#329). Two sets
    of one choice that look the same from beyond it -- the same pieces, by least instance, each
    sharing and related to the same instances beyond -- are combined with anything else to the
    same pieces, so each choice is kept as its distinct pieces, and a combination never holds
    more of them than look different from beyond it. Nothing of the check is approximated: a
    group's pieces are those its instances make, by the registered pairings among them alone."""

    def __init__(
        self,
        members: set[int],
        edges: set[frozenset[int]],
        never_true: set[frozenset[int]],
        shared: set[int],
    ) -> None:
        self._members = members
        self._shared = frozenset(shared)
        self._related: dict[int, set[int]] = {m: set() for m in members}
        self._partners: dict[int, set[int]] = {m: set() for m in members}
        for edge, into in [(e, self._related) for e in edges] + [
            (e, self._partners) for e in never_true
        ]:
            if len(edge) == 2 and edge <= members:
                a, b = tuple(edge)
                into[a].add(b)
                into[b].add(a)
        self._near = {m: self._related[m] | self._partners[m] for m in members}
        self._memo: dict[int, list[tuple[_Piece, ...]]] = {}

    def _beyond_seen(self, instance: int, private: frozenset[int]) -> bool:
        """Whether something beyond the choice (outside ``private``) can be ``instance`` or be
        related to it."""
        return instance not in private or any(o not in private for o in self._near[instance])

    def _split(self, instances: frozenset[int], private: frozenset[int]) -> tuple[_Piece, ...]:
        held = {i for i in instances if i in self._members}
        pieces: list[_Piece] = []
        left = set(held)
        while left:
            start = left.pop()
            piece, frontier = {start}, [start]
            while frontier:
                for other in self._related[frontier.pop()] & left:
                    left.discard(other)
                    piece.add(other)
                    frontier.append(other)
            least = min(piece)
            named = bool(self._partners[least] & held)
            pieces.append(
                _Piece(
                    least,
                    named,
                    frozenset(i for i in piece if self._beyond_seen(i, private)),
                    frozenset(o for i in piece for o in self._related[i] if o not in private),
                    frozenset()
                    if named
                    else frozenset(p for p in self._partners[least] if p not in private),
                )
            )
        return _kept(pieces)

    def _join(self, pieces: tuple[_Piece, ...], private: frozenset[int]) -> tuple[_Piece, ...]:
        """``pieces`` of sets combined, connected where they share or are related to an
        instance, as seen from beyond ``private``."""
        top = list(range(len(pieces)))

        def root(i: int) -> int:
            while top[i] != i:
                top[i] = top[top[i]]
                i = top[i]
            return i

        # Each instance something beyond can see, to the piece holding it: two pieces holding
        # the same one, or one related to an instance the other holds, are connected.
        holder: dict[int, int] = {}
        for i, piece in enumerate(pieces):
            for instance in piece.seen:
                other = holder.setdefault(instance, i)
                if other != i:
                    top[root(i)] = root(other)
        for i, piece in enumerate(pieces):
            for instance in piece.reaches:
                other = holder.get(instance)
                if other is not None:
                    top[root(i)] = root(other)
        grouped: dict[int, list[_Piece]] = {}
        for i, piece in enumerate(pieces):
            grouped.setdefault(root(i), []).append(piece)
        out: list[_Piece] = []
        for together in grouped.values():
            lead = min(together, key=lambda p: p.least)
            if len(together) == 1 and not lead.seen:
                out.append(lead)  # final: nothing beyond it can change it
                continue
            named = lead.named or any(p in holder for p in lead.partners)
            seen = frozenset().union(*(p.seen for p in together))
            reaches = frozenset().union(*(p.reaches for p in together)) - seen
            out.append(
                _Piece(
                    lead.least,
                    named,
                    frozenset(i for i in seen if self._beyond_seen(i, private)),
                    frozenset(o for o in reaches if o not in private),
                    frozenset()
                    if named
                    else frozenset(p for p in lead.partners if p not in private),
                )
            )
        return _kept(out)

    def of(self, choice: _Choice) -> list[tuple[_Piece, ...]]:
        """Each distinct way ``choice``'s sets look from beyond it, in the order each first
        comes. Beyond it: every instance outside its reach, and those another part of a
        combination can read as well (``shared``)."""
        key = id(choice)
        if key in self._memo:
            return self._memo[key]
        private = choice.reach - self._shared
        if choice.kind == _ONE:
            found = [self._split(choice.instances, private)]
        elif choice.kind == _ANY_OF:
            found = list(
                dict.fromkeys(self._join(s, private) for p in choice.parts for s in self.of(p))
            )
        else:
            parts = choice.parts
            after = [frozenset()] * (len(parts) + 1)  # what the parts after each can read
            for i in range(len(parts) - 1, -1, -1):
                after[i] = after[i + 1] | parts[i].reach
            done = parts[0].reach
            mine = done - after[1] - self._shared
            found = list(dict.fromkeys(self._join(s, mine) for s in self.of(parts[0])))
            for i in range(1, len(parts)):
                done = done | parts[i].reach
                mine = done - after[i + 1] - self._shared
                more = self.of(parts[i])
                found = list(dict.fromkeys(self._join(a + b, mine) for a in found for b in more))
        self._memo[key] = found
        return found

    def whole(self, choice: _Choice) -> list[tuple[tuple[int, bool], ...]]:
        """Each distinct set of pieces ``choice``'s sets make, nothing beyond them: each piece by
        its least instance, and whether that has a never-true partner in the set."""
        return list(
            dict.fromkeys(
                tuple((p.least, p.named) for p in self._join(s, choice.reach))
                for s in self.of(choice)
            )
        )


class _Resolver:
    def __init__(self, resolve_table_id: Any, columns_of: Any) -> None:
        self._table_id = resolve_table_id
        self._columns_of = columns_of
        self._choices: dict[int, _Choice] = {}
        # Instances two parts of one combination can both read (_Choice.private).
        self.shared: set[int] = set()
        # first branch's query -> (that branch's scope, the union's scope), for every union
        self._unions: dict[int, tuple[Scope, Scope]] = {}
        self.table_of: dict[int, int] = {}  # instance -> table id
        # a marked view's derived table, and each branch of its SQL -> (its instance, view id)
        self._view_scopes: dict[int, tuple[int, int]] = {}
        self._view_sources: set[int] = set()  # the marked derived tables themselves
        self._view_instances: set[int] = set()  # each marked view's own instance
        self.view_edges: set[frozenset[int]] = set()  # a view's instance and its SQL's instances
        self._layouts: dict[int, _Layout] = {}  # a scope -> its columns, read once (_Layout)

    def _layout(self, scope: Scope) -> _Layout:
        layout = self._layouts.get(id(scope))
        if layout is None:
            layout = self._layouts[id(scope)] = _Layout(scope)
        return layout

    def unions_of(self, scopes: list[Scope]) -> None:
        """Note the statement's unions, so a recursive CTE's reading of itself is known, and its
        marked views, each an instance of the view's registered table."""
        from provisa.compiler.view_expand import marked_view

        for scope in scopes:
            if scope.set_operation_scopes:
                first = scope.set_operation_scopes[0]
                self._unions[id(first.expression)] = (first, scope)
            for name, (_node, source) in scope.selected_sources.items():
                view = marked_view(name)
                if view is None or not isinstance(source, Scope):
                    continue
                # A view keyed schema.name (an ontology Class, REQ-2163) is its schema's table.
                schema, _, name = view.rpartition(".")
                view_id = self._table_id(
                    exp.Table(
                        this=exp.to_identifier(name),
                        db=exp.to_identifier(schema) if schema else None,
                    )
                )
                if view_id is None:
                    continue
                instance = id(source.expression)
                self.table_of[instance] = view_id
                self._view_instances.add(instance)
                self._view_sources.add(id(source))
                for body in _bodies(source):
                    self._view_scopes[id(body)] = (instance, view_id)

    def _rows_of(self, source: Any) -> Any:
        """``source`` as the scope whose instances a reader of it combines with. A recursive CTE
        read from inside itself is a scope of its own over the CTE's first branch: the rows it
        goes on from start there, so that branch's instances are the ones combined with."""
        if isinstance(source, Scope):
            known = self._unions.get(id(source.expression))
            if known is not None and source is not known[0]:
                return known[0]
        return source

    def _columns_from(self, source: Any) -> Any:
        """``source`` as the scope its columns are computed in: for a recursive CTE read from
        inside itself, the whole CTE -- a column of it is any branch's."""
        if isinstance(source, Scope):
            known = self._unions.get(id(source.expression))
            if known is not None and source is not known[0]:
                return known[1]
        return source

    def _base(self, table: exp.Table) -> _Base | None:
        table_id = self._table_id(table)
        if table_id is None:
            return None
        self.table_of[id(table)] = table_id
        return _Base(id(table), table_id, "")

    def one(self, instances: frozenset[int]) -> _Choice:
        """The one set ``instances``."""
        return _Choice(_ONE, instances, (), instances)

    def any_of(self, parts: list[_Choice]) -> _Choice:
        """Each set of any of ``parts``: the branches of a union."""
        if len(parts) == 1:
            return parts[0]
        return _Choice(
            _ANY_OF, frozenset(), tuple(parts), frozenset().union(*(p.reach for p in parts))
        )

    def all_of(self, parts: list[_Choice]) -> _Choice:
        """One set of each of ``parts``, combined: the sources of one FROM."""
        if not parts:
            return self.one(frozenset())
        if len(parts) == 1:
            return parts[0]
        seen: set[int] = set()
        for part in parts:
            # An instance two parts can both read (a CTE read twice) is not either part's own.
            self.shared |= seen & part.reach
            seen |= part.reach
        return _Choice(_ALL_OF, frozenset(), tuple(parts), frozenset(seen))

    def choices(self, scope: Scope) -> _Choice:
        """The sets of instances ``scope`` combines: one set, or one per choice of branch where it
        reads a UNION (the branches of a union are alternatives, never combined with each other),
        unexpanded -- a statement reading the same unions again and again has a product of them
        far too many to list (#329: 4 ** 12)."""
        key = id(scope)
        if key in self._choices:
            return self._choices[key]
        # A recursive CTE reads itself: nothing more.
        self._choices[key] = self.one(frozenset())
        if scope.set_operation_scopes:
            found = self.any_of([self.choices(branch) for branch in scope.set_operation_scopes])
        else:
            per_source: list[_Choice] = []
            for _name, (_node, source) in scope.selected_sources.items():
                if isinstance(source, exp.Table):
                    base = self._base(source)
                    per_source.append(self.one(frozenset({base.instance}) if base else frozenset()))
                elif isinstance(source, Scope):
                    alts = self.choices(self._rows_of(source))
                    if id(source) in self._view_sources:
                        # Read as the view: its own instance beside what its SQL reads. Only
                        # here, where it is read -- inside, its SQL is checked as written.
                        instance, _view_id = self._view_scopes[id(source)]
                        self.view_edges.update(frozenset({instance, i}) for i in alts.reach)
                        alts = self.all_of([alts, self.one(frozenset({instance}))])
                    per_source.append(alts)
            found = self.all_of(per_source)
        self._choices[key] = found
        return found

    def alternatives(self, scope: Scope) -> list[frozenset[int]]:
        """Each set of instances ``scope`` combines, listed (:meth:`choices`, expanded)."""
        return project(self.choices(scope), None)

    def _has_column(self, source: Any, name: str) -> bool:
        source = self._columns_from(source)
        if isinstance(source, exp.Table):
            table_id = self._table_id(source)
            return table_id is not None and name in self._columns_of(table_id)
        if isinstance(source, Scope):
            branches = source.set_operation_scopes or [source]
            return any(self._layout(b).gives(name) for b in branches)
        return False

    def resolve(
        self, scope: Scope, column: exp.Column, seen: frozenset = frozenset()
    ) -> set[_Base]:
        """The base columns ``column`` of ``scope`` names; empty for a computed value, a column of
        no registered table, and a name nothing in reach holds."""
        marker = (id(scope), column.table, column.name)
        if marker in seen:
            return set()
        seen = seen | {marker}
        if column.table:
            # A CTE the statement names without reading it in this FROM still relates its rows
            # to the rows around the reference: it is resolved as the CTE it names.
            sources = [scope.sources.get(column.table) or scope.cte_sources.get(column.table)]
        else:
            sources = [
                source
                for _node, source in scope.selected_sources.values()
                if self._has_column(source, column.name)
            ]
        sources = [self._columns_from(s) for s in sources if s is not None]
        if not sources:
            # Not of this scope: a column of the statement around it (a correlated reference).
            return self.resolve(scope.parent, column, seen) if scope.parent is not None else set()
        out: set[_Base] = set()
        for source in sources:
            if isinstance(source, exp.Table):
                base = self._base(source)
                if base is not None:
                    out.add(_Base(base.instance, base.table_id, column.name))
            elif isinstance(source, Scope):
                out |= self._projection(source, column.name, seen)
        return out

    def touched(self, scope: Scope, column: exp.Column, seen: frozenset = frozenset()) -> set[int]:
        """Every instance ``column`` of ``scope`` draws on: the one it names, or, where it names a
        value a CTE or a derived table computes, each instance that value is computed from."""
        marker = (id(scope), column.table, column.name)
        if marker in seen:
            return set()
        seen = seen | {marker}
        plain = self.resolve(scope, column)
        if plain:
            return {b.instance for b in plain}
        if column.table:
            sources = [scope.sources.get(column.table) or scope.cte_sources.get(column.table)]
        else:
            sources = [
                source
                for _node, source in scope.selected_sources.values()
                if self._has_column(source, column.name)
            ]
        sources = [self._columns_from(s) for s in sources if s is not None]
        if not sources:
            return self.touched(scope.parent, column, seen) if scope.parent is not None else set()
        out: set[int] = set()
        for source in sources:
            if not isinstance(source, Scope):
                continue
            layout = self._layout(source)
            for index, (branch, name) in enumerate(layout.by_position(column.name)):
                for select in layout.named(index, name):
                    for inner in select.find_all(exp.Column):
                        if not _orders_a_window(inner, select):
                            out |= self.touched(branch, inner, seen)
        return out

    def _projection(self, scope: Scope, name: str, seen: frozenset) -> set[_Base]:
        if scope.set_operation_scopes:
            return set().union(
                *(self._projection(b, n, seen) for b, n in self._layout(scope).by_position(name))
            )
        out: set[_Base] = set()
        named = False  # the scope projects a column of this name
        for select in self._layout(scope).reading(0, name):
            inner = select.unalias()
            if select.is_star:
                table = inner.table if isinstance(inner, exp.Column) else ""
                out |= self.resolve(scope, exp.column(name, table=table or None), seen)
            elif select.alias_or_name == name and isinstance(inner, exp.Column):
                named = True
                passed = self.resolve(scope, inner, seen)
                if id(scope) in self._view_scopes:
                    # A column a view passes through is also a column of the view.
                    instance, view_id = self._view_scopes[id(scope)]
                    own = _Base(instance, view_id, name)
                    passed = {replace(b, views=(*b.views, own)) for b in passed}
                out |= passed
            elif select.alias_or_name == name and id(scope) in self._view_scopes:
                # A constant or an expression a view gives: a column of the view itself.
                instance, view_id = self._view_scopes[id(scope)]
                out.add(_Base(instance, view_id, name))
        if not out and named and id(scope) in self._view_scopes:
            # A column a view takes from a value computed below it (a derived table's aggregate
            # or expression) is no base column: it is a column of the view itself (#270).
            instance, view_id = self._view_scopes[id(scope)]
            return {_Base(instance, view_id, name)}
        if (
            out
            and id(scope) in self._view_scopes
            and all(b.instance in self._view_instances for b in out)
        ):
            # A constant a view reads from a view it reads is a constant it gives: a column of
            # the outermost view that gives it.
            instance, view_id = self._view_scopes[id(scope)]
            return {_Base(instance, view_id, name)}
        return out


def unrelated_tables(
    tree: exp.Expr,
    *,
    resolve_table_id: Any,
    columns_of: Any,
    registered: set[tuple[int, int, str, str]],
    exempt_table: Any,
    same_remote_source: Any,
    computed: set[tuple[int, int, str, str]] = frozenset(),  # type: ignore[assignment]
    constants: set[tuple[int, int, str, str]] = frozenset(),  # type: ignore[assignment]
    primary_keys: dict[int, frozenset[str]] | None = None,
) -> list[Unrelated]:
    """The pairs of registered tables ``tree`` combines outside the ``registered`` relationships
    -- (table id, table id, column, column), both directions. ``computed``: the relationships
    registered with a computed side, as (table id, table id, form, form), both directions, each
    form as :func:`registered_form` writes it. ``constants``: those whose source side is a
    constant, as (source table id, target table id, the constant, the target side's form) -- the
    target's rows matching the constant are related to every row of the source. ``exempt_table(table_id)``: a table
    any table may be read beside (the meta and ops domains). ``same_remote_source(a, b)``: two
    tables of one remote source, whose own model relates them, with no relationship registered
    between them here.

    A table paired with ITSELF needs no relationship in exactly two cases (REQ-603). The same
    row: equality on the table's whole registered primary key (``primary_keys``) re-reads the
    row and reaches no other; a table with no registered key has no such case. A filter only:
    IN / NOT IN / EXISTS / NOT EXISTS over the same table decides which rows appear and brings
    no second copy's columns out. Anything else that brings out a second copy of the table is a
    pairing like any other, and needs a relationship registered from the table to itself."""
    queries = [tree] if isinstance(tree, exp.Query) else []
    if not queries:
        queries = [q for q in tree.find_all(exp.Query) if q.find_ancestor(exp.Query) is None]
    found: list[Unrelated] = []
    for query in queries:
        root = build_scope(query)
        if root is not None:
            found += _unrelated_in(
                root,
                resolve_table_id,
                columns_of,
                registered,
                exempt_table,
                same_remote_source,
                computed,
                constants,
                primary_keys or {},
            )
    return found


def _orders_a_window(column: exp.Column, select: exp.Expr) -> bool:
    """Whether ``column`` only partitions or orders a window function of ``select`` (REQ-603,
    amended 2026-11-07). A filter on such a result -- ROW_NUMBER() ... = 1, the latest row per
    group -- reads a rank of rows the window's own query already combined, where its joins are
    held to the rule; it matches no rows of one table with another. A column the window function
    takes as its argument (LAG(v.id)) is a value it gives, and still touches its table."""
    node: exp.Expr = column
    while node is not select and node.parent is not None:
        parent = node.parent
        if isinstance(parent, exp.Window) and node is not parent.this:
            return True
        node = parent
    return False


def _is_filter(query: exp.Expr) -> bool:
    """Whether ``query`` is the subquery of an IN / NOT IN / EXISTS / NOT EXISTS: one that
    decides which rows of the statement around it appear, and gives it none of its own."""
    node, parent = query, query.parent
    while isinstance(parent, (exp.Subquery, exp.Paren)):
        node, parent = parent, parent.parent
    if isinstance(parent, exp.Exists):
        return True
    return isinstance(parent, exp.In) and parent.args.get("query") is node


def _own_columns(scope: Scope) -> list[exp.Column]:
    """The columns written in ``scope``'s own query. sqlglot's ``Scope.columns`` also lists a
    nested scope's columns it cannot place there (a derived table's GROUP BY column, say), as
    candidates for a correlated reference; each is judged in the scope it is written in, which
    resolves it outward when it is one."""
    query = scope.expression
    if not isinstance(query, exp.Query):
        return list(scope.columns)
    return [c for c in scope.columns if c.find_ancestor(exp.Query) is query]


def _derived_side(scope: Scope, column: exp.Column) -> str | None:
    """``column`` as written when it is read through a derived table or a CTE of the scope it is
    named in (or one around it) -- not a plain table's alias, which names its table -- else None."""
    at: Scope | None = scope
    while at is not None and column.table:
        source = at.sources.get(column.table) or at.cte_sources.get(column.table)
        if source is not None:
            return None if isinstance(source, exp.Table) else column.sql(dialect="postgres")
        at = at.parent
    return None


def _conjuncts(condition: exp.Expr | None) -> list[exp.Expr]:
    """``condition`` as the conditions it ANDs together."""
    if condition is None:
        return []
    node = condition.unnest()
    if isinstance(node, exp.And):
        return _conjuncts(node.left) + _conjuncts(node.right)
    return [node]


def _joined(instances: list[int], edges: set[frozenset[int]]) -> bool:
    """Whether ``edges`` connect every one of ``instances``."""
    reached, frontier = {instances[0]}, [instances[0]]
    while frontier:
        at = frontier.pop()
        for edge in edges:
            if at in edge:
                for other in edge - reached:
                    reached.add(other)
                    frontier.append(other)
    return set(instances) <= reached


def _unrelated_in(
    root: Scope,
    resolve_table_id: Any,
    columns_of: Any,
    registered: set[tuple[int, int, str, str]],
    exempt_table: Any,
    same_remote_source: Any,
    computed: set[tuple[int, int, str, str]],
    constants: set[tuple[int, int, str, str]],
    primary_keys: dict[int, frozenset[str]],
) -> list[Unrelated]:
    resolver = _Resolver(resolve_table_id, columns_of)
    # (instance, instance) of one table -> the key columns the statement equates between them
    same_row: dict[frozenset[int], set[str]] = {}
    # instance -> the IN / EXISTS subquery it is read in, innermost (absent: in none)
    filter_of: dict[int, int] = {}
    # instance -> the (side's form, constant) equalities the statement holds it to
    held_to: dict[int, set[tuple[str, str]]] = {}
    never_true: set[frozenset[int]] = set()
    scopes = list(root.traverse())
    resolver.unions_of(scopes)
    for scope in scopes:  # outermost last in traverse(): an inner filter's instances keep theirs
        if _is_filter(scope.expression):
            for instance in resolver.choices(scope).reach:
                filter_of.setdefault(instance, id(scope))
    by_query = {id(s.expression): s for s in scopes}
    edges: set[frozenset[int]] = set()  # registered pairings, by instance pair
    # Each set of instances combined, by the choice of union branches it may be (_Choice).
    groups: list[_Choice] = []
    found: list[Unrelated] = []
    table_of = resolver.table_of

    def refuse(item: Unrelated) -> None:
        if item not in found:
            found.append(item)

    def exempt(instance: int) -> bool:
        return exempt_table(table_of[instance])

    def pair(
        left: set[_Base],
        right: set[_Base],
        written: exp.Expr | None = None,
        sides: tuple[str | None, str | None] = (None, None),
    ) -> None:
        """A pairing of columns the statement makes: registered, or refused -- naming the
        condition as written, and each side of it read through a derived table or a CTE
        (``sides``), when the statement writes one."""
        for a in left:
            for b in right:
                if a.instance == b.instance or exempt(a.instance) or exempt(b.instance):
                    continue
                through = next(
                    (
                        (x, y)
                        for x in a.as_read()
                        for y in b.as_read()
                        if (a.views or b.views)
                        and x.instance != y.instance
                        and (x.table_id, y.table_id, x.column, y.column) in registered
                    ),
                    None,
                )
                if through is not None:
                    # Related along a relationship of a view one side is read through.
                    edges.add(frozenset({through[0].instance, through[1].instance}))
                    continue
                if a.table_id == b.table_id and filters_only(a.instance, b.instance):
                    edges.add(frozenset({a.instance, b.instance}))
                    continue
                if (
                    a.table_id == b.table_id
                    and a.column == b.column
                    and a.column in primary_keys.get(a.table_id, ())
                    and (a.table_id, b.table_id, a.column, b.column) not in registered
                ):
                    # Settled once every condition is read: the same row only on the whole key.
                    same_row.setdefault(frozenset({a.instance, b.instance}), set()).add(a.column)
                    continue
                if (a.table_id, b.table_id, a.column, b.column) in registered or same_remote_source(
                    a.table_id, b.table_id
                ):
                    edges.add(frozenset({a.instance, b.instance}))
                else:
                    (x, x_said), (y, y_said) = sorted(
                        ((a, sides[0]), (b, sides[1])), key=lambda c: (c[0].table_id, c[0].column)
                    )
                    refuse(
                        Unrelated(
                            x.table_id,
                            y.table_id,
                            UNREGISTERED,
                            x.column,
                            y.column,
                            written=None if written is None else written.sql(dialect="postgres"),
                            left_written=x_said,
                            right_written=y_said,
                        )
                    )

    def filters_only(a: int, b: int) -> bool:
        """Whether one of two instances is read in an IN / EXISTS subquery the other is outside
        of: the subquery filters the other's rows and none of its own come out."""
        return filter_of.get(a) != filter_of.get(b)

    def other_matching(instances: set[int]) -> None:
        """Instances matched by something other than a column equality."""
        held = sorted(i for i in instances if not exempt(i))
        tables = sorted({table_of[i] for i in held})
        if len(held) >= 2 and _joined(held, resolver.view_edges):
            # A view's instance and what its SQL reads: one row of the view, nothing related.
            return
        if len(tables) == 1 and all(filters_only(a, b) for a in held for b in held if a < b):
            # One table filtered by its own rows, on whatever condition.
            edges.update(frozenset({a, b}) for a in held for b in held if a < b)
        elif len(held) >= 2:
            refuse(Unrelated(tables[0], tables[-1], NOT_EQUALITY))

    for scope in scopes:
        own = resolver.choices(scope)
        groups.append(own)
        reach = own.reach
        mine = {id(c) for c in _own_columns(scope)}
        query = scope.expression
        while not isinstance(query, (exp.Select, exp.SetOperation)):
            query = query.args.get("this")
            if not isinstance(query, exp.Expr):
                break
        conditions: list[exp.Expr] = []
        if isinstance(query, exp.Select):
            for join in query.args.get("joins") or []:
                on = join.args.get("on")
                conditions += _conjuncts(on)
                if isinstance(on, exp.Boolean) and on.this is False:
                    # Joined on FALSE: what a pattern naming a relationship that does not
                    # connect its two tables lowers to.
                    joined_source = scope.sources.get(join.this.alias_or_name)
                    if isinstance(joined_source, exp.Table) and id(joined_source) in table_of:
                        for other in reach - {id(joined_source)}:
                            never_true.add(frozenset({id(joined_source), other}))
                named = [u.name for u in join.args.get("using") or []]
                joined = join.this
                source = scope.sources.get(joined.alias_or_name)
                if join.args.get("method") == "NATURAL" and source is not None:
                    named = sorted(
                        self_columns(resolver, source) & others_columns(resolver, scope, source)
                    )
                for name in named:
                    # USING (c) and NATURAL pair the joined table's c with every other c in reach.
                    inside = resolver.resolve(scope, exp.column(name, table=joined.alias_or_name))
                    around = resolver.resolve(scope, exp.column(name)) - inside
                    pair(inside, around)
            where = query.args.get("where")
            conditions += _conjuncts(where.this if where is not None else None)
            having = query.args.get("having")
            # A HAVING condition over aggregates compares totals, not rows of two tables; one
            # with no aggregate in it compares group keys, and is held to the rule.
            conditions += [
                c
                for c in _conjuncts(having.this if having is not None else None)
                if c.find(exp.AggFunc) is None
            ]
        # A walk's own check that it takes no relationship twice, as the graph compiler wrote it
        # (provisa.compiler.walk_checks): the maintainer's one exemption from this rule.
        conditions = [c for c in conditions if not walk_checks.compiled(c)]

        def one_instance(side: exp.Expr, scope: Scope = scope, mine: set[int] = mine) -> int | None:
            """The one instance every column of ``side`` is a plain column of, else None."""
            columns = [side] if isinstance(side, exp.Column) else list(side.find_all(exp.Column))
            drawn: set[int] = set()
            for column in columns:
                if id(column) not in mine:
                    return None  # a column of another scope: not a side of one table
                bases = resolver.resolve(scope, column)
                if len(bases) != 1:
                    return None
                drawn |= {b.instance for b in bases}
            return next(iter(drawn)) if len(drawn) == 1 else None

        for condition in conditions:
            tested = condition.this.unnest() if isinstance(condition, exp.Not) else condition
            if isinstance(tested, exp.In) and tested.args.get("query") is not None:
                inner = by_query.get(id(tested.args["query"].this))
                if inner is not None:
                    left = tested.this.unnest()
                    given: set[_Base] = set()
                    drawn: set[int] = set()
                    # What the subquery's selected column reads, where it reads it through a
                    # derived table or a CTE of its own scope (the first branch that does).
                    given_said: str | None = None
                    for branch in inner.set_operation_scopes or [inner]:
                        selects = _selects(branch)
                        only = selects[0].unalias() if len(selects) == 1 else None
                        if isinstance(only, exp.Column):
                            given |= resolver.resolve(branch, only)
                            given_said = given_said or _derived_side(branch, only)
                        drawn |= resolver.choices(branch).reach
                    mine_plain = (
                        resolver.resolve(scope, left) if isinstance(left, exp.Column) else set()
                    )
                    touching: set[int] = set()
                    for column in left.find_all(exp.Column):
                        touching |= resolver.touched(scope, column)
                    # x IN (SELECT y ...) matches the rows around it with the rows selected.
                    for outer_part in project(own, touching):
                        if outer_part:
                            groups.append(
                                resolver.all_of([resolver.one(outer_part), resolver.choices(inner)])
                            )
                    if mine_plain and given:
                        sides = (
                            _derived_side(scope, left) if isinstance(left, exp.Column) else None,
                            given_said,
                        )
                        pair(
                            mine_plain,
                            given,
                            tested if sides != (None, None) else None,
                            sides,
                        )
                    elif touching and drawn:
                        other_matching(touching | drawn)
                    continue
            columns = [c for c in condition.find_all(exp.Column) if id(c) in mine]
            touching = set()
            for column in columns:
                touching |= resolver.touched(scope, column)
            if len({i for i in touching if not exempt(i)}) < 2:
                # A condition on one table relates it to no other -- unless it is the constant
                # side of a registered relationship, which the connecting below reads.
                if isinstance(condition, exp.EQ) and len(touching) == 1:
                    sides = (condition.left.unnest(), condition.right.unnest())
                    for value, other in (sides, sides[::-1]):
                        if isinstance(value, exp.Literal) and one_instance(other) is not None:
                            held_to.setdefault(next(iter(touching)), set()).add(
                                (form_of(other), value.sql(dialect="postgres"))
                            )
                continue
            if isinstance(condition, exp.EQ):
                left_side, right_side = condition.left.unnest(), condition.right.unnest()
                if isinstance(left_side, exp.Column) and isinstance(right_side, exp.Column):
                    left_bases = resolver.resolve(scope, left_side)
                    right_bases = resolver.resolve(scope, right_side)
                    if left_bases and right_bases:
                        sides = (
                            _derived_side(scope, left_side),
                            _derived_side(scope, right_side),
                        )
                        pair(
                            left_bases,
                            right_bases,
                            condition if sides != (None, None) else None,
                            sides,
                        )
                        continue
                # A registered relationship's own condition, where one side of it is computed.
                left_one = one_instance(left_side)
                right_one = one_instance(right_side)
                if left_one is not None and right_one is not None:
                    key = (
                        table_of[left_one],
                        table_of[right_one],
                        form_of(left_side),
                        form_of(right_side),
                    )
                    if key in computed:
                        edges.add(frozenset({left_one, right_one}))
                        continue
            # Judged within each choice of union branch: a condition on a union's output touches
            # one instance per branch, and the branches are alternatives, never matched together.
            beyond = touching - reach
            for part in project(own, touching):
                other_matching(set(part) | beyond)
        # A column of the statement around it: a correlated subquery is combined with the
        # instance it reaches out to.
        for column in _own_columns(scope):
            for instance in resolver.touched(scope, column):
                if instance in reach:
                    continue  # one of its own, whichever branch of a union it reads it from
                if own.reach:
                    groups.append(resolver.all_of([own, resolver.one(frozenset({instance}))]))

    # A view is combined with the instances its own SQL reads (that SQL is checked as written).
    edges |= resolver.view_edges
    # A table joined to itself on its whole registered primary key is the same row read again;
    # on part of the key it is a pairing of different rows, and no relationship registers it.
    for instances, columns in same_row.items():
        table_id = table_of[next(iter(instances))]
        if columns == primary_keys[table_id]:
            edges.add(instances)
        else:
            for column in sorted(columns):
                refuse(Unrelated(table_id, table_id, UNREGISTERED, column, column))

    # Tables a statement combines must be connected by its registered pairings: one combined
    # with nothing relating it to the rest is a product of the two.
    members = {i for i in table_of if not exempt(i)}
    related = set(edges)
    # A relationship registered with a constant source: the target's rows held to that
    # constant are related to every row of the source it is combined with.
    for target, held in held_to.items():
        if target not in members:
            continue
        for form, constant in held:
            for source in members:
                if source != target and (
                    (table_of[source], table_of[target], constant, form) in constants
                ):
                    related.add(frozenset({source, target}))
    pieces = _Pieces(members, related, never_true, resolver.shared)
    for group in groups:
        # Each set the group may be, as its connected pieces, the first holding its least
        # instance; a second piece is apart from it, named by its own least instance.
        for parts in pieces.whole(group):
            if len(parts) < 2:
                continue
            (first, _), (apart, said) = parts[0], parts[1]
            tables = sorted((table_of[first], table_of[apart]))
            # Already refused for how it relates them: said once, as that.
            if not any({f.left_table, f.right_table} == set(tables) for f in found):
                refuse(Unrelated(tables[0], tables[1], NEVER_TRUE if said else NO_CONDITION))
    return found


def self_columns(resolver: _Resolver, source: Any) -> set[str]:
    """The column names ``source`` (a table or a derived scope) offers."""
    if isinstance(source, exp.Table):
        table_id = resolver._table_id(source)
        return set(resolver._columns_of(table_id)) if table_id is not None else set()
    if isinstance(source, Scope):
        return {
            s.alias_or_name for b in (source.set_operation_scopes or [source]) for s in _selects(b)
        } - {"*"}
    return set()


def others_columns(resolver: _Resolver, scope: Scope, source: Any) -> set[str]:
    """The column names every other source of ``scope`` offers."""
    out: set[str] = set()
    for _node, other in scope.selected_sources.values():
        if other is not source:
            out |= self_columns(resolver, other)
    return out
