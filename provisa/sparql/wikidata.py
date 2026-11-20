# Copyright (c) 2026 Kenneth Stott
# Canary: 1f7c3a92-6d4e-4b05-8a1c-e9b2d5f04c87
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wikidata, a branded source carried by the generic SPARQL source (REQ-1923).

The brand adds to a SPARQL source only what is particular to Wikidata: its query service
endpoint (no credential), starter reference tables people join to, and the table of the
instances of any class -- found by searching classes by name or browsing their broader and
narrower classes, with the properties its instances most use offered as columns. Everything
else is the generic SPARQL source as it is (provisa/sparql/source.py: its User-Agent and
result format included); a table is a SPARQL table on the source.

Every query the brand writes stays within the public endpoint's 60-second limit: each is
anchored on an identifier or a closed class, walks subclass of one hop at most, reads labels
directly rather than through the label service, and a class too large to read whole asks for
a filter. Wikidata tables are replicated on a schedule by default rather than read live.
"""

# Requirements: REQ-1923

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import httpx

from provisa.api_source.models import ApiColumn, ApiColumnType
from provisa.api_source.persist import data_type_for_api_column
from provisa.core.replicate import ALWAYS
from provisa.sparql.source import extract_variables, query_timeout

if TYPE_CHECKING:
    from provisa.api.admin.types import SourceInput

BRAND = "wikidata"

#: The Wikidata query service: the preset endpoint of every Wikidata source.
ENDPOINT = "https://query.wikidata.org/sparql"
#: Where classes and items are searched by name (wbsearchentities).
SEARCH_API = "https://www.wikidata.org/w/api.php"

#: REQ-1923 (amended 2026-11-18): Wikidata tables are replicated on a schedule by default; a
#: steward's own choice is kept. The schedule is the source's Cache TTL: once a day.
REFRESH_SECONDS = 86400

#: The most rows a class table may read; a larger class asks for a filter.
MAX_ROWS = 50_000
#: How many of a class's instances its suggested columns are counted over.
PROPERTY_SAMPLE = 500
#: How many suggested columns are offered.
PROPERTY_SUGGESTIONS = 30
#: How many narrower classes a class lists.
NARROWER_LIMIT = 200
#: How many matches one page of a search asks for.
SEARCH_LIMIT = 50

#: REQ-1923 (amended 2026-11-20, BROWSE CLASSES): the most-used top-level classes the browse
#: view starts from when the class search is empty, each a class whose direct instances number in
#: the hundreds of thousands or more. They are shown ordered by member count, read live.
BROWSE_CLASSES: tuple[str, ...] = (
    "Q5",  # human
    "Q16521",  # taxon
    "Q13442814",  # scholarly article
    "Q43229",  # organization
    "Q4830453",  # business
    "Q6256",  # country
    "Q515",  # city
    "Q486972",  # human settlement
    "Q11424",  # film
    "Q7725634",  # literary work
    "Q482994",  # album
    "Q3305213",  # painting
    "Q41176",  # building
    "Q8502",  # mountain
    "Q4022",  # river
    "Q3918",  # university
    "Q11173",  # chemical compound
)

_ENTITY = "http://www.wikidata.org/entity/"
_ONTOLOGY = "http://wikiba.se/ontology#"
_ITEM_ID = re.compile(r"Q[1-9][0-9]*")
_PROPERTY_ID = re.compile(r"P[1-9][0-9]*")
_LANGUAGE = re.compile(r"[a-z]{2,3}(-[a-z0-9]{2,8})*")


class InvalidWikidataSource(ValueError):
    """What was asked of the Wikidata source cannot be done; ``code`` names why."""

    def __init__(self, code: str, message: str, **params) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


# ── The preset ───────────────────────────────────────────────────────────────


def is_wikidata(federation_hints: dict) -> bool:
    return federation_hints.get("brand") == BRAND


def apply_preset(input: "SourceInput") -> bool:
    """Make ``input`` the Wikidata source when its hints name the brand: the query service
    endpoint, no credential, and scheduled replication unless the steward chose otherwise.
    False, and ``input`` untouched, for any other source."""
    import json

    if input.type != "sparql" or not is_wikidata(json.loads(input.federation_hints_json or "{}")):
        return False
    if input.username or input.password:
        raise InvalidWikidataSource(
            "wikidata.no_credential",
            "The Wikidata query service takes no credential",
        )
    input.host = ENDPOINT
    if input.replicate is None:
        input.replicate = ALWAYS
    if input.cache_ttl is None:
        input.cache_ttl = REFRESH_SECONDS
    return True


# ── Starter reference tables ─────────────────────────────────────────────────


@dataclass(frozen=True)
class StarterTable:
    """A starter reference table: its name and its query. Its columns are the query's SELECT
    variables, every binding text (REQ-1923, amended 2026-11-20): registering it never waits on
    a live query, and column types are refined afterwards on the table."""

    name: str
    query: str

    @property
    def columns(self) -> tuple[ApiColumn, ...]:
        return tuple(ApiColumn(name=v, type=_S) for v in extract_variables(self.query))


_S = ApiColumnType.string


def _qid(var: str) -> str:
    """The expression giving an entity variable's id (Q42) rather than its address."""
    return f'STRAFTER(STR(?{var}), "/entity/")'


def _label(var: str, into: str, language: str) -> str:
    return f'OPTIONAL {{ ?{var} rdfs:label ?{into} FILTER(LANG(?{into}) = "{language}") }}'


def _grouped(
    outer: list[str],
    inner_select: str,
    anchor: tuple[str, str],
    label: str,
    rest: str,
    group_by: str,
) -> str:
    """A query whose rows are grouped in a subquery and selected by plain name outside it, so
    its columns are exactly ``outer``. Its rows come from ``anchor`` -- the variables it binds
    and its pattern (an identifier or a closed class) -- written as a sub-select of its own and
    joined first: the optimizer hint keeps that order, since the query service's own reordering
    doubles these queries' time and has driven them into gateway errors. A preview limits that
    sub-select (provisa.sparql.source._probe_limit), which bounds the rows the group reads.

    ``label`` -- the anchor entity's own label -- is read right after the anchor, once per anchor
    row, before the optional patterns of ``rest`` multiply the rows by their several values (a
    population per census, a country per period): read after them it is read once per
    combination, which drove the admin_regions starter and a class table past the endpoint's
    limit (504 and 503), where read first the same query answers in seconds."""
    names = " ".join(f"?{n}" for n in outer)
    anchor_vars, anchor_pattern = anchor
    return (
        f"SELECT {names} WHERE {{\n"
        f"  {{\n"
        f"    SELECT {inner_select}\n"
        f"    WHERE {{\n"
        '      hint:SubQuery hint:optimizer "None" .\n'
        f"      {{ SELECT {anchor_vars} WHERE {{ {anchor_pattern} }} }}\n"
        f"      {label}\n" + (f"{rest}\n" if rest else "") + "    }\n"
        f"    GROUP BY {group_by}\n"
        f"  }}\n"
        f"}}"
    )


_COUNTRIES = StarterTable(
    name="countries",
    query=_grouped(
        ["id", "name", "iso_alpha2", "iso_alpha3", "capital_id", "capital", "population"],
        f"({_qid('c')} AS ?id) (SAMPLE(?label) AS ?name) (SAMPLE(?a2) AS ?iso_alpha2) "
        f"(SAMPLE(?a3) AS ?iso_alpha3) (SAMPLE({_qid('cap')}) AS ?capital_id) "
        f"(SAMPLE(?capLabel) AS ?capital) (MAX(xsd:integer(?pop)) AS ?population)",
        ("?c ?a2", "?c wdt:P297 ?a2 ."),
        _label("c", "label", "en"),
        "      OPTIONAL { ?c wdt:P298 ?a3 }\n"
        f"      OPTIONAL {{ ?c wdt:P36 ?cap . {_label('cap', 'capLabel', 'en')} }}\n"
        "      OPTIONAL { ?c wdt:P1082 ?pop }",
        "?c",
    ),
)

_CURRENCIES = StarterTable(
    name="currencies",
    query=_grouped(
        ["id", "name", "iso_4217"],
        f"({_qid('c')} AS ?id) (SAMPLE(?label) AS ?name) (SAMPLE(?code) AS ?iso_4217)",
        ("?c ?code", "?c wdt:P498 ?code ."),
        _label("c", "label", "en"),
        "",
        "?c",
    ),
)

_LANGUAGES = StarterTable(
    name="languages",
    query=_grouped(
        ["id", "name", "iso_639_1", "iso_639_2", "iso_639_3"],
        f"({_qid('l')} AS ?id) (SAMPLE(?label) AS ?name) (SAMPLE(?c1) AS ?iso_639_1) "
        "(SAMPLE(?c2) AS ?iso_639_2) (SAMPLE(?c3) AS ?iso_639_3)",
        ("?l ?c3", "?l wdt:P220 ?c3 ."),
        _label("l", "label", "en"),
        "      OPTIONAL { ?l wdt:P218 ?c1 }\n      OPTIONAL { ?l wdt:P219 ?c2 }",
        "?l",
    ),
)

# First-level administrative subdivisions: instances of a direct subclass of "first-level
# administrative country subdivision" (Q10864048), one hop, not dissolved.
_ADMIN_REGIONS = StarterTable(
    name="admin_regions",
    query=_grouped(
        ["id", "name", "iso_3166_2", "country_id", "country"],
        f"({_qid('r')} AS ?id) (SAMPLE(?label) AS ?name) (SAMPLE(?code) AS ?iso_3166_2) "
        f"(SAMPLE({_qid('country')}) AS ?country_id) (SAMPLE(?countryLabel) AS ?country)",
        (
            "?r",
            "?kind wdt:P279 wd:Q10864048 . ?r wdt:P31 ?kind . FILTER NOT EXISTS { ?r wdt:P576 [] }",
        ),
        _label("r", "label", "en"),
        "      OPTIONAL { ?r wdt:P300 ?code }\n"
        f"      OPTIONAL {{ ?r wdt:P17 ?country . {_label('country', 'countryLabel', 'en')} }}",
        "?r",
    ),
)

# Organizations listed on a stock exchange, one row per current listing (no end time): the
# exchange, the ticker the listing carries, and the organization's LEI and ISIN.
_ORGANIZATIONS = StarterTable(
    name="organizations",
    query=_grouped(
        ["id", "name", "exchange_id", "exchange", "ticker", "lei", "isin"],
        f"({_qid('org')} AS ?id) (SAMPLE(?label) AS ?name) ({_qid('ex')} AS ?exchange_id) "
        "(SAMPLE(?exLabel) AS ?exchange) (SAMPLE(?tick) AS ?ticker) (SAMPLE(?leiCode) AS ?lei) "
        "(SAMPLE(?isinCode) AS ?isin)",
        (
            "?org ?listing ?ex",
            "?org p:P414 ?listing . ?listing ps:P414 ?ex . "
            "FILTER NOT EXISTS { ?listing pq:P582 [] }",
        ),
        _label("org", "label", "en"),
        "      OPTIONAL { ?listing pq:P249 ?tick }\n"
        "      OPTIONAL { ?org wdt:P1278 ?leiCode }\n"
        "      OPTIONAL { ?org wdt:P946 ?isinCode }\n"
        f"      {_label('ex', 'exLabel', 'en')}",
        "?org ?ex",
    ),
)

STARTER_TABLES: tuple[StarterTable, ...] = (
    _COUNTRIES,
    _CURRENCIES,
    _LANGUAGES,
    _ADMIN_REGIONS,
    _ORGANIZATIONS,
)


def _described_columns(columns) -> list[dict]:
    return [{"name": c.name, "data_type": data_type_for_api_column(c.type)} for c in columns]


def describe_starter(starter: StarterTable) -> dict:
    """A starter table as the table form fills itself from it."""
    return {
        "name": starter.name,
        "query": starter.query,
        "columns": _described_columns(starter.columns),
    }


# ── The service ──────────────────────────────────────────────────────────────


def _client() -> httpx.AsyncClient:
    from provisa.security import sandbox_egress

    # REQ-2006: a held org's calls are checked at each connection, each request.
    return httpx.AsyncClient(event_hooks=sandbox_egress.http_event_hooks())


async def select(query: str) -> list[dict]:
    """The bindings the query service answers ``query`` with."""
    from provisa.sparql.source import sparql_headers

    headers = {**sparql_headers(), "Content-Type": "application/x-www-form-urlencoded"}
    async with _client() as client:
        resp = await client.post(
            ENDPOINT, data={"query": query}, headers=headers, timeout=query_timeout()
        )
        resp.raise_for_status()
    return resp.json()["results"]["bindings"]


def _check(pattern: re.Pattern[str], value: str, what: str) -> str:
    if not pattern.fullmatch(value):
        raise InvalidWikidataSource(
            "wikidata.bad_id", f"{value!r} is not a Wikidata {what}", value=value, kind=what
        )
    return value


def _language(language: str) -> str:
    if not _LANGUAGE.fullmatch(language):
        raise InvalidWikidataSource(
            "wikidata.bad_language", f"{language!r} is not a language code", language=language
        )
    return language


def _entity_id(term: dict) -> str:
    return term["value"].removeprefix(_ENTITY)


def _text(binding: dict, var: str) -> str | None:
    term = binding.get(var)
    return term["value"] if term is not None else None


async def search_entities(text: str, *, language: str, classes: bool) -> list[dict]:
    """Items whose label or alias matches ``text`` (Wikidata's wbsearchentities), one page of
    them: a value to filter a table by or, with ``classes``, a class to build a table of --
    only the items of the page that have instances, checked in one query
    (``_having_instances``), since a name search also finds items that are no class."""
    from provisa.sparql.source import sparql_headers

    params = {
        "action": "wbsearchentities",
        "search": text,
        "language": _language(language),
        "uselang": language,
        "type": "item",
        "format": "json",
        "limit": str(SEARCH_LIMIT),
    }
    headers = {"User-Agent": sparql_headers()["User-Agent"]}
    async with _client() as client:
        resp = await client.get(SEARCH_API, params=params, headers=headers, timeout=query_timeout())
        resp.raise_for_status()
    hits = [
        # An item may have no label or description in the language asked: None, shown by its id.
        {"id": hit["id"], "label": hit.get("label"), "description": hit.get("description")}
        for hit in resp.json()["search"]
    ]
    if not classes:
        return hits
    having = await _having_instances([hit["id"] for hit in hits])
    return [hit for hit in hits if hit["id"] in having]


async def _having_instances(ids: list[str]) -> set[str]:
    """Those of ``ids`` that some item is an instance of: the classes among them. One query for
    them all, each an existence probe the query service answers from its index."""
    if not ids:
        return set()
    values = " ".join(f"wd:{_check(_ITEM_ID, i, 'item id')}" for i in ids)
    query = (
        f"SELECT ?cls WHERE {{ VALUES ?cls {{ {values} }} FILTER EXISTS {{ ?i wdt:P31 ?cls }} }}"
    )
    return {_entity_id(row["cls"]) for row in await select(query)}


async def browse_classes(*, language: str) -> list[dict]:
    """The browse view's classes (``BROWSE_CLASSES``) with their labels and member counts --
    each count a COUNT(*) of one triple pattern, answered from the index -- most members
    first. A class with no members is not offered."""
    _language(language)
    branches = "\n  UNION\n".join(
        f"  {{ {{ SELECT (COUNT(*) AS ?count) WHERE {{ ?i wdt:P31 wd:{cls} }} }} "
        f"BIND(wd:{cls} AS ?cls) }}"
        for cls in BROWSE_CLASSES
    )
    query = (
        f"SELECT ?cls ?label ?count WHERE {{\n{branches}\n  {_label('cls', 'label', language)}\n}}"
    )
    found = [
        {
            "id": _entity_id(row["cls"]),
            "label": _text(row, "label"),
            "instance_count": int(row["count"]["value"]),
        }
        for row in await select(query)
    ]
    return sorted(
        (c for c in found if c["instance_count"] > 0),
        key=lambda c: c["instance_count"],
        reverse=True,
    )


@dataclass(frozen=True)
class ClassProperty:
    """A property a class's instances use: offered as a column. An ``item`` property's values
    are other items, read as their id and their label."""

    id: str
    label: str | None
    type: ApiColumnType
    item: bool
    uses: int


@dataclass(frozen=True)
class RowFilter:
    """Only the instances whose ``property`` has the item ``value``."""

    property: str
    value: str


@dataclass
class ClassInfo:
    id: str
    instance_count: int
    broader: list[dict] = field(default_factory=list)
    narrower: list[dict] = field(default_factory=list)
    properties: list[ClassProperty] = field(default_factory=list)

    @property
    def filter_required(self) -> bool:
        return self.instance_count > MAX_ROWS


# A property's datatype → the column type its values land as; every other datatype is text.
_PROPERTY_TYPES: dict[str, ApiColumnType] = {
    _ONTOLOGY + "Quantity": ApiColumnType.number,
    _ONTOLOGY + "Time": ApiColumnType.timestamp,
}
_ITEM_TYPE = _ONTOLOGY + "WikibaseItem"


def _filter_triple(row_filter: RowFilter | None) -> str:
    if row_filter is None:
        return ""
    prop = _check(_PROPERTY_ID, row_filter.property, "property id")
    value = _check(_ITEM_ID, row_filter.value, "item id")
    return f" ?item wdt:{prop} wd:{value} ."


async def count_instances(class_id: str, row_filter: RowFilter | None = None) -> int:
    """How many instances the class has. Unfiltered, the exact count: a COUNT(*) of one triple
    pattern, which the query service answers from its index at any size (a COUNT(DISTINCT)
    reads every instance and times out on a class such as human). Under ``row_filter``, a
    bounded probe: the count up to ``MAX_ROWS + 1``, enough to tell whether the table may be
    read, since an exact count of a filtered large class exceeds the endpoint's time limit."""
    if row_filter is None:
        query = f"SELECT (COUNT(*) AS ?count) WHERE {{ ?item wdt:P31 wd:{class_id} . }}"
    else:
        query = (
            "SELECT (COUNT(*) AS ?count) WHERE { { SELECT ?item WHERE { "
            f"?item wdt:P31 wd:{class_id} .{_filter_triple(row_filter)} }} LIMIT {MAX_ROWS + 1} }}"
            " }"
        )
    rows = await select(query)
    return int(rows[0]["count"]["value"])


async def _neighbours(class_id: str, language: str) -> tuple[list[dict], list[dict]]:
    query = (
        "SELECT ?cls ?label ?direction WHERE {\n"
        f'  {{ wd:{class_id} wdt:P279 ?cls . BIND("broader" AS ?direction) }}\n'
        "  UNION\n"
        f"  {{ {{ SELECT ?cls WHERE {{ ?cls wdt:P279 wd:{class_id} }} LIMIT {NARROWER_LIMIT} }}\n"
        '    BIND("narrower" AS ?direction) }\n'
        f"  {_label('cls', 'label', language)}\n"
        "}"
    )
    broader: list[dict] = []
    narrower: list[dict] = []
    for row in await select(query):
        entry = {"id": _entity_id(row["cls"]), "label": _text(row, "label")}
        (broader if row["direction"]["value"] == "broader" else narrower).append(entry)
    return broader, narrower


async def _properties(class_id: str, language: str) -> list[ClassProperty]:
    # The uses are counted per predicate over a bounded sample first, and only the counted
    # predicates are joined to their property and label. The optimizer hint keeps that order:
    # left to itself the query service starts from every property and times out.
    query = (
        "SELECT ?prop ?label ?kind ?uses WHERE {\n"
        "  {\n"
        "    SELECT ?direct (COUNT(DISTINCT ?i) AS ?uses) WHERE {\n"
        '      hint:SubQuery hint:optimizer "None" .\n'
        f"      {{ SELECT ?i WHERE {{ ?i wdt:P31 wd:{class_id} }} LIMIT {PROPERTY_SAMPLE} }}\n"
        "      ?i ?direct ?o .\n"
        "    }\n"
        "    GROUP BY ?direct\n"
        "  }\n"
        "  FILTER(?direct != wdt:P31)\n"
        "  ?prop wikibase:directClaim ?direct ; wikibase:propertyType ?kind .\n"
        f"  {_label('prop', 'label', language)}\n"
        "}\n"
        f"ORDER BY DESC(?uses) LIMIT {PROPERTY_SUGGESTIONS}"
    )
    return [
        ClassProperty(
            id=_entity_id(row["prop"]),
            label=_text(row, "label"),
            type=_PROPERTY_TYPES.get(row["kind"]["value"], ApiColumnType.string),
            item=row["kind"]["value"] == _ITEM_TYPE,
            uses=int(row["uses"]["value"]),
        )
        for row in await select(query)
    ]


async def describe_class(class_id: str, *, language: str) -> ClassInfo:
    """A class as its table is built: its instance count, its broader and narrower classes
    (subclass of, one hop each way) and the properties its instances most use."""
    _check(_ITEM_ID, class_id, "item id")
    _language(language)
    count = await count_instances(class_id)
    broader, narrower = await _neighbours(class_id, language)
    properties = await _properties(class_id, language)
    return ClassInfo(
        id=class_id,
        instance_count=count,
        broader=broader,
        narrower=narrower,
        properties=properties,
    )


def describe_class_info(info: ClassInfo) -> dict:
    return {
        "id": info.id,
        "instance_count": info.instance_count,
        "limit": MAX_ROWS,
        "filter_required": info.filter_required,
        "broader": info.broader,
        "narrower": info.narrower,
        "properties": [
            {
                "id": p.id,
                "label": p.label,
                "data_type": data_type_for_api_column(p.type),
                "item": p.item,
                "uses": p.uses,
            }
            for p in info.properties
        ],
    }


_RESERVED = ("id", "name")


def _slug(label: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (label or "").lower()).strip("_")


def column_names(properties: list[ClassProperty]) -> list[str]:
    """One column name per property: its label as an identifier (its id when the label gives
    none), with its id added when that name is already taken."""
    taken: set[str] = set(_RESERVED)
    names: list[str] = []
    for prop in properties:
        base = _slug(prop.label)
        pid = prop.id.lower()
        name = base if base and not base[0].isdigit() else pid
        wanted = {name, f"{name}_id"} if prop.item else {name}
        if wanted & taken:
            name = f"{name}_{pid}"
            wanted = {name, f"{name}_id"} if prop.item else {name}
        taken |= wanted
        names.append(name)
    return names


@dataclass(frozen=True)
class ClassTable:
    query: str
    columns: tuple[ApiColumn, ...]


def class_table_query(
    class_id: str,
    properties: list[ClassProperty],
    row_filter: RowFilter | None,
    language: str,
) -> ClassTable:
    """The SPARQL table of the class's instances, one row each: its id and label, then one
    column per property (an item property: its value's id and label)."""
    _check(_ITEM_ID, class_id, "item id")
    _language(language)
    outer = ["id", "name"]
    columns: list[ApiColumn] = [ApiColumn(name="id", type=_S), ApiColumn(name="name", type=_S)]
    select = [f"({_qid('item')} AS ?id)", "(SAMPLE(?itemLabel) AS ?name)"]
    anchor = ("?item", f"?item wdt:P31 wd:{class_id} .{_filter_triple(row_filter)}")
    where: list[str] = []
    for prop, name in zip(properties, column_names(properties)):
        pid = _check(_PROPERTY_ID, prop.id, "property id")
        value = f"v_{pid}"
        if prop.item:
            label = f"l_{pid}"
            outer += [f"{name}_id", name]
            columns += [ApiColumn(name=f"{name}_id", type=_S), ApiColumn(name=name, type=_S)]
            select += [f"(SAMPLE({_qid(value)}) AS ?{name}_id)", f"(SAMPLE(?{label}) AS ?{name})"]
            where.append(
                f"      OPTIONAL {{ ?item wdt:{pid} ?{value} . {_label(value, label, language)} }}"
            )
        else:
            outer.append(name)
            columns.append(ApiColumn(name=name, type=prop.type))
            select.append(f"(SAMPLE(?{value}) AS ?{name})")
            where.append(f"      OPTIONAL {{ ?item wdt:{pid} ?{value} }}")
    return ClassTable(
        query=_grouped(
            outer,
            " ".join(select),
            anchor,
            _label("item", "itemLabel", language),
            "\n".join(where),
            "?item",
        ),
        columns=tuple(columns),
    )


async def class_table(
    class_id: str,
    properties: list[ClassProperty],
    *,
    row_filter: RowFilter | None,
    language: str,
) -> ClassTable:
    """The class's table, refused when it would read more than ``MAX_ROWS`` rows: a class
    that large asks for a filter, and a filter that still leaves too many is refused."""
    _check(_ITEM_ID, class_id, "item id")
    count = await count_instances(class_id, row_filter)
    if count > MAX_ROWS:
        if row_filter is None:
            raise InvalidWikidataSource(
                "wikidata.filter_required",
                f"{class_id} has {count} instances, more than {MAX_ROWS}: choose a filter",
                count=count,
                limit=MAX_ROWS,
            )
        raise InvalidWikidataSource(
            "wikidata.filter_too_wide",
            f"The filter leaves more than {MAX_ROWS} instances of {class_id}",
            limit=MAX_ROWS,
        )
    return class_table_query(class_id, properties, row_filter, language)


def describe_class_table(table: ClassTable) -> dict:
    return {"query": table.query, "columns": _described_columns(table.columns)}
