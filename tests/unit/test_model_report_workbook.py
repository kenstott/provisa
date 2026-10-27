# Copyright (c) 2026 Kenneth Stott
# Canary: 4e81b3c6-5d2a-42f7-9b04-8c37fa1d6e02
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1592: the model as a reviewable workbook.

What is asserted here is what makes the file REVIEWABLE rather than merely produced: a stable
header on every sheet, a frozen header row, an autofilter, an id on every row, the Report sheet
leading and naming what was omitted, and the sheet contents coming from the governed projection
rather than from the raw config.
"""

# Requirements: REQ-1592

from __future__ import annotations

import dataclasses

import io
from datetime import UTC, datetime

import openpyxl
import pytest

from provisa.api.metadata_export import workbook as wb
from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.refs import metric_uri
from provisa.core.models import (
    Column,
    Domain,
    Metric,
    ProvisaConfig,
    Source,
    SourceType,
    Table,
    ViewMetricsSpec,
)

ORG = "acme"


def _table(name: str, **kwargs) -> Table:
    columns = kwargs.pop("columns", None) or [
        Column(name="id", data_type="integer", visible_to=["*"])
    ]
    return Table(
        source_id="wh",
        domain_id=kwargs.pop("domain_id", "sales"),
        schema_name="public",
        table_name=name,
        description=kwargs.pop("description", f"{name} table"),
        product_id=kwargs.pop("product_id", "prod"),
        columns=columns,
        **kwargs,
    )


def _config() -> ProvisaConfig:
    return ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="warehouse")],
        domains=[Domain(id="sales", description="Sales", steward="data-steward")],
        tables=[
            _table(
                "orders",
                modeling_role="fact",
                columns=[
                    Column(name="id", data_type="integer", visible_to=["*"]),
                    Column(name="amount", data_type="numeric", visible_to=["*"]),
                ],
            ),
            _table("staging", product_id=None),
            _table("order_totals", view_sql="SELECT 1", description="a view"),
            _table(
                "order_rollup",
                view_metrics=ViewMetricsSpec(metrics=["revenue"], dimensions=["orders.id"]),
                materialize=True,
                mv_refresh_interval=900,
                description="a materialized view",
            ),
        ],
        metrics=[
            Metric(name="revenue", expression="SUM(orders.amount)", description="gross revenue"),
            Metric(name="held", expression="SUM(staging.id)"),
        ],
        roles=[],
    )


def _rows(withheld: frozenset[str] = frozenset()) -> dict[str, list[list]]:
    config = _config()
    # REQ-1592: the report is a steward's view of the WHOLE registered model, so it projects with
    # the export filter off — exactly as provisa/api/admin/report_router.py does.
    snapshot = build_snapshot(
        config, org_id=ORG, dialect="postgres", contexts={}, data_products_only=False
    )
    return wb.sheet_rows(
        snapshot, config, org_id=ORG, withheld_definitions=withheld, profiles={}, inputs={}
    )


def _header(**kwargs) -> wb.ReportHeader:
    defaults = dict(
        org_id=ORG,
        environment="prod",
        domains="all",
        generated_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
        included=wb.OBJECT_SHEETS,
        omitted=(),
    )
    defaults.update(kwargs)
    return wb.ReportHeader(**defaults)  # type: ignore[arg-type]


def _load(header: wb.ReportHeader, rows: dict[str, list[list]]):
    return openpyxl.load_workbook(io.BytesIO(wb.build_workbook(header, rows)))


def test_report_sheet_leads_and_names_the_scope():
    book = _load(_header(domains="sales", omitted=(wb.ONTOLOGY,), included=(wb.TABLES,)), _rows())
    assert book.sheetnames[0] == wb.REPORT
    stated = {row[0]: row[1] for row in book[wb.REPORT].iter_rows(min_row=2, values_only=True)}
    assert stated["Organization"] == ORG
    assert stated["Environment"] == "prod"
    assert stated["Domains covered"] == "sales"
    assert stated["Generated"] == "2026-08-26T12:00:00+00:00"
    assert stated["Sheets included"] == wb.TABLES
    # A partial workbook says so on its face — otherwise it reads as a complete model.
    assert stated["Sheets omitted"] == wb.ONTOLOGY


def test_every_object_sheet_is_frozen_filtered_and_id_led():
    book = _load(_header(), _rows())
    assert book.sheetnames == [wb.REPORT, *wb.OBJECT_SHEETS]
    for name in wb.OBJECT_SHEETS:
        sheet = book[name]
        assert sheet.freeze_panes == "A2", name
        assert sheet.auto_filter.ref is not None, name
        headers = tuple(cell.value for cell in sheet[1])
        assert headers == wb.HEADERS[name], name
        assert headers[0] == "id", name


def test_ids_are_the_semantic_uris_a_comment_traces_back_to():
    rows = _rows()
    assert [row[0] for row in rows[wb.SOURCES]] == [f"provisa://{ORG}/sources/wh"]
    assert f"provisa://{ORG}/sales/tables/orders" in {row[0] for row in rows[wb.TABLES]}
    assert f"provisa://{ORG}/sales/tables/orders#field:amount" in {
        row[0] for row in rows[wb.COLUMNS]
    }
    assert metric_uri(ORG, "revenue") in {row[0] for row in rows[wb.METRICS]}


def test_an_unmarked_table_is_reported_with_the_flag_rather_than_withheld():
    # The Data Product flag is a COLUMN the reviewer filters on, not the report's export filter:
    # an unmarked table is one of the things a model review exists to find.
    rows = _rows()
    flag = wb.HEADERS[wb.TABLES].index("data_product")
    marked = {row[1]: row[flag] for row in rows[wb.TABLES]}
    assert marked["staging"] is False
    assert marked["orders"] is True
    # And the metric that references the unmarked table rides along with it.
    assert {row[1] for row in rows[wb.METRICS]} == {"revenue", "held"}


def test_views_and_materialized_views_are_separate_sheets():
    rows = _rows()
    definition = wb.HEADERS[wb.VIEWS].index("definition")
    assert [row[1] for row in rows[wb.VIEWS]] == ["order_totals"]
    assert [row[definition] for row in rows[wb.VIEWS]] == ["SELECT 1"]
    materialized = rows[wb.MATERIALIZED_VIEWS]
    assert [row[1] for row in materialized] == ["order_rollup"]
    assert materialized[0][wb.HEADERS[wb.MATERIALIZED_VIEWS].index("refresh interval (s)")] == 900
    assert materialized[0][wb.HEADERS[wb.MATERIALIZED_VIEWS].index("definition")] == (
        "metrics: revenue; dimensions: orders.id"
    )


def test_a_taggable_row_carries_one_comma_delimited_tags_cell():
    # Derived tags share the column with assigned ones on purpose: to a reviewer asking which
    # tables are facts, `fact` is the same kind of answer as `pii`, filtered the same way.
    rows = _rows()
    tags = wb.HEADERS[wb.TABLES].index("tags")
    by_name = {row[1]: row[tags] for row in rows[wb.TABLES]}
    assert "fact" in by_name["orders"].split(", ")
    assert by_name["staging"] == ""
    for sheet in (wb.SOURCES, wb.COLUMNS, wb.RELATIONSHIPS, wb.VIEWS, wb.MATERIALIZED_VIEWS):
        assert "tags" in wb.HEADERS[sheet], sheet
    # Neither is taggable — `applies_to` never names them, so a tags column would always be blank.
    for sheet in (wb.METRICS, wb.ONTOLOGY, wb.DATA_PRODUCTS):
        assert "tags" not in wb.HEADERS[sheet], sheet


def test_an_omitted_sheet_is_absent_rather_than_empty():
    book = _load(_header(included=(wb.TABLES, wb.COLUMNS), omitted=(wb.SOURCES,)), _rows())
    assert book.sheetnames == [wb.REPORT, wb.TABLES, wb.COLUMNS]


def test_an_empty_sheet_still_carries_its_filterable_header():
    rows = _rows()
    rows[wb.ONTOLOGY] = []
    sheet = _load(_header(), rows)[wb.ONTOLOGY]
    assert tuple(cell.value for cell in sheet[1]) == wb.HEADERS[wb.ONTOLOGY]
    assert sheet.auto_filter.ref == "A1:L2"


def test_there_is_no_reader_the_workbook_is_never_an_edit_path():
    # REQ-1592 one-way-only: the module exposes writers, and nothing that parses a workbook back
    # into configuration. A reader appearing here is the requirement being violated, not a feature.
    assert not [
        name
        for name in dir(wb)
        if not name.startswith("__") and ("read" in name or "parse" in name or "load" in name)
    ]


@pytest.mark.parametrize("sheet", wb.OBJECT_SHEETS)
def test_every_sheet_has_a_declared_header(sheet: str):
    assert sheet in wb.HEADERS


async def test_the_workbook_shows_only_what_its_caller_reads(monkeypatch):
    """REQ-2152: the right that opens the workbook does not widen it. Its tables and columns
    are the caller's served ones, and its metrics those granted to the caller's role."""
    import types

    from provisa.api.admin import report_router
    from provisa.api.admin.catalog_scope import CatalogScope
    from provisa.api.metadata_export.model import (
        AssetKind,
        AssetRef,
        ColumnAsset,
        MetadataSnapshot,
        TableAsset,
    )
    from provisa.core.models import Metric, ProvisaConfig

    def _table(name: str, columns: list[str]) -> TableAsset:
        return TableAsset(
            ref=AssetRef(kind=AssetKind.TABLE, parts=("pg", "public", name)),
            name=name,
            source_id="pg",
            domain_id="sales",
            description="",
            columns=[
                ColumnAsset(
                    ref=AssetRef(kind=AssetKind.COLUMN, parts=("pg", "public", name, c)),
                    name=c,
                    data_type="int",
                    description="",
                )
                for c in columns
            ],
        )

    snapshot = MetadataSnapshot(
        org_id="acme", tables=[_table("orders", ["id", "secret"]), _table("payroll", ["salary"])]
    )
    config = ProvisaConfig(
        sources=[],
        domains=[],
        tables=[],
        roles=[],
        metrics=[
            Metric(name="open", expression="COUNT(*)", visible_to=["analyst"]),
            Metric(name="hr_only", expression="COUNT(*)", visible_to=["hr"]),
        ],
    )
    scope = CatalogScope(
        whole=False,
        reach=None,
        served={1: {"id"}},
        governance=False,
        role="analyst",
        compiled=frozenset({1, 2}),
    )
    monkeypatch.setattr("provisa.api.admin.catalog_scope.catalog_scope", lambda info: scope)

    async def _reader(state, served_of):
        ids = {"orders": 1, "payroll": 2}
        return lambda table: served_of(ids[table.name])

    monkeypatch.setattr("provisa.api.metadata_export.publishing.snapshot_reader", _reader)
    monkeypatch.setattr("provisa.api.app.state.contexts", {"analyst": object()}, raising=False)

    class _Model:
        """The org's model store: the registered tables' ids (both compiled, so the scope
        answers each)."""

        def acquire(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        async def execute_core(self, stmt):  # noqa: ARG002
            rows = [types.SimpleNamespace(id=1), types.SimpleNamespace(id=2)]
            return types.SimpleNamespace(fetchall=lambda: rows)

    monkeypatch.setattr("provisa.api.app.state.model_db", _Model(), raising=False)
    narrowed, narrowed_config = await report_router._as_served(
        types.SimpleNamespace(state=types.SimpleNamespace()), snapshot, config
    )
    assert [(t.name, [c.name for c in t.columns]) for t in narrowed.tables] == [("orders", ["id"])]
    assert [m.name for m in narrowed_config.metrics] == ["open"]

    # payroll saved since the last build: judged by its stored grants, as the catalog answers it.
    saved_since = dataclasses.replace(scope, compiled=frozenset({1}))
    monkeypatch.setattr("provisa.api.admin.catalog_scope.catalog_scope", lambda info: saved_since)

    async def _stored(conn, caller, table_id):  # noqa: ARG001
        return {"salary"} if table_id == 2 else caller.served.get(table_id, set())

    monkeypatch.setattr("provisa.api.admin.read_rights.served_columns_of", _stored)
    narrowed, _ = await report_router._as_served(
        types.SimpleNamespace(state=types.SimpleNamespace()), snapshot, config
    )
    assert [(t.name, [c.name for c in t.columns]) for t in narrowed.tables] == [
        ("orders", ["id"]),
        ("payroll", ["salary"]),
    ]


def test_a_withheld_view_is_listed_with_a_blank_definition():
    """REQ-2152 (Q7): a view's definition names the columns of the tables it reads; for a
    reader not served all of them the view is listed, its definition blank."""
    rows = _rows()
    views = rows[wb.VIEWS]
    assert views, "the fixture model has a view"
    uri = views[0][0]
    withheld = next(r for r in _rows(frozenset({uri}))[wb.VIEWS] if r[0] == uri)
    assert withheld[5] == "" and views[0][5] != ""


async def test_a_view_reading_a_table_its_caller_is_not_served_all_of_is_withheld(monkeypatch):
    """REQ-2152 (Q7): the rule the audited text follows -- a definition is shown only to a
    caller served every column of each table it reads."""
    import types

    import provisa.api.app as appmod
    from provisa.api.admin import report_router
    from provisa.api.admin.catalog_scope import CatalogScope
    from provisa.api.metadata_export.refs import table_uri

    open_view = _table("open_totals", view_sql="SELECT SUM(id) FROM orders")
    held_view = _table("pay_totals", view_sql="SELECT SUM(salary) FROM payroll")
    config = ProvisaConfig(sources=[], domains=[], tables=[open_view, held_view], roles=[])
    scope = CatalogScope(
        whole=False, reach=None, served={}, governance=False, role="r", compiled=frozenset()
    )
    monkeypatch.setattr("provisa.api.admin.catalog_scope.catalog_scope", lambda info: scope)
    monkeypatch.setattr(
        appmod.state,
        "tables",
        [{"id": 1, "table_name": "orders"}, {"id": 2, "table_name": "payroll"}],
        raising=False,
    )

    class _Conn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

    monkeypatch.setattr(
        appmod.state, "model_db", types.SimpleNamespace(acquire=lambda: _Conn()), raising=False
    )

    async def _all_of_orders(conn, scope, table_id):
        return table_id == 1

    monkeypatch.setattr("provisa.api.admin.read_rights.serves_every_column", _all_of_orders)
    withheld = await report_router._withheld_definitions(
        types.SimpleNamespace(state=types.SimpleNamespace()), config, ORG
    )
    assert withheld == frozenset({table_uri(ORG, held_view)})


def _asset_table(name: str, columns: list[str], domain: str = "sales"):
    from provisa.api.metadata_export.model import AssetKind, AssetRef, ColumnAsset, TableAsset

    return TableAsset(
        ref=AssetRef(kind=AssetKind.TABLE, parts=("pg", "public", name)),
        name=name,
        source_id="pg",
        domain_id=domain,
        description="",
        columns=[
            ColumnAsset(
                ref=AssetRef(kind=AssetKind.COLUMN, parts=("pg", "public", name, c)),
                name=c,
                data_type="int",
                description="",
            )
            for c in columns
        ],
    )


def _term(term_id: int, name: str, **kwargs):
    from provisa.api.metadata_export.model import GlossaryTermAsset

    return GlossaryTermAsset(
        term_id=term_id,
        name=name,
        definition=kwargs.pop("definition", f"a {name}"),
        is_abstract=kwargs.pop("is_abstract", False),
        deprecated=False,
        status=kwargs.pop("status", "live"),
        semantic_uri=f"provisa://{ORG}/terms/{name}",
        **kwargs,
    )


def test_the_ontology_sheet_carries_each_terms_kind_bindings_edges_domains_and_experts():
    """REQ-1592 (2026-10-27): the Glossary sheet is the Ontology sheet (REQ-2163) -- a Class with
    the tables it is a union of, a Property with its columns, the term edges, domains, experts."""
    from provisa.api.metadata_export.model import GlossaryTermEdge, MetadataSnapshot

    orders = _asset_table("orders", ["id", "cust_id"])
    accounts = _asset_table("accounts", ["cust_id"])
    snapshot = MetadataSnapshot(
        org_id=ORG,
        tables=[orders, accounts],
        glossary_terms=[
            _term(
                1, "Customer", role="class", tables=(orders.ref, accounts.ref), domains=("sales",)
            ),
            _term(
                2,
                "customer id",
                role="property",
                refs=(orders.columns[1].ref, accounts.columns[0].ref),
                experts=("ann",),
            ),
            _term(3, "Party", role="concept", is_abstract=True),
        ],
        glossary_edges=[
            GlossaryTermEdge(from_term_id=1, to_term_id=3, rel_type="KIND_OF"),
            GlossaryTermEdge(from_term_id=2, to_term_id=1, rel_type="PART_OF"),
        ],
    )
    config = ProvisaConfig(sources=[], domains=[], tables=[], roles=[])
    rows = wb.sheet_rows(
        snapshot, config, org_id=ORG, withheld_definitions=frozenset(), profiles={}, inputs={}
    )
    assert wb.ONTOLOGY == "Ontology" and not hasattr(wb, "GLOSSARY")
    header = wb.HEADERS[wb.ONTOLOGY]
    by_term = {row[1]: dict(zip(header, row, strict=True)) for row in rows[wb.ONTOLOGY]}
    assert by_term["Customer"]["kind"] == "class"
    assert by_term["Customer"]["tables"] == "orders, accounts"
    assert by_term["Customer"]["edges"] == "KIND_OF Party"
    assert by_term["Customer"]["domains"] == "sales"
    assert by_term["customer id"]["kind"] == "property"
    assert by_term["customer id"]["columns"] == "orders.cust_id, accounts.cust_id"
    assert by_term["customer id"]["edges"] == "PART_OF Customer"
    assert by_term["customer id"]["experts"] == "ann"
    assert by_term["Party"]["kind"] == "concept" and by_term["Party"]["abstract"] is True
    assert by_term["Party"]["id"] == f"provisa://{ORG}/terms/Party"


def test_an_edge_to_a_term_not_shown_is_absent():
    """REQ-2163: an edge is shown only when both its terms are."""
    from provisa.api.metadata_export.model import GlossaryTermEdge, MetadataSnapshot

    snapshot = MetadataSnapshot(
        org_id=ORG,
        glossary_terms=[_term(1, "Customer")],
        glossary_edges=[GlossaryTermEdge(from_term_id=1, to_term_id=9, rel_type="KIND_OF")],
    )
    config = ProvisaConfig(sources=[], domains=[], tables=[], roles=[])
    rows = wb.sheet_rows(
        snapshot, config, org_id=ORG, withheld_definitions=frozenset(), profiles={}, inputs={}
    )
    edges = wb.HEADERS[wb.ONTOLOGY].index("edges")
    assert rows[wb.ONTOLOGY][0][edges] == ""


def test_the_data_products_sheet_lists_lifecycle_output_ports_and_inputs():
    """REQ-1592 (2026-10-27): each product with its domain, owner, status and lifecycle
    (REQ-1634), its output ports (its members) and its nearest physical inputs (REQ-2126)."""
    from datetime import date

    from provisa.api.metadata_export.model import (
        AssetKind,
        AssetRef,
        DataProductAsset,
        MetadataSnapshot,
        OwnerRef,
    )
    from provisa.core.models import DataProduct

    raw = _asset_table("raw_orders", ["id"])
    orders = _asset_table("orders", ["id"])
    totals = _asset_table("order_totals", ["id"])
    snapshot = MetadataSnapshot(
        org_id=ORG,
        tables=[raw, orders, totals],
        data_products=[
            DataProductAsset(
                ref=AssetRef(kind=AssetKind.DATA_PRODUCT, parts=("orders_prod",)),
                id="orders_prod",
                name="Orders",
                domain_id="sales",
                owner=OwnerRef(id="sales_owner", kind="data_product_owner"),
                description="every order",
                members=(orders.ref, totals.ref),
                semantic_uri=f"provisa://{ORG}/sales/products/orders_prod",
                version="2.0",
                status="deprecated",
            )
        ],
    )
    config = ProvisaConfig(
        sources=[],
        domains=[],
        tables=[],
        roles=[],
        data_products=[
            DataProduct(
                id="orders_prod",
                domain_id="sales",
                name="Orders",
                status="deprecated",
                status_reason="moved to v3",
                retire_on=date(2026, 12, 31),
            )
        ],
    )
    rows = wb.sheet_rows(
        snapshot,
        config,
        org_id=ORG,
        withheld_definitions=frozenset(),
        profiles={},
        inputs={"orders_prod": ["sales.raw_orders", "crm:public.accounts"]},
    )
    header = wb.HEADERS[wb.DATA_PRODUCTS]
    [row] = [dict(zip(header, r, strict=True)) for r in rows[wb.DATA_PRODUCTS]]
    assert row["id"] == f"provisa://{ORG}/sales/products/orders_prod"
    assert row["product"] == "Orders"
    assert row["domain"] == "sales"
    assert row["owner"] == "sales_owner"
    assert row["status"] == "deprecated"
    assert row["status reason"] == "moved to v3"
    assert row["retire on"] == "2026-12-31"
    assert row["successor"] == ""
    assert row["version"] == "2.0"
    assert row["output ports"] == "orders, order_totals"
    assert row["inputs"] == "sales.raw_orders, crm:public.accounts"
    assert row["description"] == "every order"


async def test_the_workbook_names_no_column_term_or_edge_its_caller_is_not_served(monkeypatch):
    """REQ-1592 (2026-10-27): an ungranted column is absent from every sheet that names it -- a
    relationship on it, a term's binding to it -- a term outside the caller's domains is absent,
    and so is an edge to it; a Class lists only the tables the caller is served."""
    import types

    from provisa.api.admin import report_router
    from provisa.api.admin.catalog_scope import CatalogScope
    from provisa.api.metadata_export.model import (
        GlossaryTermEdge,
        MetadataSnapshot,
        RelationshipEdge,
    )

    orders = _asset_table("orders", ["id", "cust_id", "secret"])
    payroll = _asset_table("payroll", ["id"], domain="hr")
    snapshot = MetadataSnapshot(
        org_id=ORG,
        tables=[orders, payroll],
        relationships=[
            RelationshipEdge(
                id="r1",
                source=orders.ref,
                target=orders.ref,
                source_column="id",
                target_column="id",
                cardinality="one-to-one",
                alias="self",
                owner=None,
                version=1,
                needs_review=False,
            ),
            RelationshipEdge(
                id="r2",
                source=orders.ref,
                target=orders.ref,
                source_column="secret",
                target_column="id",
                cardinality="many-to-one",
                alias="by_secret",
                owner=None,
                version=1,
                needs_review=False,
            ),
        ],
        glossary_terms=[
            _term(1, "Order", role="class", tables=(orders.ref, payroll.ref), domains=("sales",)),
            _term(
                2,
                "secret code",
                role="property",
                refs=(orders.columns[1].ref, orders.columns[2].ref),
                domains=("sales",),
            ),
            _term(3, "Salary", domains=("hr",)),
            _term(4, "Thing"),
        ],
        glossary_edges=[
            GlossaryTermEdge(from_term_id=1, to_term_id=4, rel_type="KIND_OF"),
            GlossaryTermEdge(from_term_id=3, to_term_id=4, rel_type="KIND_OF"),
        ],
    )
    config = ProvisaConfig(sources=[], domains=[], tables=[], roles=[], metrics=[])
    scope = CatalogScope(
        whole=False,
        reach=frozenset({"sales"}),
        served={1: {"id", "cust_id"}},
        governance=False,
        role="analyst",
        compiled=frozenset({1, 2}),
    )
    monkeypatch.setattr("provisa.api.admin.catalog_scope.catalog_scope", lambda info: scope)

    async def _reader(state, served_of):
        ids = {"orders": 1, "payroll": 2}
        return lambda table: served_of(ids[table.name])

    monkeypatch.setattr("provisa.api.metadata_export.publishing.snapshot_reader", _reader)

    class _Model:
        def acquire(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        async def execute_core(self, stmt):  # noqa: ARG002
            rows = [types.SimpleNamespace(id=1), types.SimpleNamespace(id=2)]
            return types.SimpleNamespace(fetchall=lambda: rows)

    monkeypatch.setattr("provisa.api.app.state.model_db", _Model(), raising=False)
    narrowed, narrowed_config = await report_router._as_served(
        types.SimpleNamespace(state=types.SimpleNamespace()), snapshot, config
    )
    rows = wb.sheet_rows(
        narrowed,
        narrowed_config,
        org_id=ORG,
        withheld_definitions=frozenset(),
        profiles={},
        inputs={},
    )
    assert [row[1] for row in rows[wb.RELATIONSHIPS]] == ["self"]
    header = wb.HEADERS[wb.ONTOLOGY]
    by_term = {row[1]: dict(zip(header, row, strict=True)) for row in rows[wb.ONTOLOGY]}
    # Salary is outside the caller's domains; Thing has none, so every reader is shown it.
    assert set(by_term) == {"Order", "secret code", "Thing"}
    assert by_term["Order"]["tables"] == "orders"
    assert by_term["Order"]["edges"] == "KIND_OF Thing"
    assert by_term["secret code"]["columns"] == "orders.cust_id"
    assert by_term["Thing"]["edges"] == ""


def test_the_data_profiling_sheet_lists_the_latest_runs_figures_per_column():
    """REQ-1592 (2026-10-27): the latest profiling run's figures per column with the run's time,
    as its reader may see them -- a restricted column's figures arrive blank (safe_run) and stay
    blank; a column not on the Columns sheet is not named."""
    from provisa.api.metadata_export.model import MetadataSnapshot

    orders = _asset_table("orders", ["id", "email"])
    orders = dataclasses.replace(
        orders,
        semantic_uri=f"provisa://{ORG}/sales/tables/orders",
        columns=[
            dataclasses.replace(
                c, semantic_uri=f"provisa://{ORG}/sales/tables/orders#field:{c.name}"
            )
            for c in orders.columns
        ],
    )
    run_time = datetime(2026, 10, 26, 9, 30, tzinfo=UTC)
    figures = dict(row_count=100, null_count=0, distinct_count=100, min_value="1", max_value="100")
    blank = dict(
        row_count=None, null_count=None, distinct_count=None, min_value=None, max_value=None
    )
    profiles = {
        orders.ref.parts: [
            dict(run_time=run_time, physical_column="id", column_name="id", **figures),
            dict(run_time=run_time, physical_column="email", column_name="email", **blank),
            dict(run_time=run_time, physical_column="gone", column_name="gone", **figures),
        ]
    }
    snapshot = MetadataSnapshot(org_id=ORG, tables=[orders])
    config = ProvisaConfig(sources=[], domains=[], tables=[], roles=[])
    rows = wb.sheet_rows(
        snapshot, config, org_id=ORG, withheld_definitions=frozenset(), profiles=profiles, inputs={}
    )
    header = wb.HEADERS[wb.DATA_PROFILING]
    assert header == (
        "id",
        "table",
        "column",
        "run time",
        "rows",
        "nulls",
        "distinct",
        "min",
        "max",
    )
    got = [dict(zip(header, r, strict=True)) for r in rows[wb.DATA_PROFILING]]
    assert got == [
        {
            "id": f"provisa://{ORG}/sales/tables/orders#field:id",
            "table": "orders",
            "column": "id",
            "run time": "2026-10-26T09:30:00+00:00",
            "rows": 100,
            "nulls": 0,
            "distinct": 100,
            "min": "1",
            "max": "100",
        },
        {
            "id": f"provisa://{ORG}/sales/tables/orders#field:email",
            "table": "orders",
            "column": "email",
            "run time": "2026-10-26T09:30:00+00:00",
            "rows": None,
            "nulls": None,
            "distinct": None,
            "min": None,
            "max": None,
        },
    ]


async def test_the_profiles_are_the_latest_measured_runs_made_safe_for_the_caller(monkeypatch):
    """REQ-1592 (2026-10-27): each served profiled table's latest measured run, its columns rows
    through governance.safe_run for the caller's role -- the View Profile Runs rule."""
    import types

    from provisa.api.admin import report_router
    from provisa.api.admin.catalog_scope import CatalogScope
    from provisa.api.metadata_export.model import MetadataSnapshot
    from provisa.profiler.governance import ColumnRule

    orders = _asset_table("orders", ["id", "email"])
    scope = CatalogScope(
        whole=False, reach=None, served={}, governance=False, role="analyst", compiled=frozenset()
    )
    monkeypatch.setattr("provisa.api.admin.catalog_scope.catalog_scope", lambda info: scope)
    run_time = datetime(2026, 10, 26, 9, 30, tzinfo=UTC)
    stored = [
        dict(
            run_id="r2",
            run_time=run_time,
            column_name=name,
            physical_column=name,
            row_count=100,
            null_count=3,
            distinct_count=90,
            min_value="a",
            max_value="z",
        )
        for name in ("id", "email")
    ]

    class _Conn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        async def execute_core(self, stmt):  # noqa: ARG002
            return types.SimpleNamespace(
                fetchall=lambda: [
                    types.SimpleNamespace(
                        id=7, source_id="pg", schema_name="public", table_name="orders"
                    )
                ]
            )

    monkeypatch.setattr(
        "provisa.api.app.state.model_db",
        types.SimpleNamespace(acquire=lambda: _Conn()),
        raising=False,
    )
    asked: list = []

    async def _previous(conn, table_name, table_id, before, limit):  # noqa: ARG001
        asked.append((table_name, table_id, limit))
        return [("r2", run_time)]

    async def _columns(conn, table_name, table_id, run_id):  # noqa: ARG001
        assert (table_name, table_id, run_id) == ("orders", 7, "r2")
        return stored

    async def _rules(conn, state, table_id):  # noqa: ARG001
        return [
            ColumnRule("id", "id", frozenset({"analyst"}), (), False),
            ColumnRule("email", "email", frozenset({"analyst"}), (), True),
        ]

    monkeypatch.setattr("provisa.profiler.history.previous_runs", _previous)
    monkeypatch.setattr(report_router, "_run_columns", _columns)
    monkeypatch.setattr("provisa.profiler.governance.column_rules", _rules)
    profiles = await report_router._profiles(
        types.SimpleNamespace(state=types.SimpleNamespace()),
        MetadataSnapshot(org_id=ORG, tables=[orders]),
    )
    assert asked == [("orders", 7, 1)]
    by_column = {row["physical_column"]: row for row in profiles[orders.ref.parts]}
    assert by_column["id"]["min_value"] == "a" and by_column["id"]["row_count"] == 100
    # pii: its shape only, so no count or value.
    assert by_column["email"]["min_value"] is None and by_column["email"]["null_count"] is None


async def test_a_products_inputs_are_its_nearest_physical_inputs_as_its_caller_reads_them(
    monkeypatch,
):
    """REQ-2126: the inputs the product's panel lists -- product_lineage_for, the one answer, for
    the caller's role: a registered input by its relation, one read straight from a source by
    the source's object."""
    import types

    from provisa.api.admin import report_router
    from provisa.api.admin.catalog_scope import CatalogScope
    from provisa.api.metadata_export.model import (
        AssetKind,
        AssetRef,
        DataProductAsset,
        MetadataSnapshot,
    )

    scope = CatalogScope(
        whole=False, reach=None, served={}, governance=False, role="analyst", compiled=frozenset()
    )
    monkeypatch.setattr("provisa.api.admin.catalog_scope.catalog_scope", lambda info: scope)
    asked = []

    async def _lineage(state, product_id, role_id):  # noqa: ARG001
        asked.append((product_id, role_id))
        return {
            "inputs": [
                {"relation": "sales.raw_orders", "kind": "source_table"},
                {"relation": "sales.rollup", "kind": "materialized_view"},
                {
                    "relation": "native:sales.orders",
                    "kind": "source",
                    "source_id": "crm",
                    "source_schema": "public",
                    "source_table": "orders",
                },
            ]
        }

    monkeypatch.setattr("provisa.api.admin.lineage_router.product_lineage_for", _lineage)
    product = DataProductAsset(
        ref=AssetRef(kind=AssetKind.DATA_PRODUCT, parts=("p",)),
        id="p",
        name="P",
        domain_id="sales",
        owner=None,
        description="",
    )
    inputs = await report_router._product_inputs(
        types.SimpleNamespace(state=types.SimpleNamespace()),
        MetadataSnapshot(org_id=ORG, data_products=[product]),
    )
    assert asked == [("p", "analyst")]
    assert inputs == {"p": ["sales.raw_orders", "sales.rollup", "crm:public.orders"]}


def _assertion(table, column: str | None, results, **outcome):
    from provisa.api.metadata_export.model import DataQualityAssertion, DataQualityOutcome

    asset = table.ref if column is None else next(c.ref for c in table.columns if c.name == column)
    return DataQualityAssertion(
        asset=asset,
        checker="soda",
        check_type="missing_count",
        definition=f"missing_count({column}) = 0",
        severity="fail",
        results_table=results.ref,
        outcome=DataQualityOutcome(**outcome),
    )


def test_the_data_quality_sheet_lists_each_checks_latest_result_per_table_and_column():
    """REQ-1592 (2026-10-27): each check's latest result, on the table or column it checks; a
    check on a table the workbook does not show, or whose outcomes land in one, is not listed."""
    from provisa.api.metadata_export.model import MetadataSnapshot

    orders = _asset_table("orders", ["id", "email"])
    orders = dataclasses.replace(
        orders,
        semantic_uri=f"provisa://{ORG}/sales/tables/orders",
        columns=[
            dataclasses.replace(
                c, semantic_uri=f"provisa://{ORG}/sales/tables/orders#field:{c.name}"
            )
            for c in orders.columns
        ],
    )
    checks = _asset_table("orders_checks", ["outcome"])
    hidden = _asset_table("hidden_checks", ["outcome"])
    when = datetime(2026, 10, 27, 6, 0, tzinfo=UTC)
    snapshot = MetadataSnapshot(
        org_id=ORG,
        tables=[orders, checks],
        assertions=[
            _assertion(
                orders,
                "email",
                checks,
                status="fail",
                scan_id="s1",
                scan_time=when,
                metric_value=4.0,
                failed_rows=4,
            ),
            _assertion(orders, None, checks, status="never_run"),
            _assertion(orders, "id", hidden, status="pass", scan_id="s1", scan_time=when),
        ],
    )
    config = ProvisaConfig(sources=[], domains=[], tables=[], roles=[])
    rows = wb.sheet_rows(
        snapshot, config, org_id=ORG, withheld_definitions=frozenset(), profiles={}, inputs={}
    )
    header = wb.HEADERS[wb.DATA_QUALITY]
    assert header == (
        "id",
        "table",
        "column",
        "checker",
        "check",
        "definition",
        "severity",
        "status",
        "scan time",
        "metric value",
        "failed rows",
        "results table",
    )
    got = [dict(zip(header, r, strict=True)) for r in rows[wb.DATA_QUALITY]]
    assert got == [
        {
            "id": f"provisa://{ORG}/sales/tables/orders#field:email",
            "table": "orders",
            "column": "email",
            "checker": "soda",
            "check": "missing_count",
            "definition": "missing_count(email) = 0",
            "severity": "fail",
            "status": "fail",
            "scan time": "2026-10-27T06:00:00+00:00",
            "metric value": 4.0,
            "failed rows": 4,
            "results table": "orders_checks",
        },
        {
            "id": f"provisa://{ORG}/sales/tables/orders",
            "table": "orders",
            "column": "",
            "checker": "soda",
            "check": "missing_count",
            "definition": "missing_count(None) = 0",
            "severity": "fail",
            "status": "never_run",
            "scan time": "",
            "metric value": None,
            "failed rows": None,
            "results table": "orders_checks",
        },
    ]


def test_a_checks_metric_on_a_column_masked_to_the_caller_is_blank():
    """REQ-1592 (2026-10-27): a masked column shows its masked form wherever a statistic would
    reveal it -- a check's metric on a column masked to the caller's role is blank, its verdict
    shown."""
    from provisa.api.admin import report_router
    from provisa.api.metadata_export.model import (
        GovernanceSignal,
        GovernanceTag,
        MetadataSnapshot,
    )

    orders = _asset_table("orders", ["id", "salary"])
    checks = _asset_table("orders_checks", ["outcome"])
    when = datetime(2026, 10, 27, 6, 0, tzinfo=UTC)
    snapshot = MetadataSnapshot(
        org_id=ORG,
        tables=[orders, checks],
        assertions=[
            _assertion(
                orders,
                "salary",
                checks,
                status="fail",
                scan_id="s",
                scan_time=when,
                metric_value=250000.0,
                failed_rows=2,
            ),
            _assertion(
                orders,
                "id",
                checks,
                status="pass",
                scan_id="s",
                scan_time=when,
                metric_value=0.0,
                failed_rows=0,
            ),
        ],
        governance_tags=[
            GovernanceTag(
                asset=orders.columns[1].ref,
                signal=GovernanceSignal.MASKED,
                rule_id="mask:x:0:hash",
                restricted_roles=("analyst",),
                exempt_roles=("hr",),
            )
        ],
    )
    shown = report_router._masked_for(snapshot, "analyst")
    by_column = {a.asset.parts[-1]: a.outcome for a in shown.assertions}
    assert by_column["salary"].metric_value is None and by_column["salary"].status == "fail"
    assert by_column["salary"].failed_rows == 2
    assert by_column["id"].metric_value == 0.0
    assert report_router._masked_for(snapshot, "hr") is snapshot


def test_the_ontology_sheet_states_each_terms_axioms():
    """REQ-1592 (2026-10-27): a term's axioms (REQ-2163 _axioms) in prefixed form; one naming a
    term the sheet does not show is absent, and skos:prefLabel and skos:definition are the term
    and definition columns already."""
    from provisa.api.metadata_export.model import GlossaryAxiom, MetadataSnapshot
    from provisa.sparql_query.vocabulary import OWL, RDF, RDFS, SKOS, XSD

    snapshot = MetadataSnapshot(
        org_id=ORG,
        glossary_terms=[_term(1, "Customer", role="class"), _term(2, "segment", role="property")],
        glossary_axioms=[
            GlossaryAxiom(1, f"{RDF}type", None, f"{OWL}Class", None),
            GlossaryAxiom(1, f"{RDFS}subClassOf", 9, None, None),
            GlossaryAxiom(1, f"{SKOS}prefLabel", None, None, "Customer"),
            GlossaryAxiom(2, f"{RDF}type", None, f"{OWL}DatatypeProperty", None),
            GlossaryAxiom(2, f"{RDFS}domain", 1, None, None),
            GlossaryAxiom(2, f"{RDFS}range", None, f"{XSD}string", None),
            GlossaryAxiom(2, f"{SKOS}definition", None, None, "a segment"),
        ],
    )
    config = ProvisaConfig(sources=[], domains=[], tables=[], roles=[])
    rows = wb.sheet_rows(
        snapshot, config, org_id=ORG, withheld_definitions=frozenset(), profiles={}, inputs={}
    )
    header = wb.HEADERS[wb.ONTOLOGY]
    by_term = {row[1]: dict(zip(header, row, strict=True)) for row in rows[wb.ONTOLOGY]}
    assert by_term["Customer"]["axioms"] == "rdf:type owl:Class"
    assert by_term["segment"]["axioms"] == (
        "rdf:type owl:DatatypeProperty; rdfs:domain Customer; rdfs:range xsd:string"
    )


def test_the_data_contracts_sheet_lists_each_output_ports_contract_with_its_served_columns():
    """REQ-1592 (2026-10-27): each output port's contract (REQ-2158) -- version, status, purpose,
    usage, limitations and the columns it promises, a column the caller is not served absent."""
    from provisa.api.metadata_export.model import (
        AssetKind,
        AssetRef,
        DataProductAsset,
        MetadataSnapshot,
    )
    from provisa.core.models import ContractColumn, ContractTerms, DataContract, DataProduct

    orders = dataclasses.replace(
        _asset_table("orders", ["id", "total"]),
        semantic_uri=f"provisa://{ORG}/sales/tables/orders",
        contract=DataContract(
            version="1.2.0",
            status="active",
            description="every order placed",
            terms=ContractTerms(usage="reporting", limitations="no PII"),
            columns=[
                ContractColumn(name="id", type="integer", required=True),
                ContractColumn(name="total", type="decimal"),
                ContractColumn(name="margin", type="decimal"),  # not served to this caller
            ],
        ),
    )
    drafts = _asset_table("drafts", ["id"])  # a member with no contract declared
    loose = dataclasses.replace(
        _asset_table("loose", ["id"]),
        contract=DataContract(version="0.1.0"),  # in no product the workbook shows
    )
    snapshot = MetadataSnapshot(
        org_id=ORG,
        tables=[orders, drafts, loose],
        data_products=[
            DataProductAsset(
                ref=AssetRef(kind=AssetKind.DATA_PRODUCT, parts=("orders_prod",)),
                id="orders_prod",
                name="Orders",
                domain_id="sales",
                owner=None,
                description="",
                members=(orders.ref, drafts.ref),
            )
        ],
    )
    config = ProvisaConfig(
        sources=[],
        domains=[],
        tables=[],
        roles=[],
        data_products=[DataProduct(id="orders_prod", domain_id="sales", name="Orders")],
    )
    rows = wb.sheet_rows(
        snapshot, config, org_id=ORG, withheld_definitions=frozenset(), profiles={}, inputs={}
    )
    header = wb.HEADERS[wb.DATA_CONTRACTS]
    assert [dict(zip(header, r, strict=True)) for r in rows[wb.DATA_CONTRACTS]] == [
        {
            "id": f"provisa://{ORG}/sales/tables/orders",
            "product": "Orders",
            "output port": "orders",
            "version": "1.2.0",
            "status": "active",
            "purpose": "every order placed",
            "usage": "reporting",
            "limitations": "no PII",
            "columns": "id (integer, required), total (decimal)",
        }
    ]
