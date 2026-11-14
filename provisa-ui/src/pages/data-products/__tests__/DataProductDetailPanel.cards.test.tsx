// Copyright (c) 2026 Kenneth Stott
// Canary: 10521925-2994-481a-8a6f-a7ac7a8a84cd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 (amended 2026-11-11, THE PRODUCT PAGE IS SUMMARY ROWS, replacing the panel's tabs): a
// data product's page is a short header -- owner, domain, at most three facts (consumers,
// quality, freshness), its support contact, its tags -- above one row per section in a fixed
// order: Offers, Quality, Profile, Lineage, Related, Requests (owners only), Access. Each row is
// the application's collapsible, a one-line summary computed from its content, expanded in place
// and open again as the viewer left it; Offers is open at first; no section has a subtitle; a
// graph is drawn only inside an expanded row. QUALITY IS STATUS AND COUNTS, NOT A SCORE: per
// offer, the worst outcome, the counts, the failing checks named and the last run. REQ-2160: one
// page for owner and reader; a reader without edit rights sees it without the owner's actions.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { ProductDqEntry, ProductLineage } from "../../../api/lineage";
import type { Bid, Offer, ProductProfile } from "../../../api/dataProductOffers";
import type { DataProduct } from "../../../types/admin";
import type { ProductCore } from "../productCore";
import { lifecycleProps } from "./lifecycleProps";
import { openSection, sectionToggle } from "./panelSections";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/OntologyGraph", () => ({
  OntologySnippet: () => <div data-testid="ontology-graph" />,
}));
vi.mock("../../../components/provisa-graph/ProvisaGraph", () => ({
  ProvisaGraph: () => <div data-testid="lineage-graph" />,
}));
// The chip itself is TagControlGovernedObjects.test's; here, that the header mounts it.
vi.mock("../../../components/TagControl", () => ({
  TagControl: (props: Record<string, unknown>) => (
    <span
      data-testid={`chip-${String(props.objectType)}`}
      data-product={String(props.productId)}
      data-readonly={String(!!props.readOnly)}
    />
  ),
}));

const NOW = Date.parse("2026-11-12T12:00:00Z");
const TWO_HOURS_AGO = "2026-11-12T10:00:00Z";

const ORDERS = 11;
const CUSTOMERS = 12;

const offer = (id: number, name: string, tables: number[], over: Partial<Offer> = {}): Offer =>
  ({
    id,
    name,
    product_id: "pet_health",
    role_id: "viewer",
    data_level: "full",
    sample_views: [],
    dq_results: true,
    profiles: false,
    open: false,
    duration_days: null,
    purpose: "",
    usage: "",
    limitations: "",
    version: 1,
    published_by: "owner",
    review: null,
    created_at: TWO_HOURS_AGO,
    updated_at: TWO_HOURS_AGO,
    contracts: tables.map((table_id) => ({
      table_id,
      table: `t${table_id}`,
      contract: {
        version: "1.0.0",
        created_at: TWO_HOURS_AGO,
        terms: {
          fields: [],
          row_rules: [],
          data_level: "full",
          sample_views: [],
          checks: [],
          dq_results: true,
          profiles: [],
          duration_days: null,
          purpose: "",
          usage: "",
          limitations: "",
        },
      },
    })),
    summary: { extension: null },
    query_now: false,
    ...over,
  }) as Offer;

const OFFERS: Offer[] = [
  offer(1, "Everything", [ORDERS, CUSTOMERS]),
  offer(2, "Orders only", [ORDERS], { open: true }),
  offer(3, "Customers, no results", [CUSTOMERS], { dq_results: false, review: "role gone" }),
];
const bid = (id: number, status: Bid["status"]): Bid => ({
  id,
  request_id: `r${id}`,
  kind: "bid",
  product_id: "pet_health",
  offer_id: 1,
  offer_version: 1,
  terms: { role_id: "viewer" },
  requester: "ana",
  reason: "reporting",
  consumer_kind: "person",
  approver: "owner",
  agreed_at: "2026-11-11T00:00:00Z",
  status,
  decided_by: null,
  decided_at: null,
  decline_reason: null,
  created_at: TWO_HOURS_AGO,
});
const BIDS = [bid(1, "pending"), bid(2, "accepted")];
const PROFILES = [
  { table_id: ORDERS, table: "orders", run: {}, results: null },
  { table_id: CUSTOMERS, table: "customers", run: null, results: null },
] as unknown as ProductProfile[];

vi.mock("../../../api/dataProductOffers", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/dataProductOffers")>()),
  fetchOffers: vi.fn(async () => OFFERS),
  fetchBids: vi.fn(async () => BIDS),
  fetchProductProfiles: vi.fn(async () => PROFILES),
}));
vi.mock("../../tables/TableAccessPanel", () => ({ TableAccessPanel: () => null }));

const promise = (
  checker: number,
  checked: number,
  rules: string[],
  over: Partial<ProductDqEntry> = {},
): ProductDqEntry => ({
  checker_table_id: checker,
  domain_id: "pet-store",
  table_name: `check_${checker}`,
  alias: null,
  description: null,
  owner_product_ids: ["pet_health"],
  checked_table_id: checked,
  checked_table: { id: checked, domain_id: "pet-store", table_name: `t${checked}`, alias: null },
  rules: rules.map((column_name) => ({ check_type: "missing", column_name, args: {} })),
  latest_outcome: "pass",
  latest_run_at: TWO_HOURS_AGO,
  failing_checks: [],
  ...over,
});

// Orders: three checks, one failing (customer_id unique). Customers: two checks, both passing.
const column = (relation: string) => ({
  id: `${relation}.id`,
  relation,
  column: "id",
  kind: "source" as const,
  materialized: false,
});
const LINEAGE: ProductLineage = {
  nodes: [column("raw.orders"), column("pet_store.orders")],
  edges: [{ source: "raw.orders.id", target: "pet_store.orders.id", transform: "", ops: [] }],
  outputs: ["pet_store.orders.id"],
  members: ["pet_store.orders"],
  consumers: [
    {
      relation: "pet_store.report",
      kind: "view",
      table_id: 90,
      domain_id: "pet-store",
      table_name: "report",
      alias: null,
      product_ids: [],
      reads: ["pet_store.orders"],
    },
  ],
  dq: {
    expectations: [],
    promises: [
      promise(20, ORDERS, ["id", "customer_id", "amount"], {
        latest_outcome: "fail",
        failing_checks: [{ name: "unique", column: "customer_id", outcome: "fail" }],
      }),
      promise(21, CUSTOMERS, ["id", "email"]),
    ],
    related: [],
    unattached: [],
  },
  inputs: [
    {
      relation: "raw.orders",
      kind: "source_table",
      table_id: 100,
      domain_id: "raw",
      table_name: "orders",
      alias: null,
      product_ids: [],
      source_id: null,
      source_type: null,
      source_schema: null,
      source_table: null,
    },
  ],
};

const PRODUCT = {
  id: "pet_health",
  domainId: "pet-store",
  name: "Pet Health",
  ownerRole: "pet_owner",
  supportContact: "pets@example.com",
  customProperties: {},
  createdAt: TWO_HOURS_AGO,
} as unknown as DataProduct;

const table = (id: number, tableName: string) => ({
  id,
  sourceId: "pg",
  domainId: "pet-store",
  schemaName: "public",
  tableName,
  alias: null,
  description: null,
  dqContract: null,
  cacheTtl: 300,
});

const CORE: ProductCore = {
  product: PRODUCT,
  tables: [table(ORDERS, "orders"), table(CUSTOMERS, "customers")],
  terms: [],
  edges: [],
  queries: { graphql: "", sql: "", jsonapi: "" },
};

/** The page as its owner (who may edit it), or as a reader without edit rights. */
function panel(as: "owner" | "reader" = "owner") {
  const owner = as === "owner";
  return render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={CORE}
      coreError={null}
      reader={null}
      canEdit={owner}
      canDelete={owner}
      owner={owner}
      seesConsumers={owner}
      lineageLoading={false}
      lineageError=""
      lineageGraph={LINEAGE}
      actingRole="analyst"
      onEdit={vi.fn()}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
  );
}

const ID = "pet_health";
const row = (section: string) => screen.queryByTestId(`data-product-row-${section}-${ID}`);
const summaryOf = (section: string) =>
  screen.getByTestId(`data-product-row-${section}-${ID}-summary`);
const ROWS = ["offers", "quality", "profile", "lineage", "related", "requests", "access"];

describe("a data product's page is summary rows", () => {
  beforeEach(() => {
    window.localStorage.clear();
    vi.spyOn(Date, "now").mockReturnValue(NOW);
  });

  it("has no tabs", () => {
    panel();
    expect(screen.queryByRole("tablist")).toBeNull();
    expect(screen.queryAllByRole("tab")).toEqual([]);
  });

  it("is its header, then one row per section in a fixed order, each the shared collapsible", () => {
    panel();
    const header = screen.getByTestId(`data-product-header-${ID}`);
    const rows = ROWS.map((s) => row(s) as HTMLElement);
    expect(rows.every((r) => r !== null)).toBe(true);
    expect(rows.map((r) => r.closest(".provisa-collapsible"))).not.toContain(null);
    for (let i = 1; i < rows.length; i++) {
      expect(rows[i - 1].compareDocumentPosition(rows[i]) & Node.DOCUMENT_POSITION_FOLLOWING).toBe(
        Node.DOCUMENT_POSITION_FOLLOWING,
      );
    }
    expect(header.compareDocumentPosition(rows[0]) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    const names = ROWS.map((s) => screen.getByTestId(`data-product-row-${s}-${ID}-name`));
    expect(names.map((n) => n.textContent)).toEqual([
      "Offers",
      "Quality",
      "Profile",
      "Lineage",
      "Related",
      "Requests",
      "Access",
    ]);
  });

  it("summarises each row from its content", async () => {
    panel();
    await waitFor(() => expect(summaryOf("offers")).toHaveTextContent("3 offers · 1 open · 1 in review"));
    expect(summaryOf("quality")).toHaveTextContent(
      "5 checks, 1 failing: customer_id unique · checked 2 hours ago",
    );
    await waitFor(() => expect(summaryOf("profile")).toHaveTextContent("1 of 2 datasets profiled"));
    expect(summaryOf("lineage")).toHaveTextContent("2 datasets · 1 input");
    expect(summaryOf("related")).toHaveTextContent(
      "0 products share a dataset · 0 related tables · 0 terms",
    );
    await waitFor(() => expect(summaryOf("requests")).toHaveTextContent("2 requests · 1 pending"));
    expect(summaryOf("access")).toHaveTextContent("2 datasets to query");
  });

  it("opens Offers at first and the others closed, then as the viewer left them", () => {
    const first = panel();
    const expanded = () =>
      ROWS.map((s) => sectionToggle(s as never, ID).getAttribute("aria-expanded"));
    expect(expanded()).toEqual(["true", "false", "false", "false", "false", "false", "false"]);
    fireEvent.click(sectionToggle("offers", ID));
    fireEvent.click(sectionToggle("quality", ID));
    first.unmount();
    panel();
    expect(expanded()).toEqual(["false", "true", "false", "false", "false", "false", "false"]);
  });

  it("has no subtitle in any row: a row's title is its name and its computed summary", () => {
    panel();
    for (const s of ROWS) {
      const title = sectionToggle(s as never, ID);
      const name = within(title).getByTestId(`data-product-row-${s}-${ID}-name`).textContent ?? "";
      const summary = within(title).getByTestId(`data-product-row-${s}-${ID}-summary`);
      const rest = (title.textContent ?? "").replace(name, "").replace(summary.textContent ?? "", "");
      // Beside them, only Quality's worst outcome.
      expect(rest.trim()).toMatch(s === "quality" ? /^(Failing|Not yet run|Passing)$/ : /^$/);
    }
  });

  it("draws a graph only inside an expanded row", () => {
    panel();
    expect(screen.queryByTestId("lineage-graph")).toBeNull();
    expect(screen.queryByTestId("ontology-graph")).toBeNull();
    openSection("lineage", ID);
    expect(screen.getByTestId("lineage-graph")).toBeInTheDocument();
    openSection("related", ID);
    expect(screen.getByTestId("ontology-graph")).toBeInTheDocument();
    fireEvent.click(sectionToggle("lineage", ID));
    expect(screen.queryByTestId("lineage-graph")).toBeNull();
  });

  it("expands Quality from an offer's See Quality", async () => {
    panel();
    fireEvent.click(await screen.findByTestId("offer-card-1-quality-link"));
    expect(sectionToggle("quality", ID)).toHaveAttribute("aria-expanded", "true");
  });

  it("keeps a row's content inside it: what cannot wrap scrolls within the row", () => {
    panel();
    openSection("quality", ID);
    let body = screen.getByTestId(`data-product-quality-${ID}`).parentElement as HTMLElement;
    expect(body.style.overflowX).toBe("auto");
    expect(body.style.overflowWrap).toBe("anywhere");
    body = row("quality") as HTMLElement;
    expect(body).toContainElement(screen.getByTestId(`data-product-quality-${ID}`));
  });
});

describe("its header", () => {
  beforeEach(() => {
    window.localStorage.clear();
    vi.spyOn(Date, "now").mockReturnValue(NOW);
  });

  it("shows at most consumers, quality and freshness, and the support contact", () => {
    panel();
    const header = screen.getByTestId(`data-product-header-${ID}`);
    expect(within(header).getByTestId(`data-product-header-consumers-${ID}`)).toHaveTextContent(
      "1",
    );
    const quality = within(header).getByTestId(`data-product-header-quality-${ID}`);
    expect(quality).toHaveTextContent("Failing");
    expect(quality).toHaveTextContent("4 of 5 checks passing");
    expect(within(header).getByTestId(`data-product-header-freshness-${ID}`)).toBeInTheDocument();
    expect(within(header).getByTestId(`data-product-header-support-${ID}`)).toHaveTextContent(
      "pets@example.com",
    );
    // Its ways in are a section of their own, not the header's.
    expect(within(header).queryByTestId(`data-product-query-now-${ID}`)).toBeNull();
  });

  it("shows the product's tags with the tag chip, which its editor changes", () => {
    panel();
    const chip = within(screen.getByTestId(`data-product-header-tags-${ID}`)).getByTestId(
      "chip-product",
    );
    expect(chip.dataset.product).toBe(ID);
    expect(chip.dataset.readonly).toBe("false");
  });

  it("shows a reader who is not the owner no consumers, and the tags read-only", () => {
    panel("reader");
    expect(screen.queryByTestId(`data-product-header-consumers-${ID}`)).toBeNull();
    expect(screen.getByTestId("chip-product").dataset.readonly).toBe("true");
  });
});

describe("quality per offer is status and counts, never a score", () => {
  beforeEach(() => {
    window.localStorage.clear();
    vi.spyOn(Date, "now").mockReturnValue(NOW);
  });

  it("gives each offer the worst outcome, the counts, the failing checks named and the last run", async () => {
    panel();
    openSection("quality", ID);
    await screen.findByTestId("offer-quality-1");
    expect(screen.getByTestId("offer-quality-1-worst")).toHaveTextContent("Failing");
    expect(screen.getByTestId("offer-quality-1-words")).toHaveTextContent(
      "5 checks, 1 failing: customer_id unique · checked 2 hours ago",
    );
    expect(screen.getByTestId("offer-quality-2-words")).toHaveTextContent(
      "3 checks, 1 failing: customer_id unique · checked 2 hours ago",
    );
    expect(screen.getByTestId("offer-quality-3-worst")).toHaveTextContent("Passing");
    expect(screen.getByTestId("offer-quality-3-words")).toHaveTextContent(
      "2 checks · checked 2 hours ago",
    );
    expect(screen.getByTestId("offer-quality-3-excluded")).toHaveTextContent(
      "results not included",
    );
  });

  it("names no score anywhere on the page", async () => {
    panel();
    openSection("quality", ID);
    await screen.findByTestId("offer-quality-1");
    expect(document.body.textContent).not.toMatch(/score/i);
    expect(document.body.textContent).not.toMatch(/\d+\s*%/);
  });
});

describe("one page for owner and reader", () => {
  beforeEach(() => window.localStorage.clear());

  it("shows a reader without edit rights the same rows, without the owner's actions", async () => {
    panel("reader");
    expect(ROWS.filter((s) => row(s) !== null)).toEqual(ROWS.filter((s) => s !== "requests"));
    await screen.findByTestId("offer-card-1");
    expect(screen.queryByTestId(`data-product-detail-edit-${ID}`)).toBeNull();
    expect(screen.queryByTestId(`data-product-detail-delete-${ID}`)).toBeNull();
    expect(screen.queryByTestId("add-offer-open-button")).toBeNull();
    expect(screen.queryByTestId("offer-card-1-remove")).toBeNull();
  });

  it("shows its owner the same page with the owner's actions", async () => {
    panel("owner");
    await screen.findByTestId("offer-card-1");
    expect(screen.getByTestId(`data-product-detail-edit-${ID}`)).toBeInTheDocument();
    expect(screen.getByTestId("add-offer-open-button")).toBeInTheDocument();
    expect(screen.getByTestId("offer-card-1-remove")).toBeInTheDocument();
  });
});
