// Copyright (c) 2026 Kenneth Stott
// Canary: 5d2b7e90-3a4c-4f18-b6e1-c9f0a2d8e731
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 CONTRACTS ARE DERIVED FROM THE OFFERS, THE UI SHOWS CONTRACTS PER OFFER (from REQ-2158's
// contracts list): each offer opens to its contracts, one collapsed row per dataset, headed by the
// dataset, its version, SLA headline, last check and a warning where its newest version breaks a
// reader; a row opens to the offer's terms on the dataset and to its ODCS document, read when that
// opens from the offer's contract routes. Output Ports stay in the panel's header.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../../test-utils/render";
import { DataProductDetailPanel } from "../../DataProductDetailPanel";
import type { ProductCore } from "../../productCore";
import type { OdcsContract } from "../../../../api/dataProductContracts";
import type { Offer, OfferDatasetContract } from "../../../../api/dataProductOffers";
import type { ContractVersion, DataProduct } from "../../../../types/admin";
import type { ProductDqEntry, ProductLineage } from "../../../../api/lineage";
import { lifecycleProps } from "../../__tests__/lifecycleProps";

vi.mock("../../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../../components/graph3d/OntologyGraph", () => ({ OntologySnippet: () => null }));
vi.mock("../../../tables/ColumnGlossaryHover", () => ({
  ColumnGlossaryHover: ({ children }: { children: unknown }) => children,
}));
const fetchProductContract =
  vi.fn<(id: string, offer: string, member: string) => Promise<OdcsContract>>();
const fetchContractVersions =
  vi.fn<(id: string, offer: string, member: string) => Promise<ContractVersion[]>>();
vi.mock("../../../../api/dataProductContracts", () => ({
  fetchProductContract: (id: string, offer: string, member: string) =>
    fetchProductContract(id, offer, member),
  fetchContractVersions: (id: string, offer: string, member: string) =>
    fetchContractVersions(id, offer, member),
}));
const offers = vi.hoisted(() => ({ list: [] as Offer[] }));
// Every section is mounted (Lineage and Related only once expanded): the reads the others make
// answer nothing here.
vi.mock("../../../../api/dataProductOffers", () => ({
  fetchOffers: () => Promise.resolve(offers.list),
  fetchBids: () => Promise.resolve([]),
  fetchProductProfiles: () => Promise.resolve([]),
}));

const PRODUCT = {
  id: "pet_health",
  domainId: "pet-store",
  name: "Pet Health",
  customProperties: {},
} as unknown as DataProduct;

const REMOVED = {
  kind: "field_removed",
  subject: "note",
  before: "TEXT",
  after: null,
  breaking: true,
};
const ADDED = {
  kind: "field_added",
  subject: "age",
  before: null,
  after: "INTEGER",
  breaking: false,
};

const saved = (
  version: string,
  changes: ContractVersion["changes"] = [],
  savedBy: string | null = "ken@example.com",
): ContractVersion => ({ version, changes, saved_at: "2026-10-20T10:00:00Z", saved_by: savedBy });

const port = (id: number, name: string, cacheTtl: number | null = null) => ({
  id,
  sourceId: "pg",
  domainId: "pet-store",
  schemaName: "public",
  tableName: name,
  alias: null,
  description: null,
  dqContract: null,
  cacheTtl,
});

const CORE = {
  product: PRODUCT,
  queries: { graphql: "", sql: "", jsonapi: "" },
  tables: [
    port(1, "pets", 300),
    port(2, "vets"),
    port(3, "visits", -1),
    {
      ...port(9, "pets_quality"),
      dqContract: "checks: []",
      checks: { id: 1, domainId: "pet-store", tableName: "pets", alias: null },
    },
  ],
  commands: [{ name: "add_pet", description: "Add a pet", domainId: "pet-store" }],
} as unknown as ProductCore;

const contract = (
  tableId: number,
  table: string,
  version: string,
  versions: ContractVersion[] = [],
): OfferDatasetContract => ({
  table_id: tableId,
  table,
  contract: {
    version,
    created_at: "2026-10-01T00:00:00Z",
    versions,
    terms: {
      fields: [{ name: "id", type: "integer", description: null, masked: null }],
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
});

/** The default offer, its contracts the first ``count`` of three. */
const offer = (count: 0 | 1 | 3, versions: ContractVersion[] = []): Offer => ({
  id: 1,
  product_id: "pet_health",
  name: "default",
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
  published_by: null,
  review: null,
  created_at: "2026-10-01T00:00:00Z",
  updated_at: "2026-10-01T00:00:00Z",
  summary: { extension: null },
  query_now: false,
  contracts: [
    contract(1, "pet-store.pets", "1.2.0", versions),
    contract(2, "pet-store.vets", "0.1.0"),
    contract(3, "pet-store.visits", "2.0.0"),
  ].slice(0, count),
});

const promise = (checker: number, checked: number, at: string | null): ProductDqEntry => ({
  checker_table_id: checker,
  domain_id: "pet-store",
  table_name: `check_${checker}`,
  alias: null,
  description: null,
  owner_product_ids: ["pet_health"],
  checked_table_id: checked,
  checked_table: { id: checked, domain_id: "pet-store", table_name: "pets", alias: null },
  rules: [{ check_type: "expect_column_values_to_not_be_null", column_name: "id", args: {} }],
  latest_outcome: at ? "pass" : null,
  latest_run_at: at,
  failing_checks: [],
});

const LINEAGE: ProductLineage = {
  nodes: [],
  edges: [],
  outputs: [],
  members: [],
  inputs: [],
  consumers: [
    {
      relation: "pet_store.pet_report",
      kind: "view",
      table_id: 40,
      domain_id: "pet-store",
      table_name: "pet_report",
      alias: null,
      product_ids: [],
      reads: ["pet_store.pets"],
    },
  ],
  dq: {
    expectations: [],
    promises: [
      promise(9, 1, "2026-10-20T10:00:00Z"),
      promise(10, 1, "2026-10-28T10:00:00Z"),
      promise(11, 2, null),
    ],
    related: [],
    unattached: [],
  },
};

const odcs = (name: string): OdcsContract => ({
  apiVersion: "v3.1.0",
  kind: "DataContract",
  id: `provisa://pet-store/${name}`,
  name,
  version: "1.2.0",
  status: "active",
  dataProduct: "Pet Health",
  contractCreatedTs: "2026-10-01T00:00:00Z",
  schema: [
    {
      name,
      physicalName: `pg.public.${name}`,
      properties: [{ name: "id", logicalType: "integer", physicalType: "INTEGER" }],
    },
  ],
  slaProperties: [{ property: "latency", value: 300, unit: "s" }],
});

async function panel(
  count: 0 | 1 | 3,
  link = "/data-products?product=pet_health",
  versions: ContractVersion[] = [],
) {
  offers.list = [offer(count, versions)];
  const view = render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={CORE}
      coreError={null}
      reader={null}
      canEdit={false}
      canDelete={false}
      owner
      seesConsumers
      lineageLoading={false}
      lineageError=""
      lineageGraph={LINEAGE}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
    { initialEntries: [link] },
  );
  await screen.findByTestId("offer-card-1");
  return view;
}

/** Opens the offer's contracts. */
const openContracts = () => fireEvent.click(screen.getByTestId("offer-card-1-toggle"));
/** Opens dataset ``id``'s row and its document. */
async function openDocument(id: number) {
  fireEvent.click(screen.getByTestId(`offer-1-contract-${id}-toggle`));
  fireEvent.click(await screen.findByTestId(`offer-1-contract-${id}-document-toggle`));
  return screen.findByTestId(`data-contract-${id}`);
}
const rows = () =>
  document.querySelectorAll("[data-testid^='offer-1-contract-'][data-testid$='-toggle']");

beforeEach(() => {
  localStorage.clear();
  fetchProductContract.mockReset();
  fetchContractVersions.mockReset();
  fetchContractVersions.mockResolvedValue([]);
  fetchProductContract.mockImplementation((_id, _offer, member) =>
    Promise.resolve(odcs(member.split(".")[2])),
  );
});

describe("an offer's contracts (REQ-2180)", () => {
  it("are no card of their own, and Output Ports still are, in the header", async () => {
    await panel(3);
    expect(screen.queryByTestId("data-product-card-contracts-pet_health")).toBeNull();
    expect(screen.queryByTestId("data-product-contracts-pet_health")).toBeNull();
    expect(screen.getByTestId("data-product-detail-graphql-queries-pet_health")).toHaveTextContent(
      "pet-store.visits",
    );
  });

  it("are one collapsed row per dataset, inside the offer, nothing read", async () => {
    await panel(3);
    openContracts();
    await waitFor(() => expect(rows()).toHaveLength(3));
    for (const id of [1, 2, 3]) {
      expect(
        screen.getByTestId(`offer-1-contract-${id}-toggle`).getAttribute("aria-expanded"),
      ).toBe("false");
      expect(screen.queryByTestId(`data-contract-${id}`)).toBeNull();
    }
    expect(fetchProductContract).not.toHaveBeenCalled();
  });

  it("say when the offer gives none", async () => {
    await panel(0);
    openContracts();
    expect(await screen.findByText("No contracts derived yet.")).toBeInTheDocument();
    expect(rows()).toHaveLength(0);
  });

  it("have no row for a command or a checker", async () => {
    await panel(1);
    openContracts();
    await waitFor(() => expect(rows()).toHaveLength(1));
    expect(screen.getByTestId("offer-card-1")).not.toHaveTextContent("add_pet");
    expect(screen.queryByTestId("offer-1-contract-9-toggle")).toBeNull();
  });

  it("head each row with its dataset, version, SLA headline and last check", async () => {
    await panel(3);
    openContracts();
    const pets = await screen.findByTestId("offer-1-contract-1-row");
    expect(pets).toHaveTextContent("pet-store.pets");
    expect(screen.getByTestId("offer-1-contract-1-version")).toHaveTextContent("v1.2.0");
    // Who consumes a dataset is the owner's, not the contract's (REQ-2160).
    for (const id of [1, 2, 3])
      expect(screen.getByTestId(`offer-1-contract-${id}-row`)).not.toHaveTextContent(/consumer/i);
    // Its freshness, entered on the product; none declared is said so.
    expect(screen.getByTestId("offer-1-contract-1-sla")).toHaveTextContent("Fresh within 300s");
    expect(screen.getByTestId("offer-1-contract-2-sla")).toHaveTextContent(
      "No service level declared",
    );
    expect(screen.getByTestId("offer-1-contract-3-sla")).toHaveTextContent(
      "Loaded once, never refreshed",
    );
    // The latest run of the promises on the dataset.
    expect(screen.getByTestId("offer-1-contract-1-checked")).toHaveTextContent(
      new Date("2026-10-28T10:00:00Z").toLocaleString("en"),
    );
    expect(screen.getByTestId("offer-1-contract-2-checked")).toHaveTextContent("Never checked");
  });

  it("read a document when it opens, from the offer's contract, and show its cards", async () => {
    await panel(3);
    openContracts();
    await openDocument(1);
    expect(fetchProductContract).toHaveBeenCalledTimes(1);
    expect(fetchProductContract).toHaveBeenCalledWith("pet_health", "default", "pg.public.pets");
    expect(fetchContractVersions).toHaveBeenCalledWith("pet_health", "default", "pg.public.pets");
    for (const card of ["details", "fields", "sla", "quality", "versions"]) {
      expect(screen.getByTestId(`data-contract-1-${card}`)).toBeInTheDocument();
    }
    // No card says who reads the dataset, though this reader owns the product.
    expect(screen.queryByTestId("data-contract-1-consumers")).toBeNull();
    expect(screen.getByTestId("data-contract-1")).not.toHaveTextContent("pet_report");
    // Its promises are its own dataset's.
    expect(screen.getByTestId("data-contract-promises-1")).toHaveTextContent("2 promises");
    expect(screen.getByTestId("data-contract-1-versions")).toHaveTextContent(
      "contractCreatedTs2026-10-01T00:00:00Z",
    );
    // A second stays open beside it.
    await openDocument(3);
    expect(fetchProductContract).toHaveBeenCalledWith("pet_health", "default", "pg.public.visits");
    expect(screen.getByTestId("data-contract-1")).toBeInTheDocument();
    expect(screen.queryByTestId("data-contract-2")).toBeNull();
  });

  it("warn where the newest version breaks a reader, naming it", async () => {
    const breaking = await panel(3, undefined, [saved("1.0.0"), saved("2.0.0", [REMOVED])]);
    openContracts();
    expect(await screen.findByTestId("offer-1-contract-1-breaking")).toHaveTextContent("v2.0.0");
    breaking.unmount();
    // The offer opens as the reader left it: start again from closed.
    localStorage.clear();
    // A newer version that breaks nothing: no warning.
    await panel(3, undefined, [saved("1.0.0"), saved("2.0.0", [REMOVED]), saved("2.1.0", [ADDED])]);
    openContracts();
    await screen.findByTestId("offer-1-contract-1-row");
    expect(screen.queryByTestId("offer-1-contract-1-breaking")).toBeNull();
  });

  it("list a document's versions newest first, with when, by whom and whether each breaks", async () => {
    fetchContractVersions.mockResolvedValue([
      saved("0.1.0", [], null),
      saved("1.0.0", [ADDED]),
      saved("2.0.0", [REMOVED]),
    ]);
    await panel(3);
    openContracts();
    await openDocument(1);
    const card = screen.getByTestId("data-contract-1-history");
    const shown = [...card.querySelectorAll("[data-testid$='-saved']")].map((e) => e.textContent);
    const when = new Date("2026-10-20T10:00:00Z").toLocaleString("en");
    expect(shown).toEqual([
      `Saved ${when} by ken@example.com`,
      `Saved ${when} by ken@example.com`,
      `Saved ${when} by the system`,
    ]);
    expect(screen.getByTestId("data-contract-version-row-1-2.0.0-breaking")).toHaveTextContent(
      "Breaking",
    );
    expect(screen.queryByTestId("data-contract-version-row-1-1.0.0-breaking")).toBeNull();
  });

  it("show every version's differences in its row", async () => {
    fetchContractVersions.mockResolvedValue([
      saved("0.1.0"),
      saved("1.0.0", [ADDED]),
      saved("2.0.0", [REMOVED]),
    ]);
    await panel(3);
    openContracts();
    await openDocument(1);
    expect(screen.getByTestId("data-contract-version-row-1-2.0.0-changes")).toHaveTextContent(
      "Field note removed, was TEXT",
    );
    expect(screen.getByTestId("data-contract-version-row-1-1.0.0-changes")).toHaveTextContent(
      "Field age added, INTEGER",
    );
    expect(screen.getByTestId("data-contract-version-row-1-0.1.0-no-changes")).toHaveTextContent(
      "First version",
    );
  });
});
