// Copyright (c) 2026 Kenneth Stott
// Canary: 5f9a0c3e-2d7b-4e81-a6c4-9b3e1f7d0a52
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158 (amended 2026-10-29, A PRODUCT'S CONTRACTS ARE A LIST BENEATH IT): an open product's
// contracts are a list beneath its lineage, one collapsed row per output port's contract; a row
// opening reads that contract and shows it in the product's card panels, and a link can name the
// row to open. The Contracts card has left the top panels; Output Ports is unchanged.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { ProductCore } from "../productCore";
import type { OdcsContract } from "../../../api/dataProductContracts";
import type { ContractVersion } from "../../../types/admin";
import type { ProductDqEntry, ProductLineage } from "../../../api/lineage";
import type { DataProduct } from "../../../types/admin";
import { lifecycleProps } from "./lifecycleProps";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/GraphPanel", () => ({ GraphPanel: () => null }));
const fetchProductContract = vi.fn<(id: string, member: string) => Promise<OdcsContract>>();
const fetchContractVersions = vi.fn<(id: string, member: string) => Promise<ContractVersion[]>>();
vi.mock("../../../api/dataProductContracts", () => ({
  fetchProductContract: (id: string, member: string) => fetchProductContract(id, member),
  fetchContractVersions: (id: string, member: string) => fetchContractVersions(id, member),
}));

const PRODUCT = {
  id: "pet_health",
  domainId: "pet-store",
  name: "Pet Health",
  customProperties: {},
} as unknown as DataProduct;

const stored = (
  version: string,
  status: string,
  sla: unknown[] = [],
  versions: ContractVersion[] = [],
) => ({
  version,
  status,
  sla,
  columns: [{ name: "id", type: "INTEGER", required: true }],
  versions,
});

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
  status: string,
  changes: ContractVersion["changes"] = [],
  savedBy: string | null = "ken@example.com",
): ContractVersion => ({
  contract: { version, status },
  changes,
  saved_at: "2026-10-20T10:00:00Z",
  saved_by: savedBy,
});

const port = (id: number, name: string, contract: unknown, cacheTtl: number | null = null) => ({
  id,
  sourceId: "pg",
  domainId: "pet-store",
  schemaName: "public",
  tableName: name,
  alias: null,
  description: null,
  dqContract: null,
  cacheTtl: cacheTtl,
  contract,
});

/** A product of ``count`` contracts (0, 1 or 3), with a command and a checker, neither a port with
 * a contract. */
function core(count: 0 | 1 | 3, versions: ContractVersion[] = []): ProductCore {
  const ports = [
    port(1, "pets", stored("1.2.0", "active", [], versions), 300),
    port(
      2,
      "vets",
      stored("0.1.0", "draft", [{ property: "availability", value: 99.5, unit: "%" }]),
    ),
    port(3, "visits", stored("2.0.0", "active"), -1),
  ].slice(0, count);
  return {
    product: PRODUCT,
    queries: { graphql: "", sql: "", jsonapi: "" },
    tables: [
      ...ports,
      {
        ...port(9, "pets_quality", null),
        dqContract: "checks: []",
        checks: { id: 1, domainId: "pet-store", tableName: "pets", alias: null },
      },
    ],
    commands: [{ name: "add_pet", description: "Add a pet", domainId: "pet-store" }] as never,
  } as unknown as ProductCore;
}

const promise = (checker: number, checked: number, at: string | null): ProductDqEntry => ({
  checker_table_id: checker,
  domain_id: "pet-store",
  table_name: `check_${checker}`,
  alias: null,
  description: null,
  owner_product_id: "pet_health",
  checked_table_id: checked,
  checked_table: { id: checked, domain_id: "pet-store", table_name: "pets", alias: null },
  rules: [{ check_type: "expect_column_values_to_not_be_null", column_name: "id", args: {} }],
  latest_outcome: at ? "pass" : null,
  latest_run_at: at,
});

const LINEAGE: ProductLineage = {
  nodes: [],
  edges: [],
  outputs: [],
  members: [],
  inputs: [],
  // Two read pets, one reads vets.
  consumers: [
    {
      relation: "pet_store.pet_report",
      kind: "view",
      table_id: 40,
      domain_id: "pet-store",
      table_name: "pet_report",
      alias: null,
      product_id: null,
      reads: ["pet_store.pets"],
    },
    {
      relation: "pet_store.care_mv",
      kind: "materialized_view",
      table_id: 41,
      domain_id: "pet-store",
      table_name: "care_mv",
      alias: null,
      product_id: "care",
      reads: ["pet_store.pets", "pet_store.vets"],
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

function panel(
  count: 0 | 1 | 3,
  link = "/data-products?product=pet_health",
  versions: ContractVersion[] = [],
) {
  return render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={core(count, versions)}
      coreError={null}
      reader={null}
      canEdit={false}
      canDelete={false}
      owner
      lineageLoading={false}
      lineageError=""
      lineageGraph={LINEAGE}
      contractsVersion={0}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
    { initialEntries: [link] },
  );
}

beforeEach(() => {
  localStorage.clear();
  fetchProductContract.mockReset();
  fetchContractVersions.mockReset();
  fetchContractVersions.mockResolvedValue([]);
  fetchProductContract.mockImplementation((_id, member) =>
    Promise.resolve(odcs(member.split(".")[2])),
  );
});

const list = () => screen.getByTestId("data-product-contracts-pet_health");
const rows = () =>
  list().querySelectorAll("[data-testid^='data-contract-item-'][data-testid$='-toggle']");

describe("a product's contracts list (REQ-2158)", () => {
  it("is no card in the top panels, and Output Ports still is", () => {
    panel(3);
    expect(screen.queryByTestId("data-product-card-contracts-pet_health")).toBeNull();
    expect(screen.getByTestId("data-product-card-outputPorts-pet_health")).toBeInTheDocument();
    expect(screen.getByTestId("data-product-detail-tables-pet_health")).toHaveTextContent(
      "pet-store.visits",
    );
  });

  it("follows the Lineage panel, one collapsed row per contract, nothing read", () => {
    panel(3);
    const lineage = screen.getByTestId("data-product-detail-lineage-toggle-pet_health");
    // Beneath the lineage in the document's order.
    expect(lineage.compareDocumentPosition(list()) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(rows()).toHaveLength(3);
    for (const id of [1, 2, 3]) {
      expect(
        screen.getByTestId(`data-contract-item-${id}-toggle`).getAttribute("aria-expanded"),
      ).toBe("false");
      expect(screen.queryByTestId(`data-contract-${id}`)).toBeNull();
    }
    expect(fetchProductContract).not.toHaveBeenCalled();
  });

  it("says when the product has no contract", () => {
    panel(0);
    expect(rows()).toHaveLength(0);
    expect(list()).toHaveTextContent("no output port with a contract");
  });

  it("has no row for a command port or a checker", () => {
    panel(1);
    expect(rows()).toHaveLength(1);
    expect(list()).not.toHaveTextContent("add_pet");
    expect(screen.queryByTestId("data-contract-item-9")).toBeNull();
  });

  it("heads each row with its port, version, status, SLA, last check and warning", () => {
    panel(3);
    const pets = screen.getByTestId("data-contract-row-1");
    expect(pets).toHaveTextContent("pet-store.pets");
    expect(screen.getByTestId("data-contract-version-1")).toHaveTextContent("v1.2.0");
    expect(screen.getByTestId("data-contract-status-1")).toHaveTextContent("Active");
    // Who consumes a port is the owner's, not the contract's (REQ-2160): no count, owner or not.
    for (const id of [1, 2, 3]) {
      expect(screen.queryByTestId(`data-contract-consumer-count-${id}`)).toBeNull();
      expect(screen.getByTestId(`data-contract-row-${id}`)).not.toHaveTextContent(/consumer/i);
    }
    // Freshness first; else the first declared service level; never expiring is said so.
    expect(screen.getByTestId("data-contract-sla-headline-1")).toHaveTextContent("300s");
    expect(screen.getByTestId("data-contract-sla-headline-2")).toHaveTextContent(
      "availability 99.5%",
    );
    expect(screen.getByTestId("data-contract-sla-headline-3")).toHaveTextContent(
      "Loaded once, never refreshed",
    );
    // The latest run of the promises on the port.
    expect(screen.getByTestId("data-contract-checked-1")).toHaveTextContent(
      new Date("2026-10-28T10:00:00Z").toLocaleString("en"),
    );
    expect(screen.getByTestId("data-contract-checked-2")).toHaveTextContent("Never checked");
    // A draft is warned of; an active contract is not.
    expect(screen.getByTestId("data-contract-warning-2")).toHaveTextContent("Draft");
    expect(screen.queryByTestId("data-contract-warning-1")).toBeNull();
  });

  it("reads a contract when its row opens and shows its cards; two rows stay open", async () => {
    panel(3);
    fireEvent.click(screen.getByTestId("data-contract-item-1-toggle"));
    await screen.findByTestId("data-contract-1");
    expect(fetchProductContract).toHaveBeenCalledTimes(1);
    expect(fetchProductContract).toHaveBeenCalledWith("pet_health", "pg.public.pets");
    for (const card of ["details", "fields", "sla", "quality", "versions"]) {
      expect(screen.getByTestId(`data-contract-1-${card}`)).toBeInTheDocument();
    }
    // No card says who reads the port, though this reader owns the product and two consumers do.
    expect(screen.queryByTestId("data-contract-1-consumers")).toBeNull();
    expect(screen.queryByTestId("data-contract-consumers-1")).toBeNull();
    expect(screen.getByTestId("data-contract-1")).not.toHaveTextContent("pet_report");
    // Its promises are its own port's.
    expect(screen.getByTestId("data-contract-promises-1")).toHaveTextContent("2 promises");
    expect(screen.getByTestId("data-contract-1-versions")).toHaveTextContent(
      "contractCreatedTs2026-10-01T00:00:00Z",
    );

    fireEvent.click(screen.getByTestId("data-contract-item-3-toggle"));
    await screen.findByTestId("data-contract-3");
    expect(fetchProductContract).toHaveBeenCalledWith("pet_health", "pg.public.visits");
    expect(screen.getByTestId("data-contract-1")).toBeInTheDocument();
    expect(fetchProductContract).toHaveBeenCalledTimes(2);
    expect(screen.queryByTestId("data-contract-2")).toBeNull();
  });

  it("opens the contract a link names", async () => {
    panel(3, "/data-products?product=pet_health&contract=pg.public.vets");
    await screen.findByTestId("data-contract-2");
    expect(fetchProductContract).toHaveBeenCalledWith("pet_health", "pg.public.vets");
    expect(screen.queryByTestId("data-contract-1")).toBeNull();
    expect(screen.queryByTestId("data-contract-3")).toBeNull();
  });

  it("opens the contract a link names by its contract id", async () => {
    panel(3, "/data-products?product=pet_health&contract=pet_health/public.visits");
    await waitFor(() =>
      expect(fetchProductContract).toHaveBeenCalledWith("pet_health", "pg.public.visits"),
    );
    expect(await screen.findByTestId("data-contract-3")).toBeInTheDocument();
  });

  const LINK = "/data-products?product=pet_health";

  it("warns only of a breaking change not yet in force, naming its version", () => {
    const { unmount } = panel(3, LINK, [
      saved("1.0.0", "active"),
      saved("2.0.0", "draft", [REMOVED]),
    ]);
    expect(screen.getByTestId("data-contract-breaking-1")).toHaveTextContent(
      "Breaking change in v2.0.0, not yet in force",
    );
    unmount();

    // In force, it is no longer ahead of anyone.
    const inForce = panel(3, LINK, [saved("1.0.0", "active"), saved("2.0.0", "active", [REMOVED])]);
    expect(screen.getByTestId("data-contract-row-1")).toBeInTheDocument();
    expect(screen.queryByTestId("data-contract-breaking-1")).toBeNull();
    inForce.unmount();

    // Not yet in force, but it breaks nothing.
    panel(3, LINK, [saved("1.0.0", "active"), saved("1.1.0", "proposed", [ADDED])]);
    expect(screen.getByTestId("data-contract-row-1")).toBeInTheDocument();
    expect(screen.queryByTestId("data-contract-breaking-1")).toBeNull();
  });

  it("warns of the breaking version behind a later draft", () => {
    panel(3, LINK, [
      saved("1.0.0", "active"),
      saved("2.0.0", "draft", [REMOVED]),
      saved("2.1.0", "draft", [ADDED]),
    ]);
    expect(screen.getByTestId("data-contract-breaking-1")).toHaveTextContent("v2.0.0");
  });

  it("lists the versions newest first, with when, by whom and whether each breaks", async () => {
    fetchContractVersions.mockResolvedValue([
      saved("0.1.0", "active", [], null),
      saved("1.0.0", "active", [ADDED]),
      saved("2.0.0", "draft", [REMOVED]),
    ]);
    panel(3);
    fireEvent.click(screen.getByTestId("data-contract-item-1-toggle"));
    await screen.findByTestId("data-contract-1");
    expect(fetchContractVersions).toHaveBeenCalledWith("pet_health", "pg.public.pets");
    const card = screen.getByTestId("data-contract-1-versions");
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
    expect(screen.getByTestId("data-contract-version-row-1-2.0.0-status")).toHaveTextContent(
      "Draft",
    );
    expect(screen.queryByTestId("data-contract-version-row-1-1.0.0-breaking")).toBeNull();
  });

  it("shows a version's differences, the newest open and the others on opening", async () => {
    fetchContractVersions.mockResolvedValue([
      saved("0.1.0", "active"),
      saved("1.0.0", "active", [ADDED]),
      saved("2.0.0", "draft", [REMOVED]),
    ]);
    panel(3);
    fireEvent.click(screen.getByTestId("data-contract-item-1-toggle"));
    expect(
      await screen.findByTestId("data-contract-version-row-1-2.0.0-changes"),
    ).toHaveTextContent("Field note removed, was TEXT");
    expect(screen.queryByTestId("data-contract-version-row-1-1.0.0-changes")).toBeNull();

    fireEvent.click(screen.getByTestId("data-contract-version-row-1-1.0.0-toggle"));
    expect(
      await screen.findByTestId("data-contract-version-row-1-1.0.0-changes"),
    ).toHaveTextContent("Field age added, INTEGER");

    fireEvent.click(screen.getByTestId("data-contract-version-row-1-0.1.0-toggle"));
    expect(
      await screen.findByTestId("data-contract-version-row-1-0.1.0-no-changes"),
    ).toHaveTextContent("First version");
  });
});
