// Copyright (c) 2026 Kenneth Stott
// Canary: 1d07e402-f70d-4129-a32b-20c314293d04
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Creating a relationship: "+" opens an empty form whatever relationship was selected, or whatever
// draft was typed and closed before it, and Duplicate (the panel head, beside Edit) opens the same
// create form filled from the selected relationship under a new id: its endpoints and columns
// (rule: components/list/__tests__/panelDuplicateGuard.test.ts).

import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, within } from "@testing-library/react";
import { render } from "../../test-utils/render";
import { expectHoldsNoneOf } from "../../test-utils/createForm";
import { RelationshipsPage } from "../RelationshipsPage";

const rel = {
  id: "orders-to-customers",
  sourceTableId: 1,
  targetTableId: 2,
  sourceTableName: "orders",
  sourceDomainId: "sales",
  targetTableName: "customers",
  sourceColumn: "customer_id",
  targetColumn: "cust_key",
  cardinality: "many-to-one",
  materialize: false,
  refreshInterval: 300,
  targetFunctionName: null,
  functionArg: null,
  alias: null,
  graphqlAlias: null,
  computedCypherAlias: null,
  autoSuggested: false,
  disableCypher: false,
  sameEntity: false,
  parameters: [],
  viaTableName: null,
};
const col = (columnName: string) => ({ columnName });
const tbl = (id: number, tableName: string, columns: string[]) => ({
  id,
  tableName,
  alias: null,
  domainId: "sales",
  columns: columns.map(col),
});

const RELS = { relationships: [rel], loading: false, refetch: vi.fn() };
const TABLES = {
  tables: [tbl(1, "orders", ["customer_id", "total"]), tbl(2, "customers", ["cust_key", "name"])],
  loading: false,
};
const UPSERT = { upsertRelationship: vi.fn(async () => ({ success: true, message: "" })) };
const AUTH = { capabilities: ["create_relationship"] };
const FILTER = {
  domainsEnabled: false,
  domains: [] as string[],
  checkedDomains: new Set<string>(),
};

vi.mock("../../hooks/useAdminQueries", () => ({
  useRelationships: () => RELS,
  useAllRelationships: () => RELS,
  useTables: () => TABLES,
  useDomains: () => ({ domains: [] }),
  useUpsertRelationship: () => UPSERT,
  useDeleteRelationship: () => ({ deleteRelationship: vi.fn() }),
  useDeleteRelationships: () => vi.fn(),
}));
vi.mock("../../context/AuthContext", () => ({ useAuth: () => AUTH }));
vi.mock("../../context/DomainFilterContext", () => ({ useDomainFilter: () => FILTER }));
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchCandidates: vi.fn().mockResolvedValue([]),
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
  fetchSettings: vi.fn().mockResolvedValue({
    naming: { convention: "apollo_graphql", use_domains: false, default_domain: "" },
  }),
}));
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

const VALUES = ["orders-to-customers", "orders", "customers", "customer_id", "cust_key"];
const rowOf = () =>
  within(screen.getByTestId("relationships-list")).getByText("orders.customer_id").closest("tr")!;
const panel = () => screen.getByTestId("relationships-split-panel");

beforeEach(() => vi.clearAllMocks());

describe("+ opens an empty relationship form", () => {
  it("holds none of the values of the relationship selected before it", () => {
    render(<RelationshipsPage />);
    fireEvent.click(rowOf());
    fireEvent.click(screen.getByTestId("rels-add-toggle"));
    expectHoldsNoneOf(panel(), VALUES);
  });

  it("holds none of a draft that was typed and closed before it", () => {
    render(<RelationshipsPage />);
    fireEvent.click(screen.getByTestId("rels-add-toggle"));
    fireEvent.change(screen.getByTestId("rel-form-id"), { target: { value: "draft-id" } });
    // Closed (the toggle is the form's close), then opened again.
    fireEvent.click(screen.getByTestId("rels-add-toggle"));
    fireEvent.click(screen.getByTestId("rels-add-toggle"));
    expect(screen.getByTestId("rel-form-id")).toHaveValue("");
    expectHoldsNoneOf(panel(), ["draft-id"]);
  });

  it("holds none of a draft the form's own Cancel closed", () => {
    render(<RelationshipsPage />);
    fireEvent.click(screen.getByTestId("rels-add-toggle"));
    fireEvent.change(screen.getByTestId("rel-form-id"), { target: { value: "draft-id" } });
    fireEvent.click(within(panel()).getByRole("button", { name: "Cancel" }));
    fireEvent.click(screen.getByTestId("rels-add-toggle"));
    expect(screen.getByTestId("rel-form-id")).toHaveValue("");
  });
});

describe("Duplicate, in a relationship's panel head", () => {
  it("is beside Edit and Delete, named Duplicate", () => {
    render(<RelationshipsPage />);
    fireEvent.click(rowOf());
    expect(within(panel()).getByRole("button", { name: "Duplicate" })).toBeTruthy();
  });

  it("opens the create form filled from the relationship, under a new id", () => {
    render(<RelationshipsPage />);
    fireEvent.click(rowOf());
    fireEvent.click(within(panel()).getByRole("button", { name: "Duplicate" }));
    expect(screen.getByTestId("relationships-split-title")).toHaveTextContent("New relationship");
    expect(screen.getByTestId("rel-form-id")).toHaveValue("orders-to-customers-copy");
    const form = within(screen.getByTestId("add-relationship-form"));
    expect(form.getAllByDisplayValue("orders").length).toBeGreaterThan(0);
    expect(form.getAllByDisplayValue("customer_id").length).toBeGreaterThan(0);
    expect(form.getAllByDisplayValue("cust_key").length).toBeGreaterThan(0);
  });

  it("creates the copy with the endpoints and columns, as a create", async () => {
    render(<RelationshipsPage />);
    fireEvent.click(rowOf());
    fireEvent.click(within(panel()).getByRole("button", { name: "Duplicate" }));
    fireEvent.click(within(panel()).getByRole("button", { name: "Save" }));
    await vi.waitFor(() => expect(UPSERT.upsertRelationship).toHaveBeenCalledTimes(1));
    const [saved, creating] = UPSERT.upsertRelationship.mock.calls[0] as unknown as [
      Record<string, unknown>,
      boolean,
    ];
    expect(creating).toBe(true);
    expect(saved).toMatchObject({
      id: "orders-to-customers-copy",
      sourceTableId: "orders",
      sourceColumn: "customer_id",
      targetTableId: "customers",
      targetColumn: "cust_key",
      cardinality: "many-to-one",
    });
  });
});
