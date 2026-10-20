// Copyright (c) 2026 Kenneth Stott
// Canary: 0d4b7e29-c3a1-4f86-9e52-7b1a8c6f3d90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A relationship save always ends: in success, or with the form open saying why. The reverse
// form names each table by the name the save resolves -- its alias when it has one -- so the
// reverse of a relationship from the weblogs demo's shop_access (alias access_log) targets
// access_log, not shop_access, which resolves to no table.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, within, waitFor } from "@testing-library/react";
import { render } from "../../test-utils/render";
import { RelationshipsPage } from "../RelationshipsPage";

const FORWARD = {
  id: "access-to-customer",
  sourceTableId: 1,
  targetTableId: 2,
  sourceTableName: "shop_access",
  sourceDomainId: "shop",
  targetTableName: "customers",
  sourceColumn: "customer_id",
  targetColumn: "id",
  cardinality: "many-to-one",
  materialize: false,
  refreshInterval: 300,
  targetFunctionName: null,
  functionArg: null,
  alias: "BY_CUSTOMER",
  graphqlAlias: null,
  computedCypherAlias: null,
  autoSuggested: false,
  disableCypher: false,
  parameters: [],
  viaTableId: null,
  viaTableName: null,
};

const RELS = { relationships: [FORWARD], loading: false, refetch: vi.fn() };
const TABLES = {
  tables: [
    {
      id: 1,
      domainId: "shop",
      sourceId: "shop_access",
      schemaName: "default",
      tableName: "shop_access",
      alias: "access_log",
      columns: [],
    },
    {
      id: 2,
      domainId: "shop",
      sourceId: "shop-db",
      schemaName: "shop",
      tableName: "customers",
      alias: null,
      columns: [],
    },
  ],
  loading: false,
};
const DOMAINS = { domains: [] };
const UPSERT = {
  upsertRelationship: vi.fn<(input: Record<string, unknown>, isNew: boolean) => Promise<unknown>>(),
};
const DELETE = { deleteRelationship: vi.fn() };
const AUTH = { capabilities: ["create_relationship"] };
const FILTER = { domainsEnabled: false, domains: [], checkedDomains: new Set<string>() };

vi.mock("../../hooks/useAdminQueries", () => ({
  useRelationships: () => RELS,
  useAllRelationships: () => RELS,
  useTables: () => TABLES,
  useDomains: () => DOMAINS,
  useUpsertRelationship: () => UPSERT,
  useDeleteRelationship: () => DELETE,
}));
vi.mock("../../context/AuthContext", () => ({ useAuth: () => AUTH }));
vi.mock("../../context/DomainFilterContext", () => ({ useDomainFilter: () => FILTER }));
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchCandidates: vi.fn().mockResolvedValue([]),
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
}));
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

const panel = () => screen.getByTestId("relationships-split-panel");

function openReverse() {
  render(<RelationshipsPage />);
  fireEvent.click(
    within(screen.getByTestId("relationships-list")).getByText("shop_access.customer_id"),
  );
  fireEvent.click(within(panel()).getByRole("button", { name: "Generate reverse relationship" }));
}

beforeEach(() => {
  UPSERT.upsertRelationship.mockReset();
  DELETE.deleteRelationship.mockReset();
});

describe("RelationshipsPage: the reverse form", () => {
  it("names each table by the name its save resolves, the alias when there is one", async () => {
    UPSERT.upsertRelationship.mockResolvedValueOnce({ success: true, message: "" });
    openReverse();
    expect(screen.getByTestId("reverse-rel-id")).toHaveValue("customers-to-shop_access");
    fireEvent.click(screen.getByTestId("reverse-rel-save"));
    await waitFor(() => expect(UPSERT.upsertRelationship).toHaveBeenCalledTimes(1));
    expect(UPSERT.upsertRelationship.mock.calls[0][0]).toMatchObject({
      sourceTableId: "customers",
      targetTableId: "access_log",
      sourceColumn: "id",
      targetColumn: "customer_id",
      cardinality: "one-to-many",
    });
    expect(UPSERT.upsertRelationship.mock.calls[0][1]).toBe(true); // the reverse is created
    await waitFor(() => expect(screen.queryByTestId("reverse-relationship-form")).toBeNull());
  });

  it("shows a refusal by name and keeps the form open", async () => {
    UPSERT.upsertRelationship.mockResolvedValueOnce({
      success: false,
      message: "Alias 'BY_CUSTOMER' is already the alias of relationship 'x' from 'customers'",
      code: "schema.relationship_alias_taken",
      params: { alias: "BY_CUSTOMER", table: "customers", relationship: "x" },
    });
    openReverse();
    fireEvent.click(screen.getByTestId("reverse-rel-save"));
    const problem = await screen.findByTestId("form-problem");
    expect(problem).toHaveTextContent("BY_CUSTOMER");
    expect(problem).toHaveTextContent("x");
    expect(screen.getByTestId("reverse-relationship-form")).toBeTruthy();
    expect(screen.getByTestId("reverse-rel-save")).not.toHaveAttribute("data-loading");
  });

  it("ends a save that errors instead of answering, and says why", async () => {
    UPSERT.upsertRelationship.mockRejectedValueOnce(
      new Error("Target table not registered: shop_access"),
    );
    openReverse();
    fireEvent.click(screen.getByTestId("reverse-rel-save"));
    expect(await screen.findByTestId("form-problem")).toHaveTextContent(
      "Target table not registered: shop_access",
    );
    expect(screen.getByTestId("reverse-relationship-form")).toBeTruthy();
    expect(screen.getByTestId("reverse-rel-save")).not.toHaveAttribute("data-loading");
  });
});

describe("RelationshipsPage: editing a relationship", () => {
  it("names its tables by the name the save resolves, and ends a save that errors", async () => {
    UPSERT.upsertRelationship.mockRejectedValueOnce(new Error("network down"));
    render(<RelationshipsPage />);
    fireEvent.click(
      within(screen.getByTestId("relationships-list")).getByText("shop_access.customer_id"),
    );
    fireEvent.click(within(panel()).getByRole("button", { name: "Edit" }));
    fireEvent.click(within(panel()).getByRole("button", { name: "Save" }));
    expect(await screen.findByTestId("form-problem")).toHaveTextContent("network down");
    expect(UPSERT.upsertRelationship.mock.calls[0][0]).toMatchObject({
      sourceTableId: "access_log",
      targetTableId: "customers",
    });
    expect(UPSERT.upsertRelationship.mock.calls[0][1]).toBe(false); // the same id: an update
    expect(within(panel()).getByRole("button", { name: "Save" })).not.toHaveAttribute(
      "data-loading",
    );
    expect(DELETE.deleteRelationship).not.toHaveBeenCalled();
  });
});
