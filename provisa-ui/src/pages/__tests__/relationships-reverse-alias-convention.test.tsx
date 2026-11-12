// Copyright (c) 2026 Kenneth Stott
// Canary: 552a4fad-3ef1-44e2-a62e-e291ee74a1e2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-194 (audit B16): the GraphQL alias a reverse relationship is offered is named by the
// deployment's GraphQL convention through naming.ts -- snake_case under hasura_graphql, camelCase
// under apollo_graphql -- never by a hard camelCase of the page's own.

import { describe, it, expect, vi } from "vitest";
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
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
const TABLES = { tables: [], loading: false };
const DOMAINS = { domains: [] };
const UPSERT = { upsertRelationship: vi.fn() };
const DELETE = { deleteRelationship: vi.fn() };
const AUTH = { capabilities: ["create_relationship"] };
const FILTER = { domainsEnabled: false, domains: [], checkedDomains: new Set<string>() };
const SETTINGS = vi.hoisted(() => ({ convention: "apollo_graphql" }));

vi.mock("../../hooks/useAdminQueries", () => ({
  useRelationships: () => RELS,
  useAllRelationships: () => RELS,
  useTables: () => TABLES,
  useDomains: () => DOMAINS,
  useUpsertRelationship: () => UPSERT,
  useDeleteRelationship: () => DELETE,
  useDeleteRelationships: () => vi.fn(),
}));
vi.mock("../../context/AuthContext", () => ({ useAuth: () => AUTH }));
vi.mock("../../context/DomainFilterContext", () => ({ useDomainFilter: () => FILTER }));
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchCandidates: vi.fn().mockResolvedValue([]),
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
  fetchSettings: vi.fn(async () => ({
    naming: { convention: SETTINGS.convention, use_domains: false, default_domain: "" },
  })),
}));
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

async function suggestedGraphqlAlias(convention: string): Promise<string> {
  SETTINGS.convention = convention;
  render(<RelationshipsPage />);
  fireEvent.click(
    within(screen.getByTestId("relationships-list")).getByText("shop_access.customer_id"),
  );
  const reverse = within(screen.getByTestId("relationships-split-panel")).getByRole("button", {
    name: "Generate reverse relationship",
  });
  await waitFor(() => expect(reverse).toBeEnabled());
  fireEvent.click(reverse);
  // The Cypher alias is the same under either convention; only the GraphQL one follows it.
  expect(screen.getByTestId("reverse-rel-alias")).toHaveValue("BY_CUSTOMER_BY");
  return (screen.getByTestId("reverse-rel-graphql-alias") as HTMLInputElement).value;
}

describe("RelationshipsPage: the reverse form's GraphQL alias follows the convention", () => {
  it("is snake_case under hasura_graphql", async () => {
    expect(await suggestedGraphqlAlias("hasura_graphql")).toBe("by_customer_by");
  });

  it("is camelCase under apollo_graphql", async () => {
    expect(await suggestedGraphqlAlias("apollo_graphql")).toBe("byCustomerBy");
  });
});
