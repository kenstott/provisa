// Copyright (c) 2026 Kenneth Stott
// Canary: 552a4fad-3ef1-44e2-a62e-e291ee74a1e2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2361, REQ-194 (audit B16): the aliases a reverse relationship is offered are the server's
// naming authority's suggestion (naming.suggest_reverse_rel_alias, under the org's GraphQL
// convention), asked with the forward relationship's own aliases; the page builds none itself.

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
  shape: "direct",
  properties: [],
};

const RELS = { relationships: [FORWARD], loading: false, refetch: vi.fn() };
const TABLES = { tables: [], loading: false, refetch: () => Promise.resolve() };
const DOMAINS = { domains: [] };
const UPSERT = { upsertRelationship: vi.fn() };
const DELETE = { deleteRelationship: vi.fn() };
const AUTH = { capabilities: ["create_relationship"] };
const FILTER = { domainsEnabled: false, domains: [], checkedDomains: new Set<string>() };
const SUGGEST = vi.hoisted(() => ({
  asked: [] as [string | null, string | null][],
  answer: { graphqlAlias: "", cypherAlias: "" },
}));

vi.mock("../../hooks/useAdminQueries", () => ({
  // REQ-2249: no code resolver set is in service here.
  useCodeResolvers: () => ({
    codeResolvers: [],
    loading: false,
    saveCodeResolver: vi.fn(),
    retireCodeResolver: vi.fn(),
  }),
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
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
}));
vi.mock("../../hooks/useNameSuggestions", () => ({
  useSuggestReverseRelAlias: () => async (gql: string | null, cql: string | null) => {
    SUGGEST.asked.push([gql, cql]);
    return SUGGEST.answer;
  },
}));
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

async function openReverse(answer: { graphqlAlias: string; cypherAlias: string }) {
  SUGGEST.asked = [];
  SUGGEST.answer = answer;
  render(<RelationshipsPage />);
  fireEvent.click(
    within(screen.getByTestId("relationships-list")).getByText("shop_access.customer_id"),
  );
  const reverse = within(screen.getByTestId("relationships-split-panel")).getByRole("button", {
    name: "Generate reverse relationship",
  });
  await waitFor(() => expect(reverse).toBeEnabled());
  fireEvent.click(reverse);
  await screen.findByTestId("reverse-rel-alias");
}

describe("RelationshipsPage: the reverse form's aliases are the naming authority's", () => {
  it("asks the server with the forward relationship's aliases", async () => {
    await openReverse({ graphqlAlias: "byCustomerBy", cypherAlias: "BY_CUSTOMER_BY" });
    expect(SUGGEST.asked).toEqual([[null, "BY_CUSTOMER"]]);
  });

  it("fills both aliases with the server's suggestion as given", async () => {
    await openReverse({ graphqlAlias: "by_customer_by", cypherAlias: "BY_CUSTOMER_BY" });
    expect(screen.getByTestId("reverse-rel-alias")).toHaveValue("BY_CUSTOMER_BY");
    expect(screen.getByTestId("reverse-rel-graphql-alias")).toHaveValue("by_customer_by");
  });

  it("shows a suggestion the page could not have computed, so none is built locally", async () => {
    await openReverse({ graphqlAlias: "<gql>", cypherAlias: "<cql>" });
    expect(screen.getByTestId("reverse-rel-alias")).toHaveValue("<cql>");
    expect(screen.getByTestId("reverse-rel-graphql-alias")).toHaveValue("<gql>");
  });
});
