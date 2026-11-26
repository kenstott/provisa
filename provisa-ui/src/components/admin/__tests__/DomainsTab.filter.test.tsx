// Copyright (c) 2026 Kenneth Stott
// Canary: 0f6d2b84-91c3-4e7a-b5d8-2a4c6e9f1b37
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Domains filter finds a domain by every text its row shows, and a list the filter empties
// says nothing matches, never that no domain is defined.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { expectFilterFindsEveryCell } from "../../../test-utils/listFilter";
import type { Domain } from "../../../types/admin";

const domain = (over: Partial<Domain>): Domain => ({
  id: "sales",
  description: "Sales data",
  graphqlAlias: null,
  stewards: [],
  stewardField: { people: [], roles: [] },
  parentDomainId: null,
  effectiveStewards: [],
  isSystem: false,
  ...over,
});
const DOMAINS = [
  domain({ stewardField: { people: ["ada@example.test"], roles: ["sales_steward"] } }),
  domain({
    id: "emea_sales",
    description: "European sales",
    graphqlAlias: "emeaSales",
    parentDomainId: "sales",
    effectiveStewards: ["ada@example.test"],
  }),
];

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useDomains: () => ({ domains: DOMAINS, loading: false, refetch: vi.fn() }),
  useRoles: () => ({ roles: [], loading: false, refetch: vi.fn() }),
  useCreateDomain: () => ({ createDomain: vi.fn() }),
  useDeleteDomain: () => ({ deleteDomain: vi.fn() }),
  useDeleteDomains: () => vi.fn(),
}));
vi.mock("../settingsCards", () => ({
  ApolloSubgraphCard: () => null,
  DomainModeCard: () => null,
  NameClashPolicyCard: () => null,
  NamingConventionsCard: () => null,
  OrgProcessingCards: () => null,
}));

import { DomainsTab } from "../DomainsTab";

describe("DomainsTab filter", () => {
  it("finds each domain by every text its row shows", async () => {
    render(<DomainsTab title="Domains" domainsEnabled />);
    await screen.findByTestId("domain-row-emea_sales");
    await expectFilterFindsEveryCell(screen.getByTestId("domains-filter"), () =>
      screen.getByTestId("domains-list"),
    );
  });

  it("says nothing matches when the filter empties the list", async () => {
    render(<DomainsTab title="Domains" domainsEnabled />);
    await screen.findByTestId("domain-row-emea_sales");
    fireEvent.change(screen.getByTestId("domains-filter"), { target: { value: "no-such-domain" } });
    expect(screen.getByTestId("domains-empty")).toHaveTextContent("No domains match the filter");
  });
});
