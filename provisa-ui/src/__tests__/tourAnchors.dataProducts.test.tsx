// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors (test-utils/tourAnchor): the steps on the Data Products page. The steps that open a
// product are offered only to an org holding one (tourPrerequisites.json: quality, publish); the
// new-product form is offered with none and with some.

import { beforeEach, describe, it, expect, vi } from "vitest";
import { render } from "../test-utils/render";
import { walkTourStep } from "../test-utils/tourAnchor";
import type { DataProduct } from "../types/admin";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "admin", capabilities: [] } }),
}));
vi.mock("../hooks/useCapability", () => ({ useCapability: () => true }));
vi.mock("../api/actions", () => ({
  fetchActions: () => Promise.resolve({ functions: [] }),
  saveFunction: vi.fn(),
}));
vi.mock("../api/lineage", () => ({
  fetchFederationGraph: () => Promise.resolve({ nodes: [], edges: [] }),
}));
vi.mock("../api/glossary", () => ({
  fetchRelatedGlossaryTerms: () => Promise.resolve([]),
}));
vi.mock("../components/lineage/LineageDag", () => ({ LineageDag: () => null }));

// One value per hook across renders: a fresh array each render reruns the page's effects forever.
const REFETCH = vi.fn();
const NONE: never[] = [];
const PET_HEALTH = [
  {
    id: "pet_health",
    domainId: "pet-store",
    name: "Pet Health",
    purpose: "Pet health records",
    limitations: "",
    usage: "",
    customProperties: {},
  } as unknown as DataProduct,
];
let PRODUCTS = {
  dataProducts: NONE as DataProduct[],
  loading: false,
  error: null,
  refetch: REFETCH,
};
const EMPTY = { loading: false, refetch: REFETCH };

vi.mock("../hooks/useDataProductMutations", () => ({
  useCreateDataProduct: () => ({ createDataProduct: vi.fn(), loading: false }),
  useUpdateDataProduct: () => ({ updateDataProduct: vi.fn(), loading: false }),
  useDeleteDataProduct: () => ({ deleteDataProduct: vi.fn(), loading: false }),
}));
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useDataProducts: () => PRODUCTS,
  useDomains: () => ({ ...EMPTY, domains: NONE }),
  useTables: () => ({ ...EMPTY, tables: NONE }),
  useSources: () => ({ ...EMPTY, sources: NONE }),
  useRelationships: () => ({ ...EMPTY, relationships: NONE }),
  useRoles: () => ({ ...EMPTY, roles: NONE }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { DataProductsPage } from "../pages/DataProductsPage";

const STATES: Record<string, { products: DataProduct[]; steps: string[] }> = {
  "no data product": { products: NONE, steps: ["stepProductForm"] },
  "the pet store's product": {
    products: PET_HEALTH,
    steps: ["stepQualityProduct", "stepDataProducts", "stepProductForm"],
  },
};

describe("the tour's steps on the Data Products page", () => {
  for (const [name, { products, steps }] of Object.entries(STATES)) {
    describe(name, () => {
      beforeEach(() => {
        localStorage.clear();
        PRODUCTS = { dataProducts: products, loading: false, error: null, refetch: REFETCH };
      });
      for (const key of steps) {
        it(`finds ${key}'s anchor`, async () => {
          render(<DataProductsPage />, { initialEntries: ["/data-products"] });
          expect(await walkTourStep(key)).toBeInTheDocument();
        });
      }
    });
  }
});
