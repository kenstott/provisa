// Copyright (c) 2026 Kenneth Stott
// Canary: f693da4a-13ee-4c29-9809-70607fb6361c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors (test-utils/tourAnchor): the Relationships Deep Dive on its page, with no
// relationship declared and with some. The topic needs nothing of the org beyond its pages
// (tourPrerequisites.json), so both are offered.

import { beforeEach, describe, it, expect, vi } from "vitest";
import { render } from "../test-utils/render";
import { walkTourStep } from "../test-utils/tourAnchor";
import { RelationshipsPage } from "../pages/RelationshipsPage";

const rel = (id: string, source: string, column: string, target: string) => ({
  id,
  sourceTableId: 1,
  targetTableId: 2,
  sourceTableName: source,
  sourceDomainId: "pet-store",
  targetTableName: target,
  sourceColumn: column,
  targetColumn: "id",
  cardinality: "many-to-one",
  materialize: false,
  refreshInterval: 300,
  targetFunctionName: null,
  functionArg: null,
  alias: null as string | null,
  graphqlAlias: null,
  computedCypherAlias: null,
  autoSuggested: false,
  disableCypher: false,
  viaTableName: null,
});

// One value per hook across renders: a fresh array each render reruns the page's effects forever.
const DECLARED = [rel("pets-to-owners", "pets", "owner_id", "owners")];
let RELS = { relationships: [] as unknown[], loading: false, refetch: vi.fn() };
const TABLES = { tables: [], loading: false };
const DOMAINS = { domains: [] };
const UPSERT = { upsertRelationship: vi.fn(async () => ({ success: true, message: "" })) };
const DELETE = { deleteRelationship: vi.fn() };
const AUTH = { capabilities: ["create_relationship"] };
const FILTER = { domainsEnabled: false, domains: [], checkedDomains: new Set<string>() };

vi.mock("../hooks/useAdminQueries", () => ({
  useRelationships: () => RELS,
  useAllRelationships: () => RELS,
  useTables: () => TABLES,
  useDomains: () => DOMAINS,
  useUpsertRelationship: () => UPSERT,
  useDeleteRelationship: () => DELETE,
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => AUTH }));
vi.mock("../context/DomainFilterContext", () => ({ useDomainFilter: () => FILTER }));
vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  fetchCandidates: vi.fn().mockResolvedValue([]),
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
}));
vi.mock("../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../components/TagControl", () => ({ TagControl: () => null }));
// jsdom draws no canvas; the step's anchor is the ERD panel's header, not the graph. Every call on
// the stand-in answers another stand-in, and as a collection it is empty.
vi.mock("cytoscape", () => {
  const stub: unknown = new Proxy(function () {}, {
    get: (_t, prop) => {
      if (prop === "length") return 0;
      if (prop === Symbol.iterator) return function* () {};
      if (prop === "then") return undefined;
      if (prop === "map" || prop === "toArray") return () => [];
      if (prop === "forEach") return () => undefined;
      return stub;
    },
    apply: () => stub,
  });
  return { default: stub };
});

const STATES: Record<string, unknown[]> = {
  "no relationship declared": [],
  "the pet store's relationship declared": DECLARED,
};

describe("the Relationships Deep Dive's steps on its page", () => {
  for (const [name, relationships] of Object.entries(STATES)) {
    describe(name, () => {
      beforeEach(() => {
        RELS = { relationships, loading: false, refetch: vi.fn() };
      });
      for (const key of [
        "step15",
        "stepRelsAgents",
        "step16",
        "stepRelsSuggest",
        "stepRelsList",
        "step17",
      ]) {
        it(`finds ${key}'s anchor`, async () => {
          render(<RelationshipsPage />, { initialEntries: ["/relationships"] });
          expect(await walkTourStep(key)).toBeInTheDocument();
        });
      }
    });
  }
});

describe("a demo tour's relationship step", () => {
  it("finds its row by alias however many relationships the org holds", async () => {
    // Sixty relationships, the one the step names last: past the page's first fifty, it would
    // mount no anchor unless the step's search brings it forward.
    const many = Array.from({ length: 60 }, (_, i) =>
      rel(`r${i}`, `table_${String(i).padStart(2, "0")}`, "x_id", "other"),
    );
    many[59] = { ...many[59], alias: "SCORED_AS" };
    RELS = { relationships: many, loading: false, refetch: vi.fn() };
    render(<RelationshipsPage />, { initialEntries: ["/relationships?search=SCORED_AS"] });
    await vi.waitFor(() =>
      expect(document.querySelector('[data-tour-relationship="SCORED_AS"]')).not.toBeNull(),
    );
  });
});
