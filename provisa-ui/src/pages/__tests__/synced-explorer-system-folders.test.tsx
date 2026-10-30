// Copyright (c) 2026 Kenneth Stott
// Canary: 6f0d6a52-3c1e-4c52-9d0e-8a9b3f7a21c4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2163: the GraphiQL explorer groups the ontology and ops root fields (on_terms, op_queries)
// in folders beside each domain's, rendered through the real QueryPage component and the real
// explorer fork (nothing here replaces either).
import React from "react";
import { describe, it, expect, vi } from "vitest";
import { render } from "@testing-library/react";
import { createRequire } from "module";

vi.mock("react-router-dom", () => {
  const passthrough = ({ children }: { children?: React.ReactNode }) =>
    React.createElement(React.Fragment, null, children);
  return {
    useNavigate: () => vi.fn(),
    useSearchParams: () => [new URLSearchParams(), vi.fn()],
    useParams: () => ({}),
    useLocation: () => ({ pathname: "/", search: "", hash: "", state: null }),
    Link: passthrough,
    NavLink: passthrough,
    MemoryRouter: passthrough,
    Outlet: passthrough,
  };
});

// The fork requires the CommonJS graphql; a schema built by the ESM copy is a foreign realm to it.
const { buildSchema } = createRequire(import.meta.url)("graphql") as typeof import("graphql");

// The schema a role with every domain ticked is introspected into (REQ-471, REQ-2163).
const schema = buildSchema(`type Query {
  pet_store__pets: [String]
  shelter__dogs: [String]
  on_terms: [String]
  on_domains: [String]
  op_queries: [String]
  health: String
}`);

vi.mock("@graphiql/react", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@graphiql/react")>();
  return {
    ...actual,
    useGraphiQL: vi.fn((selector: (s: Record<string, unknown>) => unknown) =>
      selector({ schema, initialQuery: "", activeTabIndex: 0, tabs: [{ query: "" }] }),
    ),
    useGraphiQLActions: vi.fn(() => ({ setOperationName: vi.fn(), run: vi.fn() })),
    useOperationsEditorState: vi.fn(() => ["", vi.fn()]),
    useOptimisticState: vi.fn((state: [string, (v: string) => void]) => state),
  };
});

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useDomains: () => ({
    domains: [
      { id: "pet_store", graphqlAlias: null },
      { id: "shelter", graphqlAlias: null },
      { id: "ontology", graphqlAlias: null },
      { id: "ops", graphqlAlias: null },
    ],
    loading: false,
    refetch: vi.fn(),
  }),
  useCompileQuery: () => ({
    compileQuery: vi.fn().mockResolvedValue({ queries: [] }),
    loading: false,
  }),
}));

import { SyncedExplorerContent } from "../QueryPage";

describe("SyncedExplorerContent system schema folders (REQ-2163)", () => {
  it("folds on_* and op_* root fields into ontology and ops folders, not loose", () => {
    const { container } = render(<SyncedExplorerContent />);
    const folders = [...container.querySelectorAll("details.graphiql-explorer-domain")];
    const names = folders.map((f) => f.querySelector("summary")?.textContent?.replace("▶", ""));
    expect(names).toEqual(["ontology", "ops", "pet_store", "shelter"]);

    const inFolder = (folder: string) =>
      [
        ...folders
          .find((f) => f.querySelector("summary")?.textContent?.includes(folder))!
          .querySelectorAll("[data-field-name], span"),
      ]
        .map((e) => e.textContent)
        .join("|");
    expect(inFolder("ontology")).toContain("on_terms");
    expect(inFolder("ontology")).toContain("on_domains");
    expect(inFolder("ops")).toContain("op_queries");

    // Outside every folder only the genuinely domainless field remains.
    const loose = [...container.querySelectorAll("span")]
      .filter((e) => !e.closest("details"))
      .map((e) => e.textContent ?? "");
    expect(loose.some((t) => /^(on_|op_)/.test(t))).toBe(false);
    expect(loose.some((t) => t === "health")).toBe(true);
  });
});
