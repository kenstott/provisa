// Copyright (c) 2026 Kenneth Stott
// Canary: b6c0ab29-0be3-425a-a68e-1570b61e49b6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render } from "@testing-library/react";

// ── Mocks ────────────────────────────────────────────────────────────────────

let mockLiveQuery = "";
let mockSchema: unknown = null;
let mockInitialQuery = "";

vi.mock("@graphiql/react", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@graphiql/react")>();
  return {
    ...actual,
    useGraphiQL: vi.fn((selector: (s: Record<string, unknown>) => unknown) =>
      selector({
        schema: mockSchema,
        initialQuery: mockInitialQuery,
        activeTabIndex: 0,
        tabs: [{ query: mockInitialQuery }],
      }),
    ),
    useGraphiQLActions: vi.fn(() => ({
      setOperationName: vi.fn(),
      run: vi.fn(),
    })),
    useOperationsEditorState: vi.fn(() => [mockLiveQuery, vi.fn()]),
    useOptimisticState: vi.fn((state: [string, (v: string) => void]) => state),
  };
});

let lastExplorerQuery: string | undefined;

vi.mock("graphiql-explorer", () => ({
  Explorer: vi.fn((props: { query: string }) => {
    lastExplorerQuery = props.query;
    return null;
  }),
}));

// ── Import after mocks ────────────────────────────────────────────────────────

import { SyncedExplorerContent } from "../QueryPage";

// ── Tests ────────────────────────────────────────────────────────────────────

describe("SyncedExplorerContent — query fallback logic", () => {
  beforeEach(() => {
    lastExplorerQuery = undefined;
    mockLiveQuery = "";
    mockSchema = null;
    mockInitialQuery = "";
  });

  it("passes liveQuery to Explorer when liveQuery is set", () => {
    mockLiveQuery = "{ orders { id } }";
    mockInitialQuery = "{ products { sku } }";

    render(<SyncedExplorerContent />);

    expect(lastExplorerQuery).toBe("{ orders { id } }");
  });

  it("falls back to initialQuery when liveQuery is empty", () => {
    mockLiveQuery = "";
    mockInitialQuery = "{ customers { name } }";

    render(<SyncedExplorerContent />);

    expect(lastExplorerQuery).toBe("{ customers { name } }");
  });

  it("passes empty string when both liveQuery and initialQuery are empty", () => {
    mockLiveQuery = "";
    mockInitialQuery = "";

    render(<SyncedExplorerContent />);

    expect(lastExplorerQuery).toBe("");
  });

  it("prefers liveQuery over initialQuery", () => {
    mockLiveQuery = "{ live }";
    mockInitialQuery = "{ stored }";

    render(<SyncedExplorerContent />);

    expect(lastExplorerQuery).toBe("{ live }");
  });
});
