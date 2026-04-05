import { describe, it, expect, vi, beforeEach } from "vitest";
import { render } from "@testing-library/react";

// ── Mocks ────────────────────────────────────────────────────────────────────

let mockLiveQuery = "";
let mockSchema: unknown = null;

vi.mock("@graphiql/react", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@graphiql/react")>();
  return {
    ...actual,
    useGraphiQL: vi.fn((selector: (s: Record<string, unknown>) => unknown) =>
      selector({ schema: mockSchema }),
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
    localStorage.clear();
  });

  it("passes liveQuery to Explorer when Monaco is ready", () => {
    mockLiveQuery = "{ orders { id } }";
    localStorage.setItem("graphiql:query", "{ products { sku } }");

    render(<SyncedExplorerContent />);

    expect(lastExplorerQuery).toBe("{ orders { id } }");
  });

  it("falls back to localStorage query when liveQuery is empty (pre-Monaco)", () => {
    mockLiveQuery = "";
    localStorage.setItem("graphiql:query", "{ customers { name } }");

    render(<SyncedExplorerContent />);

    expect(lastExplorerQuery).toBe("{ customers { name } }");
  });

  it("passes empty string when both liveQuery and localStorage are empty", () => {
    mockLiveQuery = "";

    render(<SyncedExplorerContent />);

    expect(lastExplorerQuery).toBe("");
  });

  it("prefers liveQuery over localStorage query", () => {
    mockLiveQuery = "{ live }";
    localStorage.setItem("graphiql:query", "{ stored }");

    render(<SyncedExplorerContent />);

    expect(lastExplorerQuery).toBe("{ live }");
  });
});
