import { describe, it, expect, vi, beforeEach } from "vitest";
import { render } from "@testing-library/react";

// ── Mocks ────────────────────────────────────────────────────────────────────

let mockLiveQuery = "";
let mockInitialQuery = "";
let mockSchema: unknown = null;

vi.mock("@graphiql/react", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@graphiql/react")>();
  return {
    ...actual,
    useGraphiQL: vi.fn((selector: (s: Record<string, unknown>) => unknown) =>
      selector({ schema: mockSchema, initialQuery: mockInitialQuery }),
    ),
    useGraphiQLActions: vi.fn(() => ({
      setOperationName: vi.fn(),
      run: vi.fn(),
    })),
    useOperationsEditorState: vi.fn(() => [mockLiveQuery, vi.fn()]),
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
    mockInitialQuery = "";
    mockSchema = null;
  });

  it("passes liveQuery to Explorer when Monaco is ready", () => {
    mockLiveQuery = "{ orders { id } }";
    mockInitialQuery = "{ products { sku } }";

    render(<SyncedExplorerContent />);

    expect(lastExplorerQuery).toBe("{ orders { id } }");
  });

  it("falls back to initialQuery when liveQuery is empty (pre-Monaco)", () => {
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

  it("prefers liveQuery over initialQuery even when initialQuery is non-empty", () => {
    mockLiveQuery = "{ live }";
    mockInitialQuery = "{ initial }";

    render(<SyncedExplorerContent />);

    expect(lastExplorerQuery).toBe("{ live }");
  });
});
