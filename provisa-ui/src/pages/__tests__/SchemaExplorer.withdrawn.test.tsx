// Copyright (c) 2026 Kenneth Stott
// Canary: 2c9e5f17-8a3d-4b60-9e2f-7d1a4c83b506
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: Explore's schema ERD offers Show withdrawn to a person who may preview withdrawn
// objects, and only then reads the withdrawn tables and relationships.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../test-utils/render";

const asked = vi.hoisted(() => ({ tables: [] as unknown[], relationships: [] as unknown[] }));
const auth = vi.hoisted(() => ({
  role: "steward",
  actingRoles: "steward",
  capabilities: ["query_development", "table_registration"] as string[],
}));
const slot = vi.hoisted(() => ({ node: null as HTMLElement | null }));

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => auth,
}));
vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ domains: ["sales"], checkedDomains: new Set<string>() }),
}));
vi.mock("../../context/subnavExtraSlot", () => ({
  useSubnavExtraSlot: () => ({ node: slot.node }),
}));
vi.mock("../../hooks/useAdminQueries", () => ({
  useTables: (opts?: unknown) => {
    asked.tables.push(opts);
    return { tables: [], loading: false, refetch: vi.fn() };
  },
  useAllRelationships: (opts?: unknown) => {
    asked.relationships.push(opts);
    return { relationships: [], loading: false, refetch: vi.fn() };
  },
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
}));
// The canvas: whether it was handed the switch, and the switch itself.
vi.mock("../../components/erd/ErdPanel", () => ({
  ErdPanel: ({ withdrawn }: { withdrawn?: { shown: boolean; onChange: (v: boolean) => void } }) =>
    withdrawn ? (
      <button
        data-testid="switch"
        data-shown={withdrawn.shown}
        onClick={() => withdrawn.onChange(!withdrawn.shown)}
      />
    ) : (
      <div data-testid="no-switch" />
    ),
}));

import { SchemaExplorer } from "../SchemaExplorer";

function openErd() {
  render(<SchemaExplorer />);
  fireEvent.click(screen.getByText("ERD"));
}

describe("Explore's schema ERD and Show withdrawn", () => {
  beforeEach(() => {
    asked.tables = [];
    asked.relationships = [];
    slot.node = document.createElement("div");
    document.body.appendChild(slot.node);
    vi.stubGlobal(
      "fetch",
      vi.fn(() => new Promise(() => {})),
    );
    auth.capabilities = ["query_development", "table_registration"];
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    slot.node?.remove();
  });

  it("offers it to a holder of table registration, off, and on reads the withdrawn", () => {
    openErd();
    const control = screen.getByTestId("switch");
    expect(control).toHaveAttribute("data-shown", "false");
    expect(asked.tables.at(-1)).toEqual({ includeWithdrawn: false });
    fireEvent.click(control);
    expect(screen.getByTestId("switch")).toHaveAttribute("data-shown", "true");
    expect(asked.tables.at(-1)).toEqual({ includeWithdrawn: true });
    expect(asked.relationships.at(-1)).toEqual({ includeWithdrawn: true });
  });

  it("does not offer it to a person without table registration", () => {
    auth.capabilities = ["query_development"];
    openErd();
    expect(screen.getByTestId("no-switch")).toBeInTheDocument();
    expect(asked.tables.at(-1)).toEqual({ includeWithdrawn: false });
  });
});
