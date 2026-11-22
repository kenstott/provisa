// Copyright (c) 2026 Kenneth Stott
// Canary: 3b7d2e91-6a4c-4f08-b5e2-9c1a7d4f8e63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2253: relationship discovery works only within the domains selected in the navbar's domain
// filter. The dialog offers only them, starts from them, and asks the count and the run within
// them; a domain of the run before outside them is left out and said.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import {
  LAST,
  SCOPE,
  SLOW,
  countMock,
  lastScope,
  openDialog,
  resetDiscoveryMocks,
  runButton,
  runMock,
  runOf,
  REPORT,
  startMock,
} from "./discoverDialog.fixtures";
import { FILTER } from "./discoverDialog.state";

vi.mock("../../hooks/useAdminQueries", async () =>
  (await import("./discoverDialog.state")).adminQueries(),
);
vi.mock("../../context/AuthContext", async () => {
  const { AUTH } = await import("./discoverDialog.state");
  return { useAuth: () => AUTH };
});
vi.mock("../../context/DomainFilterContext", async () => {
  const { FILTER } = await import("./discoverDialog.state");
  return { useDomainFilter: () => FILTER };
});
vi.mock("../../api/admin", async (importOriginal) =>
  (await import("./discoverDialog.state")).adminApi(
    await importOriginal<typeof import("../../api/admin")>(),
  ),
);
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

const SHOP_ONLY = new Set(["shop"]);

beforeEach(() => {
  resetDiscoveryMocks();
  // The Domains picker has shop ticked and crm not.
  FILTER.checkedDomains = new Set(SHOP_ONLY);
});

async function offered(name: string): Promise<(string | null)[]> {
  await userEvent.click(screen.getByRole("textbox", { name }));
  return (await screen.findAllByRole("option", { hidden: true })).map((o) => o.textContent);
}

describe("Discovery dialog: within the selected domains", () => {
  it("offers only the domains ticked in the Domains picker, and their tables", async () => {
    await openDialog();
    const domains = await offered("Source domains");
    expect(domains).toContain("shop");
    expect(domains).not.toContain("crm");
    const tables = await offered("Source tables");
    expect(tables).toEqual(expect.arrayContaining(["orders", "customers"]));
    expect(tables).not.toContain("accounts");
    expect(tables).not.toContain("contacts");
  });

  it("starts from the ticked domains and asks the count and the run within them", async () => {
    await openDialog();
    await waitFor(() => expect(lastScope()).toEqual({ ...SCOPE, source_domains: ["shop"] }), SLOW);
    const counted = countMock.mock.calls[countMock.mock.calls.length - 1];
    expect(counted[1]).toEqual(SHOP_ONLY);
    fireEvent.click(runButton());
    await waitFor(() =>
      expect(startMock).toHaveBeenCalledWith({ ...SCOPE, source_domains: ["shop"] }, SHOP_ONLY),
    );
  });

  it("asks with no narrowing when the Domains picker narrows nothing", async () => {
    FILTER.checkedDomains = new Set(FILTER.domains);
    await openDialog();
    await waitFor(() => expect(countMock).toHaveBeenCalled(), SLOW);
    expect(countMock.mock.calls[countMock.mock.calls.length - 1][1]).toBeNull();
    fireEvent.click(runButton());
    await waitFor(() => expect(startMock).toHaveBeenCalled());
    expect(startMock.mock.calls[0][1]).toBeNull();
  });

  it("leaves out a domain of the run before that is not ticked, and says so", async () => {
    // The run before compared crm with shop; only shop is ticked now.
    runMock.mockResolvedValue(runOf({ status: "done", scope: LAST, report: REPORT }));
    startMock.mockResolvedValue(runOf({ scope: LAST }));
    await openDialog();
    fireEvent.click(runButton());
    await screen.findByTestId("discover-new-run", {}, SLOW);
    fireEvent.click(screen.getByTestId("discover-new-run"));
    const notes = await screen.findByTestId("discover-restore-notes", {}, SLOW);
    expect(notes).toHaveTextContent("crm");
    await waitFor(() => expect(lastScope().source_domains).toEqual(["shop"]), SLOW);
    expect(lastScope().source_tables).toBeNull();
  });
});
