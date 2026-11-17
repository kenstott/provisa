// Copyright (c) 2026 Kenneth Stott
// Canary: 089e0a50-4a29-46c1-be76-b054cdc8e14a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: a discovery run after the first starts from the fields of the signed-in person's run
// before. A value that no longer holds is left out and said; a set that held everything registered
// when that run started is restored as everything; tables registered since outside it are named,
// with one action adding their domains.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor } from "@testing-library/react";
import { type DiscoveryScope } from "../../api/admin";
import {
  LAST,
  REPORT,
  SCOPE,
  SLOW,
  fkBox,
  lastScope,
  openDialog,
  resetDiscoveryMocks,
  runButton,
  runMock,
  runOf,
  runsMock,
  sameBox,
  semanticsBox,
  startMock,
} from "./discoverDialog.fixtures";
import { REGISTERED, SINCE, TABLES, table } from "./discoverDialog.state";

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

beforeEach(resetDiscoveryMocks);

describe("Discovery dialog: a run starts from the fields of the one before", () => {
  it("New run opens the form with the ended run's fields, not the defaults", async () => {
    runMock.mockResolvedValue(runOf({ status: "done", scope: LAST, report: REPORT }));
    startMock.mockResolvedValue(runOf({ scope: LAST }));
    await openDialog();
    fireEvent.click(runButton());
    fireEvent.click(await screen.findByTestId("discover-new-run", {}, SLOW));
    await waitFor(() => expect(runButton()).toBeEnabled(), SLOW);
    expect(screen.getByTestId("discover-from-last-run")).toHaveTextContent(
      "These are the fields of your last run.",
    );
    expect(fkBox().checked).toBe(false);
    expect(semanticsBox().checked).toBe(true);
    expect(sameBox().checked).toBe(false);
    await waitFor(() => expect(lastScope()).toEqual(LAST), SLOW);
    expect(screen.queryByTestId("discover-restore-notes")).toBeNull();
  });

  it("opening the dialog starts from the latest run the signed-in person started", async () => {
    runsMock.mockImplementation(async (_limit, mine) =>
      mine ? [runOf({ id: 5, status: "done", scope: LAST })] : [],
    );
    await openDialog();
    expect(runsMock).toHaveBeenCalledWith(1, true);
    expect(fkBox().checked).toBe(false);
    expect(sameBox().checked).toBe(false);
    await waitFor(() => expect(lastScope()).toEqual(LAST), SLOW);
  });

  it("starts from the defaults when the person has run none", async () => {
    await openDialog();
    expect(screen.queryByTestId("discover-from-last-run")).toBeNull();
    expect(screen.queryByTestId("discover-reset-defaults")).toBeNull();
    expect(fkBox().checked).toBe(true);
    await waitFor(() => expect(lastScope()).toEqual(SCOPE), SLOW);
  });

  it("leaves out what no longer holds, says which, and keeps the rest", async () => {
    const stale: DiscoveryScope = {
      ...LAST,
      source_domains: ["crm", "legacy"],
      source_tables: [3, 99],
      target_tables: [2, 98],
      vendor: "mistral",
      model: "mistral-large",
    };
    runsMock.mockImplementation(async (_limit, mine) =>
      mine ? [runOf({ id: 5, status: "done", scope: stale })] : [],
    );
    await openDialog();
    const notes = await screen.findByTestId("discover-restore-notes");
    expect(notes).toHaveTextContent("Left out of them, because it no longer holds:");
    expect(notes).toHaveTextContent("The domain legacy is no longer available.");
    expect(notes).toHaveTextContent("2 tables are no longer registered there.");
    expect(notes).toHaveTextContent("mistral has no key, so the model is the assigned one.");
    // The rest is kept: the kinds, the domains and tables still there, and the target set.
    await waitFor(() => expect(lastScope()).toEqual({ ...LAST, vendor: null, model: null }), SLOW);
  });

  it("leaves out a model its vendor no longer lists", async () => {
    runsMock.mockImplementation(async (_limit, mine) =>
      mine ? [runOf({ id: 5, status: "done", scope: { ...LAST, model: "gpt-3" } })] : [],
    );
    await openDialog();
    expect(await screen.findByTestId("discover-restore-notes")).toHaveTextContent(
      "openai no longer lists gpt-3, so the model is the assigned one.",
    );
    await waitFor(() => expect(lastScope()).toEqual({ ...LAST, vendor: null, model: null }), SLOW);
  });

  it("Reset to defaults sets the form up as a first run", async () => {
    runsMock.mockImplementation(async (_limit, mine) =>
      mine ? [runOf({ id: 5, status: "done", scope: LAST })] : [],
    );
    await openDialog();
    expect(fkBox().checked).toBe(false);
    fireEvent.click(screen.getByTestId("discover-reset-defaults"));
    await waitFor(() => expect(fkBox().checked).toBe(true));
    expect(sameBox().checked).toBe(true);
    expect(screen.queryByTestId("discover-from-last-run")).toBeNull();
    await waitFor(() => expect(lastScope()).toEqual(SCOPE), SLOW);
  });

  it("restores a run that chose every table of a domain as every table, so one registered since is in it", async () => {
    TABLES.tables = [...REGISTERED, table(6, "crm", "leads", SINCE)];
    const everyCrmTable: DiscoveryScope = {
      ...SCOPE,
      source_domains: ["crm"],
      source_tables: [3, 5],
    };
    runsMock.mockImplementation(async (_limit, mine) =>
      mine ? [runOf({ id: 5, status: "done", scope: everyCrmTable })] : [],
    );
    await openDialog();
    await waitFor(
      () => expect(lastScope()).toEqual({ ...SCOPE, source_domains: ["crm"], source_tables: null }),
      SLOW,
    );
    expect(screen.queryByTestId("discover-new-tables")).toBeNull();
  });

  it("restores a run over every domain then as every domain, so one registered since is in it", async () => {
    TABLES.tables = [...REGISTERED, table(6, "files", "permits", SINCE)];
    runsMock.mockImplementation(async (_limit, mine) =>
      mine
        ? [runOf({ id: 5, status: "done", scope: { ...SCOPE, source_domains: ["crm", "shop"] } })]
        : [],
    );
    await openDialog();
    await waitFor(() => expect(lastScope().source_domains).toEqual(["crm", "files", "shop"]), SLOW);
    expect(screen.queryByTestId("discover-new-tables")).toBeNull();
  });

  it("keeps a run's own choice of some of a domain's tables as it was", async () => {
    TABLES.tables = [...REGISTERED, table(6, "crm", "leads", SINCE)];
    runsMock.mockImplementation(async (_limit, mine) =>
      mine
        ? [
            runOf({
              id: 5,
              status: "done",
              scope: { ...SCOPE, source_domains: ["crm"], source_tables: [3] },
            }),
          ]
        : [],
    );
    await openDialog();
    await waitFor(() => expect(lastScope().source_tables).toEqual([3]), SLOW);
  });

  it("names the tables registered since the last run outside it, and adds their domains", async () => {
    TABLES.tables = [
      ...REGISTERED,
      table(6, "files", "permits", SINCE),
      table(7, "files", "inspections", SINCE),
      table(8, "shop", "returns", SINCE),
    ];
    runsMock.mockImplementation(async (_limit, mine) =>
      mine ? [runOf({ id: 5, status: "done", scope: SCOPE })] : [],
    );
    await openDialog();
    // The new shop table is in the run already: only the files tables are outside it.
    expect(screen.getByTestId("discover-new-tables")).toHaveTextContent(
      "2 tables registered since your last run are in files, which it did not look at.",
    );
    await waitFor(() => expect(lastScope().source_domains).toEqual(["shop"]), SLOW);
    fireEvent.click(screen.getByTestId("discover-add-new-tables"));
    await waitFor(() => expect(lastScope().source_domains).toEqual(["shop", "files"]), SLOW);
    expect(screen.queryByTestId("discover-new-tables")).toBeNull();
  });

  it("adds them to a target of its own too, and to a table list set by hand", async () => {
    TABLES.tables = [...REGISTERED, table(6, "files", "permits", SINCE)];
    runsMock.mockImplementation(async (_limit, mine) =>
      mine ? [runOf({ id: 5, status: "done", scope: LAST })] : [],
    );
    await openDialog();
    expect(screen.getByTestId("discover-new-tables")).toHaveTextContent(
      "1 table registered since your last run is in files, which it did not look at.",
    );
    fireEvent.click(screen.getByTestId("discover-add-new-tables"));
    await waitFor(
      () =>
        expect(lastScope()).toEqual({
          ...LAST,
          source_domains: ["crm", "files"],
          source_tables: [3, 6],
          target_domains: ["shop", "files"],
          target_tables: [2, 6],
        }),
      SLOW,
    );
  });

  it("says nothing of tables registered since when the form starts from the defaults", async () => {
    TABLES.tables = [...REGISTERED, table(6, "files", "permits", SINCE)];
    await openDialog();
    expect(screen.queryByTestId("discover-new-tables")).toBeNull();
  });

  it("New run sets up another run after one has ended", async () => {
    runMock.mockResolvedValue(runOf({ status: "done", report: REPORT }));
    await openDialog();
    fireEvent.click(runButton());
    fireEvent.click(await screen.findByTestId("discover-new-run", {}, SLOW));
    expect(await screen.findByTestId("discover-run")).toBeInTheDocument();
  });
});
