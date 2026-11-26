// Copyright (c) 2026 Kenneth Stott
// Canary: fec7ff0c-4300-47a7-8afd-c449d44413ae
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the Discover relationships dialog holds a multi-step task, so it has a deep link.
// `?discover=new` is the dialog setting up a run, `?discover=<runId>` the dialog showing that run;
// the link survives a reload because the page opens the dialog from it.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor } from "@testing-library/react";
import { useLocation } from "react-router-dom";
import { render } from "../../test-utils/render";
import { RelationshipsPage } from "../RelationshipsPage";
import {
  REPORT,
  SLOW,
  dialog,
  discoverButton,
  modelsMock,
  resetDiscoveryMocks,
  runButton,
  runMock,
  runOf,
} from "./discoverDialog.fixtures";

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

function Search() {
  const { search } = useLocation();
  return <span data-testid="search">{search}</span>;
}

const renderAt = (entry: string) =>
  render(
    <>
      <RelationshipsPage />
      <Search />
    </>,
    { initialEntries: [entry] },
  );
const discoverParam = () =>
  new URLSearchParams(screen.getByTestId("search").textContent ?? "").get("discover");

const ENDED = runOf({ status: "done", requests_total: 4, requests_done: 4, report: REPORT });

describe("Discover relationships: the dialog's deep link", () => {
  it("opening the dialog names it in the URL and closing it drops it", async () => {
    renderAt("/relationships");
    expect(discoverParam()).toBeNull();
    fireEvent.click(discoverButton());
    await waitFor(() => expect(modelsMock).toHaveBeenCalled());
    expect(discoverParam()).toBe("new");
    fireEvent.click(screen.getByTestId("discover-cancel"));
    await waitFor(() => expect(discoverParam()).toBeNull());
    // The modal's root stays through its close transition; its content goes at once.
    expect(screen.queryByTestId("discover-run")).toBeNull();
  });

  it("a link to the form opens the dialog setting up a run", async () => {
    renderAt("/relationships?discover=new");
    await waitFor(() => expect(runButton()).toBeEnabled(), SLOW);
    expect(dialog()).toBeInTheDocument();
    expect(discoverParam()).toBe("new");
  });

  it("a started run is named in the URL, and New run names the form again", async () => {
    runMock.mockResolvedValue(ENDED);
    renderAt("/relationships?discover=new");
    await waitFor(() => expect(runButton()).toBeEnabled(), SLOW);
    fireEvent.click(runButton());
    await waitFor(() => expect(discoverParam()).toBe("7"));
    fireEvent.click(await screen.findByTestId("discover-new-run", {}, SLOW));
    await waitFor(() => expect(discoverParam()).toBe("new"));
  });

  it("a link to a run opens the dialog on that run, read from the server", async () => {
    runMock.mockResolvedValue(ENDED);
    renderAt("/relationships?discover=7");
    expect(await screen.findByTestId("discover-run-block", {}, SLOW)).toBeInTheDocument();
    expect(runMock).toHaveBeenCalledWith(7);
    expect(screen.getByTestId("discover-new-run")).toBeInTheDocument();
    expect(discoverParam()).toBe("7");
  });

  it("a link to a run that cannot be read says so on the form", async () => {
    runMock.mockRejectedValue(new Error("No discovery run 99."));
    renderAt("/relationships?discover=99");
    expect(await screen.findByTestId("discover-link-error", {}, SLOW)).toHaveTextContent(
      "No discovery run 99.",
    );
    await waitFor(() => expect(discoverParam()).toBe("new"));
  });
});
