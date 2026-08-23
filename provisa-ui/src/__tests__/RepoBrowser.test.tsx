// Copyright (c) 2026 Kenneth Stott
// Canary: e2c85a13-7d64-4f09-b3a1-58d97e0c26bf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1524: BROWSE. The branch list comes from the repository, not the environment registry — a
// ref outlives the environment that wrote it — and a file is read at a named ref, so a selection
// that does not exist under a newly-chosen ref is dropped rather than asked for and 404'd.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent, within } from "../test-utils/render";
import { RepoBrowser } from "../components/admin/RepoBrowser";

vi.mock("../api/environments", () => ({
  fetchRepoBranches: vi.fn(),
  fetchRepoHistory: vi.fn(),
  fetchRepoFiles: vi.fn(),
  fetchRepoFile: vi.fn(),
}));

import {
  fetchRepoBranches,
  fetchRepoFile,
  fetchRepoFiles,
  fetchRepoHistory,
} from "../api/environments";

const mockBranches = vi.mocked(fetchRepoBranches);
const mockHistory = vi.mocked(fetchRepoHistory);
const mockFiles = vi.mocked(fetchRepoFiles);
const mockFile = vi.mocked(fetchRepoFile);

const SHA = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678";

function pick(input: HTMLElement, value: string) {
  fireEvent.click(input);
  const listId = input.getAttribute("aria-controls");
  if (listId === null) throw new Error("select has no dropdown");
  const list = document.getElementById(listId);
  if (list === null) throw new Error(`dropdown ${listId} is not open`);
  fireEvent.click(within(list).getByText(value));
}

beforeEach(() => {
  vi.clearAllMocks();
  mockBranches.mockResolvedValue(["prod", "retired-branch"]);
  mockHistory.mockResolvedValue([
    { sha: SHA, author: "sam", message: "add orders", committed_at: 1 },
  ]);
  mockFiles.mockResolvedValue(["domains/sales.yaml"]);
  mockFile.mockResolvedValue("name: sales\n");
});

describe("RepoBrowser", () => {
  it("opens on the first branch the repository reports", async () => {
    render(<RepoBrowser orgId="acme" />);
    await waitFor(() => expect(mockHistory).toHaveBeenCalledWith("acme", "prod"));
    expect(mockFiles).toHaveBeenCalledWith("acme", "prod");
  });

  it("offers a branch whose environment is gone", async () => {
    render(<RepoBrowser orgId="acme" />);
    await screen.findByTestId("repo-ref-select");
    pick(screen.getByTestId("repo-ref-select"), "retired-branch");
    await waitFor(() => expect(mockFiles).toHaveBeenCalledWith("acme", "retired-branch"));
  });

  it("reads a file at the selected ref", async () => {
    render(<RepoBrowser orgId="acme" />);
    fireEvent.click(await screen.findByTestId("repo-path-domains/sales.yaml"));
    await waitFor(() =>
      expect(mockFile).toHaveBeenCalledWith("acme", "prod", "domains/sales.yaml"),
    );
    expect(await screen.findByTestId("repo-file-text")).toHaveTextContent("name: sales");
  });

  it("drops a path the newly-selected ref does not have", async () => {
    render(<RepoBrowser orgId="acme" />);
    fireEvent.click(await screen.findByTestId("repo-path-domains/sales.yaml"));
    await waitFor(() => expect(mockFile).toHaveBeenCalled());
    mockFiles.mockResolvedValue(["domains/hr.yaml"]);
    mockFile.mockClear();
    pick(screen.getByTestId("repo-ref-select"), "retired-branch");
    expect(await screen.findByTestId("repo-no-file")).toBeInTheDocument();
    expect(mockFile).not.toHaveBeenCalled();
  });

  it("takes a commit as a ref of its own", async () => {
    render(<RepoBrowser orgId="acme" />);
    fireEvent.click(await screen.findByTestId(`repo-commit-${SHA.slice(0, 8)}`));
    await waitFor(() => expect(mockFiles).toHaveBeenCalledWith("acme", SHA));
  });

  it("asks for a second ref only once comparing is turned on", async () => {
    render(<RepoBrowser orgId="acme" />);
    await screen.findByTestId("repo-browser");
    expect(screen.queryByTestId("repo-against-select")).toBeNull();
    fireEvent.click(screen.getByTestId("repo-compare-toggle"));
    expect(await screen.findByTestId("repo-against-select")).toBeInTheDocument();
  });
});
