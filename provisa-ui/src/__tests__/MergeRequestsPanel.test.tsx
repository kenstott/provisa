// Copyright (c) 2026 Kenneth Stott
// Canary: 5a71f2e8-4c39-4bd0-b6d7-90e18c3f4a26
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1504: who may decide a proposed merge, and which requests are still open to decide. A state
// of `stale` is derived by the server at read time, so a stale request is shown as one nobody can
// act on rather than one this table recomputes.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { MergeRequestsPanel } from "../components/admin/MergeRequestsPanel";
import type { MergeRequest } from "../api/environments";

vi.mock("../api/environments", () => ({
  fetchMergeRequests: vi.fn(),
  decideMergeRequest: vi.fn(),
}));

import { decideMergeRequest, fetchMergeRequests } from "../api/environments";

const mockList = vi.mocked(fetchMergeRequests);
const mockDecide = vi.mocked(decideMergeRequest);

function request(id: number, overrides: Partial<MergeRequest> = {}): MergeRequest {
  return {
    id,
    source_env: "dev",
    source_ref: null,
    source_sha: null,
    seed: false,
    target_env: "prod",
    state: "open",
    requested_by: "sam",
    requested_at: "2026-08-01T00:00:00Z",
    decided_by: null,
    decided_at: null,
    decision_note: null,
    applied_at: null,
    report: { added: 1, changed: 2, removed: 0 },
    message: "ship it",
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockList.mockResolvedValue([request(7)]);
});

describe("MergeRequestsPanel", () => {
  it("asks for open requests only until told otherwise", async () => {
    render(<MergeRequestsPanel orgId="acme" canDecide />);
    await waitFor(() => expect(mockList).toHaveBeenCalledWith("acme", true));
    fireEvent.click(screen.getByTestId("merge-requests-open-only"));
    await waitFor(() => expect(mockList).toHaveBeenCalledWith("acme", false));
  });

  it("carries the approver's note with the decision", async () => {
    mockDecide.mockResolvedValue(request(7, { state: "approved", decided_by: "lee" }));
    render(<MergeRequestsPanel orgId="acme" canDecide />);
    fireEvent.change(await screen.findByTestId("merge-request-note-7"), {
      target: { value: "reviewed the diff" },
    });
    fireEvent.click(screen.getByTestId("merge-request-approve-7"));
    await waitFor(() =>
      expect(mockDecide).toHaveBeenCalledWith("acme", 7, true, "reviewed the diff"),
    );
  });

  it("rejects with the same controls", async () => {
    mockDecide.mockResolvedValue(request(7, { state: "rejected" }));
    render(<MergeRequestsPanel orgId="acme" canDecide />);
    fireEvent.click(await screen.findByTestId("merge-request-reject-7"));
    await waitFor(() => expect(mockDecide).toHaveBeenCalledWith("acme", 7, false, undefined));
  });

  it("shows the request to somebody who cannot decide it, without the controls", async () => {
    render(<MergeRequestsPanel orgId="acme" canDecide={false} />);
    expect(await screen.findByTestId("merge-request-7")).toBeInTheDocument();
    expect(screen.queryByTestId("merge-request-approve-7")).toBeNull();
    expect(screen.queryByTestId("merge-request-reject-7")).toBeNull();
  });

  it("offers no decision on a request that is no longer open", async () => {
    mockList.mockResolvedValue([request(7, { state: "stale" })]);
    render(<MergeRequestsPanel orgId="acme" canDecide />);
    expect(await screen.findByTestId("merge-request-state-7")).toHaveTextContent("stale");
    expect(screen.queryByTestId("merge-request-approve-7")).toBeNull();
  });

  it("names the branch and the pinned commit when the request is a load", async () => {
    // REQ-1496: an approver of a load is agreeing to one COMMIT, so the sha is on the row beside
    // the branch name — the branch may already point somewhere else.
    mockList.mockResolvedValue([
      request(7, {
        source_env: null,
        source_ref: "main",
        source_sha: "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c",
      }),
    ]);
    render(<MergeRequestsPanel orgId="acme" canDecide />);
    expect(await screen.findByTestId("merge-request-7")).toHaveTextContent("main@0f1e2d3 → prod");
  });

  it("says so when there is nothing waiting", async () => {
    mockList.mockResolvedValue([]);
    render(<MergeRequestsPanel orgId="acme" canDecide />);
    expect(await screen.findByTestId("merge-requests-empty")).toBeInTheDocument();
  });
});
