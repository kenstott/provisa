// Copyright (c) 2026 Kenneth Stott
// Canary: 1f8c4b27-6e3a-4d90-9b52-c7a0e8d3f614
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2185: the Admin Reference library page lists the library's documents; the selected one's
// panel shows its facts and passages and keeps, refreshes, replaces, rescopes or removes it; and a
// document is added by a file or a URL with its domains, the whole org standing alone.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";

const api = vi.hoisted(() => ({
  fetchLibrary: vi.fn(),
  fetchLibraryDocument: vi.fn(),
  addLibraryDocument: vi.fn(),
  setLibraryDomains: vi.fn(),
  keepLibraryDocument: vi.fn(),
  refreshLibraryDocument: vi.fn(),
  reembedLibraryDocument: vi.fn(),
  reembedLibrary: vi.fn(),
  uploadLibraryVersion: vi.fn(),
  removeLibraryDocument: vi.fn(),
}));
vi.mock("../api/referenceLibrary", () => api);
vi.mock("../hooks/useAdminQueries", () => ({
  useDomains: () => ({ domains: [{ id: "sales" }, { id: "finance" }] }),
}));

import { ReferenceLibraryTab } from "../components/admin/ReferenceLibraryTab";
import { scopeChange } from "../components/admin/referenceScope";

const URL_DOC = {
  id: 3,
  title: "Client onboarding policy",
  source_kind: "url",
  source: "https://policies.example.com/client.html",
  media_type: "text/html",
  status: "kept",
  domains: ["sales"],
  embedding_model: "embed-small",
  needs_reembedding: false,
  passages: 2,
  added_by: "ada",
  added_at: "2026-10-20T09:12:00Z",
  refreshed_at: null,
  replaced_at: null,
};
const PROPOSED = {
  ...URL_DOC,
  id: 2,
  title: "Payments standard",
  source_kind: "file",
  source: "payments.md",
  status: "proposed",
  domains: ["*"],
};

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
  api.fetchLibrary.mockResolvedValue([URL_DOC, PROPOSED]);
  api.fetchLibraryDocument.mockImplementation(async (id: number) => ({
    ...(id === 3 ? URL_DOC : PROPOSED),
    passage_list: [
      { ordinal: 0, section: "Who is a Client", page: null, text: "A Client holds an account." },
    ],
  }));
});

describe("scopeChange", () => {
  it("lets the whole org stand alone", () => {
    expect(scopeChange(["sales"], ["sales", "*"])).toEqual(["*"]);
    expect(scopeChange(["*"], ["*", "finance"])).toEqual(["finance"]);
    expect(scopeChange(["sales"], ["sales", "finance"])).toEqual(["sales", "finance"]);
  });
});

describe("ReferenceLibraryTab", () => {
  it("lists each document with its source, status and domains", async () => {
    render(<ReferenceLibraryTab />);
    const row = await screen.findByTestId("reflib-row-3");
    expect(row).toHaveTextContent("Client onboarding policy");
    expect(row).toHaveTextContent("https://policies.example.com/client.html");
    expect(row).toHaveTextContent("Kept");
    expect(row).toHaveTextContent("sales");
    const proposed = screen.getByTestId("reflib-row-2");
    expect(proposed).toHaveTextContent("Proposed");
    expect(proposed).toHaveTextContent("Whole organization");
  });

  it("shows a document's passages and refreshes a URL document", async () => {
    api.refreshLibraryDocument.mockResolvedValue({
      ...URL_DOC,
      refreshed_at: "2026-10-29T10:00:00Z",
    });
    render(<ReferenceLibraryTab />);
    fireEvent.click(await screen.findByTestId("reflib-row-3"));
    const detail = await screen.findByTestId("reflib-detail-3");
    expect(await within(detail).findByText("A Client holds an account.")).toBeInTheDocument();
    expect(screen.queryByTestId("reflib-upload-version")).toBeNull();
    fireEvent.click(screen.getByTestId("reflib-refresh"));
    await waitFor(() => expect(api.refreshLibraryDocument).toHaveBeenCalledWith(3));
  });

  it("keeps or discards a proposed document", async () => {
    api.keepLibraryDocument.mockResolvedValue({ ...PROPOSED, status: "kept" });
    api.removeLibraryDocument.mockResolvedValue(undefined);
    render(<ReferenceLibraryTab />);
    fireEvent.click(await screen.findByTestId("reflib-row-2"));
    expect(await screen.findByTestId("reflib-upload-version")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("reflib-keep"));
    await waitFor(() => expect(api.keepLibraryDocument).toHaveBeenCalledWith(2));
    await waitFor(() => expect(screen.getByTestId("reflib-row-2")).toHaveTextContent("Kept"));
    expect(screen.queryByTestId("reflib-keep")).toBeNull();
    fireEvent.click(screen.getByTestId("reflib-remove"));
    fireEvent.click(await screen.findByTestId("reflib-confirm-confirm"));
    await waitFor(() => expect(api.removeLibraryDocument).toHaveBeenCalledWith(2));
    await waitFor(() => expect(screen.queryByTestId("reflib-row-2")).toBeNull());
  });

  it("adds a document by its URL with its domains", async () => {
    api.addLibraryDocument.mockResolvedValue({ ...URL_DOC, id: 9, title: "Records schedule" });
    render(<ReferenceLibraryTab />);
    fireEvent.click(await screen.findByTestId("reflib-add"));
    fireEvent.click(screen.getByRole("radio", { name: "URL" }));
    fireEvent.click(screen.getByTestId("reflib-add-save"));
    expect(await screen.findByText("Enter the document's URL.")).toBeInTheDocument();
    fireEvent.change(screen.getByTestId("reflib-add-url"), {
      target: { value: "https://x.example.com/records.html" },
    });
    fireEvent.click(screen.getByTestId("reflib-add-save"));
    expect(
      await screen.findByText("Choose at least one domain, or the whole organization."),
    ).toBeInTheDocument();
    expect(api.addLibraryDocument).not.toHaveBeenCalled();
  });

  it("names a document embedded with another model and re-embeds it from its stored text", async () => {
    const old = { ...URL_DOC, embedding_model: "embed-old", needs_reembedding: true };
    api.fetchLibrary.mockResolvedValue([old, PROPOSED]);
    api.fetchLibraryDocument.mockResolvedValue({ ...old, passage_list: [] });
    api.reembedLibraryDocument.mockResolvedValue({ ...URL_DOC, embedding_model: "embed-small" });
    render(<ReferenceLibraryTab />);
    expect(await screen.findByTestId("reflib-stale-3")).toHaveTextContent("Needs re-embedding");
    expect(screen.queryByTestId("reflib-stale-2")).toBeNull();
    fireEvent.click(screen.getByTestId("reflib-row-3"));
    expect(await screen.findByTestId("reflib-read-3")).toHaveTextContent("embed-old");
    fireEvent.click(screen.getByTestId("reflib-reembed"));
    await waitFor(() => expect(api.reembedLibraryDocument).toHaveBeenCalledWith(3));
    await waitFor(() => expect(screen.queryByTestId("reflib-stale-3")).toBeNull());
    expect(screen.queryByTestId("reflib-reembed")).toBeNull();
    expect(api.refreshLibraryDocument).not.toHaveBeenCalled();
  });

  it("re-embeds all and reports each document that failed", async () => {
    const old = { ...URL_DOC, embedding_model: "embed-old", needs_reembedding: true };
    const old2 = { ...PROPOSED, embedding_model: "embed-old", needs_reembedding: true };
    api.fetchLibrary.mockResolvedValueOnce([old, old2]).mockResolvedValueOnce([URL_DOC, old2]);
    api.reembedLibrary.mockResolvedValue({
      embedding_model: "embed-small",
      reembedded: [3],
      failed: [{ document_id: 2, title: "Payments standard", reason: "quota exceeded" }],
    });
    render(<ReferenceLibraryTab />);
    const all = await screen.findByTestId("reflib-reembed-all");
    expect(all).toHaveTextContent("Re-embed all (2)");
    fireEvent.click(all);
    const result = await screen.findByTestId("reflib-reembed-result");
    expect(result).toHaveTextContent("Documents re-embedded: 1");
    expect(screen.getByTestId("reflib-reembed-failures")).toHaveTextContent(
      "Payments standard: quota exceeded",
    );
    await waitFor(() => expect(screen.getByTestId("reflib-reembed-all")).toHaveTextContent("(1)"));
    expect(screen.queryByTestId("reflib-stale-3")).toBeNull();
    expect(screen.getByTestId("reflib-stale-2")).toBeInTheDocument();
  });

  it("offers no Re-embed when every document is on the current model", async () => {
    render(<ReferenceLibraryTab />);
    await screen.findByTestId("reflib-row-3");
    expect(screen.queryByTestId("reflib-reembed-all")).toBeNull();
  });
});
