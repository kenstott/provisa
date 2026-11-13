// Copyright (c) 2026 Kenneth Stott
// Canary: 1f8c4b27-6e3a-4d90-9b52-c7a0e8d3f614
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2185: the Library page lists the documents the caller may read under the shared filter; the
// selected one's panel shows it in section tabs and keeps, refreshes, replaces, rescopes or removes
// it; a document is added in one step, a file or a URL and its readers, and indexed in the
// background; and only the library-editing right sees the controls that change the library.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { act, fireEvent, render, screen, waitFor, within } from "../test-utils/render";

const api = vi.hoisted(() => ({
  fetchLibrary: vi.fn(),
  fetchLibraryDocument: vi.fn(),
  fetchCitedBy: vi.fn(),
  retryLibraryDocument: vi.fn(),
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
  useDomains: () => ({
    domains: ["sales", "meta", "ontology", "ops", "finance"].map((id) => ({
      id,
      isSystem: id !== "sales" && id !== "finance",
    })),
  }),
}));

const rights = vi.hoisted(() => ({ canEdit: true }));
vi.mock("../hooks/useCapability", () => ({ useCapability: () => rights.canEdit }));

import { LibraryPage } from "../pages/library/LibraryPage";
import { scopeChange } from "../components/admin/referenceScope";

const URL_DOC = {
  id: 3,
  title: "Client onboarding policy",
  source_kind: "url",
  source: "https://policies.example.com/client.html",
  media_type: "text/html",
  status: "kept",
  index_status: "ready",
  index_error: null,
  index_step: null,
  index_done: null,
  index_total: null,
  terms_note: null,
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
  rights.canEdit = true;
  api.fetchCitedBy.mockResolvedValue({ terms: [], drifted: 0, tags: [], tag_assignments: [] });
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

describe("the Library in the app", () => {
  const SRC = resolve(__dirname, "..");
  const read = (path: string) => readFileSync(resolve(SRC, path), "utf-8");

  it("sits beside the Ontology and before the Security group, on the glossary's read right", () => {
    const nav = read("components/NavBar.tsx");
    const ontology = nav.indexOf('to="/admin/ontology"');
    const library = nav.indexOf('to="/library"');
    const groups = nav.indexOf("NAV_GROUPS.filter(");
    expect(ontology).toBeGreaterThan(-1);
    expect(library).toBeGreaterThan(ontology);
    expect(library).toBeLessThan(groups);
    expect(nav.slice(ontology, library)).toContain('<CapabilityGate capability="glossary_read">');
    expect(read("App.tsx")).toMatch(
      /path="\/library"\s+element=\{\s+<CapabilityGate\s+capability="glossary_read"/,
    );
  });

  it("is no longer an Admin page", () => {
    expect(read("components/navGroups.ts")).not.toContain("reference-library");
    expect(read("pages/AdminPage.tsx")).not.toContain("Reference library");
    expect(read("App.tsx")).not.toContain("/admin/reference-library");
  });
});

describe("LibraryPage", () => {
  it("says why the page is there, in one line above the list", async () => {
    render(<LibraryPage />);
    expect(await screen.findByTestId("reflib-why")).toHaveTextContent(
      "Why: the documents that give your data its meaning",
    );
    expect(screen.getByTestId("page-title")).toHaveTextContent("Library");
  });

  it("filters the list by title, source or reader", async () => {
    render(<LibraryPage />);
    await screen.findByTestId("reflib-row-3");
    fireEvent.change(screen.getByTestId("reflib-filter"), { target: { value: "payments" } });
    await waitFor(() => expect(screen.queryByTestId("reflib-row-3")).toBeNull());
    expect(screen.getByTestId("reflib-row-2")).toBeInTheDocument();
    fireEvent.change(screen.getByTestId("reflib-filter"), { target: { value: "sales" } });
    expect(await screen.findByTestId("reflib-row-3")).toBeInTheDocument();
    await waitFor(() => expect(screen.queryByTestId("reflib-row-2")).toBeNull());
  });

  it("shows only what reading needs to a role without the library-editing right", async () => {
    rights.canEdit = false;
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-row-3"));
    await screen.findByTestId("reflib-detail-3");
    for (const control of ["reflib-add", "reflib-retrieval", "reflib-refresh", "reflib-remove"]) {
      expect(screen.queryByTestId(control)).toBeNull();
    }
    fireEvent.click(screen.getByTestId("reflib-tab-readers"));
    expect(await screen.findByTestId("reflib-readers-3")).toHaveTextContent("sales");
    expect(screen.queryByTestId("reflib-domains")).toBeNull();
  });

  it("shows a document's sections in one set of tabs", async () => {
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-row-3"));
    const detail = await screen.findByTestId("reflib-detail-3");
    for (const tab of ["overview", "sections", "cited", "readers"]) {
      expect(within(detail).getByTestId(`reflib-tab-${tab}`)).toBeInTheDocument();
    }
    expect(within(detail).getByTestId("reflib-read-3")).toHaveTextContent("Index status");
  });

  it("lists what cites a document and flags a drifted citation", async () => {
    api.fetchCitedBy.mockResolvedValue({
      drifted: 1,
      tags: [],
      tag_assignments: [],
      terms: [
        {
          term_id: 1,
          term: "Client",
          ordinal: 0,
          section: "Who is a Client",
          page: null,
          quote: "A Client holds an account.",
          state: "current",
        },
        {
          term_id: 2,
          term: "Dormant",
          ordinal: 4,
          section: "Closing",
          page: null,
          quote: "A dormant account is closed.",
          state: "drifted",
        },
      ],
    });
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-row-3"));
    expect(await screen.findByTestId("reflib-drifted-count")).toHaveTextContent("1");
    fireEvent.click(screen.getByTestId("reflib-tab-cited"));
    expect(await screen.findByTestId("reflib-cite-1")).toHaveTextContent("Client");
    expect(screen.getByTestId("reflib-cite-2")).toHaveTextContent("A dormant account is closed.");
    expect(within(screen.getByTestId("reflib-cite-2")).getByTestId("reflib-drifted")).toBeTruthy();
    expect(within(screen.getByTestId("reflib-cite-1")).queryByTestId("reflib-drifted")).toBeNull();
  });

  it("lists the tags and the tag assignments that cite a document, each linked", async () => {
    const place = { text_sha256: "x", cited_by: null, cited_at: "2026-11-13T00:00:00" };
    api.fetchCitedBy.mockResolvedValue({
      drifted: 0,
      terms: [],
      tags: [
        {
          tag_id: "finance",
          section_heading: "Scope",
          section_page: null,
          state: "current",
          needs_review: false,
          ...place,
        },
      ],
      tag_assignments: [
        {
          tag_id: "finance",
          object_type: "table",
          object_key: "table:7",
          section_heading: null,
          section_page: 4,
          state: "drifted",
          needs_review: true,
          ...place,
        },
      ],
    });
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-row-3"));
    await screen.findByTestId("reflib-detail-3");
    fireEvent.click(screen.getByTestId("reflib-tab-cited"));
    const definition = await screen.findByTestId("reflib-cite-tag-finance");
    expect(definition).toHaveTextContent("Scope");
    expect(within(definition).getByRole("link", { name: "finance" })).toHaveAttribute(
      "href",
      "/admin/tags?tag=finance",
    );
    const onObject = screen.getByTestId("reflib-cite-assignment-finance-table:7");
    expect(within(onObject).getByRole("link", { name: /7/ })).toHaveAttribute(
      "href",
      "/tables?table=7",
    );
    expect(onObject).toHaveTextContent("page 4");
    expect(within(onObject).getByTestId("reflib-cite-drifted")).toBeInTheDocument();
  });

  it("says when nothing cites a document", async () => {
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-row-3"));
    await screen.findByTestId("reflib-detail-3");
    fireEvent.click(screen.getByTestId("reflib-tab-cited"));
    expect(await screen.findByTestId("reflib-cited-empty")).toBeInTheDocument();
  });

  it("shows indexing progress and reads the list again until it is ready", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      const indexing = {
        ...URL_DOC,
        index_status: "indexing",
        index_step: "indexing",
        index_done: 64,
        index_total: 128,
        passages: 0,
      };
      api.fetchLibrary.mockResolvedValueOnce([indexing]).mockResolvedValue([URL_DOC]);
      render(<LibraryPage />);
      expect(await screen.findByTestId("reflib-index-3")).toHaveTextContent("Indexing 64 of 128");
      await act(async () => {
        await vi.advanceTimersByTimeAsync(3100);
      });
      await waitFor(() => expect(screen.getByTestId("reflib-index-3")).toHaveTextContent("Ready"));
      const reads = api.fetchLibrary.mock.calls.length;
      await act(async () => {
        await vi.advanceTimersByTimeAsync(9000);
      });
      expect(api.fetchLibrary.mock.calls.length).toBe(reads);
    } finally {
      vi.useRealTimers();
    }
  });

  it("says a document's passages wait for it to be indexed, and does not read them", async () => {
    api.fetchLibrary.mockResolvedValue([
      { ...URL_DOC, index_status: "indexing", index_step: "reading", passages: 0 },
    ]);
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-row-3"));
    await screen.findByTestId("reflib-detail-3");
    fireEvent.click(screen.getByTestId("reflib-tab-sections"));
    expect(await screen.findByTestId("reflib-not-indexed")).toBeInTheDocument();
    expect(api.fetchLibraryDocument).not.toHaveBeenCalled();
    expect(api.fetchCitedBy).not.toHaveBeenCalled();
    expect(screen.queryByTestId("reflib-refresh")).toBeNull();
  });

  it("shows why a document failed and retries it", async () => {
    const failed = {
      ...URL_DOC,
      index_status: "failed",
      index_error: "the org has no vector model",
      passages: 0,
    };
    api.fetchLibrary.mockResolvedValue([failed]);
    api.retryLibraryDocument.mockResolvedValue({
      ...failed,
      index_status: "indexing",
      index_error: null,
    });
    render(<LibraryPage />);
    expect(await screen.findByTestId("reflib-index-3")).toHaveTextContent("Failed");
    fireEvent.click(screen.getByTestId("reflib-row-3"));
    expect(await screen.findByTestId("reflib-read-3")).toHaveTextContent(
      "the org has no vector model",
    );
    fireEvent.click(screen.getByTestId("reflib-retry"));
    await waitFor(() => expect(api.retryLibraryDocument).toHaveBeenCalledWith(3));
    await waitFor(() => expect(screen.getByTestId("reflib-index-3")).toHaveTextContent("Indexing"));
    expect(screen.queryByTestId("reflib-retry")).toBeNull();
  });

  it("says why a ready document's terms were not looked for", async () => {
    const noted = { ...URL_DOC, terms_note: "no model is named for term finding" };
    api.fetchLibrary.mockResolvedValue([noted]);
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-row-3"));
    expect(await screen.findByTestId("reflib-read-3")).toHaveTextContent(
      "no model is named for term finding",
    );
  });

  it("adds a document in one step, with no title to fill in, and selects it", async () => {
    const added = {
      ...URL_DOC,
      id: 9,
      title: "records.html",
      index_status: "indexing",
      passages: 0,
    };
    api.addLibraryDocument.mockResolvedValue(added);
    api.fetchLibrary.mockResolvedValueOnce([URL_DOC]).mockResolvedValue([added, URL_DOC]);
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-add"));
    expect(screen.queryByTestId("reflib-add-title")).toBeNull();
    fireEvent.click(screen.getByRole("radio", { name: "URL" }));
    fireEvent.change(screen.getByTestId("reflib-add-url"), {
      target: { value: "https://x.example.com/records.html" },
    });
    const field = screen.getByTestId("reflib-add-domains");
    fireEvent.click(field);
    const listbox = document.getElementById(field.getAttribute("aria-controls") ?? "");
    if (!listbox) throw new Error("Readers opened no list");
    fireEvent.click(within(listbox).getByRole("option", { name: "sales", hidden: true }));
    fireEvent.click(screen.getByTestId("reflib-add-save"));
    await waitFor(() =>
      expect(api.addLibraryDocument).toHaveBeenCalledWith({
        url: "https://x.example.com/records.html",
        domains: ["sales"],
      }),
    );
    expect(await screen.findByTestId("reflib-detail-9")).toBeInTheDocument();
    expect(screen.getByTestId("reflib-index-9")).toHaveTextContent("Indexing");
  });

  it("offers no choice of retrieval: the library has one", async () => {
    render(<LibraryPage />);
    await screen.findByTestId("reflib-row-3");
    expect(screen.queryByTestId("reflib-retrieval")).toBeNull();
    expect(screen.queryByText("Retrieval")).toBeNull();
  });

  it("heads the page as the other list pages do: the filter beside the title, then + Document", async () => {
    render(<LibraryPage />);
    await screen.findByTestId("reflib-row-3");
    const filter = screen.getByTestId("reflib-filter");
    const header = filter.closest(".page-header");
    expect(header).not.toBeNull();
    expect(filter.closest("[data-toolbar], .page-actions")).toBeNull();
    const add = screen.getByTestId("reflib-add");
    expect(add).toHaveTextContent(/^\+ Document$/);
    expect(add.closest(".page-actions")?.parentElement).toBe(header);
  });

  it("lists each document with its source, index status and readers, marking a proposed one", async () => {
    render(<LibraryPage />);
    const row = await screen.findByTestId("reflib-row-3");
    expect(row).toHaveTextContent("Client onboarding policy");
    expect(row).toHaveTextContent("https://policies.example.com/client.html");
    expect(row).toHaveTextContent("Ready");
    expect(row).not.toHaveTextContent("Proposed");
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
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-row-3"));
    const detail = await screen.findByTestId("reflib-detail-3");
    fireEvent.click(within(detail).getByTestId("reflib-tab-sections"));
    expect(await within(detail).findByText("A Client holds an account.")).toBeInTheDocument();
    expect(screen.queryByTestId("reflib-upload-version")).toBeNull();
    fireEvent.click(screen.getByTestId("reflib-refresh"));
    await waitFor(() => expect(api.refreshLibraryDocument).toHaveBeenCalledWith(3));
  });

  it("keeps or discards a proposed document", async () => {
    api.keepLibraryDocument.mockResolvedValue({ ...PROPOSED, status: "kept" });
    api.removeLibraryDocument.mockResolvedValue(undefined);
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-row-2"));
    expect(await screen.findByTestId("reflib-upload-version")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("reflib-keep"));
    await waitFor(() => expect(api.keepLibraryDocument).toHaveBeenCalledWith(2));
    await waitFor(() =>
      expect(screen.getByTestId("reflib-row-2")).not.toHaveTextContent("Proposed"),
    );
    expect(screen.queryByTestId("reflib-keep")).toBeNull();
    fireEvent.click(screen.getByTestId("reflib-remove"));
    fireEvent.click(await screen.findByTestId("reflib-confirm-confirm"));
    await waitFor(() => expect(api.removeLibraryDocument).toHaveBeenCalledWith(2));
    await waitFor(() => expect(screen.queryByTestId("reflib-row-2")).toBeNull());
  });

  it("offers eCFR's XML as a file it reads, as it reads an eCFR URL", async () => {
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-add"));
    const pickers = document.querySelectorAll<HTMLInputElement>('input[type="file"]');
    expect(pickers.length).toBeGreaterThan(0);
    pickers.forEach((picker) => expect(picker.accept.split(",")).toContain(".xml"));
    pickers.forEach((picker) => expect(picker.accept.split(",")).toContain(".xlsx"));
    expect(screen.getByRole("img", { name: /eCFR XML/ })).toBeInTheDocument();
  });

  it("offers the whole organization and the user's domains, never a system domain", async () => {
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-add"));
    const field = screen.getByTestId("reflib-add-domains");
    fireEvent.click(field);
    const listbox = document.getElementById(field.getAttribute("aria-controls") ?? "");
    if (!listbox) throw new Error("Domains opened no list");
    const labels = within(listbox)
      .queryAllByRole("option", { hidden: true })
      .map((o) => o.textContent ?? "");
    expect(labels).toContain("sales");
    expect(labels).toContain("finance");
    expect(labels.some((l) => /whole/i.test(l))).toBe(true);
    expect(labels.filter((l) => ["meta", "ontology", "ops"].includes(l))).toEqual([]);
  });

  it("adds a document by its URL with its domains", async () => {
    api.addLibraryDocument.mockResolvedValue({ ...URL_DOC, id: 9, title: "Records schedule" });
    render(<LibraryPage />);
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
    render(<LibraryPage />);
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
    render(<LibraryPage />);
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
    render(<LibraryPage />);
    await screen.findByTestId("reflib-row-3");
    expect(screen.queryByTestId("reflib-reembed-all")).toBeNull();
  });

  it("carries the page's (?) help, last among its actions", async () => {
    render(<LibraryPage />);
    await screen.findByTestId("reflib-row-3");
    const help = screen.getByRole("button", { name: "About the reference library" });
    expect(help).toBe(screen.getByTestId("reflib-page-help"));
    expect(help.parentElement?.lastElementChild).toBe(help);
    fireEvent.click(help);
    const card = await screen.findByTestId("reflib-page-help-card");
    expect(card).toHaveTextContent("Not a document store.");
    expect(card).toHaveTextContent("search_reference_library");
    expect(card).toHaveTextContent("Re-embed is needed only when that model changes.");
    expect(card).not.toHaveTextContent(/\bRAG\b/);
  });
});
