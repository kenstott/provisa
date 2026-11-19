// Copyright (c) 2026 Kenneth Stott
// Canary: 2f8c6a19-7e4b-4d03-9c51-b6a0e3d7f824
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: the read-only Glossary page -- approved terms as a book glossary, numbered senses per
// domain, a letter bar, search, a domain filter, a sort by name or recent change, and the term's
// name linked to the Ontology editor only for a viewer who holds its right.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";
import type { GlossaryEntries } from "../../../api/glossary";

const auth = vi.hoisted(() => ({ capabilities: ["glossary_read"] as string[] }));
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));

import { GlossaryPage } from "../GlossaryPage";

// What the server sends: approved terms only (glossary_entries) -- the page adds no status rule.
const DATA: GlossaryEntries = {
  terms: [
    {
      id: 1,
      name: "account",
      definition: "A record of money held for a customer.",
      domains: ["finance"],
      changed_at: "2026-11-01T10:00:00Z",
    },
    {
      id: 2,
      name: "Account",
      definition: "A company we sell to.",
      domains: ["sales"],
      changed_at: "2026-11-18T10:00:00Z",
    },
    {
      id: 3,
      name: "savings account",
      definition: "An account that earns interest.",
      domains: ["finance"],
      changed_at: "2026-10-01T10:00:00Z",
    },
    {
      id: 4,
      name: "client",
      definition: "Someone we serve.",
      domains: ["sales"],
      changed_at: "2026-09-01T10:00:00Z",
    },
    {
      id: 5,
      name: "patron",
      definition: "Someone we serve.",
      domains: ["sales"],
      changed_at: "2026-08-01T10:00:00Z",
    },
    {
      id: 6,
      name: "zone",
      definition: "A region we deliver to.",
      domains: ["*"],
      changed_at: "2026-11-10T10:00:00Z",
    },
  ],
  edges: [
    { from_term_id: 3, to_term_id: 1, rel_type: "KIND_OF" },
    { from_term_id: 2, to_term_id: 4, rel_type: "RELATED_TO" },
    { from_term_id: 4, to_term_id: 5, rel_type: "SYNONYM_OF" },
  ],
};

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  auth.capabilities = ["glossary_read"];
  fetchMock = vi.fn(async () => new Response(JSON.stringify(DATA), { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  Element.prototype.scrollIntoView = vi.fn();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

async function page() {
  render(<GlossaryPage />);
  return screen.findByTestId("glossary-page");
}

function entryNames(): string[] {
  return screen
    .getAllByTestId(/^glossary-entry-/)
    .map((e) => e.getAttribute("data-testid")!.replace("glossary-entry-", ""));
}

describe("GlossaryPage", () => {
  it("reads the approved-terms endpoint, and shows only what it sends", async () => {
    await page();
    expect(fetchMock).toHaveBeenCalledWith("/admin/glossary/entries");
    expect(entryNames()).toEqual(["account", "client", "patron", "savings account", "zone"]);
    // No grounding, provenance, tags or status on the page.
    const text = screen.getByTestId("glossary-page").textContent!;
    for (const word of ["Draft", "Approved", "Grounded", "Tags", "Source"])
      expect(text).not.toContain(word);
  });

  it("makes a name defined differently per domain one entry with numbered senses by domain", async () => {
    await page();
    const entry = screen.getByTestId("glossary-entry-account");
    expect(within(entry).getByTestId("glossary-sense-number-1")).toHaveTextContent("1.");
    expect(within(entry).getByTestId("glossary-sense-number-2")).toHaveTextContent("2.");
    expect(within(entry).getByTestId("glossary-sense-domain-1")).toHaveTextContent("finance");
    expect(within(entry).getByTestId("glossary-sense-domain-2")).toHaveTextContent("sales");
    expect(within(entry).getByTestId("glossary-definition-1")).toHaveTextContent(
      "A record of money",
    );
    expect(within(entry).getByTestId("glossary-definition-2")).toHaveTextContent(
      "A company we sell to.",
    );
    // A single sense is not numbered.
    expect(screen.queryByTestId("glossary-sense-number-4")).toBeNull();
  });

  it("names related terms in plain words, as links within the glossary", async () => {
    await page();
    expect(screen.getByTestId("glossary-kindof-3")).toHaveTextContent("A kind of: account");
    expect(screen.getByTestId("glossary-kinds-1")).toHaveTextContent("Kinds: savings account");
    expect(screen.getByTestId("glossary-seealso-2")).toHaveTextContent("See also: client");
    expect(screen.getByTestId("glossary-also-4")).toHaveTextContent("Also called: patron");
    expect(within(screen.getByTestId("glossary-also-4")).getByRole("link")).toHaveAttribute(
      "href",
      "#glossary-term-5",
    );
  });

  it("jumps to a letter from the letter bar; a letter with no term is disabled", async () => {
    await page();
    expect(screen.getByTestId("glossary-letter-B")).toBeDisabled();
    fireEvent.click(screen.getByTestId("glossary-letter-S"));
    expect(Element.prototype.scrollIntoView).toHaveBeenCalledTimes(1);
    const scrolled = vi.mocked(Element.prototype.scrollIntoView).mock.contexts[0] as Element;
    expect(scrolled.id).toBe("glossary-letter-S");
  });

  it("searches names, other names and definitions", async () => {
    await page();
    fireEvent.change(screen.getByTestId("glossary-search"), { target: { value: "interest" } });
    expect(entryNames()).toEqual(["savings account"]);
    fireEvent.change(screen.getByTestId("glossary-search"), { target: { value: "patron" } });
    // client is also called patron; patron is its own entry.
    expect(entryNames()).toEqual(["client", "patron"]);
    fireEvent.change(screen.getByTestId("glossary-search"), {
      target: { value: "nothing like it" },
    });
    expect(screen.getByTestId("glossary-empty")).toHaveTextContent("No terms match.");
  });

  it("filters by domain, keeping only the senses in it and the whole-org terms", async () => {
    await page();
    fireEvent.click(screen.getByTestId("glossary-domain-filter"));
    fireEvent.click(await screen.findByRole("option", { name: "finance", hidden: true }));
    await waitFor(() => expect(entryNames()).toEqual(["account", "savings account", "zone"]));
    const entry = screen.getByTestId("glossary-entry-account");
    expect(within(entry).queryByTestId("glossary-sense-2")).toBeNull();
    // One sense left: no number.
    expect(within(entry).queryByTestId("glossary-sense-number-1")).toBeNull();
  });

  it("sorts by recent change, newest first, without the letter bar", async () => {
    await page();
    fireEvent.click(screen.getByRole("radio", { name: "Recently changed", hidden: true }));
    await waitFor(() =>
      expect(entryNames()).toEqual(["account", "zone", "savings account", "client", "patron"]),
    );
    expect(screen.queryByTestId("glossary-letters")).toBeNull();
    fireEvent.click(screen.getByRole("radio", { name: "A–Z", hidden: true }));
    await waitFor(() =>
      expect(entryNames()).toEqual(["account", "client", "patron", "savings account", "zone"]),
    );
  });

  it("links the term's name to the Ontology editor only for a viewer with its right", async () => {
    await page();
    expect(screen.getByTestId("glossary-name-4")).toHaveAttribute(
      "href",
      "/context/ontology?term=4",
    );
    expect(screen.getByTestId("glossary-name-1")).toHaveAttribute(
      "href",
      "/context/ontology?term=1",
    );
  });

  it("shows the name as plain text to a viewer without the editor's right", async () => {
    auth.capabilities = [];
    await page();
    expect(screen.getByTestId("glossary-name-4")).not.toHaveAttribute("href");
    expect(screen.getByTestId("glossary-name-4")).toHaveTextContent("client");
  });

  it("clears the filters to reach a related term they hide", async () => {
    await page();
    fireEvent.change(screen.getByTestId("glossary-search"), { target: { value: "interest" } });
    fireEvent.click(within(screen.getByTestId("glossary-kindof-3")).getByRole("link"));
    await waitFor(() => expect(entryNames()).toContain("account"));
    expect(screen.getByTestId("glossary-search")).toHaveValue("");
    await waitFor(() => expect(Element.prototype.scrollIntoView).toHaveBeenCalled());
  });
});
