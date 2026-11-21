// Copyright (c) 2026 Kenneth Stott
// Canary: 6a1d4e92-3c57-4f08-b2e6-9d8c5b7a3f14
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923 (amended 2026-11-20): on a Wikidata source the table of a class shows its table name,
// SQL alias and SPARQL query only once the class is used; choosing another class or another kind
// of custom table resets everything the last one gave.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";
import type { WikidataClass } from "../../../api/wikidata";

const api = vi.hoisted(() => ({
  fetchWikidataStarters: vi.fn(),
  fetchWikidataBrowse: vi.fn(),
  searchWikidata: vi.fn(),
  fetchWikidataClass: vi.fn(),
  buildWikidataClassTable: vi.fn(),
  fetchSparqlColumns: vi.fn(),
}));
vi.mock("../../../api/wikidata", () => api);
const preview = vi.hoisted(() => vi.fn());
vi.mock("../../../hooks/useQueryPreview", () => ({ useQueryPreview: () => ({ preview }) }));
vi.mock("../../../hooks/useTableQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useTableQueries")>()),
  useRegisterTables: () => ({ registerTables: vi.fn(), loading: false }),
}));

const { WikidataRegisterForm } = await import("../WikidataRegisterForm");

const klass = (id: string): WikidataClass => ({
  id,
  instance_count: 100,
  limit: 50000,
  filter_required: false,
  broader: [],
  narrower: [],
  properties: [{ id: "P17", label: "country", data_type: "text", item: true, uses: 90 }],
});

beforeEach(() => {
  vi.clearAllMocks();
  api.fetchWikidataStarters.mockResolvedValue([]);
  api.fetchWikidataBrowse.mockResolvedValue([
    { id: "Q515", label: "city", instance_count: 10542 },
    { id: "Q6256", label: "country", instance_count: 200 },
  ]);
  api.fetchWikidataClass.mockImplementation(async (id: string) => klass(id));
  api.buildWikidataClassTable.mockImplementation(async (body: { class_id: string }) => ({
    query: `SELECT ?id ?name WHERE { ?item wdt:P31 wd:${body.class_id} }`,
    columns: [
      { name: "id", data_type: "text" },
      { name: "name", data_type: "text" },
    ],
  }));
  preview.mockResolvedValue({ rows: [{ id: "Q1", name: "Paris" }], columns: [], error: null });
});

function renderForm(domainId = "sales") {
  render(
    <WikidataRegisterForm
      identifiers={null}
      sourceId="wd"
      domainId={domainId}
      regionInput={{}}
      roleIds={["admin"]}
      registeredNames={[]}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      language="en"
      onRegisteredMany={vi.fn()}
      onCancel={vi.fn()}
      setError={vi.fn()}
    />,
  );
}

const derived = [
  "register-table-sparql-table-name",
  "register-table-alias",
  "register-table-sparql-query",
];

describe("WikidataRegisterForm: the table of a class", () => {
  it("hides the table name, alias and query until the class is used", async () => {
    renderForm();
    fireEvent.click(
      within(screen.getByTestId("wikidata-custom-mode")).getByText("Table of a class"),
    );
    fireEvent.click(await screen.findByTestId("wikidata-browse-Q515"));
    await screen.findByTestId("wikidata-class-use");
    for (const id of derived) expect(screen.queryByTestId(id)).toBeNull();

    fireEvent.click(screen.getByTestId("wikidata-class-use"));
    expect(await screen.findByTestId("register-table-sparql-table-name")).toHaveValue("city");
    expect(screen.getByTestId("register-table-sparql-query")).toHaveValue(
      "SELECT ?id ?name WHERE { ?item wdt:P31 wd:Q515 }",
    );
    expect(screen.getByTestId("wikidata-custom-columns")).toHaveTextContent("Columns: id, name");
  });

  it("resets the name, alias, query and preview when another class is chosen", async () => {
    renderForm();
    fireEvent.click(
      within(screen.getByTestId("wikidata-custom-mode")).getByText("Table of a class"),
    );
    fireEvent.click(await screen.findByTestId("wikidata-browse-Q515"));
    fireEvent.click(await screen.findByTestId("wikidata-class-use"));
    fireEvent.change(await screen.findByTestId("register-table-alias"), {
      target: { value: "towns" },
    });
    fireEvent.click(screen.getByTestId("register-table-sparql-preview"));
    await screen.findByTestId("register-table-sparql-preview-rows");

    // Another class, from the narrower list of the class picker's search: the search box.
    api.searchWikidata.mockResolvedValue([{ id: "Q6256", label: "country", description: null }]);
    const search = screen.getByTestId("wikidata-class-search");
    fireEvent.change(search, { target: { value: "country" } });
    await waitFor(() => {
      const listbox = document.getElementById(search.getAttribute("aria-controls") ?? "");
      expect(
        within(listbox as HTMLElement).getByRole("option", {
          name: "country (Q6256)",
          hidden: true,
        }),
      );
    });
    const listbox = document.getElementById(search.getAttribute("aria-controls") as string);
    fireEvent.click(
      within(listbox as HTMLElement).getByRole("option", { name: "country (Q6256)", hidden: true }),
    );

    await waitFor(() => expect(api.fetchWikidataClass).toHaveBeenLastCalledWith("Q6256", "en"));
    for (const id of derived) expect(screen.queryByTestId(id)).toBeNull();
    expect(screen.queryByTestId("register-table-sparql-preview-rows")).toBeNull();
    fireEvent.click(await screen.findByTestId("wikidata-class-use"));
    expect(await screen.findByTestId("register-table-sparql-table-name")).toHaveValue("country");
    expect(screen.getByTestId("register-table-alias")).toHaveValue("");
  });

  it("resets what the class gave when the kind of custom table changes", async () => {
    renderForm();
    fireEvent.click(
      within(screen.getByTestId("wikidata-custom-mode")).getByText("Table of a class"),
    );
    fireEvent.click(await screen.findByTestId("wikidata-browse-Q515"));
    fireEvent.click(await screen.findByTestId("wikidata-class-use"));
    await screen.findByTestId("register-table-sparql-table-name");
    fireEvent.click(within(screen.getByTestId("wikidata-custom-mode")).getByText("SPARQL query"));
    expect(screen.getByTestId("register-table-sparql-table-name")).toHaveValue("");
    expect(screen.getByTestId("register-table-sparql-query")).toHaveValue("");
  });
});

// REQ-1531 (amended): a table is registered into a domain; Continue waits until one is chosen.
describe("WikidataRegisterForm: the domain", () => {
  it("keeps Continue disabled until a domain is chosen", () => {
    renderForm("");
    expect(screen.getByTestId("register-table-submit")).toBeDisabled();
  });

  it("enables Continue once a domain is chosen", () => {
    renderForm("sales");
    expect(screen.getByTestId("register-table-submit")).not.toBeDisabled();
  });
});
