// Copyright (c) 2026 Kenneth Stott
// Canary: 8d2b6e93-f41a-4c57-b8e0-2a7c5d9f1b36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: a Wikidata source's table is chosen: a starter table fills the form at once; a class
// is found by name, its count shown, its broader and narrower classes browsed, its columns picked
// from the properties its instances use, and a filter asked for when it is too large.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor, within } from "../../../test-utils/render";
import type { WikidataClass } from "../../../api/wikidata";

const api = vi.hoisted(() => ({
  fetchWikidataStarters: vi.fn(),
  searchWikidata: vi.fn(),
  fetchWikidataClass: vi.fn(),
  buildWikidataClassTable: vi.fn(),
}));
vi.mock("../../../api/wikidata", () => api);

const { WikidataTableChooser } = await import("../WikidataTableChooser");
const { tableNameOf } = await import("../wikidataChoice");

const COUNTRIES = {
  name: "countries",
  query: "SELECT ?id ?name WHERE { }",
  columns: [
    { name: "id", data_type: "text" },
    { name: "population", data_type: "integer" },
  ],
};

const CITY: WikidataClass = {
  id: "Q515",
  instance_count: 10542,
  limit: 50000,
  filter_required: false,
  broader: [{ id: "Q486972", label: "human settlement" }],
  narrower: [{ id: "Q1637706", label: "million city" }],
  properties: [
    { id: "P17", label: "country", data_type: "text", item: true, uses: 990 },
    { id: "P1082", label: "population", data_type: "double", item: false, uses: 870 },
  ],
};

const HUMAN: WikidataClass = {
  ...CITY,
  id: "Q5",
  instance_count: 12000000,
  filter_required: true,
  properties: [
    { id: "P106", label: "occupation", data_type: "text", item: true, uses: 400 },
    { id: "P569", label: "date of birth", data_type: "timestamp", item: false, uses: 480 },
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
  api.fetchWikidataStarters.mockResolvedValue([COUNTRIES]);
  api.buildWikidataClassTable.mockResolvedValue({
    query: "SELECT ?id ?name WHERE { ?item wdt:P31 wd:Q515 }",
    columns: [{ name: "id", data_type: "text" }],
  });
});

async function pick(testId: string, option: string) {
  const input = screen.getByTestId(testId);
  fireEvent.click(input);
  // The options of this select: several selects may offer the same item.
  await waitFor(() => expect(input.getAttribute("aria-controls")).toBeTruthy());
  const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
  fireEvent.click(
    within(listbox as HTMLElement).getByRole("option", { name: option, hidden: true }),
  );
}

async function searchAndPick(testId: string, text: string, option: string) {
  const input = screen.getByTestId(testId);
  fireEvent.change(input, { target: { value: text } });
  // The options of this select: several selects may offer the same item.
  await waitFor(() => {
    const listbox = document.getElementById(input.getAttribute("aria-controls") ?? "");
    expect(within(listbox as HTMLElement).getByRole("option", { name: option, hidden: true }));
  });
  const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
  fireEvent.click(
    within(listbox as HTMLElement).getByRole("option", { name: option, hidden: true }),
  );
}

describe("WikidataTableChooser", () => {
  it("fills the form from a starter table", async () => {
    const onChoose = vi.fn();
    renderInForm(<WikidataTableChooser language="en" onChoose={onChoose} />);
    await waitFor(() => expect(api.fetchWikidataStarters).toHaveBeenCalled());
    await pick("wikidata-starter", "Countries (ISO codes, capital, population)");
    expect(onChoose).toHaveBeenCalledWith({
      tableName: "countries",
      query: COUNTRIES.query,
      columns: COUNTRIES.columns,
    });
  });

  it("builds the table of a class found by name, with its suggested columns", async () => {
    api.searchWikidata.mockResolvedValue([{ id: "Q515", label: "city", description: null }]);
    api.fetchWikidataClass.mockResolvedValue(CITY);
    const onChoose = vi.fn();
    renderInForm(<WikidataTableChooser language="en" onChoose={onChoose} />);
    fireEvent.click(screen.getByRole("radio", { name: "Table of a class" }));
    await searchAndPick("wikidata-class-search", "city", "city (Q515)");
    expect(api.searchWikidata).toHaveBeenCalledWith("city", "en");
    expect(await screen.findByTestId("wikidata-class-count")).toHaveTextContent("Instances: 10,542");
    expect(api.fetchWikidataClass).toHaveBeenCalledWith("Q515", "en");
    expect(screen.queryByTestId("wikidata-filter-property")).toBeNull();
    fireEvent.click(screen.getByTestId("wikidata-class-use"));
    await waitFor(() => expect(onChoose).toHaveBeenCalled());
    expect(api.buildWikidataClassTable).toHaveBeenCalledWith({
      class_id: "Q515",
      properties: [
        { id: "P17", label: "country", data_type: "text", item: true },
        { id: "P1082", label: "population", data_type: "double", item: false },
      ],
      filter: null,
      language: "en",
    });
    expect(onChoose.mock.calls[0][0].tableName).toBe("city");
  });

  it("browses to a broader or narrower class", async () => {
    api.searchWikidata.mockResolvedValue([{ id: "Q515", label: "city", description: null }]);
    api.fetchWikidataClass.mockResolvedValue(CITY);
    renderInForm(<WikidataTableChooser language="en" onChoose={vi.fn()} />);
    fireEvent.click(screen.getByRole("radio", { name: "Table of a class" }));
    await searchAndPick("wikidata-class-search", "city", "city (Q515)");
    await screen.findByTestId("wikidata-class-count");
    await pick("wikidata-class-narrower", "million city (Q1637706)");
    await waitFor(() => expect(api.fetchWikidataClass).toHaveBeenLastCalledWith("Q1637706", "en"));
  });

  it("requires a filter when the class is over the limit", async () => {
    api.searchWikidata
      .mockResolvedValueOnce([{ id: "Q5", label: "human", description: null }])
      .mockResolvedValue([{ id: "Q82955", label: "politician", description: null }]);
    api.fetchWikidataClass.mockResolvedValue(HUMAN);
    const onChoose = vi.fn();
    renderInForm(<WikidataTableChooser language="en" onChoose={onChoose} />);
    fireEvent.click(screen.getByRole("radio", { name: "Table of a class" }));
    await searchAndPick("wikidata-class-search", "human", "human (Q5)");
    await screen.findByTestId("wikidata-filter-property");
    expect(screen.getByTestId("wikidata-class-use")).toBeDisabled();
    await pick("wikidata-filter-property", "occupation (P106)");
    expect(screen.getByTestId("wikidata-class-use")).toBeDisabled();
    await searchAndPick("wikidata-filter-value", "politician", "politician (Q82955)");
    expect(screen.getByTestId("wikidata-class-use")).toBeEnabled();
    fireEvent.click(screen.getByTestId("wikidata-class-use"));
    await waitFor(() => expect(onChoose).toHaveBeenCalled());
    expect(api.buildWikidataClassTable.mock.calls[0][0].filter).toEqual({
      property: "P106",
      value: "Q82955",
    });
  });

  it("shows why a class table was refused", async () => {
    api.searchWikidata.mockResolvedValue([{ id: "Q515", label: "city", description: null }]);
    api.fetchWikidataClass.mockResolvedValue(CITY);
    api.buildWikidataClassTable.mockRejectedValue(new Error("choose a filter"));
    renderInForm(<WikidataTableChooser language="en" onChoose={vi.fn()} />);
    fireEvent.click(screen.getByRole("radio", { name: "Table of a class" }));
    await searchAndPick("wikidata-class-search", "city", "city (Q515)");
    fireEvent.click(await screen.findByTestId("wikidata-class-use"));
    expect(await screen.findByTestId("wikidata-table-error")).toHaveTextContent("choose a filter");
  });

  it("names a table after its class", () => {
    expect(tableNameOf("Human settlement!")).toBe("human_settlement");
    expect(tableNameOf("名前")).toBe("");
  });
});
