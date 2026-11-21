// Copyright (c) 2026 Kenneth Stott
// Canary: 8d2b6e93-f41a-4c57-b8e0-2a7c5d9f1b36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: the table of a class on a Wikidata source. A class is found by name (only classes
// with instances are asked for) or browsed from the most-used top-level classes when the search
// is empty; its count is shown, its broader and narrower classes lead on, its columns are picked
// from the properties its instances use, a filter is asked for when it is too large, and "Use
// this class" closes the picker under its columns.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor, within } from "../../../test-utils/render";
import type { WikidataClass } from "../../../api/wikidata";

const api = vi.hoisted(() => ({
  fetchWikidataBrowse: vi.fn(),
  searchWikidata: vi.fn(),
  fetchWikidataClass: vi.fn(),
  buildWikidataClassTable: vi.fn(),
}));
vi.mock("../../../api/wikidata", () => api);

const { WikidataClassPicker } = await import("../WikidataClassPicker");
const { tableNameOf } = await import("../wikidataChoice");

const BROWSE = [
  { id: "Q5", label: "human", instance_count: 12000000 },
  { id: "Q515", label: "city", instance_count: 10542 },
];

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
  api.fetchWikidataBrowse.mockResolvedValue(BROWSE);
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

describe("WikidataClassPicker", () => {
  it("browses the most-used top-level classes with their member counts when the search is empty", async () => {
    api.fetchWikidataClass.mockResolvedValue(CITY);
    const onChange = vi.fn();
    renderInForm(<WikidataClassPicker language="en" onUse={vi.fn()} onChange={onChange} />);
    const browse = await screen.findByTestId("wikidata-class-browse");
    expect(api.fetchWikidataBrowse).toHaveBeenCalledWith("en");
    expect(within(browse).getByText("12,000,000")).toBeInTheDocument();
    fireEvent.click(within(browse).getByTestId("wikidata-browse-Q515"));
    expect(await screen.findByTestId("wikidata-class-count")).toHaveTextContent(
      "Instances: 10,542",
    );
    expect(api.fetchWikidataClass).toHaveBeenCalledWith("Q515", "en");
    expect(onChange).toHaveBeenCalled();
    expect(screen.queryByTestId("wikidata-class-browse")).toBeNull();
    // The class browsed to is the class field's value, and is not searched for by name.
    await waitFor(() =>
      expect(screen.getByTestId("wikidata-class-search")).toHaveValue("city (Q515)"),
    );
    expect(api.searchWikidata).not.toHaveBeenCalled();
  });

  it("searches only for classes and builds the table of the one found", async () => {
    api.searchWikidata.mockResolvedValue([{ id: "Q515", label: "city", description: null }]);
    api.fetchWikidataClass.mockResolvedValue(CITY);
    const onUse = vi.fn();
    renderInForm(<WikidataClassPicker language="en" onUse={onUse} onChange={vi.fn()} />);
    await searchAndPick("wikidata-class-search", "city", "city (Q515)");
    expect(api.searchWikidata).toHaveBeenCalledWith("city", "en", "class");
    await screen.findByTestId("wikidata-class-count");
    expect(screen.queryByTestId("wikidata-filter-property")).toBeNull();
    fireEvent.click(screen.getByTestId("wikidata-class-use"));
    await waitFor(() => expect(onUse).toHaveBeenCalled());
    expect(api.buildWikidataClassTable).toHaveBeenCalledWith({
      class_id: "Q515",
      properties: [
        { id: "P17", label: "country", data_type: "text", item: true },
        { id: "P1082", label: "population", data_type: "double", item: false },
      ],
      filter: null,
      language: "en",
    });
    expect(onUse.mock.calls[0][0].tableName).toBe("city");
  });

  it("closes with Use this class on its own row under the columns", async () => {
    api.fetchWikidataClass.mockResolvedValue(CITY);
    renderInForm(<WikidataClassPicker language="en" onUse={vi.fn()} onChange={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("wikidata-browse-Q515"));
    const use = await screen.findByTestId("wikidata-class-use");
    const columns = screen.getByTestId("wikidata-class-columns");
    // After the columns in reading order, and in a full-width field of its own.
    expect(columns.compareDocumentPosition(use) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(use.closest(".form-field")).toHaveClass("form-field--full");
    expect(use.closest(".form-field")?.contains(columns)).toBe(false);
  });

  it("browses to a narrower class, which is a change of class", async () => {
    api.searchWikidata.mockResolvedValue([{ id: "Q515", label: "city", description: null }]);
    api.fetchWikidataClass.mockResolvedValue(CITY);
    const onChange = vi.fn();
    renderInForm(<WikidataClassPicker language="en" onUse={vi.fn()} onChange={onChange} />);
    await searchAndPick("wikidata-class-search", "city", "city (Q515)");
    await screen.findByTestId("wikidata-class-count");
    expect(onChange).toHaveBeenCalledTimes(1);
    await pick("wikidata-class-narrower", "million city (Q1637706)");
    await waitFor(() => expect(api.fetchWikidataClass).toHaveBeenLastCalledWith("Q1637706", "en"));
    expect(onChange).toHaveBeenCalledTimes(2);
  });

  it("requires a filter when the class is over the limit", async () => {
    api.searchWikidata
      .mockResolvedValueOnce([{ id: "Q5", label: "human", description: null }])
      .mockResolvedValue([{ id: "Q82955", label: "politician", description: null }]);
    api.fetchWikidataClass.mockResolvedValue(HUMAN);
    const onUse = vi.fn();
    renderInForm(<WikidataClassPicker language="en" onUse={onUse} onChange={vi.fn()} />);
    await searchAndPick("wikidata-class-search", "human", "human (Q5)");
    await screen.findByTestId("wikidata-filter-property");
    expect(screen.getByTestId("wikidata-class-use")).toBeDisabled();
    await pick("wikidata-filter-property", "occupation (P106)");
    expect(screen.getByTestId("wikidata-class-use")).toBeDisabled();
    await searchAndPick("wikidata-filter-value", "politician", "politician (Q82955)");
    expect(api.searchWikidata).toHaveBeenLastCalledWith("politician", "en", "item");
    expect(screen.getByTestId("wikidata-class-use")).toBeEnabled();
    fireEvent.click(screen.getByTestId("wikidata-class-use"));
    await waitFor(() => expect(onUse).toHaveBeenCalled());
    expect(api.buildWikidataClassTable.mock.calls[0][0].filter).toEqual({
      property: "P106",
      value: "Q82955",
    });
  });

  it("shows why a class table was refused", async () => {
    api.fetchWikidataClass.mockResolvedValue(CITY);
    api.buildWikidataClassTable.mockRejectedValue(new Error("choose a filter"));
    renderInForm(<WikidataClassPicker language="en" onUse={vi.fn()} onChange={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("wikidata-browse-Q515"));
    fireEvent.click(await screen.findByTestId("wikidata-class-use"));
    expect(await screen.findByTestId("wikidata-table-error")).toHaveTextContent("choose a filter");
  });

  it("shows a loader in the class select while the search is in flight, and the error if it fails", async () => {
    let finish: (v: unknown) => void = () => {};
    api.searchWikidata.mockReturnValueOnce(new Promise((r) => (finish = r)));
    renderInForm(<WikidataClassPicker language="en" onUse={vi.fn()} onChange={vi.fn()} />);
    fireEvent.change(screen.getByTestId("wikidata-class-search"), { target: { value: "city" } });
    expect(await screen.findByTestId("wikidata-class-searching")).toBeInTheDocument();
    finish([{ id: "Q515", label: "city", description: null }]);
    await waitFor(() => expect(screen.queryByTestId("wikidata-class-searching")).toBeNull());
    api.searchWikidata.mockRejectedValueOnce(new Error("search failed"));
    fireEvent.change(screen.getByTestId("wikidata-class-search"), { target: { value: "town" } });
    expect(await screen.findByTestId("wikidata-table-error")).toHaveTextContent("search failed");
    expect(screen.queryByTestId("wikidata-class-searching")).toBeNull();
  });

  it("shows the class's query being built after a pick, until the answer arrives", async () => {
    let finish: (v: unknown) => void = () => {};
    api.fetchWikidataClass.mockReturnValueOnce(new Promise((r) => (finish = r)));
    renderInForm(<WikidataClassPicker language="en" onUse={vi.fn()} onChange={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("wikidata-browse-Q515"));
    expect(await screen.findByTestId("wikidata-class-loading")).toHaveTextContent(
      "Building the class's query…",
    );
    finish(CITY);
    await screen.findByTestId("wikidata-class-count");
    expect(screen.queryByTestId("wikidata-class-loading")).toBeNull();
  });

  it("shows the error and stops loading when the class cannot be fetched", async () => {
    api.fetchWikidataClass.mockRejectedValueOnce(new Error("class failed"));
    renderInForm(<WikidataClassPicker language="en" onUse={vi.fn()} onChange={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("wikidata-browse-Q515"));
    expect(await screen.findByTestId("wikidata-table-error")).toHaveTextContent("class failed");
    expect(screen.queryByTestId("wikidata-class-loading")).toBeNull();
  });

  it("names a table after its class", () => {
    expect(tableNameOf("Human settlement!")).toBe("human_settlement");
    expect(tableNameOf("名前")).toBe("");
  });
});
