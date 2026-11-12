// Copyright (c) 2026 Kenneth Stott
// Canary: 0c5a7f3e-8d9b-4e61-a2f4-6b1d3c9e7a52
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1494: the Test data dialog offers only what can work -- the kinds and methods whose values
// the column's family (as the server names it) can hold, Stable only for a fake that can be
// stable, with the reason as its tip -- and shows the text the column saves as "Saved as", once
// per section, folded under "Edit as text" until a choice is made.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../../test-utils/render";

const api = vi.hoisted(() => ({
  fetchFakeCatalog: vi.fn(),
  checkColumnFake: vi.fn(),
  proposeFakes: vi.fn(),
}));
vi.mock("../../../../api/fakes", () => api);

import { ColumnFakeDialog } from "../ColumnFakeDialog";
import { TestDataColumns } from "../TestDataColumns";
import { parseArgs, stableBlock } from "../declaration";
import type { FakeCatalog } from "../../../../api/fakes";
import type { RegisteredTable, TableColumn } from "../../../../types/admin";

const ALL = ["text", "integer", "numeric", "boolean", "date", "timestamp", "other"];

const CATALOG: FakeCatalog = {
  kinds: [
    {
      category: "values",
      name: "categories",
      positional: true,
      ruleOnly: false,
      args: [
        { name: "values", kind: "list", required: false },
        { name: "shares", kind: "list", required: false },
      ],
      families: ALL,
      stable: true,
      stableNeeds: ["values"],
    },
    {
      category: "distribution",
      name: "poisson",
      positional: false,
      ruleOnly: false,
      args: [{ name: "mean", kind: "number", required: true }],
      families: ["integer"],
      stable: true,
      stableNeeds: [],
    },
    {
      category: "transform",
      name: "pattern",
      positional: true,
      ruleOnly: false,
      args: [],
      families: ["text"],
      stable: false,
      stableNeeds: [],
    },
    {
      category: "transform",
      name: "truncate",
      positional: true,
      ruleOnly: false,
      args: [{ name: "unit", kind: "text", required: true }],
      families: ["date", "timestamp"],
      stable: true,
      stableNeeds: [],
    },
    {
      category: "rule",
      name: "sql_group",
      positional: true,
      ruleOnly: true,
      args: [{ name: "expression", kind: "expression", required: true }],
      families: ALL,
      stable: true,
      stableNeeds: [],
    },
  ],
  methods: [
    {
      name: "email",
      category: "internet",
      params: [],
      families: ["text"],
      stable: true,
      stableNeeds: [],
    },
    {
      name: "swift",
      category: "bank",
      params: [],
      families: ["text"],
      stable: false,
      stableNeeds: [],
    },
    {
      name: "random_digit",
      category: "pattern",
      params: [],
      families: ["integer", "numeric"],
      stable: false,
      stableNeeds: [],
    },
    {
      name: "pyint",
      category: "python",
      params: [
        { name: "min_value", required: false, default: "0" },
        { name: "max_value", required: false, default: "9999" },
      ],
      families: ["integer", "numeric"],
      stable: false,
      stableNeeds: [],
    },
    {
      name: "sentence",
      category: "lorem",
      params: [{ name: "nb_words", required: false, default: "6" }],
      families: ["text"],
      stable: true,
      stableNeeds: [],
    },
    {
      name: "date_object",
      category: "date_time",
      params: [],
      families: ["date"],
      stable: false,
      stableNeeds: [],
    },
  ],
  typeFamilies: { integer: "integer", varchar: "text", date: "date" },
};

function column(
  columnName: string,
  dataType: string,
  extra: Partial<TableColumn> = {},
): TableColumn {
  return {
    id: columnName.length,
    columnName,
    visibleTo: [],
    masks: [],
    alias: null,
    computedSqlAlias: columnName,
    computedGqlAlias: columnName,
    description: null,
    dataType,
    nativeFilterType: null,
    isPrimaryKey: false,
    isForeignKey: false,
    isAlternateKey: false,
    scope: "domain",
    isImplicitMeasure: false,
    isImplicitDimension: false,
    ...extra,
  } satisfies TableColumn;
}

const TABLE = { id: 4, tableName: "schedules", columns: [] } as unknown as RegisteredTable;

function openDialog(col: TableColumn, onSave = vi.fn()) {
  render(
    <ColumnFakeDialog
      table={TABLE}
      column={col}
      catalog={CATALOG}
      onClose={vi.fn()}
      onSave={onSave}
    />,
  );
}

// The names a section's list offers, opened as the reader opens it.
async function offered(section: "fake" | "rule"): Promise<string[]> {
  const input = screen.getByTestId(`testdata-dialog-${section}-pick`);
  fireEvent.click(input);
  const listbox = await waitFor(() => {
    const box = document.getElementById(input.getAttribute("aria-controls") ?? "");
    if (box === null) throw new Error("the list did not open");
    return box;
  });
  const names = within(listbox)
    .getAllByRole("option", { hidden: true })
    .map((o) => o.textContent ?? "");
  fireEvent.keyDown(input, { key: "Escape" });
  return names;
}

async function choose(section: "fake" | "rule", name: string) {
  const input = screen.getByTestId(`testdata-dialog-${section}-pick`);
  fireEvent.click(input);
  const listbox = await waitFor(() => {
    const box = document.getElementById(input.getAttribute("aria-controls") ?? "");
    if (box === null) throw new Error("the list did not open");
    return box;
  });
  fireEvent.click(within(listbox).getByRole("option", { name, hidden: true }));
}

beforeEach(() => {
  api.fetchFakeCatalog.mockReset().mockResolvedValue(CATALOG);
  api.checkColumnFake.mockReset().mockResolvedValue({ ok: true });
  api.proposeFakes.mockReset().mockResolvedValue({ runId: "r1", columns: {}, unmatchedPii: [] });
});

describe("the Test data dialog offers what the column can hold (REQ-1494)", () => {
  it("offers an integer column integer fakes and no text-only one", async () => {
    openDialog(column("id", "integer"));
    const fakes = await offered("fake");
    expect(fakes).toEqual(
      expect.arrayContaining(["random_digit", "pyint", "poisson", "categories"]),
    );
    for (const textOnly of ["swift", "email", "pattern", "sentence", "date_object", "truncate"])
      expect(fakes).not.toContain(textOnly);
    // A fake list never offers a synthetic-rule-only kind; the rule list does, filtered alike.
    expect(fakes).not.toContain("sql_group");
    const rules = await offered("rule");
    expect(rules).toEqual(expect.arrayContaining(["sql_group", "random_digit"]));
    expect(rules).not.toContain("swift");
  });

  it("offers a text column text fakes and no number-only one", async () => {
    openDialog(column("code", "varchar"));
    const fakes = await offered("fake");
    expect(fakes).toEqual(expect.arrayContaining(["swift", "email", "pattern", "categories"]));
    for (const numberOnly of ["random_digit", "pyint", "poisson", "date_object", "truncate"])
      expect(fakes).not.toContain(numberOnly);
  });

  it("offers a date column its own", async () => {
    openDialog(column("due", "date"));
    const fakes = await offered("fake");
    expect(fakes).toEqual(expect.arrayContaining(["date_object", "truncate", "categories"]));
    for (const other of ["email", "swift", "random_digit", "poisson", "pattern"])
      expect(fakes).not.toContain(other);
  });
});

describe("Stable only when the fake can be (REQ-1494)", () => {
  it("disables Stable for random_digit with its reason, and enables it for email", async () => {
    openDialog(column("id", "integer", { fake: "random_digit()" }));
    const stable = screen.getByTestId("testdata-dialog-stable") as HTMLInputElement;
    expect(stable).toBeDisabled();
    expect(stable.checked).toBe(false);
    fireEvent.mouseOver(screen.getByTestId("testdata-dialog-stable-tip"));
    expect(
      await screen.findByText(
        "random_digit() cannot be stable: it is not computed the same way on every engine",
      ),
    ).toBeInTheDocument();
  });

  it("enables Stable for email", () => {
    openDialog(column("mail", "varchar", { fake: "email()" }));
    expect(screen.getByTestId("testdata-dialog-stable")).not.toBeDisabled();
  });

  it("turns Stable off when a fake that cannot be stable is chosen", async () => {
    const onSave = vi.fn();
    openDialog(column("mail", "varchar", { fake: "email()", fakeStable: true }), onSave);
    const stable = () => screen.getByTestId("testdata-dialog-stable") as HTMLInputElement;
    expect(stable().checked).toBe(true);
    await choose("fake", "swift");
    await waitFor(() => expect(stable()).toBeDisabled());
    expect(stable().checked).toBe(false);
    fireEvent.click(screen.getByTestId("testdata-dialog-save"));
    await waitFor(() => expect(onSave).toHaveBeenCalledWith("swift()", "", false));
  });

  it("names what a kind needs to be stable, and that a stable method takes no arguments", () => {
    expect(stableBlock(CATALOG, "categories()")).toEqual({
      reason: "needs",
      name: "categories",
      args: ["values"],
    });
    expect(stableBlock(CATALOG, "categories((a, b))")).toBeNull();
    expect(stableBlock(CATALOG, "pattern()")).toEqual({ reason: "never", name: "pattern" });
    expect(stableBlock(CATALOG, "sentence(nb_words=3)")).toEqual({
      reason: "noArguments",
      name: "sentence",
    });
    expect(stableBlock(CATALOG, "sentence()")).toBeNull();
    expect(stableBlock(CATALOG, "")).toEqual({ reason: "noFake" });
    expect(parseArgs(CATALOG, "categories((a, b), (0.5, 0.5))")).toEqual({
      values: "(a, b)",
      shares: "(0.5, 0.5)",
    });
    expect(parseArgs(CATALOG, "pyint(min_value=1, max_value=9)")).toEqual({
      min_value: "1",
      max_value: "9",
    });
  });
});

describe("the saved text is called Saved as (REQ-1494)", () => {
  it("shows Saved as once per section, folded until a choice is made", async () => {
    openDialog(column("id", "integer"));
    for (const section of ["fake", "rule"]) {
      expect(screen.getByTestId(`testdata-dialog-${section}-edit-as-text`)).toHaveTextContent(
        "Edit as text",
      );
      expect(screen.queryByTestId(`testdata-dialog-${section}-text`)).toBeNull();
    }
    expect(screen.queryByText("Declaration, e.g. email() or categories((a, b))")).toBeNull();
    await choose("fake", "random_digit");
    const saved = (await screen.findByTestId("testdata-dialog-fake-text")) as HTMLInputElement;
    expect(saved.value).toBe("random_digit()");
    expect(screen.getAllByText("Saved as")).toHaveLength(1);
    expect(
      screen.getByText("Written from your choice above; edit it to set arguments by hand"),
    ).toBeInTheDocument();
    // The rule's stays folded until the reader opens it.
    expect(screen.queryByTestId("testdata-dialog-rule-text")).toBeNull();
    fireEvent.click(screen.getByTestId("testdata-dialog-rule-edit-as-text"));
    expect(await screen.findByTestId("testdata-dialog-rule-text")).toBeInTheDocument();
    expect(screen.getAllByText("Saved as")).toHaveLength(2);
  });

  it("opens a column's saved fake with its arguments filled", () => {
    openDialog(column("id", "integer", { fake: "pyint(min_value=1)" }));
    expect((screen.getByTestId("testdata-dialog-fake-text") as HTMLInputElement).value).toBe(
      "pyint(min_value=1)",
    );
    expect(
      (screen.getByTestId("testdata-dialog-fake-arg-min_value") as HTMLInputElement).value,
    ).toBe("1");
  });
});

describe("the columns table's Stable follows the same rule (REQ-1494)", () => {
  it("disables Stable for a fake that cannot be stable and turns it off when one is typed", async () => {
    const updateEditCol = vi.fn();
    const table = {
      ...TABLE,
      columns: [
        column("id", "integer", { fake: "random_digit()" }),
        column("mail", "varchar", { fake: "email()", fakeStable: true }),
      ],
    } as RegisteredTable;
    render(<TestDataColumns table={table} updateEditCol={updateEditCol} />);
    await waitFor(() => expect(screen.getByTestId("testdata-stable-mail")).not.toBeDisabled());
    expect(api.fetchFakeCatalog).toHaveBeenCalledWith(["integer", "varchar"]);
    expect(screen.getByTestId("testdata-stable-id")).toBeDisabled();
    fireEvent.mouseOver(screen.getByTestId("testdata-stable-tip-id"));
    expect(
      await screen.findByText(
        "random_digit() cannot be stable: it is not computed the same way on every engine",
      ),
    ).toBeInTheDocument();
    fireEvent.change(screen.getByTestId("testdata-fake-mail"), { target: { value: "swift()" } });
    expect(updateEditCol).toHaveBeenCalledWith(1, "fake", "swift()");
    expect(updateEditCol).toHaveBeenCalledWith(1, "fakeStable", false);
  });
});
