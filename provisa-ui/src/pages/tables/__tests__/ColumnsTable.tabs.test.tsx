// Copyright (c) 2026 Kenneth Stott
// Canary: de147aa1-9688-4cca-be42-bc2af620ac4d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The column editor shows a column's attributes a topic at a time. The column's name is first on
// every tab; an edit made on one tab is still there after a look at another; what Save would
// refuse on a tab that is not in view is counted on that tab's label; and the name's hover lists
// the column's whole definition, so nothing has to be remembered across tabs.

import { useEffect, useState } from "react";
import { describe, it, expect } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import "../../../i18n";
import { ColumnsTable } from "../ColumnsTable";
import type { ColumnForm } from "../types";

const column = (name: string, over: Partial<ColumnForm> = {}): ColumnForm => ({
  name,
  selected: true,
  dataType: "integer",
  isPrimaryKey: false,
  visibleTo: ["analyst"],
  writableBy: [],
  masks: [],
  alias: "",
  description: "",
  scope: "domain",
  nativeFilterType: null,
  ...over,
});

// What the form holds, as its Save would send it.
const held: { columns: ColumnForm[] } = { columns: [] };

function Editor({
  initial,
  sourceType = "postgresql",
}: {
  initial: ColumnForm[];
  sourceType?: string;
}) {
  const [columns, setColumns] = useState(initial);
  useEffect(() => {
    held.columns = columns;
  }, [columns]);
  return (
    <ColumnsTable
      columns={columns}
      updateCol={(i, key, value) =>
        setColumns((cols) => cols.map((c, n) => (n === i ? { ...c, [key]: value } : c)))
      }
      roles={[{ id: "analyst" }, { id: "steward" }] as never}
      irTypes={["integer", "text", "timestamp"]}
      sourceType={sourceType}
    />
  );
}

const header = (name: string) => screen.queryByRole("columnheader", { name });
const tab = (id: string) => fireEvent.click(screen.getByTestId(`column-tab-${id}`));

describe("the column editor's tabs", () => {
  it("shows the column name first and one topic's attributes on each tab", () => {
    render(<Editor initial={[column("id"), column("customer")]} />);
    // Naming is in view first.
    expect(header("Column")).toBeInTheDocument();
    expect(header("SQL Alias")).toBeInTheDocument();
    expect(header("Description")).toBeInTheDocument();
    expect(header("Data Type")).toBeNull();
    expect(header("Visible To (Read)")).toBeNull();

    tab("type");
    expect(header("Column")).toBeInTheDocument();
    expect(header("Data Type")).toBeInTheDocument();
    expect(header("PK")).toBeInTheDocument();
    expect(header("Scope")).toBeInTheDocument();
    expect(header("SQL Alias")).toBeNull();
    expect(screen.getByTestId("register-table-col-datatype-id")).toHaveValue("integer");

    tab("access");
    expect(header("Column")).toBeInTheDocument();
    expect(header("Visible To (Read)")).toBeInTheDocument();
    expect(header("Writable By (R/W)")).toBeInTheDocument();
    expect(header("Masking")).toBeInTheDocument();
    expect(header("Data Type")).toBeNull();
    // The column's own tick is on every tab.
    expect(screen.getByTestId("register-table-col-selected-customer")).toBeChecked();
  });

  it("keeps the JSON path with the names, for a source that has one", () => {
    render(<Editor initial={[column("id", { path: "payload.id" })]} sourceType="ingest" />);
    expect(header("JSON Path")).toBeInTheDocument();
    expect(screen.getByTestId("register-table-col-path-id")).toHaveValue("payload.id");
    tab("type");
    expect(header("JSON Path")).toBeNull();
  });

  it("keeps an edit made on one tab while another is looked at", () => {
    render(<Editor initial={[column("id")]} />);
    fireEvent.change(screen.getByRole("textbox", { name: "Description" }), {
      target: { value: "the order's number" },
    });
    tab("type");
    fireEvent.click(screen.getByTestId("register-table-col-pk-id"));
    tab("naming");
    expect(screen.getByRole("textbox", { name: "Description" })).toHaveValue("the order's number");
    // Both edits are in what the form will save.
    expect(held.columns[0]).toMatchObject({
      description: "the order's number",
      isPrimaryKey: true,
    });
  });

  it("counts on a tab's label what Save would refuse there", () => {
    render(<Editor initial={[column("id"), column("payload", { dataType: "" })]} />);
    // The untyped column is on Type and keys, which is not the tab in view.
    expect(screen.getByTestId("column-tab-type-errors")).toHaveTextContent("1");
    expect(screen.queryByTestId("column-tab-naming-errors")).toBeNull();
    // A column left out of the registration needs no type.
    fireEvent.click(screen.getByTestId("register-table-col-selected-payload"));
    expect(screen.queryByTestId("column-tab-type-errors")).toBeNull();
  });

  it("lists the column's whole definition on its name, by hover and by keyboard", async () => {
    render(
      <Editor
        initial={[
          column("customer", {
            alias: "buyer",
            description: "who placed it",
            dataType: "text",
            isPrimaryKey: true,
            visibleTo: ["analyst", "steward"],
            masks: [{ to: ["*"], mask: { type: "constant", value: null } }],
          }),
        ]}
      />,
    );
    const name = screen.getByTestId("column-definition-customer");
    expect(name).toHaveAttribute("tabindex", "0");
    fireEvent.focus(name);
    const card = await screen.findByTestId("column-definition-card");
    const line = (label: string) => within(card).getByText(label).closest("div") as HTMLElement;
    expect(line("Column")).toHaveTextContent("customer");
    expect(line("SQL Alias")).toHaveTextContent("buyer");
    expect(line("Description")).toHaveTextContent("who placed it");
    // What the other two tabs edit is here as well.
    expect(line("Data Type")).toHaveTextContent("text");
    expect(line("PK")).toHaveTextContent("✓");
    expect(line("Scope")).toHaveTextContent("domain");
    expect(line("Visible To (Read)")).toHaveTextContent("analyst, steward");
    expect(line("Masking")).toHaveTextContent("1 entry");
    // An attribute with no value is left out.
    expect(within(card).queryByText("Writable By (R/W)")).toBeNull();
  });

  it("offers no Testing tab: a table not registered yet has no test data", () => {
    render(<Editor initial={[column("id")]} />);
    expect(screen.getAllByRole("tab").map((t) => t.textContent)).toEqual([
      "Naming",
      "Type and keys",
      "Access",
    ]);
  });
});
