// Copyright (c) 2026 Kenneth Stott
// Canary: 8d2a4c7e-51f3-4b96-a0e8-6c9f2b3d1e75
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1986: Register Table's table picker lists the tables that need a parameter to be read
// (lookups) apart from those that list rows with none, each marked and naming what it needs.

import { useState } from "react";
import { describe, expect, it } from "vitest";
import userEvent from "@testing-library/user-event";
import { renderInForm, screen, within } from "../../../test-utils/render";
import { LOOKUP_GLYPH, requiredParametersOf } from "../lookups";
import { SchemaTableSelects } from "../SchemaTableSelects";

type Offered = { name: string; comment: string | null; requiredParameters?: string[] };

function Picker({ tables, registered = [] }: { tables: Offered[]; registered?: string[] }) {
  const [tableName, setTableName] = useState("");
  return (
    <SchemaTableSelects
      sourceId="shop"
      schemaName="graphql"
      setSchemaName={() => {}}
      tableName={tableName}
      setTableName={setTableName}
      isRegistered={(tbl) => registered.includes(tbl.name)}
      requiredOf={(tbl) => requiredParametersOf(tables.find((o) => o.name === tbl.name) ?? {})}
      schemas={["graphql"]}
      loadingSchemas={false}
      fixedSchema
      tables={tables}
      loadingTables={false}
      allRegistered={false}
      starting={false}
      startingTimedOut={false}
    />
  );
}

const SHOP: Offered[] = [
  { name: "shop__companies", comment: null, requiredParameters: [] },
  { name: "shop__company", comment: null, requiredParameters: ["id"] },
  { name: "shop__orders", comment: null },
  { name: "shop__order_by_owner", comment: null, requiredParameters: ["owner", "name"] },
];

const groupLabels = () =>
  Array.from(screen.getByTestId("register-table-table-select").querySelectorAll("optgroup")).map(
    (g) => g.label,
  );

const valuesIn = (label: string) =>
  Array.from(
    screen
      .getByTestId("register-table-table-select")
      .querySelectorAll(`optgroup[label="${label}"] option`),
  ).map((o) => (o as HTMLOptionElement).value);

describe("the table picker lists lookups apart", () => {
  it("in two groups: the tables that list rows, then the lookups", () => {
    renderInForm(<Picker tables={SHOP} />);
    expect(groupLabels()).toEqual(["Tables", "Lookups (need a parameter)"]);
    expect(valuesIn("Tables")).toEqual(["shop__companies", "shop__orders"]);
    expect(valuesIn("Lookups (need a parameter)")).toEqual([
      "shop__company",
      "shop__order_by_owner",
    ]);
  });

  it("each lookup is marked and names the parameters it needs", () => {
    renderInForm(<Picker tables={SHOP} />);
    const company = screen.getByRole("option", { name: `${LOOKUP_GLYPH} shop__company` });
    expect(company).toHaveAttribute("title", "needs id");
    const byOwner = screen.getByRole("option", { name: `${LOOKUP_GLYPH} shop__order_by_owner` });
    expect(byOwner).toHaveAttribute("title", "needs owner, name");
    const companies = screen.getByRole("option", { name: "shop__companies" });
    expect(companies).not.toHaveAttribute("title");
  });

  it("a source with no lookup lists its tables as before, under no heading", () => {
    renderInForm(<Picker tables={SHOP.filter((t) => !t.requiredParameters?.length)} />);
    expect(groupLabels()).toEqual([]);
    expect(screen.getByRole("option", { name: "shop__orders" })).toBeInTheDocument();
  });

  it("a lookup picked is marked beside the picker, its hint naming what it needs", async () => {
    renderInForm(<Picker tables={SHOP} />);
    expect(screen.queryByTestId("register-table-lookup-mark")).not.toBeInTheDocument();
    await userEvent.selectOptions(
      screen.getByTestId("register-table-table-select"),
      "shop__company",
    );
    const mark = screen.getByTestId("register-table-lookup-mark");
    expect(mark).toHaveAccessibleName("needs id");
    await userEvent.hover(mark);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("needs id");
    await userEvent.selectOptions(
      screen.getByTestId("register-table-table-select"),
      "shop__companies",
    );
    expect(screen.queryByTestId("register-table-lookup-mark")).not.toBeInTheDocument();
  });

  it("a registered lookup is still listed apart, and cannot be picked again", () => {
    renderInForm(<Picker tables={SHOP} registered={["shop__company"]} />);
    const lookups = screen
      .getByTestId("register-table-table-select")
      .querySelector('optgroup[label="Lookups (need a parameter)"]') as HTMLElement;
    expect(
      within(lookups).getByRole("option", { name: `${LOOKUP_GLYPH} shop__company` }),
    ).toBeDisabled();
  });
});

describe("requiredParametersOf", () => {
  it("names what the source requires, then what a steward made required once registered", () => {
    const registered = {
      columns: [
        { columnName: "_nf_id", nativeFilterType: "graphql_arg", nativeFilterRequired: true },
        { columnName: "status", nativeFilterType: "column_value", requiredBySteward: true },
        { columnName: "_nf_region", nativeFilterType: "query_param", nativeFilterRequired: false },
        { columnName: "name", nativeFilterType: null },
      ],
    };
    expect(requiredParametersOf({ requiredParameters: ["id"] }, registered)).toEqual([
      "id",
      "status",
    ]);
  });

  it("names none for a table that lists rows with none", () => {
    expect(requiredParametersOf({ requiredParameters: [] })).toEqual([]);
    expect(requiredParametersOf({})).toEqual([]);
  });
});
