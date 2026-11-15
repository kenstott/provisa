// Copyright (c) 2026 Kenneth Stott
// Canary: 6b2e8f14-9c37-4d1a-a5f8-2e7c4b9d0a61
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1986: Register Table's Tables field lists the tables that need a parameter to be read
// (lookups) apart from those that list rows with none, each marked and naming what it needs; a
// table registered already is not offered (REQ-2234). REQ-2115: Polly picks in it by name.

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { useTranslation } from "react-i18next";
import { renderInForm, screen } from "../../../test-utils/render";
import { MultiSelect } from "../../../components/MultiSelect";
import { LOOKUP_GLYPH, requiredParametersOf } from "../lookups";
import { tableChoices } from "../tableChoices";
import { readScreen } from "../../../mcp/screenState";
import { runUiActions } from "../../../mcp/uiActions";

type Offered = { name: string; comment: string | null; requiredParameters?: string[] };

const READ = { state: "read" as const, columns: [], needs: null };

function Picker({ tables, registered = [] }: { tables: Offered[]; registered?: string[] }) {
  const { t } = useTranslation();
  const [picked, setPicked] = useState<string[]>([]);
  const options = tableChoices({
    t,
    tables,
    isRegistered: (tbl) => registered.includes(tbl.name),
    requiredOf: (tbl) => requiredParametersOf(tbl),
    readings: Object.fromEntries(tables.map((tbl) => [tbl.name, READ])),
    needsCacheTtl: false,
    onRegisterOne: vi.fn(),
    opening: null,
  });
  return <MultiSelect label="Tables" options={options} value={picked} onChange={setPicked} />;
}

const SHOP: Offered[] = [
  { name: "shop__companies", comment: null, requiredParameters: [] },
  { name: "shop__company", comment: null, requiredParameters: ["id"] },
  { name: "shop__orders", comment: null },
  { name: "shop__order_by_owner", comment: null, requiredParameters: ["owner", "name"] },
];

/** The open list's headings and, under each, its options. */
async function openList() {
  await userEvent.click(screen.getByRole("textbox", { name: "Tables" }));
  await screen.findAllByRole("option", { hidden: true });
  const list = document.querySelector("[role='listbox']") as HTMLElement;
  return [...list.querySelectorAll(".mantine-MultiSelect-group")].map((g) => [
    g.querySelector(".mantine-MultiSelect-groupLabel")?.textContent ?? "",
    [...g.querySelectorAll("[role='option']")].map((o) => o.textContent),
  ]);
}

describe("the Tables field lists lookups apart", () => {
  it("in two groups: the tables that list rows, then the lookups, each naming what it needs", async () => {
    renderInForm(<Picker tables={SHOP} />);
    expect(await openList()).toEqual([
      ["Tables", ["shop__companies", "shop__orders"]],
      [
        "Lookups (need a parameter)",
        [
          `${LOOKUP_GLYPH} shop__company — needs id`,
          `${LOOKUP_GLYPH} shop__order_by_owner — needs owner, name`,
        ],
      ],
    ]);
  });

  it("a source with no lookup lists its tables under no heading", async () => {
    renderInForm(<Picker tables={SHOP.filter((t) => !t.requiredParameters?.length)} />);
    expect(await openList()).toEqual([]);
    expect(screen.getByRole("option", { name: "shop__orders", hidden: true })).toBeInTheDocument();
  });

  it("a registered lookup is not offered", async () => {
    renderInForm(<Picker tables={SHOP} registered={["shop__company"]} />);
    expect(await openList()).toEqual([
      ["Tables", ["shop__companies", "shop__orders"]],
      ["Lookups (need a parameter)", [`${LOOKUP_GLYPH} shop__order_by_owner — needs owner, name`]],
    ]);
  });
});

// REQ-2115: Polly names the field as the user reads it, "Tables", and picks a table by its name.
describe("Polly and the Tables field", () => {
  const steps = { confirm: vi.fn(), paceMs: 0, waitMs: 300 };

  it("picks a table in the field named by its label", async () => {
    HTMLElement.prototype.checkVisibility = () => true;
    renderInForm(<Picker tables={SHOP} />);
    const out = await runUiActions(
      [{ do: "choose", target: { role: "combobox", name: "Tables" }, option: "shop__orders" }],
      steps,
    );
    expect(out.steps).toEqual(['Chose "shop__orders" in the combobox named "Tables".']);
    const read = await readScreen(300);
    if (typeof read === "string") throw new Error(read);
    expect(screen.getByText("shop__orders", { selector: ".mantine-Pill-label" })).toBeVisible();
  });

  it("answers back a registered table, which is not offered", async () => {
    HTMLElement.prototype.checkVisibility = () => true;
    renderInForm(<Picker tables={SHOP} registered={["shop__orders"]} />);
    const out = await runUiActions(
      [{ do: "choose", target: { role: "combobox", name: "Tables" }, option: "shop__orders" }],
      steps,
    );
    expect(out.success).toBe(false);
    expect(out.steps[0]).toContain('it offers no option "shop__orders"');
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
