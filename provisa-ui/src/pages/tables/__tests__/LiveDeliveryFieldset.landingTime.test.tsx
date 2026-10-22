// Copyright (c) 2026 Kenneth Stott
// Canary: aa1f43d8-0220-4f05-ac70-2a324abb8fd1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-813 (amended 2026-10-21): a live Kafka table's landing-time column is chosen in the table
// form among the topic's settings, from the table's timestamp columns, and saved with the table.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import { FormLayout } from "../../../components/form/FormLayout";
import { LiveDeliveryFieldset } from "../LiveDeliveryFieldset";
import { buildTableUpdateInput } from "../helpers";
import type { RegisteredTable, Source } from "../../../types/admin";

const column = (columnName: string, dataType: string) =>
  ({ columnName, dataType, visibleTo: [] }) as unknown as RegisteredTable["columns"][number];

const table = {
  id: 1,
  sourceId: "feeds",
  domainId: "provisioning",
  schemaName: "default",
  tableName: "orders",
  changeSignal: "kafka",
  roleTtl: [],
  columns: [
    column("order_id", "varchar"),
    column("ordered_at", "timestamp"),
    column("landed_at", "timestamp"),
  ],
  live: {
    strategy: "kafka",
    pollInterval: 10,
    kafka: { topic: "orders", landingTimeColumn: null },
    outputs: [],
  },
} as unknown as RegisteredTable;

const SOURCES = [{ id: "feeds", type: "kafka" } as unknown as Source];

describe("LiveDeliveryFieldset — landing time (REQ-813)", () => {
  it("offers the table's timestamp columns and keeps the one chosen", async () => {
    const setEditingTable = vi.fn();
    render(
      <FormLayout>
        <LiveDeliveryFieldset
          part="settings"
          editingTable={table}
          setEditingTable={setEditingTable}
          editingColumnTypes={{}}
          sources={SOURCES}
          settings={null}
        />
      </FormLayout>,
    );
    const input = screen.getByTestId("live-kafka-landing-time-column");
    fireEvent.click(input);
    // Mantine keeps the combobox dropdown out of the accessibility tree in jsdom: reach it through
    // the input's aria-controls listbox.
    const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
    if (listbox === null) throw new Error("the landing-time drop-down did not open");
    const options = within(listbox)
      .getAllByRole("option", { hidden: true })
      .map((o) => o.textContent);
    expect(options).toEqual(["ordered_at", "landed_at"]);
    fireEvent.click(within(listbox).getByText("landed_at"));
    const saved = setEditingTable.mock.calls.at(-1)?.[0] as RegisteredTable;
    expect(saved.live?.kafka?.landingTimeColumn).toBe("landed_at");
  });

  it("is saved with the table", () => {
    const chosen = {
      ...table,
      live: { ...table.live!, kafka: { topic: "orders", landingTimeColumn: "landed_at" } },
    } as RegisteredTable;
    expect(buildTableUpdateInput(chosen).live?.kafka?.landingTimeColumn).toBe("landed_at");
  });
});
