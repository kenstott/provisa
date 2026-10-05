// Copyright (c) 2026 Kenneth Stott
// Canary: 1255c862-e4a9-44d6-a9d1-bfc14dbca639
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1938: the source-type picker lists the form's grouped types, filters in place, and picks.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "../../test-utils/render";
import { SourceTypePicker, type PickerGroup } from "../SourceTypePicker";

const GROUPS: PickerGroup[] = [
  {
    group: "RDBMS",
    items: [
      { value: "postgresql", label: "PostgreSQL" },
      { value: "sqlserver", label: "SQL Server" },
      { value: "oracle", label: "Oracle (needs Trino)", disabled: true },
    ],
  },
  { group: "Streaming", items: [{ value: "kafka", label: "Kafka" }] },
];

function open(onPick = vi.fn(), onClose = vi.fn()) {
  render(
    <SourceTypePicker
      opened
      onClose={onClose}
      groups={GROUPS}
      value="postgresql"
      onPick={onPick}
    />,
  );
  return { onPick, onClose };
}

describe("SourceTypePicker", () => {
  beforeEach(() => window.localStorage.clear());

  it("lists every type under its category, with a logo or a lettered tile", async () => {
    open();
    expect(await screen.findByTestId("source-type-picker-section-RDBMS")).toBeInTheDocument();
    expect(screen.getByTestId("source-type-picker-section-Streaming")).toBeInTheDocument();
    expect(screen.getByTestId("source-logo-postgresql").tagName.toLowerCase()).toBe("svg");
    expect(screen.getByTestId("source-logo-sqlserver").textContent).toBe("SS");
  });

  it("filters by name and by alias", async () => {
    open();
    fireEvent.change(await screen.findByTestId("source-type-picker-search"), {
      target: { value: "mssql" },
    });
    expect(screen.getByTestId("source-type-option-sqlserver")).toBeInTheDocument();
    expect(screen.queryByTestId("source-type-option-postgresql")).toBeNull();
    expect(screen.queryByTestId("source-type-picker-section-Streaming")).toBeNull();
  });

  it("says so when nothing matches", async () => {
    open();
    fireEvent.change(await screen.findByTestId("source-type-picker-search"), {
      target: { value: "zzz-nothing" },
    });
    expect(screen.getByTestId("source-type-picker-empty")).toBeInTheDocument();
  });

  it("a category chip shows only that category", async () => {
    open();
    fireEvent.click(await screen.findByTestId("source-type-picker-cat-Streaming"));
    expect(screen.getByTestId("source-type-option-kafka")).toBeInTheDocument();
    expect(screen.queryByTestId("source-type-option-postgresql")).toBeNull();
  });

  it("picking a type reports it, closes, and remembers it as recent", async () => {
    const { onPick, onClose } = open();
    fireEvent.click(await screen.findByTestId("source-type-option-kafka"));
    expect(onPick).toHaveBeenCalledWith("kafka");
    expect(onClose).toHaveBeenCalled();
    expect(JSON.parse(window.localStorage.getItem("provisa.sourceTypePicker.recent")!)).toEqual([
      "kafka",
    ]);
  });

  it("a type the engine cannot reach is shown but cannot be picked", async () => {
    const { onPick } = open();
    const oracle = await screen.findByTestId("source-type-option-oracle");
    expect(oracle).toBeDisabled();
    fireEvent.click(oracle);
    expect(onPick).not.toHaveBeenCalled();
  });
});
