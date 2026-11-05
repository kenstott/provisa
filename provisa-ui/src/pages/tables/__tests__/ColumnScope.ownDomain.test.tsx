// Copyright (c) 2026 Kenneth Stott
// Canary: 2f7c9a14-6e30-4b85-9d1a-c84e0b5f3a62
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2153: a column's scope is domain, listed, public or own-domain, in the register form and
// the table edit form alike, each select's help saying what own-domain does.

import { useState } from "react";
import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import "../../../i18n";
import { ColumnsTable } from "../ColumnsTable";
import { TableColumnsSection } from "../TableColumnsSection";
import type { ColumnForm } from "../types";
import type { RegisteredTable, TableColumn } from "../../../types/admin";

vi.mock("../../../api/admin", async (actual) => ({
  ...(await actual<typeof import("../../../api/admin")>()),
  fetchIrTypes: () => Promise.resolve(["integer", "text"]),
}));

const SCOPES = ["domain", "listed", "public", "own-domain"];

const column = (name: string): ColumnForm => ({
  name,
  selected: true,
  dataType: "integer",
  isPrimaryKey: false,
  visibleTo: ["analyst"],
  masks: [],
  alias: "",
  description: "",
  scope: "domain",
  nativeFilterType: null,
});

const held: { scope?: string } = {};

function Register() {
  const [columns, setColumns] = useState([column("settlement_amount")]);
  return (
    <ColumnsTable
      columns={columns}
      updateCol={(i, key, value) => {
        if (key === "scope") held.scope = value as string;
        setColumns((cols) => cols.map((c, n) => (n === i ? { ...c, [key]: value } : c)));
      }}
      roles={[{ id: "analyst" }] as never}
      irTypes={["integer", "text"]}
      sourceType="postgresql"
    />
  );
}

function Edit() {
  const [columns, setColumns] = useState<TableColumn[]>([
    {
      columnName: "settlement_amount",
      visibleTo: ["analyst"],
      masks: [],
      scope: "domain",
    } as unknown as TableColumn,
  ]);
  return (
    <TableColumnsSection
      editingTable={{ id: 1, sourceId: "pg", columns } as unknown as RegisteredTable}
      updateEditCol={((i: number, key: string, value: unknown) => {
        if (key === "scope") held.scope = value as string;
        setColumns((cols) => cols.map((c, n) => (n === i ? { ...c, [key]: value } : c)));
      }) as never}
      editSource={undefined}
      roles={[{ id: "analyst" }] as never}
      hidingOnly={false}
    />
  );
}

function offered(): string[] {
  return screen.getAllByRole("option").map((o) => o.textContent ?? "");
}

describe("a column's scope select", () => {
  it.each([
    ["register form", Register],
    ["table edit form", Edit],
  ])("offers own-domain in the %s and says what it does", async (_form, Form) => {
    held.scope = undefined;
    render(<Form />);
    fireEvent.click(screen.getByTestId("column-tab-type"));
    const select = screen.getByRole("textbox", { name: "Scope" });
    expect(select.getAttribute("title")).toMatch(
      /own-domain: served only to roles whose own domain access lists this table's domain/,
    );
    expect(select.getAttribute("title")).toMatch(/only through a parent domain is not served/);
    fireEvent.click(select);
    expect(offered()).toEqual(SCOPES);
    fireEvent.click(await screen.findByRole("option", { name: "own-domain" }));
    expect(held.scope).toBe("own-domain");
  });
});
