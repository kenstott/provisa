// Copyright (c) 2026 Kenneth Stott
// Canary: e10edf87-a43c-406a-8147-d1d3c16581f9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1531: a table's domain is changed in its editor: the Domain field is a select of the
// domains the member may change, with the domain the table is in kept listed. The server gates
// the move on both domains.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, within } from "../../../test-utils/render";
import { TableIdentitySection } from "../TableIdentitySection";
import { draftFromRegistration } from "../draftTable";
import i18n from "../../../i18n";

const t = i18n.getFixedT("en");

const table = (domainId: string) =>
  draftFromRegistration({
    input: { sourceId: "wikidata", domainId, schemaName: "wd", tableName: "beer" },
    pagination: null,
    pagingKind: null,
    pagingCeilingRows: null,
    rowRules: [],
  });

function open(domainId: string, domainChoices: string[], setEditingTable = vi.fn()) {
  renderInForm(
    <TableIdentitySection
      editingTable={table(domainId)}
      setEditingTable={setEditingTable}
      isView={false}
      domainChoices={domainChoices}
      relationships={[]}
    />,
  );
  const field = screen.getByRole("textbox", { name: t("tableSections.domain") });
  fireEvent.click(field);
  const list = document.getElementById(field.getAttribute("aria-controls") ?? "");
  if (list === null) throw new Error("the Domain field opened no option list");
  return { field, list };
}

const names = (list: HTMLElement) =>
  within(list)
    .getAllByRole("option", { hidden: true })
    .map((o) => o.textContent ?? "");

describe("the table editor's Domain field (REQ-1531)", () => {
  it("is a select, not a read-only field", () => {
    const { field } = open("pet-store", ["pet-store", "brewery"]);
    expect(field).not.toHaveAttribute("readonly");
    expect(field).toBeRequired();
  });

  it("offers the domains the member may change, with the table's own kept listed", () => {
    const { list } = open("pet-store", ["brewery", "sales"]);
    expect(names(list)).toEqual(["pet-store", "brewery", "sales"]);
  });

  it("moves the table to the domain picked", () => {
    const setEditingTable = vi.fn();
    const { list } = open("pet-store", ["pet-store", "brewery"], setEditingTable);
    fireEvent.click(within(list).getByRole("option", { name: "brewery", hidden: true }));
    expect(setEditingTable).toHaveBeenCalledWith(expect.objectContaining({ domainId: "brewery" }));
  });

  it("asks for a domain when the table has none", () => {
    const { field, list } = open("", ["pet-store"]);
    expect(field).toHaveValue("");
    expect(field).toHaveAttribute("placeholder", t("tableEditForm.domainPlaceholder"));
    expect(names(list)).toEqual(["pet-store"]);
  });
});
