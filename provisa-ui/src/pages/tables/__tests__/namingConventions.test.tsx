// Copyright (c) 2026 Kenneth Stott
// Canary: 701271f1-b1e6-4cdf-ac9e-f09103bc71bb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-416: the server is the authority on naming conventions. A table's form offers exactly the
// conventions the server lists, read from it, and keeps no list of its own.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, renderInForm, screen, within } from "../../../test-utils/render";
import { NamingConventions } from "../../../hooks/admin.graphql";
import { TableIdentitySection } from "../TableIdentitySection";
import { draftFromRegistration } from "../draftTable";
import i18n from "../../../i18n";

const t = i18n.getFixedT("en");

const listed = (conventions: string[]) => ({
  request: { query: NamingConventions },
  result: { data: { namingConventions: conventions } },
});

const table = draftFromRegistration({
  input: { sourceId: "pg", domainId: "sales", schemaName: "public", tableName: "orders" },
  pagination: null,
  pagingKind: null,
  pagingCeilingRows: null,
  rowRules: [],
});

async function offered(conventions: string[]): Promise<string[]> {
  renderInForm(
    <TableIdentitySection editingTable={table} setEditingTable={vi.fn()} isView={false} />,
    { mocks: [listed(conventions)] },
  );
  const select = screen.getByRole("textbox", { name: t("tableEditForm.namingConventionLabel") });
  fireEvent.click(select);
  const list = document.getElementById(select.getAttribute("aria-controls") ?? "");
  if (list === null) throw new Error("the naming convention field opened no option list");
  // The server's list arrives after the first render.
  await within(list).findByRole("option", {
    name: t("adminPage.namingConventionSnake"),
    hidden: true,
  });
  return within(list)
    .getAllByRole("option", { hidden: true })
    .map((o) => o.textContent ?? "");
}

describe("the naming conventions a table may name (REQ-416)", () => {
  it("are the server's, with Inherit, and no others", async () => {
    expect(await offered(["snake", "hasura_graphql", "apollo_graphql"])).toEqual([
      t("tablesPage.inheritSource"),
      t("adminPage.namingConventionSnake"),
      t("adminPage.namingConventionHasura"),
      t("adminPage.namingConventionApollo"),
    ]);
  });

  it("follow what the server lists, not a list the UI keeps", async () => {
    expect(await offered(["snake"])).toEqual([
      t("tablesPage.inheritSource"),
      t("adminPage.namingConventionSnake"),
    ]);
  });
});
