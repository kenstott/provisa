// Copyright (c) 2026 Kenneth Stott
// Canary: 41724a80-6161-408d-89cf-4da0d737d012
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Roles list's derived roles follow the page's one search: a role is listed when every word
// of the search (or either word either side of or) is in its attribute or one of its value roles,
// and no word with a leading - is.

import { describe, expect, it, vi } from "vitest";
import { Table } from "@mantine/core";
import { render, screen } from "../../../test-utils/render";
import { DerivedRoleRows } from "../DerivedRoles";
import type { DeclaredDerivedRole } from "../../../api/admin";

const role = (attribute: string, values: string[]): DeclaredDerivedRole => ({
  attribute,
  description: "",
  values,
  uses: [],
});
const ROLES = [role("region", ["emea", "apac"]), role("tier", ["gold"]), role("team", ["ops"])];

function rows(search: string) {
  render(
    <Table>
      <Table.Tbody>
        <DerivedRoleRows roles={ROLES} search={search} picked={null} onPick={vi.fn()} />
      </Table.Tbody>
    </Table>,
  );
  return screen
    .queryAllByTestId(/^derived-role-row-/)
    .map((r) => r.getAttribute("data-testid")?.replace("derived-role-row-", ""));
}

describe("the derived roles in the Roles list's search", () => {
  it("lists the roles matching every word of the search, or either side of or", () => {
    expect(rows("apac or gold")).toEqual(["region", "tier"]);
  });

  it("lists none when no role holds every word", () => {
    expect(rows("apac gold")).toEqual([]);
  });

  it("lists the role holding every word", () => {
    expect(rows("region apac")).toEqual(["region"]);
  });

  it("leaves out the roles holding a word with a leading -", () => {
    expect(rows("-apac")).toEqual(["tier", "team"]);
  });
});
