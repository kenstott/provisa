// Copyright (c) 2026 Kenneth Stott
// Canary: 2d82d19d-2221-4fb9-9977-63ef01516bd8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1620: the modeling SQL runs as the selected roles until another is picked: under "Role: All"
// the whole set, offered as All, never a role the picker did not select.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../test-utils/render";
import i18n from "../../i18n";

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({ actingRoles: "analyst,steward" }),
}));
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: [{ id: "analyst" }, { id: "org_admin" }, { id: "steward" }] }),
  useDomains: () => ({ domains: [] }),
}));

import { SqlModelingModal } from "../SqlModelingModal";

describe("SqlModelingModal run-as role", () => {
  it("starts as the selected set of roles", () => {
    render(
      <SqlModelingModal
        tables={[]}
        existingRels={[]}
        onClose={() => {}}
        onPromote={async () => {}}
      />,
    );
    expect(screen.getByTestId("sql-modeling-role")).toHaveValue(i18n.t("roleSelector.all"));
  });
});
