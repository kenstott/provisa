// Copyright (c) 2026 Kenneth Stott
// Canary: 7b3e1f58-4c92-4d06-a8e5-1f9c3b6d2a47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2278: a tag's or a classification kind's column rules are listed in its panel, added with a
// role, an effect and (for a mask) the mask, and removed; the Context pages list them read-only.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";
import type { ColumnRule } from "../../../api/columnRules";
import { ColumnRules } from "../ColumnRules";

const api = vi.hoisted(() => ({
  fetchColumnRules: vi.fn(),
  saveColumnRule: vi.fn(),
  deleteColumnRule: vi.fn(),
}));
vi.mock("../../../api/columnRules", () => api);
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: [{ id: "analyst" }, { id: "org_admin" }], loading: false }),
}));

const HIDE: ColumnRule = {
  id: 1,
  tagId: "pii",
  classification: null,
  roleId: "analyst",
  action: "hide",
  mask: null,
};
const MASK: ColumnRule = {
  id: 2,
  tagId: "pii",
  classification: null,
  roleId: "org_admin",
  action: "mask",
  mask: null,
};

beforeEach(() => {
  api.fetchColumnRules.mockReset().mockResolvedValue({ rules: [HIDE, MASK] });
  api.saveColumnRule.mockReset().mockResolvedValue({ rule: HIDE });
  api.deleteColumnRule.mockReset().mockResolvedValue({ deleted: 1 });
});

async function pick(testId: string, option: string) {
  const user = userEvent.setup();
  await user.click(screen.getByTestId(testId));
  await user.click(await screen.findByRole("option", { name: option, hidden: true }));
}

describe("ColumnRules", () => {
  it("lists a tag's rules with their effect", async () => {
    render(<ColumnRules tagId="pii" testId="rules" />);
    const list = await screen.findByTestId("rules-list");
    expect(api.fetchColumnRules).toHaveBeenCalledWith({ tagId: "pii" });
    expect(within(screen.getByTestId("rules-rule-1")).getByText("Hidden")).toBeTruthy();
    expect(within(screen.getByTestId("rules-rule-2")).getByText("Masked completely")).toBeTruthy();
    expect(list).toBeTruthy();
  });

  it("asks for a classification kind's rules by the kind", async () => {
    api.fetchColumnRules.mockResolvedValue({ rules: [] });
    render(<ColumnRules classification="email" testId="rules" />);
    expect(await screen.findByTestId("rules-empty")).toBeTruthy();
    expect(api.fetchColumnRules).toHaveBeenCalledWith({ classification: "email" });
  });

  it("adds a hide rule for a role", async () => {
    render(<ColumnRules tagId="pii" testId="rules" />);
    await screen.findByTestId("rules-list");
    await pick("rules-role", "analyst");
    fireEvent.click(screen.getByTestId("rules-add"));
    await waitFor(() =>
      expect(api.saveColumnRule).toHaveBeenCalledWith({ tagId: "pii" }, "analyst", "hide", null),
    );
  });

  it("adds a constant mask rule", async () => {
    render(<ColumnRules tagId="pii" testId="rules" />);
    await screen.findByTestId("rules-list");
    await pick("rules-role", "analyst");
    await pick("rules-action", "Mask");
    await pick("rules-mask", "Constant");
    fireEvent.change(screen.getByTestId("rules-constant"), { target: { value: "REDACTED" } });
    fireEvent.click(screen.getByTestId("rules-add"));
    await waitFor(() =>
      expect(api.saveColumnRule).toHaveBeenCalledWith({ tagId: "pii" }, "analyst", "mask", {
        type: "constant",
        value: "REDACTED",
      }),
    );
  });

  it("removes a rule", async () => {
    render(<ColumnRules tagId="pii" testId="rules" />);
    await screen.findByTestId("rules-list");
    fireEvent.click(screen.getByTestId("rules-remove-1"));
    await waitFor(() => expect(api.deleteColumnRule).toHaveBeenCalledWith(1));
  });

  it("offers no control that writes when read-only", async () => {
    render(<ColumnRules tagId="pii" readOnly testId="rules" />);
    await screen.findByTestId("rules-list");
    expect(screen.queryByTestId("rules-form")).toBeNull();
    expect(screen.queryByTestId("rules-remove-1")).toBeNull();
  });
});
