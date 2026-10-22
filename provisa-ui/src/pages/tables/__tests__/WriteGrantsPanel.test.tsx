// Copyright (c) 2026 Kenneth Stott
// Canary: 462a95e8-afd6-406a-aafe-3259528d623a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2074: a table's Writes section lists what each role may write and, edited, saves the
// grants together.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import "../../../i18n";
import en from "../../../i18n/locales/en/writeGrants.json";
import { WriteGrantsPanel } from "../WriteGrantsPanel";
import type { WriteGrant } from "../../../types/admin";

const W = en.writeGrants;

const GRANTS: WriteGrant[] = [
  {
    role: "customer",
    operation: "insert",
    columns: ["ship_to"],
    rows: null,
    check: "customer_id = current_setting('provisa.user_id')",
    presets: [{ column: "customer_id", source: "session", name: "user_id", value: null }],
  },
  {
    role: "support",
    operation: "update",
    columns: ["status"],
    rows: null,
    check: "TRUE",
    presets: [],
  },
];

function renderPanel(props: Partial<Parameters<typeof WriteGrantsPanel>[0]> = {}) {
  return render(
    <MantineProvider>
      <WriteGrantsPanel
        grants={GRANTS}
        columns={["id", "customer_id", "status", "ship_to"]}
        roles={[
          { id: "customer", label: "customer" },
          { id: "support", label: "support" },
        ]}
        kept={false}
        {...props}
      />
    </MantineProvider>,
  );
}

describe("WriteGrantsPanel", () => {
  it("reads each grant: its role, operation, columns, rows, check and presets", () => {
    renderPanel();
    const table = screen.getByTestId("write-grants-read");
    expect(table.textContent).toContain("customer");
    expect(table.textContent).toContain(W.op_insert);
    expect(table.textContent).toContain("ship_to");
    expect(table.textContent).toContain("customer_id from user_id");
    // A grant that states no rows takes the role's row filter, and says so.
    expect(table.textContent).toContain(W.readFilter);
    expect(table.textContent).toContain("TRUE");
  });

  it("says when no role may write the table, and when the grants are not shown", () => {
    renderPanel({ grants: [] });
    expect(screen.getByTestId("write-grants-empty").textContent).toBe(W.empty);
  });

  it("does not show the grants to a caller without governance", () => {
    renderPanel({ grants: null });
    expect(screen.getByTestId("write-grants-not-shown").textContent).toBe(W.notShown);
  });

  it("saves the grants together, a removed one gone", async () => {
    const onSave = vi.fn().mockResolvedValue({ success: true, message: "" });
    renderPanel({ onSave });
    fireEvent.click(screen.getByTestId("write-grant-remove-1"));
    fireEvent.click(screen.getByTestId("write-grants-save"));
    expect(await screen.findByTestId("write-grants-saved")).toBeTruthy();
    expect(onSave).toHaveBeenCalledWith([GRANTS[0]]);
  });

  it("shows a refusal from the server", async () => {
    const onSave = vi.fn().mockResolvedValue({
      success: false,
      message: "two update grants for role 'support'",
      code: "schema.write_grant_invalid",
      params: { reason: "two update grants for role 'support'" },
    });
    renderPanel({ onSave });
    fireEvent.click(screen.getByTestId("write-grants-save"));
    const refused = await screen.findByTestId("write-grants-refused");
    expect(refused.textContent).toContain("two update grants for role 'support'");
  });
});
