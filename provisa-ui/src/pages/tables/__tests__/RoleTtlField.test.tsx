// Copyright (c) 2026 Kenneth Stott
// Canary: 2f6b8d13-9e4a-4c75-b0d2-5a1c7e9f3b64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1907: the table edit form's Role TTL list — one row per role, no duplicate roles, a whole
// number of seconds >= 0, and each row's effective TTL max(cache_ttl, role_ttl) shown beside it.

import { useState } from "react";
import { describe, it, expect, vi } from "vitest";
import { render, screen, within, fireEvent, waitFor } from "../../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { RoleTtlField } from "../RoleTtlField";
import { roleTtlRowError, roleTtlValid } from "../roleTtl";
import type { RoleTtl } from "../../../types/admin";
import type { Role } from "../../../types/auth";

const ROLES: Role[] = [
  { id: "trader", capabilities: [] } as unknown as Role,
  { id: "analyst", capabilities: [] } as unknown as Role,
];

function Harness({
  initial,
  floorTtl,
  onRows,
}: {
  initial: RoleTtl[];
  floorTtl: number | null;
  onRows?: (rows: RoleTtl[]) => void;
}) {
  const [rows, setRows] = useState<RoleTtl[]>(initial);
  return (
    <RoleTtlField
      rows={rows}
      onChange={(next) => {
        setRows(next);
        onRows?.(next);
      }}
      roles={ROLES}
      floorTtl={floorTtl}
    />
  );
}

describe("RoleTtlField (REQ-1907)", () => {
  it("renders each row with its effective TTL and the unlisted-roles note", () => {
    render(
      <Harness
        initial={[
          { role: "trader", ttl: 0 },
          { role: "analyst", ttl: 360 },
        ]}
        floorTtl={60}
      />,
    );
    const effective = screen.getAllByTestId("role-ttl-effective");
    expect(effective[0]).toHaveTextContent("Effective TTL: 60s");
    expect(effective[1]).toHaveTextContent("Effective TTL: 360s");
    expect(screen.getByTestId("role-ttl-unlisted")).toHaveTextContent(
      "Roles not listed use the Cache TTL (60s).",
    );
  });

  it("flags an entry below the Cache TTL as having no effect", () => {
    render(<Harness initial={[{ role: "trader", ttl: 0 }]} floorTtl={60} />);
    expect(screen.getByTestId("role-ttl-no-effect")).toHaveTextContent(
      "below the Cache TTL of 60s",
    );
  });

  it("does not flag an entry at or above the Cache TTL", () => {
    render(<Harness initial={[{ role: "analyst", ttl: 360 }]} floorTtl={60} />);
    expect(screen.queryByTestId("role-ttl-no-effect")).toBeNull();
  });

  it("adds a row for the first unlisted role, starting at the Cache TTL", async () => {
    const onRows = vi.fn();
    render(<Harness initial={[{ role: "trader", ttl: 0 }]} floorTtl={60} onRows={onRows} />);
    await userEvent.click(screen.getByRole("button", { name: "Add role TTL" }));
    expect(onRows).toHaveBeenLastCalledWith([
      { role: "trader", ttl: 0 },
      { role: "analyst", ttl: 60 },
    ]);
    expect(screen.getAllByTestId("role-ttl-row")).toHaveLength(2);
  });

  it("removes a row", async () => {
    const onRows = vi.fn();
    render(
      <Harness
        initial={[
          { role: "trader", ttl: 0 },
          { role: "analyst", ttl: 360 },
        ]}
        floorTtl={60}
        onRows={onRows}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "Remove role TTL for trader" }));
    expect(onRows).toHaveBeenLastCalledWith([{ role: "analyst", ttl: 360 }]);
    expect(screen.getAllByTestId("role-ttl-row")).toHaveLength(1);
  });

  it("disables Add once every role is listed", () => {
    render(
      <Harness
        initial={[
          { role: "trader", ttl: 0 },
          { role: "analyst", ttl: 360 },
        ]}
        floorTtl={60}
      />,
    );
    expect(screen.getByRole("button", { name: "Add role TTL" })).toBeDisabled();
  });

  it("never offers a role that another row already lists", async () => {
    render(
      <Harness
        initial={[
          { role: "trader", ttl: 0 },
          { role: "analyst", ttl: 360 },
        ]}
        floorTtl={60}
      />,
    );
    const firstRow = screen.getAllByTestId("role-ttl-row")[0];
    const input = within(firstRow).getByRole("textbox", { name: "Role" });
    fireEvent.click(input);
    await waitFor(() => {
      if (!input.getAttribute("aria-controls")) throw new Error("dropdown not open");
    });
    const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
    const options = within(listbox as HTMLElement)
      .getAllByRole("option", { hidden: true })
      .map((o) => o.textContent);
    expect(options).toEqual(["trader"]);
  });

  it("rejects an empty TTL and a duplicate role", () => {
    const empty = [{ role: "trader", ttl: Number.NaN }];
    expect(roleTtlRowError(empty, 0)).toBe("tableEditForm.roleTtlInvalidTtl");
    expect(roleTtlRowError([{ role: "trader", ttl: -1 }], 0)).toBe(
      "tableEditForm.roleTtlInvalidTtl",
    );
    expect(roleTtlRowError([{ role: "trader", ttl: 1.5 }], 0)).toBe(
      "tableEditForm.roleTtlInvalidTtl",
    );
    const dup = [
      { role: "trader", ttl: 0 },
      { role: "trader", ttl: 30 },
    ];
    expect(roleTtlRowError(dup, 1)).toBe("tableEditForm.roleTtlDuplicateRole");
    expect(roleTtlValid(dup)).toBe(false);
    expect(roleTtlValid([{ role: "trader", ttl: 0 }])).toBe(true);
  });

  it("shows the invalid-TTL error when the seconds field is cleared", async () => {
    render(<Harness initial={[{ role: "trader", ttl: 30 }]} floorTtl={60} />);
    await userEvent.clear(screen.getByRole("textbox", { name: "TTL (seconds)" }));
    expect(
      await screen.findByText("Enter a whole number of seconds, 0 or more."),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("role-ttl-effective")).toBeNull();
  });

  it("with no table/source Cache TTL, a role's effective TTL is its own entry", () => {
    render(<Harness initial={[{ role: "trader", ttl: 30 }]} floorTtl={null} />);
    expect(screen.getByTestId("role-ttl-effective")).toHaveTextContent("Effective TTL: 30s");
    expect(screen.queryByTestId("role-ttl-no-effect")).toBeNull();
    expect(screen.queryByTestId("role-ttl-unlisted")).toBeNull();
  });
});
