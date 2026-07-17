// Copyright (c) 2026 Kenneth Stott
// Canary: 0eb82c55-2b29-4b2c-b375-608af501cf76
// REQ-1093: UniquesPanel add/edit/remove behavior.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import "../../../i18n";
import { UniquesPanel } from "../UniquesPanel";
import type { UniqueConstraint } from "../../../types/admin";

function renderPanel(uniques: UniqueConstraint[], onChange = vi.fn()) {
  render(
    <MantineProvider>
      <UniquesPanel uniques={uniques} columns={["tenant_id", "email", "sku"]} onChange={onChange} />
    </MantineProvider>,
  );
  return onChange;
}

describe("UniquesPanel", () => {
  it("adds an empty constraint row on + Constraint", () => {
    const onChange = renderPanel([]);
    fireEvent.click(screen.getByTestId("unique-add-button"));
    expect(onChange).toHaveBeenCalledWith([{ name: "", columns: [] }]);
  });

  it("renders seeded constraints with name and columns", () => {
    renderPanel([{ name: "users_tenant_email_key", columns: ["tenant_id", "email"] }]);
    expect(screen.getByTestId("unique-name-0")).toHaveValue("users_tenant_email_key");
    expect(screen.getByTestId("unique-row-0")).toBeInTheDocument();
  });

  it("removes a constraint row", () => {
    const onChange = renderPanel([{ name: "sku_key", columns: ["sku"] }]);
    fireEvent.click(screen.getByTestId("unique-remove-0"));
    expect(onChange).toHaveBeenCalledWith([]);
  });

  it("edits a constraint name", () => {
    const onChange = renderPanel([{ name: "old", columns: ["sku"] }]);
    fireEvent.change(screen.getByTestId("unique-name-0"), { target: { value: "new_name" } });
    expect(onChange).toHaveBeenCalledWith([{ name: "new_name", columns: ["sku"] }]);
  });
});
