// Copyright (c) 2026 Kenneth Stott
// Canary: 0e5b7a14-93c6-4d28-8f1a-b6c24d7e90a3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1634: the data product form is grouped into titled panels; a required field left empty says so
// on its field and on its panel, and the same fifteen controls serve create and edit.

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import { DataProductFormCard } from "../DataProductFormCard";
import { EMPTY_FORM, type DataProductForm } from "../types";

const INPUT_TESTIDS = [
  "data-product-id-input",
  "data-product-domain-input",
  "data-product-name-input",
  "data-product-owner-input",
  "data-product-team-input",
  "data-product-purpose-input",
  "data-product-limitations-input",
  "data-product-usage-input",
  "data-product-version-input",
  "data-product-status-input",
  "data-product-sla-input",
  "data-product-support-input",
  "data-product-tables-input",
  "data-product-commands-input",
];

const PANELS: Record<string, string[]> = {
  identity: [
    "data-product-id-input",
    "data-product-name-input",
    "data-product-domain-input",
    "data-product-version-input",
    "data-product-status-input",
  ],
  ownership: [
    "data-product-owner-input",
    "data-product-team-input",
    "data-product-support-input",
    "data-product-sla-input",
  ],
  description: [
    "data-product-purpose-input",
    "data-product-limitations-input",
    "data-product-usage-input",
  ],
  contents: ["data-product-tables-input", "data-product-commands-input"],
};

function Harness({
  initial,
  editingId,
  onSave,
}: {
  initial: DataProductForm;
  editingId: string | null;
  onSave: () => void;
}) {
  const [form, setForm] = useState(initial);
  const [tableIds, setTableIds] = useState<string[]>([]);
  const [fnNames, setFnNames] = useState<string[]>([]);
  return (
    <DataProductFormCard
      editingId={editingId}
      form={form}
      setForm={setForm}
      domainOptions={["sales"]}
      roleOptions={["admin"]}
      tables={[]}
      selectedTableIds={tableIds}
      setSelectedTableIds={setTableIds}
      functions={[]}
      selectedFunctionNames={fnNames}
      setSelectedFunctionNames={setFnNames}
      saving={false}
      msg=""
      onSave={onSave}
      onCancel={vi.fn()}
    />
  );
}

const EDITED: DataProductForm = { ...EMPTY_FORM, id: "p1", domainId: "sales", name: "Orders" };

describe("DataProductFormCard", () => {
  it.each([
    ["create", EMPTY_FORM, null],
    ["edit", EDITED, "p1"],
  ])("groups all fourteen inputs into four titled panels on %s", (_mode, initial, editingId) => {
    render(<Harness initial={initial} editingId={editingId} onSave={vi.fn()} />);
    expect(screen.getByTestId("data-product-form")).toBeInTheDocument();
    for (const id of INPUT_TESTIDS) expect(screen.getByTestId(id)).toBeInTheDocument();
    for (const [panel, ids] of Object.entries(PANELS)) {
      const el = screen.getByTestId(`data-product-panel-${panel}`);
      for (const id of ids) expect(within(el).getByTestId(id)).toBeInTheDocument();
    }
    for (const heading of ["Identity", "Ownership and support", "Description", "Contents"]) {
      expect(screen.getByRole("heading", { name: heading })).toBeInTheDocument();
    }
    expect(screen.getByTestId("data-product-save-button")).toBeInTheDocument();
    expect(screen.getByTestId("data-product-cancel-button")).toBeInTheDocument();
  });

  it("locks the id on edit only", () => {
    const { unmount } = render(<Harness initial={EMPTY_FORM} editingId={null} onSave={vi.fn()} />);
    expect(screen.getByTestId("data-product-id-input")).not.toBeDisabled();
    unmount();
    render(<Harness initial={EDITED} editingId="p1" onSave={vi.fn()} />);
    expect(screen.getByTestId("data-product-id-input")).toBeDisabled();
  });

  it("flags the empty required fields and their panel on Save, and does not save", () => {
    const onSave = vi.fn();
    render(<Harness initial={EMPTY_FORM} editingId={null} onSave={onSave} />);
    const identity = screen.getByTestId("data-product-panel-identity");
    expect(identity).not.toHaveAttribute("data-has-error");
    fireEvent.click(screen.getByTestId("data-product-save-button"));
    expect(onSave).not.toHaveBeenCalled();
    expect(identity).toHaveAttribute("data-has-error", "true");
    expect(within(identity).getAllByText("Required")).toHaveLength(3);
    expect(screen.getByTestId("data-product-panel-identity-error")).toBeInTheDocument();
    expect(screen.getByTestId("data-product-panel-ownership")).not.toHaveAttribute("data-has-error");
  });

  it("saves a complete form without flagging anything", () => {
    const onSave = vi.fn();
    render(<Harness initial={EDITED} editingId="p1" onSave={onSave} />);
    fireEvent.click(screen.getByTestId("data-product-save-button"));
    expect(onSave).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId("data-product-panel-identity")).not.toHaveAttribute("data-has-error");
  });

  it("keeps Save and Cancel in a footer that sticks to the bottom", () => {
    render(<Harness initial={EMPTY_FORM} editingId={null} onSave={vi.fn()} />);
    const footer = screen.getByTestId("data-product-form-footer");
    expect(footer).toHaveStyle({ position: "sticky", bottom: "0px" });
    expect(within(footer).getByTestId("data-product-save-button")).toBeInTheDocument();
  });
});
