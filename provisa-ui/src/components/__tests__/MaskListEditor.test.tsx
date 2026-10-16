// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A column's ordered mask list (REQ-2059): a new list starts with one entry for everyone, an entry
// is added before it, entries move and go, and the server's check is shown -- entries no reader
// reaches, a list that leaves readers it does not name unmasked, the entry chosen roles read
// through.

import { useState } from "react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../test-utils/render";
import { MaskListEditor } from "../MaskListEditor";
import { maskValueOf, moved, newMaskList, withNewEntry, type MaskEntry } from "../../lib/masks";

const checkMasks = vi.fn();
vi.mock("../../api/admin", () => ({
  checkMasks: (...args: unknown[]) => checkMasks(...args),
}));

const ROLES = [
  { id: "analyst", label: "analyst" },
  { id: "contractor", label: "contractor" },
];

function Editor({ initial, seen }: { initial: MaskEntry[]; seen?: (m: MaskEntry[]) => void }) {
  const [value, setValue] = useState(initial);
  return (
    <MaskListEditor
      value={value}
      onChange={(next) => {
        seen?.(next);
        setValue(next);
      }}
      roleOptions={ROLES}
    />
  );
}

const report = (over: object = {}) => ({
  unreachable: [],
  unlisted_unmasked: false,
  applies: null,
  ...over,
});

beforeEach(() => {
  checkMasks.mockReset();
  checkMasks.mockResolvedValue(report());
});

describe("the list's helpers", () => {
  it("starts a list with one entry for everyone, masked to NULL", () => {
    expect(newMaskList()).toEqual([{ to: ["*"], mask: { type: "constant", value: null } }]);
  });

  it("adds an entry before the final entry for everyone", () => {
    const list = withNewEntry(newMaskList());
    expect(list).toEqual([{ to: [], mask: null }, newMaskList()[0]]);
    expect(withNewEntry([{ to: ["analyst"], mask: null }])).toEqual([
      { to: ["analyst"], mask: null },
      { to: [], mask: null },
    ]);
  });

  it("moves an entry within the list only", () => {
    const list: MaskEntry[] = [
      { to: ["a"], mask: null },
      { to: ["b"], mask: null },
    ];
    expect(moved(list, 0, 1).map((e) => e.to[0])).toEqual(["b", "a"]);
    expect(moved(list, 0, -1)).toBe(list);
  });

  it("keeps a constant's number a number and blank a NULL", () => {
    expect(maskValueOf("0")).toBe(0);
    expect(maskValueOf("2.5")).toBe(2.5);
    expect(maskValueOf("***")).toBe("***");
    expect(maskValueOf("  ")).toBeNull();
  });
});

describe("MaskListEditor", () => {
  it("starts an empty list with its entry for everyone", () => {
    const seen = vi.fn();
    render(<Editor initial={[]} seen={seen} />);
    fireEvent.click(screen.getByTestId("mask-list-start"));
    expect(seen).toHaveBeenLastCalledWith(newMaskList());
    expect(screen.getByTestId("mask-list-entry-0")).toBeInTheDocument();
    expect(screen.queryByTestId("mask-list-entry-1")).toBeNull();
  });

  it("adds an entry before everyone's and removes one", () => {
    const seen = vi.fn();
    render(<Editor initial={newMaskList()} seen={seen} />);
    fireEvent.click(screen.getByTestId("mask-list-add"));
    expect(seen).toHaveBeenLastCalledWith([{ to: [], mask: null }, newMaskList()[0]]);
    fireEvent.click(screen.getByRole("button", { name: "Remove entry 1" }));
    expect(seen).toHaveBeenLastCalledWith(newMaskList());
  });

  it("flags an entry an earlier one covers, naming it", async () => {
    checkMasks.mockResolvedValue(report({ unreachable: [{ entry: 1, covered_by: 0 }] }));
    render(
      <Editor
        initial={[
          { to: ["analyst"], mask: null },
          { to: ["analyst"], mask: { type: "constant", value: null } },
        ]}
      />,
    );
    expect(await screen.findByTestId("mask-list-unreachable-1")).toHaveTextContent(
      "No reader reaches this entry: entry 1 applies to each of them first.",
    );
    expect(screen.queryByTestId("mask-list-unreachable-0")).toBeNull();
  });

  it("warns when readers it does not name read the column unmasked", async () => {
    checkMasks.mockResolvedValue(report({ unlisted_unmasked: true }));
    render(<Editor initial={[{ to: ["contractor"], mask: { type: "constant", value: null } }]} />);
    expect(await screen.findByTestId("mask-list-unlisted")).toHaveTextContent(
      "Readers this list does not name see the column unmasked.",
    );
  });

  it("asks which entry the chosen roles read through and marks it", async () => {
    render(<Editor initial={newMaskList()} />);
    await waitFor(() => expect(checkMasks).toHaveBeenCalledWith(newMaskList(), undefined));
    checkMasks.mockResolvedValue(report({ applies: 0 }));
    const input = screen.getByRole("textbox", { name: "Check access" });
    fireEvent.change(input, { target: { value: "analyst" } });
    fireEvent.keyDown(input, { key: "Enter" });
    await waitFor(() => expect(checkMasks).toHaveBeenLastCalledWith(newMaskList(), ["analyst"]));
    expect(await screen.findByTestId("mask-list-applies")).toHaveTextContent("Entry 1 applies");
    expect(screen.getByTestId("mask-list-entry-0")).toHaveAttribute("data-applies", "true");
  });

  it("sets a constant's value as typed, and an unmasked entry has no value", () => {
    const seen = vi.fn();
    render(<Editor initial={[{ to: ["analyst"], mask: null }, ...newMaskList()]} seen={seen} />);
    // One value field: the unmasked entry has none.
    const [value] = screen.getAllByLabelText("Constant value");
    expect(screen.getAllByLabelText("Constant value")).toHaveLength(1);
    fireEvent.change(value, { target: { value: "0" } });
    expect(seen).toHaveBeenLastCalledWith([
      { to: ["analyst"], mask: null },
      { to: ["*"], mask: { type: "constant", value: 0 } },
    ]);
  });
});
