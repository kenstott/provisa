// Copyright (c) 2026 Kenneth Stott
// Canary: 758cea57-0119-4927-9b4b-a827bd054fc2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-826: the Replicate drop-down shared by the table form and the source's Load Management
// panel — Default, Never, the Hot thresholds and Always; a non-standard stored threshold is shown
// and kept; Never with load protection is flagged.

import { useState } from "react";
import { describe, it, expect, vi } from "vitest";
import { render, screen, within, fireEvent } from "../../../test-utils/render";
import { ReplicateSelect } from "../ReplicateSelect";
import {
  parseReplicateOption,
  replicateContradictsLoadProtection,
  replicateOptionValue,
  replicateValues,
  resolvedReplicate,
  saysReplicated,
} from "../replicate";

const STANDARD = [
  "Default",
  "Never",
  "Hot-50",
  "Hot-100",
  "Hot-500",
  "Hot-1000",
  "Hot-10000",
  "Always",
];

function Harness({
  initial,
  loadProtected = false,
  onChange,
}: {
  initial: number | null;
  loadProtected?: boolean;
  onChange?: (v: number | null) => void;
}) {
  const [value, setValue] = useState<number | null>(initial);
  return (
    <ReplicateSelect
      value={value}
      onChange={(v) => {
        setValue(v);
        onChange?.(v);
      }}
      scope="table"
      loadProtected={loadProtected}
      testId="replicate"
    />
  );
}

/** Open the drop-down and return its option labels, in order. */
function openOptions(): { labels: string[]; listbox: HTMLElement } {
  const input = screen.getByTestId("replicate");
  fireEvent.click(input);
  // Mantine keeps the combobox dropdown out of the accessibility tree in jsdom: reach it through
  // the input's aria-controls listbox.
  const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
  if (listbox === null) throw new Error("the Replicate drop-down did not open");
  const labels = within(listbox)
    .getAllByRole("option", { hidden: true })
    .map((o) => o.textContent ?? "");
  return { labels, listbox };
}

describe("ReplicateSelect", () => {
  it("is labelled Replicate and lists the standard entries in order", () => {
    render(<Harness initial={null} />);
    expect(screen.getByText("Replicate")).toBeInTheDocument();
    expect(openOptions().labels).toEqual(STANDARD);
  });

  it.each([
    [null, "Default"],
    [-1, "Never"],
    [0, "Always"],
    [500, "Hot-500"],
  ])("shows the stored value %s as %s", (stored, label) => {
    render(<Harness initial={stored} />);
    expect(screen.getByTestId("replicate")).toHaveValue(label);
  });

  it("shows a non-standard stored threshold among the Hot entries and keeps it", () => {
    const onChange = vi.fn();
    render(<Harness initial={750} onChange={onChange} />);
    expect(screen.getByTestId("replicate")).toHaveValue("Hot-750");
    const { labels } = openOptions();
    expect(labels).toEqual([
      "Default",
      "Never",
      "Hot-50",
      "Hot-100",
      "Hot-500",
      "Hot-750",
      "Hot-1000",
      "Hot-10000",
      "Always",
    ]);
    // Nothing was chosen: the stored value is untouched.
    expect(onChange).not.toHaveBeenCalled();
  });

  it("reports the integer the chosen entry stands for", () => {
    const onChange = vi.fn();
    render(<Harness initial={null} onChange={onChange} />);
    for (const [label, value] of [
      ["Always", 0],
      ["Never", -1],
      ["Hot-1000", 1000],
      ["Default", null],
    ] as const) {
      const { listbox } = openOptions();
      fireEvent.click(within(listbox).getByText(label));
      expect(onChange).toHaveBeenLastCalledWith(value);
    }
  });

  it("flags Never when load protection is on, and only then", () => {
    const message =
      "Never cannot be combined with load protection: a load-protected table is never read live.";
    const { unmount } = render(<Harness initial={-1} loadProtected />);
    expect(screen.getByText(message)).toBeInTheDocument();
    unmount();
    render(<Harness initial={-1} />);
    expect(screen.queryByText(message)).toBeNull();
  });
});

describe("replicate values", () => {
  it("round-trips every value through its option", () => {
    for (const value of [null, -1, 0, 50, 750, 10000]) {
      expect(parseReplicateOption(replicateOptionValue(value))).toBe(value);
    }
  });

  it("lists a standard stored value once", () => {
    expect(replicateValues(500)).toEqual([null, -1, 50, 100, 500, 1000, 10000, 0]);
    expect(replicateValues(null)).toEqual(replicateValues(0));
  });

  it("a table's own value wins over its source's", () => {
    expect(resolvedReplicate(-1, 0)).toBe(-1);
    expect(resolvedReplicate(null, 500)).toBe(500);
    expect(resolvedReplicate(null, null)).toBeNull();
    expect(resolvedReplicate(0, null)).toBe(0);
  });

  it("Always, a Hot threshold and load protection say the table is replicated", () => {
    expect(saysReplicated(0, false)).toBe(true);
    expect(saysReplicated(500, false)).toBe(true);
    expect(saysReplicated(null, true)).toBe(true);
    expect(saysReplicated(null, false)).toBe(false);
    expect(saysReplicated(-1, false)).toBe(false);
  });

  it("only Never contradicts load protection", () => {
    expect(replicateContradictsLoadProtection(-1, true)).toBe(true);
    for (const value of [null, 0, 500]) {
      expect(replicateContradictsLoadProtection(value, true)).toBe(false);
    }
    expect(replicateContradictsLoadProtection(-1, false)).toBe(false);
  });
});
