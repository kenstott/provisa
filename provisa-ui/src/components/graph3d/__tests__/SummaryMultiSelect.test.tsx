// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4e2a71-6b85-4d03-a1f8-e7d2b5c9a346
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128, REQ-2130: a graph filter picking several values stays one line high: it says the one
// value chosen, all of them, or how many of how many, and its choices are ticked in its dropdown.

import { useState } from "react";
import { describe, expect, it } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { SummaryMultiSelect } from "../SummaryMultiSelect";

function Picker({ start, searchable = false }: { start: string[]; searchable?: boolean }) {
  const [value, setValue] = useState(start);
  return (
    <SummaryMultiSelect
      label="Edge types"
      data={["KIND_OF", "PART_OF", { value: "SYNONYM_OF", label: "Synonym of" }]}
      value={value}
      onChange={setValue}
      testId="pick"
      allText="All edge types"
      someText={(count, total) => `${count} of ${total} edge types`}
      searchable={searchable}
    />
  );
}

describe("a graph filter picking several values", () => {
  it("says its name with nothing chosen, the one chosen, how many of how many, or all", () => {
    const { unmount } = render(<Picker start={[]} />);
    expect(screen.getByTestId("pick")).toHaveTextContent("Edge types");
    unmount();
    const one = render(<Picker start={["KIND_OF"]} />);
    expect(screen.getByTestId("pick")).toHaveTextContent("KIND_OF");
    one.unmount();
    const two = render(<Picker start={["KIND_OF", "PART_OF"]} />);
    expect(screen.getByTestId("pick")).toHaveTextContent("2 of 3 edge types");
    two.unmount();
    const label = render(<Picker start={["SYNONYM_OF"]} />);
    expect(screen.getByTestId("pick")).toHaveTextContent("Synonym of");
    label.unmount();
    render(<Picker start={["KIND_OF", "PART_OF", "SYNONYM_OF"]} />);
    expect(screen.getByTestId("pick")).toHaveTextContent("All edge types");
  });

  it("narrows a long list by what is typed in its search", async () => {
    render(<Picker start={[]} searchable />);
    fireEvent.click(screen.getByTestId("pick"));
    fireEvent.change(await screen.findByTestId("pick-search"), { target: { value: "syn" } });
    expect(screen.getByTestId("pick-option-SYNONYM_OF")).toBeInTheDocument();
    expect(screen.queryByTestId("pick-option-KIND_OF")).toBeNull();
  });

  it("ticks a choice on and off from its dropdown", async () => {
    render(<Picker start={["KIND_OF"]} />);
    fireEvent.click(screen.getByTestId("pick"));
    fireEvent.click(await screen.findByTestId("pick-option-PART_OF"));
    expect(screen.getByTestId("pick")).toHaveTextContent("2 of 3 edge types");
    fireEvent.click(await screen.findByTestId("pick-option-KIND_OF"));
    expect(screen.getByTestId("pick")).toHaveTextContent("PART_OF");
  });
});
