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
// value chosen, or how many, and its choices are ticked in its dropdown.

import { useState } from "react";
import { describe, expect, it } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { SummaryMultiSelect } from "../SummaryMultiSelect";

function Picker({ start }: { start: string[] }) {
  const [value, setValue] = useState(start);
  return (
    <SummaryMultiSelect
      label="Edge types"
      data={["KIND_OF", "PART_OF", "SYNONYM_OF"]}
      value={value}
      onChange={setValue}
      testId="pick"
    />
  );
}

describe("a graph filter picking several values", () => {
  it("says its name with nothing chosen, the value with one, and the count with more", () => {
    const { unmount } = render(<Picker start={[]} />);
    expect(screen.getByTestId("pick")).toHaveTextContent("Edge types");
    unmount();
    const one = render(<Picker start={["KIND_OF"]} />);
    expect(screen.getByTestId("pick")).toHaveTextContent("KIND_OF");
    one.unmount();
    render(<Picker start={["KIND_OF", "PART_OF"]} />);
    expect(screen.getByTestId("pick")).toHaveTextContent("2 selected");
  });

  it("ticks a choice on and off from its dropdown", async () => {
    render(<Picker start={["KIND_OF"]} />);
    fireEvent.click(screen.getByTestId("pick"));
    fireEvent.click(await screen.findByTestId("pick-option-PART_OF"));
    expect(screen.getByTestId("pick")).toHaveTextContent("2 selected");
    fireEvent.click(await screen.findByTestId("pick-option-KIND_OF"));
    expect(screen.getByTestId("pick")).toHaveTextContent("PART_OF");
  });
});
