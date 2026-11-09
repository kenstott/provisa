// Copyright (c) 2026 Kenneth Stott
// Canary: 3a91f6d2-8c47-4e15-b0d3-6f2e7a41c958
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2224: the table's setting for a read with nothing landed.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import { ReadWaitField, READ_WAIT_NO_ROWS } from "../ReadWaitField";

function renderField(value: number | null, onChange = vi.fn()) {
  render(<ReadWaitField value={value} onChange={onChange} />);
  return onChange;
}

const choose = (text: string) => {
  const select = screen.getByTestId("table-read-wait-select");
  fireEvent.click(select);
  const listbox = document.getElementById(select.getAttribute("aria-controls") as string);
  fireEvent.click(within(listbox as HTMLElement).getByText(text));
};

describe("ReadWaitField", () => {
  it("shows the request timeout when the table sets nothing, with no seconds to enter", () => {
    renderField(null);
    expect(screen.getByTestId("table-read-wait-select")).toHaveValue("Wait for the request timeout");
    expect(screen.queryByTestId("table-read-wait-seconds")).toBeNull();
  });

  it("stores a read wait in seconds, and shows the seconds to edit", () => {
    const onChange = renderField(120);
    expect(screen.getByTestId("table-read-wait-select")).toHaveValue("Wait up to a set time");
    fireEvent.change(screen.getByTestId("table-read-wait-seconds"), { target: { value: "45" } });
    expect(onChange).toHaveBeenLastCalledWith(45);
  });

  it("stores the answer-no-rows choice, and hides the seconds", () => {
    const onChange = renderField(null);
    choose("Answer no rows and start the landing");
    expect(onChange).toHaveBeenLastCalledWith(READ_WAIT_NO_ROWS);
  });

  it("goes back to the request timeout by storing nothing", () => {
    const onChange = renderField(READ_WAIT_NO_ROWS);
    expect(screen.queryByTestId("table-read-wait-seconds")).toBeNull();
    choose("Wait for the request timeout");
    expect(onChange).toHaveBeenLastCalledWith(null);
  });

  it("starts a timed wait at a default that can be edited", () => {
    const onChange = renderField(null);
    choose("Wait up to a set time");
    expect(onChange).toHaveBeenLastCalledWith(300);
  });
});
