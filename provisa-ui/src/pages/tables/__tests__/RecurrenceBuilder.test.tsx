// Copyright (c) 2026 Kenneth Stott
// Canary: ae571eb7-0740-4e06-a936-69777bfbb68a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1169: the Outlook/iCalendar recurrence builder emits an RFC 5545 RRULE string the server's
// parse_grain_spec resolves; nesting-grain presets pass through as their plain token.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";
import { RecurrenceBuilder } from "../RecurrenceBuilder";

function renderBuilder(value: string | null) {
  const onChange = vi.fn();
  render(
    <RecurrenceBuilder value={value} onChange={onChange} label="Grain" placeholder="Select a grain" testId="mv-grain" />,
  );
  return onChange;
}

// Open the Mantine Select rendered at data-testid and click the option with the given text.
function selectOption(testId: string, optionText: string) {
  fireEvent.click(screen.getByTestId(testId));
  const opt = screen.getByRole("option", { name: optionText });
  fireEvent.click(opt);
}

describe("RecurrenceBuilder", () => {
  it("emits a nesting-grain preset token unchanged", () => {
    const onChange = renderBuilder(null);
    selectOption("mv-grain", "Monthly (end of month)");
    expect(onChange).toHaveBeenLastCalledWith("monthly");
  });

  it("does not show the builder for a preset value", () => {
    renderBuilder("monthly");
    expect(screen.queryByTestId("recurrence-builder")).not.toBeInTheDocument();
  });

  it("opens the builder and emits an RRULE when Custom is chosen", () => {
    const onChange = renderBuilder(null);
    selectOption("mv-grain", "Custom recurrence…");
    expect(onChange).toHaveBeenCalled();
    expect(String(onChange.mock.lastCall?.[0])).toMatch(/^RRULE:FREQ=MONTHLY/);
  });

  it("renders the builder for an RRULE value and summarizes it", () => {
    renderBuilder("RRULE:FREQ=MONTHLY;BYDAY=+3WE");
    expect(screen.getByTestId("recurrence-builder")).toBeInTheDocument();
    expect(screen.getByTestId("recurrence-summary").textContent).toContain("3rd Wednesday");
  });

  it("upgrades the legacy 3WE shorthand into an editable RRULE", () => {
    renderBuilder("3WE");
    expect(screen.getByTestId("recurrence-builder")).toBeInTheDocument();
    expect(screen.getByTestId("recurrence-summary").textContent).toContain("Wednesday");
  });

  it("emits BYMONTHDAY=-1 when 'Last day' is chosen", () => {
    const onChange = renderBuilder("RRULE:FREQ=MONTHLY;BYMONTHDAY=1");
    selectOption("recurrence-monthday", "Last day");
    expect(String(onChange.mock.lastCall?.[0])).toContain("BYMONTHDAY=-1");
  });

  it("switches monthly mode to the Nth-weekday form", () => {
    const onChange = renderBuilder("RRULE:FREQ=MONTHLY;BYMONTHDAY=15");
    fireEvent.click(screen.getByText("On the"));
    expect(String(onChange.mock.lastCall?.[0])).toMatch(/BYDAY=\+?1MO/);
  });
});
