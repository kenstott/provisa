// Copyright (c) 2026 Kenneth Stott
// Canary: 5c1f8e2a-7b3d-4e96-a0d4-2f6b9c7e1a38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219 (amended 2026-11-23): an Owner or Steward field names people. The picker finds them
// through the one search by name or email and answers with each person's principal; several may
// be named, one already named is kept, and a typed email that matches no one names no one.

import { useState } from "react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../../test-utils/render";

vi.mock("../../../api/entitlements", async (orig) => ({
  ...(await orig<typeof import("../../../api/entitlements")>()),
  searchPeople: vi.fn(),
}));

import { searchPeople } from "../../../api/entitlements";
import { PeopleMultiPicker } from "../PeoplePicker";

const mockSearch = vi.mocked(searchPeople);

function Harness({ initial, onPicked }: { initial: string[]; onPicked: (v: string[]) => void }) {
  const [value, setValue] = useState(initial);
  return (
    <PeopleMultiPicker
      label="Owners"
      value={value}
      onChange={(v) => {
        setValue(v);
        onPicked(v);
      }}
      onError={vi.fn()}
      testId="owners"
    />
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  mockSearch.mockResolvedValue([]);
});

describe("PeopleMultiPicker", () => {
  it("names several people the search finds, by their principals", async () => {
    mockSearch.mockImplementation(async (q: string) =>
      q.startsWith("ke")
        ? [{ principal: "okta|31", name: "Kenji Sato", email: "kenji@acme.example" }]
        : [{ principal: "okta|7", name: "Ana Ruiz", email: "ana@acme.example" }],
    );
    const picked = vi.fn();
    render(<Harness initial={[]} onPicked={picked} />);
    const input = screen.getByTestId("owners");
    await userEvent.type(input, "kenji");
    await userEvent.click(
      await screen.findByRole("option", { name: "Kenji Sato · kenji@acme.example", hidden: true }),
    );
    expect(mockSearch).toHaveBeenCalledWith("kenji");
    await userEvent.type(input, "ana");
    await userEvent.click(
      await screen.findByRole("option", { name: "Ana Ruiz · ana@acme.example", hidden: true }),
    );
    await waitFor(() => expect(picked).toHaveBeenLastCalledWith(["okta|31", "okta|7"]));
    // The first person keeps their name once the search has moved on.
    expect(screen.getAllByText("Kenji Sato · kenji@acme.example").length).toBeGreaterThan(0);
  });

  it("keeps a person named before the form opened, shown by their principal", () => {
    render(<Harness initial={["okta|9"]} onPicked={vi.fn()} />);
    expect(screen.getAllByText("okta|9").length).toBeGreaterThan(0);
    expect(mockSearch).not.toHaveBeenCalled();
  });

  it("does not search under two characters", async () => {
    render(<Harness initial={[]} onPicked={vi.fn()} />);
    await userEvent.type(screen.getByTestId("owners"), "k");
    await new Promise((r) => setTimeout(r, 300));
    expect(mockSearch).not.toHaveBeenCalled();
  });

  it("offers no typed email that matches no one: an owner row names a person", async () => {
    render(<Harness initial={[]} onPicked={vi.fn()} />);
    await userEvent.type(screen.getByTestId("owners"), "new@acme.example");
    await waitFor(() => expect(mockSearch).toHaveBeenCalledWith("new@acme.example"));
    expect(screen.queryByRole("option", { hidden: true })).toBeNull();
  });
});
