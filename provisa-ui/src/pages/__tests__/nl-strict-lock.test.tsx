// Copyright (c) 2026 Kenneth Stott
// Canary: 7671c9ab-d075-4be6-b0e6-be9e5e025452
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-603: non-strict NL is a role's that may bypass the relationship guard. For any other role the
// Strict switch is held on and cannot be turned off, and says why; for a role that may, it is free.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { setStored } from "../../lib/browserStorage";

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({ role: { id: "analyst" } }),
}));

const fetchNlMode = vi.fn();
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchNlMode: (role: string) => fetchNlMode(role),
}));

import { NlPage } from "../NlPage";

function strictSwitch(): HTMLInputElement {
  return screen.getByTestId("nl-strict-toggle") as HTMLInputElement;
}

describe("NL page Strict switch", () => {
  beforeEach(() => {
    fetchNlMode.mockReset();
    setStored("nl-strict", "0"); // the user last chose non-strict
  });

  it("is held on for a role that may only use approved relationships", async () => {
    fetchNlMode.mockResolvedValue({ non_strict: false });
    render(<NlPage />);
    await waitFor(() => expect(fetchNlMode).toHaveBeenCalledWith("analyst"));
    expect(strictSwitch()).toBeChecked();
    expect(strictSwitch()).toBeDisabled();
    await userEvent.hover(strictSwitch().closest("span") as HTMLElement);
    expect(await screen.findByText("This role may only use approved relationships")).toBeTruthy();
  });

  it("is free for a role that may bypass the relationship guard", async () => {
    fetchNlMode.mockResolvedValue({ non_strict: true });
    render(<NlPage />);
    await waitFor(() => expect(strictSwitch()).not.toBeDisabled());
    expect(strictSwitch()).not.toBeChecked();
    await userEvent.click(strictSwitch());
    expect(strictSwitch()).toBeChecked();
  });
});
