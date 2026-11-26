// Copyright (c) 2026 Kenneth Stott
// Canary: 1e97c611-9400-4290-9cc6-42d4aa9d16dd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A member's attributes beside the Team page's list: each with its source, the provider's value
// struck through where the org's record overrides it, and only the org's own editable.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor, within } from "../test-utils/render";

vi.mock("../api/admin", () => ({
  fetchMemberAttributes: vi.fn(),
  setMemberAttribute: vi.fn(),
  deleteMemberAttribute: vi.fn(),
}));

import { deleteMemberAttribute, fetchMemberAttributes, setMemberAttribute } from "../api/admin";
import { MemberAttributes } from "../components/admin/MemberAttributes";

const mockFetch = vi.mocked(fetchMemberAttributes);
const mockSet = vi.mocked(setMemberAttribute);
const mockDelete = vi.mocked(deleteMemberAttribute);

const ROWS = [
  { name: "dept", value: "sales", source: "idp" as const, overridden: false },
  { name: "region", value: "eu", source: "org" as const, overridden: false },
  { name: "region", value: "us", source: "idp" as const, overridden: true },
];

beforeEach(() => {
  vi.clearAllMocks();
  mockFetch.mockResolvedValue(ROWS);
  mockSet.mockResolvedValue(ROWS[1]);
  mockDelete.mockResolvedValue(undefined);
});

describe("a member's attributes", () => {
  it("shows the provider's overridden value struck through, and only the org's as editable", async () => {
    render(<MemberAttributes userId="u|1" />);
    const overridden = await screen.findByTestId("member-attr-idp-region");
    expect(within(overridden).getByText("us")).toHaveStyle({ textDecoration: "line-through" });
    expect(within(overridden).queryByRole("button")).toBeNull();
    const org = screen.getByTestId("member-attr-org-region");
    expect(within(org).getByRole("button", { name: "Edit region" })).toBeInTheDocument();
    expect(mockFetch).toHaveBeenCalledWith("u|1");
  });

  it("records a new attribute and reloads", async () => {
    render(<MemberAttributes userId="u1" />);
    await screen.findByTestId("member-attr-org-region");
    await userEvent.type(screen.getByTestId("member-attr-name"), "team");
    await userEvent.type(screen.getByTestId("member-attr-value"), "a");
    await userEvent.click(screen.getByTestId("member-attr-add"));
    await waitFor(() => expect(mockSet).toHaveBeenCalledWith("u1", "team", "a"));
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  // REQ-1968: Add is never disabled by a missing name; pressing it marks the name and refuses.
  it("marks a missing name when Add is pressed, and records nothing", async () => {
    render(<MemberAttributes userId="u1" />);
    await screen.findByTestId("member-attr-org-region");
    const add = screen.getByTestId("member-attr-add");
    expect(add).toBeEnabled();
    await userEvent.click(add);
    expect(mockSet).not.toHaveBeenCalled();
    expect(screen.getByTestId("member-attr-name")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByTestId("member-attr-name")).toHaveFocus();
  });

  it("edits the org's value in place", async () => {
    render(<MemberAttributes userId="u1" />);
    await userEvent.click(await screen.findByRole("button", { name: "Edit region" }));
    const input = screen.getByTestId("member-attr-edit-region");
    await userEvent.clear(input);
    await userEvent.type(input, "uk{Enter}");
    await waitFor(() => expect(mockSet).toHaveBeenCalledWith("u1", "region", "uk"));
  });

  it("removes the org's attribute", async () => {
    render(<MemberAttributes userId="u1" />);
    await userEvent.click(await screen.findByTestId("member-attr-remove-region"));
    await waitFor(() => expect(mockDelete).toHaveBeenCalledWith("u1", "region"));
  });

  it("names a refused write", async () => {
    mockSet.mockRejectedValue(new Error("'user_id' is bound from the acting identity"));
    render(<MemberAttributes userId="u1" />);
    await screen.findByTestId("member-attr-org-region");
    await userEvent.type(screen.getByTestId("member-attr-name"), "user_id");
    await userEvent.click(screen.getByTestId("member-attr-add"));
    expect(await screen.findByTestId("member-attributes-error")).toHaveTextContent(
      "bound from the acting identity",
    );
  });
});
