// Copyright (c) 2026 Kenneth Stott
// Canary: 83ed5472-703d-4fb8-9005-1e0815bc03e5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The claim types an org declares as claim roles: read with where they come from, saved as the
// org's own list.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../test-utils/render";

vi.mock("../api/admin", () => ({ fetchClaimRoles: vi.fn(), saveClaimRoles: vi.fn() }));

import { fetchClaimRoles, saveClaimRoles } from "../api/admin";
import { ClaimRolesForm } from "../components/admin/ClaimRolesForm";

const mockFetch = vi.mocked(fetchClaimRoles);
const mockSave = vi.mocked(saveClaimRoles);

beforeEach(() => {
  vi.clearAllMocks();
  mockFetch.mockResolvedValue({ claim_types: ["region"], source: "deployment" });
  mockSave.mockImplementation(async (types) => ({ claim_types: types, source: "org" }));
});

describe("claim roles", () => {
  it("says the list is the deployment's until the org saves its own", async () => {
    render(<ClaimRolesForm onClose={vi.fn()} />);
    expect(await screen.findByText("region")).toBeInTheDocument();
    expect(
      screen.getByText("From the deployment configuration until this organization saves its own."),
    ).toBeInTheDocument();
  });

  it("saves the org's own list", async () => {
    render(<ClaimRolesForm onClose={vi.fn()} />);
    await screen.findByText("region");
    await userEvent.type(screen.getByTestId("claim-roles-input"), "department{Enter}");
    await userEvent.click(screen.getByTestId("claim-roles-save"));
    await waitFor(() => expect(mockSave).toHaveBeenCalledWith(["region", "department"]));
    expect(await screen.findByText("This organization's own list.")).toBeInTheDocument();
    expect(screen.getByTestId("claim-roles-saved")).toHaveTextContent("Saved.");
  });

  it("saves a claim type typed but not entered", async () => {
    render(<ClaimRolesForm onClose={vi.fn()} />);
    await screen.findByText("region");
    await userEvent.type(screen.getByTestId("claim-roles-input"), "department");
    await userEvent.click(screen.getByTestId("claim-roles-save"));
    await waitFor(() => expect(mockSave).toHaveBeenCalledWith(["region", "department"]));
    expect(await screen.findByTestId("claim-roles-saved")).toHaveTextContent("Saved.");
  });

  it("saves typed text even when Save is reached without leaving the field", async () => {
    render(<ClaimRolesForm onClose={vi.fn()} />);
    await screen.findByText("region");
    await userEvent.type(screen.getByTestId("claim-roles-input"), "department");
    screen.getByTestId("claim-roles-save").click();
    await waitFor(() => expect(mockSave).toHaveBeenCalledWith(["region", "department"]));
  });

  it("names a refused save", async () => {
    mockSave.mockRejectedValue(new Error("Claim type '9x' must be letters"));
    render(<ClaimRolesForm onClose={vi.fn()} />);
    await screen.findByText("region");
    await userEvent.click(screen.getByTestId("claim-roles-save"));
    expect(await screen.findByText("Claim type '9x' must be letters")).toBeInTheDocument();
    expect(screen.queryByTestId("claim-roles-saved")).not.toBeInTheDocument();
  });
});
