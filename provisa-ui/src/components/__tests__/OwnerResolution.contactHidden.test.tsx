// Copyright (c) 2026 Kenneth Stott
// Canary: 5e2a9c07-1d48-4f63-b7a0-8c3f6d1e9b25
// An owner resolved without the user id and e-mail (the caller lacks user_management) is listed by
// display name; one with nothing to show is left out rather than rendered blank.

import { describe, it, expect, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { render } from "../../test-utils/render";
import { OwnerResolutionInline } from "../OwnerResolution";

const resolveOwners = vi.fn();
vi.mock("../../hooks/useAdminQueries", () => ({ useResolveOwners: () => resolveOwners }));

describe("OwnerResolutionInline", () => {
  it("lists display names when the user id and e-mail are withheld", async () => {
    resolveOwners.mockResolvedValue([
      { userId: null, displayName: "Ada Lovelace", email: null },
      { userId: null, displayName: null, email: null },
      { userId: null, displayName: "Grace Hopper", email: null },
    ]);
    render(<OwnerResolutionInline refs={["steward"]} />);
    await waitFor(() => expect(screen.getByText("Ada Lovelace, Grace Hopper")).toBeTruthy());
  });

  it("shows the empty note when no owner can be named", async () => {
    resolveOwners.mockResolvedValue([{ userId: null, displayName: null, email: null }]);
    render(<OwnerResolutionInline refs={["steward"]} />);
    await waitFor(() =>
      expect(screen.getByText("No individuals currently hold this")).toBeTruthy(),
    );
  });
});
