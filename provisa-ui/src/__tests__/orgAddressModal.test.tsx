// Copyright (c) 2026 Kenneth Stott
// Canary: 2b8c4f16-70d3-4a91-b5e2-6c9d0f3a7185
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * The address a new org is reached at (REQ-1276) and the invitation to dress it in the org's own
 * branding (REQ-1486). Neither is discoverable from the workspace, so creation says both out loud.
 */

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../test-utils/render";
import { MemoryRouter, Routes, Route } from "react-router-dom";
import { OrgAddressModal } from "../components/OrgAddressModal";

function renderModal(onClose = () => {}) {
  return render(
    <MemoryRouter initialEntries={["/onboard"]}>
      <Routes>
        <Route
          path="/onboard"
          element={<OrgAddressModal url="https://acme.provisa.dev" opened onClose={onClose} />}
        />
        <Route path="/team" element={<div data-testid="team-page-stub" />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe("org address modal", () => {
  it("shows the org's own address", () => {
    renderModal();
    expect(screen.getByTestId("org-address-url")).toHaveTextContent("https://acme.provisa.dev");
  });

  it("sends the administrator to the branding editor", () => {
    const onClose = vi.fn();
    renderModal(onClose);
    fireEvent.click(screen.getByTestId("org-address-brand"));
    expect(onClose).toHaveBeenCalled();
    expect(screen.getByTestId("team-page-stub")).toBeInTheDocument();
  });

  it("closes on acknowledgement", () => {
    const onClose = vi.fn();
    renderModal(onClose);
    fireEvent.click(screen.getByTestId("org-address-done"));
    expect(onClose).toHaveBeenCalled();
  });
});
