// Copyright (c) 2026 Kenneth Stott
// Canary: a503a4ff-fdcd-4fe9-9dbf-00891ce6d47b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The domain mode changes only while the org has no catalog (REQ-1919). The switch deletes
// nothing, so there is no typed confirmation; when the server refuses it, the card shows the
// refusal, which counts what exists.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../../test-utils/render";
import { DomainModeCard } from "../settingsCards";
import * as api from "../../../api/admin";

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchSettings: vi.fn(),
  setDomainPolicy: vi.fn(),
}));

const SETTINGS = { naming: { use_domains: true, default_domain: "default" } };

describe("DomainModeCard", () => {
  beforeEach(() => {
    vi.mocked(api.fetchSettings).mockResolvedValue(
      SETTINGS as unknown as Awaited<ReturnType<typeof api.fetchSettings>>,
    );
    vi.mocked(api.setDomainPolicy).mockReset();
  });

  it("applies the mode directly, with no confirmation to type", async () => {
    vi.mocked(api.setDomainPolicy).mockResolvedValue({ success: true, use_domains: true });
    const onApplied = vi.fn();
    render(<DomainModeCard onApplied={onApplied} />);
    await waitFor(() => expect(api.fetchSettings).toHaveBeenCalled());

    fireEvent.click(screen.getByTestId("apply-domain-policy"));

    await screen.findByText("Domain policy applied.");
    expect(api.setDomainPolicy).toHaveBeenCalledWith({
      use_domains: true,
      default_domain: "default",
    });
    expect(onApplied).toHaveBeenCalled();
    expect(screen.queryByTestId("domain-policy-refused")).toBeNull();
  });

  it("shows the server's refusal when the org has a catalog", async () => {
    const refusal =
      "The domain policy cannot change while this organization has a catalog: 3 table(s), 1 source(s), 2 domain(s). Delete them first.";
    vi.mocked(api.setDomainPolicy).mockRejectedValue(new Error(refusal));
    const onApplied = vi.fn();
    render(<DomainModeCard onApplied={onApplied} />);
    await waitFor(() => expect(api.fetchSettings).toHaveBeenCalled());

    fireEvent.click(screen.getByTestId("apply-domain-policy"));

    expect(await screen.findByTestId("domain-policy-refused")).toHaveTextContent(refusal);
    expect(onApplied).not.toHaveBeenCalled();
    expect(screen.queryByText("Domain policy applied.")).toBeNull();
  });
});
