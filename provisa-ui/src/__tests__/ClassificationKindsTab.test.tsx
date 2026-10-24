// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2147: the classification kinds are listed with their test, threshold, whether they are
// sensitive and the mask they offer; an org administrator disables or enables a kind, and a caller
// who may not change org settings sees the list without the switches working.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "../test-utils/render";

const api = vi.hoisted(() => ({
  fetchClassificationKinds: vi.fn(),
  setEnabledClassificationKinds: vi.fn(),
}));
vi.mock("../api/profiler", () => api);

import { ClassificationKindsTab } from "../components/admin/ClassificationKindsTab";

const KINDS = [
  {
    id: "us_ssn",
    label: "US social security number",
    sensitive: true,
    test: "us_ssn",
    families: ["text", "numeric"],
    threshold: 0.9,
    term: "us social security number",
    maskPreset: "ssn_last4",
    enabled: true,
  },
  {
    id: "city",
    label: "World city",
    sensitive: false,
    test: "reference:cities",
    families: ["text"],
    threshold: 0.9,
    term: "city",
    maskPreset: null,
    enabled: true,
  },
];

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
});

describe("ClassificationKindsTab", () => {
  it("lists each kind with its test, threshold and mask", async () => {
    api.fetchClassificationKinds.mockResolvedValue({ kinds: KINDS, canEdit: true });
    render(<ClassificationKindsTab />);
    const ssn = await screen.findByTestId("classification-kind-us_ssn");
    expect(ssn).toHaveTextContent("US social security number");
    expect(ssn).toHaveTextContent("Sensitive");
    expect(ssn).toHaveTextContent("the area is never 000, 666 or 900 to 999");
    expect(ssn).toHaveTextContent("90%");
    expect(ssn).toHaveTextContent("SSN, last 4");
    expect(screen.getByTestId("classification-kind-city")).toHaveTextContent("None");
  });

  it("disables a kind, sending every kind left enabled", async () => {
    api.fetchClassificationKinds.mockResolvedValue({ kinds: KINDS, canEdit: true });
    api.setEnabledClassificationKinds.mockResolvedValue({
      kinds: [KINDS[0], { ...KINDS[1], enabled: false }],
      canEdit: true,
    });
    render(<ClassificationKindsTab />);
    fireEvent.click(await screen.findByTestId("classification-kind-enabled-city"));
    await waitFor(() => expect(api.setEnabledClassificationKinds).toHaveBeenCalledWith(["us_ssn"]));
    await waitFor(() =>
      expect(screen.getByTestId("classification-kind-enabled-city")).not.toBeChecked(),
    );
  });

  it("shows the switches read-only to a caller who may not change org settings", async () => {
    api.fetchClassificationKinds.mockResolvedValue({ kinds: KINDS, canEdit: false });
    render(<ClassificationKindsTab />);
    expect(await screen.findByTestId("classification-kind-enabled-us_ssn")).toBeDisabled();
  });
});
