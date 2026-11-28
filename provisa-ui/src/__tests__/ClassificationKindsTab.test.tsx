// Copyright (c) 2026 Kenneth Stott
// Canary: 88ccbcc7-8ac4-49e8-a70d-9c776e53b046
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
import { expectFilterFindsEveryCell } from "../test-utils/listFilter";

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
    api.fetchClassificationKinds.mockResolvedValue({ kinds: KINDS });
    render(<ClassificationKindsTab title="Classification" />);
    const ssn = await screen.findByTestId("classification-kind-us_ssn");
    expect(ssn).toHaveTextContent("US social security number");
    expect(ssn).toHaveTextContent("Sensitive");
    expect(ssn).toHaveTextContent("the area is never 000, 666 or 900 to 999");
    expect(ssn).toHaveTextContent("90%");
    expect(ssn).toHaveTextContent("SSN, last 4");
    expect(screen.getByTestId("classification-kind-city")).toHaveTextContent("None");
  });

  it("finds each kind by every text its row shows", async () => {
    api.fetchClassificationKinds.mockResolvedValue({
      kinds: [KINDS[0], { ...KINDS[1], threshold: 0.75, enabled: false }],
    });
    render(<ClassificationKindsTab title="Classification" />);
    await screen.findByTestId("classification-kind-city");
    await expectFilterFindsEveryCell(screen.getByTestId("classification-kinds-filter"), () =>
      screen.getByTestId("classification-kinds-list"),
    );
  });

  it("says nothing matches when the filter empties the list", async () => {
    api.fetchClassificationKinds.mockResolvedValue({ kinds: KINDS });
    render(<ClassificationKindsTab title="Classification" />);
    await screen.findByTestId("classification-kind-city");
    fireEvent.change(screen.getByTestId("classification-kinds-filter"), {
      target: { value: "no-such-kind" },
    });
    expect(screen.getByTestId("classification-kinds-empty")).toHaveTextContent(
      "No kinds match the filter",
    );
  });

  it("disables a kind, sending every kind left enabled", async () => {
    api.fetchClassificationKinds.mockResolvedValue({ kinds: KINDS });
    api.setEnabledClassificationKinds.mockResolvedValue({
      kinds: [KINDS[0], { ...KINDS[1], enabled: false }],
    });
    render(<ClassificationKindsTab title="Classification" />);
    // REQ-2277: the row opens the kind's panel; its Edit opens the form with the switch.
    fireEvent.click(await screen.findByTestId("classification-kind-city"));
    fireEvent.click(screen.getByTestId("classification-kind-edit-city"));
    fireEvent.click(screen.getByTestId("classification-kind-enabled-city"));
    fireEvent.click(screen.getByTestId("classification-kind-save"));
    await waitFor(() => expect(api.setEnabledClassificationKinds).toHaveBeenCalledWith(["us_ssn"]));
    await waitFor(() =>
      expect(screen.getByTestId("classification-kind-state-city")).toHaveTextContent("Disabled"),
    );
  });

  it("holds the switch while its change is saving", async () => {
    // The page is reached only with the right the switch needs (sensitive_data), so a switch is
    // never shown read-only; it waits only for the change already on its way.
    api.fetchClassificationKinds.mockResolvedValue({ kinds: KINDS });
    api.setEnabledClassificationKinds.mockReturnValue(new Promise(() => {}));
    render(<ClassificationKindsTab title="Classification" />);
    fireEvent.click(await screen.findByTestId("classification-kind-city"));
    fireEvent.click(screen.getByTestId("classification-kind-edit-city"));
    fireEvent.click(screen.getByTestId("classification-kind-enabled-city"));
    fireEvent.click(screen.getByTestId("classification-kind-save"));
    await waitFor(() =>
      expect(screen.getByTestId("classification-kind-enabled-city")).toBeDisabled(),
    );
  });
});
