// Copyright (c) 2026 Kenneth Stott
// Canary: 2a6e9c14-7d3b-4f58-8b01-e5c7a3d9f426
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2216: the org's request delivery setting -- email (the default), events only, or both --
// shown as it is stored and saved as it is changed; a refused save is put back and said.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";

const api = vi.hoisted(() => ({
  fetchRequestDelivery: vi.fn(),
  saveRequestDelivery: vi.fn(),
}));
vi.mock("../api/requestDelivery", () => api);

import { RequestDeliverySetting } from "../components/admin/RequestDeliverySetting";

const checked = (setting: HTMLElement, label: string) =>
  (within(setting).getByLabelText(label) as HTMLInputElement).checked;

beforeEach(() => {
  api.fetchRequestDelivery.mockReset().mockResolvedValue("email");
  api.saveRequestDelivery.mockReset().mockImplementation(async (next: string) => next);
});

describe("RequestDeliverySetting", () => {
  it("shows the stored delivery and saves events only", async () => {
    render(<RequestDeliverySetting onError={vi.fn()} />);
    const setting = await screen.findByTestId("request-delivery");
    expect(setting).toHaveTextContent("Notify by");
    await waitFor(() => expect(checked(setting, "Email")).toBe(true));
    fireEvent.click(within(setting).getByText("Events only"));
    await waitFor(() => expect(api.saveRequestDelivery).toHaveBeenCalledWith("events"));
    await waitFor(() => expect(checked(setting, "Events only")).toBe(true));
  });

  it("offers both", async () => {
    render(<RequestDeliverySetting onError={vi.fn()} />);
    const setting = await screen.findByTestId("request-delivery");
    await waitFor(() => expect(checked(setting, "Email")).toBe(true));
    fireEvent.click(within(setting).getByText("Both"));
    await waitFor(() => expect(api.saveRequestDelivery).toHaveBeenCalledWith("both"));
  });

  it("puts the setting back and says why when it is refused", async () => {
    api.saveRequestDelivery.mockRejectedValue(new Error("Missing capability: 'org_settings'"));
    const onError = vi.fn();
    render(<RequestDeliverySetting onError={onError} />);
    const setting = await screen.findByTestId("request-delivery");
    await waitFor(() => expect(checked(setting, "Email")).toBe(true));
    fireEvent.click(within(setting).getByText("Events only"));
    await waitFor(() => expect(onError).toHaveBeenCalledWith("Missing capability: 'org_settings'"));
    expect(checked(setting, "Email")).toBe(true);
  });
});
