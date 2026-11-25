// Copyright (c) 2026 Kenneth Stott
// Canary: 7f1c3a85-4d26-4e9b-8a07-6b2e9d5c1f34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 (amended 2026-10-28, SUGGEST A PRODUCT FROM A LISTED TABLE): the reader of a listed table
// suggests a product of it, or of several listed tables of its domain, stating why; the dialog
// says who it went to, and a refusal is shown as the server words it.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, within } from "../../../test-utils/render";
import userEvent from "@testing-library/user-event";
import i18n from "../../../i18n";
import type { Suggestion, SuggestionInput } from "../../../api/productSuggestions";

const suggestProduct = vi.fn<(input: SuggestionInput) => Promise<Suggestion>>();
vi.mock("../../../api/productSuggestions", () => ({
  suggestProduct: (input: SuggestionInput) => suggestProduct(input),
}));

import { SuggestProductModal, type SuggestTarget } from "../SuggestProductModal";

const t = i18n.t.bind(i18n);
const TARGET: SuggestTarget = {
  domainId: "hr",
  tableName: "staff",
  domainTables: ["staff", "payroll"],
};

beforeEach(() => {
  suggestProduct.mockReset();
});

describe("suggesting a product from a listed table", () => {
  it("opens on the chosen table, and sends its tables and reason", async () => {
    suggestProduct.mockImplementation(async (input) => ({
      ...input,
      id: 1,
      suggested_by: "eve",
      recipients: ["ana", "ben"],
      created_at: "2026-11-24T12:00:00Z",
    }));
    render(<SuggestProductModal target={TARGET} onClose={() => {}} />);
    const dialog = await screen.findByTestId("suggest-product-modal");
    expect(screen.getByText(t("suggestProduct.title", { domain: "hr" }))).toBeInTheDocument();
    const send = within(dialog).getByTestId("suggest-product-send");
    // REQ-1968: Send is never disabled by a missing field; it marks the reason and sends nothing.
    expect(send).toBeEnabled();
    await userEvent.click(send);
    const reason = within(dialog).getByTestId("suggest-product-reason");
    expect(reason).toHaveAttribute("aria-invalid", "true");
    expect(reason).toHaveFocus();
    expect(suggestProduct).not.toHaveBeenCalled();
    await userEvent.type(reason, "headcount plan");
    expect(reason).not.toHaveAttribute("aria-invalid", "true");
    await userEvent.click(send);
    await waitFor(() =>
      expect(suggestProduct).toHaveBeenCalledWith({
        domain_id: "hr",
        tables: ["staff"],
        reason: "headcount plan",
      }),
    );
    expect(await screen.findByTestId("suggest-product-sent")).toHaveTextContent(
      t("suggestProduct.sent", { count: 2, domain: "hr" }),
    );
  });

  it("names another listed table of the domain too", async () => {
    suggestProduct.mockImplementation(async (input) => ({
      ...input,
      id: 2,
      suggested_by: "eve",
      recipients: ["ben"],
      created_at: "2026-11-24T12:00:00Z",
    }));
    render(<SuggestProductModal target={TARGET} onClose={() => {}} />);
    const dialog = await screen.findByTestId("suggest-product-modal");
    await userEvent.click(within(dialog).getByTestId("suggest-product-tables"));
    // The dropdown is mid-transition in jsdom, so its options are queried hidden.
    await userEvent.click(await screen.findByRole("option", { name: "payroll", hidden: true }));
    await userEvent.type(within(dialog).getByTestId("suggest-product-reason"), "costs");
    await userEvent.click(within(dialog).getByTestId("suggest-product-send"));
    await waitFor(() =>
      expect(suggestProduct).toHaveBeenCalledWith({
        domain_id: "hr",
        tables: ["staff", "payroll"],
        reason: "costs",
      }),
    );
    expect(await screen.findByTestId("suggest-product-sent")).toHaveTextContent(
      t("suggestProduct.sent", { count: 1, domain: "hr" }),
    );
  });

  it("shows a refusal and stays open", async () => {
    const refusal = t("serverErrors.product_suggestion.no_recipient", { domain: "hr" });
    suggestProduct.mockRejectedValue(new Error(refusal));
    render(<SuggestProductModal target={TARGET} onClose={() => {}} />);
    const dialog = await screen.findByTestId("suggest-product-modal");
    await userEvent.type(within(dialog).getByTestId("suggest-product-reason"), "why");
    await userEvent.click(within(dialog).getByTestId("suggest-product-send"));
    expect(await within(dialog).findByText(refusal)).toBeInTheDocument();
    expect(screen.queryByTestId("suggest-product-sent")).toBeNull();
  });

  it("sends nothing without a table", async () => {
    render(
      <SuggestProductModal target={{ ...TARGET, domainTables: ["staff"] }} onClose={() => {}} />,
    );
    const dialog = await screen.findByTestId("suggest-product-modal");
    await userEvent.type(within(dialog).getByTestId("suggest-product-reason"), "why");
    // Remove the chosen table's pill.
    const pill = within(dialog).getByText("staff").closest(".mantine-Pill-root");
    const remove = pill?.querySelector("button");
    expect(remove).not.toBeNull();
    await userEvent.click(remove as HTMLElement);
    await userEvent.click(within(dialog).getByTestId("suggest-product-send"));
    expect(within(dialog).getByTestId("suggest-product-tables")).toHaveAttribute(
      "aria-invalid",
      "true",
    );
    expect(suggestProduct).not.toHaveBeenCalled();
  });

  it("is closed with no target", () => {
    render(<SuggestProductModal target={null} onClose={() => {}} />);
    // A closed Mantine Modal keeps its empty root; a closed dialog draws no form.
    expect(screen.queryByTestId("suggest-product-reason")).toBeNull();
    expect(screen.queryByTestId("suggest-product-send")).toBeNull();
  });
});
