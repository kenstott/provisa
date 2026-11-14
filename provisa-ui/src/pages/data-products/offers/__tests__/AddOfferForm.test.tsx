// Copyright (c) 2026 Kenneth Stott
// Canary: 4a1e7c39-0b8d-4f25-96e3-c7d2a5b0f814
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 OFFERS: the form that adds an offer. Its role picker lists the roles the owner may grant
// that reach every dataset, and shows the picked role's masks and row rules; "open" is offered only
// when that role reads the data as public; a sample names its views; a synthetic offer has no
// profiles; the summary says the offer in words; Save sends it, and a refusal is shown.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../../test-utils/render";
import type { OfferInput } from "../../../../api/dataProductOffers";
import type { TableAccess } from "../../../../api/roleRights";

const api = vi.hoisted(() => ({
  grantable: vi.fn<() => Promise<string[]>>(),
  add: vi.fn<(id: string, offer: OfferInput) => Promise<unknown>>(),
  access: vi.fn<(tableId: number) => Promise<TableAccess>>(),
}));
vi.mock("../../../../api/dataProductOffers", () => ({
  fetchGrantableRoles: () => api.grantable(),
  addOffer: (id: string, offer: OfferInput) => api.add(id, offer),
}));
vi.mock("../../../../api/roleRights", () => ({
  fetchTableAccess: (tableId: number) => api.access(tableId),
}));

import { AddOfferForm } from "../AddOfferForm";

const read = (
  role: string,
  over: { mask?: string; rowRule?: string; reach?: "domain" | "public" } = {},
) => ({
  role,
  standard: false,
  read: true,
  write: [],
  mask: over.mask ? { type: over.mask } : null,
  row_rule: over.rowRule ?? null,
  granted_by: role,
  via: [],
  reach: over.reach ?? "domain",
});

// Two datasets. analyst reads both, email masked and EU rows only; reference reads both as public
// data; partner reads the first alone.
const ACCESS: Record<number, TableAccess> = {
  1: {
    table_id: 1,
    domain: "sales",
    name: "sales.customers",
    table_name: "customers",
    draft: false,
    columns: [
      {
        name: "id",
        scope: "domain",
        access: [read("analyst"), read("reference", { reach: "public" }), read("partner")],
      },
      {
        name: "email",
        scope: "domain",
        access: [
          read("analyst", { mask: "regex", rowRule: "region = 'EU'" }),
          read("reference", { reach: "public" }),
        ],
      },
    ],
  },
  2: {
    table_id: 2,
    domain: "sales",
    name: "sales.orders",
    table_name: "orders",
    draft: false,
    columns: [
      {
        name: "total",
        scope: "domain",
        access: [read("analyst"), read("reference", { reach: "public" })],
      },
    ],
  },
};

const DATASETS = [
  { id: 1, name: "sales.customers" },
  { id: 2, name: "sales.orders" },
];
const VIEWS = [{ id: 9, name: "sales.customers_sample" }];

function form() {
  const onSaved = vi.fn();
  render(
    <AddOfferForm
      productId="c360"
      datasets={DATASETS}
      views={VIEWS}
      onSaved={onSaved}
      onCancel={vi.fn()}
    />,
  );
  return onSaved;
}

async function pickRole(role: string) {
  fireEvent.click(await screen.findByTestId("add-offer-role"));
  fireEvent.click(await screen.findByRole("option", { name: role, hidden: true }));
}

async function pickLevel(label: string) {
  fireEvent.click(screen.getByTestId("add-offer-level"));
  fireEvent.click(await screen.findByRole("option", { name: label, hidden: true }));
}

beforeEach(() => {
  api.grantable.mockReset().mockResolvedValue(["analyst", "partner", "reference"]);
  api.add.mockReset().mockResolvedValue({});
  api.access.mockReset().mockImplementation((id) => Promise.resolve(ACCESS[id]));
});

describe("adding an offer", () => {
  it("picks a role from those the owner may grant that reach every dataset", async () => {
    form();
    fireEvent.click(await screen.findByTestId("add-offer-role"));
    const options = (await screen.findAllByRole("option", { hidden: true })).map(
      (o) => o.textContent,
    );
    // partner reads one dataset of the two: not offered.
    const roles = ["analyst", "partner", "reference"];
    expect(options.filter((o) => roles.includes(o ?? ""))).toEqual(["analyst", "reference"]);
    expect(api.access.mock.calls.map((c) => c[0]).sort()).toEqual([1, 2]);
  });

  it("shows the picked role's masks and row rules on each dataset", async () => {
    form();
    await pickRole("analyst");
    const customers = screen.getByTestId("add-offer-role-access-1");
    expect(customers).toHaveTextContent("sales.customers");
    expect(customers).toHaveTextContent("email");
    expect(customers).toHaveTextContent("regex");
    expect(customers).toHaveTextContent("region = 'EU'");
    expect(screen.getByTestId("add-offer-role-access-2")).toHaveTextContent("total");
  });

  it("offers open only when the role reads the data as public", async () => {
    form();
    await pickRole("analyst");
    expect(screen.getByTestId("add-offer-open")).toBeDisabled();
    await pickRole("reference");
    expect(screen.getByTestId("add-offer-open")).toBeEnabled();
  });

  it("names a sample's views, and saves none until one is named", async () => {
    form();
    fireEvent.change(await screen.findByTestId("add-offer-name"), {
      target: { value: "sample" },
    });
    await pickRole("analyst");
    await pickLevel("Sample");
    const save = screen.getByTestId("add-offer-save");
    expect(save).toBeDisabled();
    fireEvent.click(screen.getByRole("textbox", { name: "Sample views" }));
    fireEvent.click(
      await screen.findByRole("option", { name: "sales.customers_sample", hidden: true }),
    );
    expect(save).toBeEnabled();
    expect(screen.getByTestId("add-offer-summary")).toHaveTextContent("a sample through 1 view");
  });

  it("gives a synthetic offer no profiles", async () => {
    form();
    await screen.findByTestId("add-offer-name");
    fireEvent.click(screen.getByTestId("add-offer-profiles"));
    expect(screen.getByTestId("add-offer-profiles")).toBeChecked();
    await pickLevel("Synthetic");
    expect(screen.getByTestId("add-offer-profiles")).not.toBeChecked();
    expect(screen.getByTestId("add-offer-profiles")).toBeDisabled();
  });

  it("says the offer in one sentence as it is filled in", async () => {
    form();
    await pickRole("analyst");
    fireEvent.change(screen.getByTestId("add-offer-duration"), { target: { value: "30" } });
    expect(screen.getByTestId("add-offer-summary")).toHaveTextContent(
      "analyst gets the full data of 2 datasets, with quality results, without profiles. For 30 days. Granted when a bid is accepted.",
    );
  });

  it("saves the offer as filled in", async () => {
    const onSaved = form();
    fireEvent.change(await screen.findByTestId("add-offer-name"), {
      target: { value: "analysts" },
    });
    await pickRole("analyst");
    fireEvent.change(screen.getByTestId("add-offer-duration"), { target: { value: "90" } });
    fireEvent.change(screen.getByTestId("add-offer-usage"), {
      target: { value: "  Monthly churn models  " },
    });
    fireEvent.click(screen.getByTestId("add-offer-save"));
    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    expect(api.add).toHaveBeenCalledWith("c360", {
      name: "analysts",
      role_id: "analyst",
      data_level: "full",
      sample_views: [],
      dq_results: true,
      profiles: false,
      open: false,
      duration_days: 90,
      purpose: "",
      usage: "Monthly churn models",
      limitations: "",
    });
  });

  it("writes the offer's terms of use: its purpose, usage and limitations", async () => {
    form();
    await screen.findByTestId("add-offer-name");
    expect(screen.getByText("Terms of use")).toBeInTheDocument();
    for (const [term, label] of [
      ["purpose", "Purpose"],
      ["usage", "Usage"],
      ["limitations", "Limitations"],
    ]) {
      const field = screen.getByTestId(`add-offer-${term}`);
      expect(screen.getByRole("textbox", { name: label })).toBe(field);
      fireEvent.change(field, { target: { value: `${label} text` } });
      expect(field).toHaveValue(`${label} text`);
    }
    // The terms of use are optional: an offer is saved with a name and a role.
    fireEvent.change(screen.getByTestId("add-offer-name"), { target: { value: "analysts" } });
    await pickRole("analyst");
    expect(screen.getByTestId("add-offer-save")).toBeEnabled();
  });

  it("shows the server's refusal, by name, and stays open", async () => {
    api.add.mockRejectedValueOnce(new Error("role 'analyst' is not one you may grant"));
    const onSaved = form();
    fireEvent.change(await screen.findByTestId("add-offer-name"), {
      target: { value: "analysts" },
    });
    await pickRole("analyst");
    fireEvent.click(screen.getByTestId("add-offer-save"));
    const problem = await screen.findByTestId("form-problem");
    expect(within(problem).getByText(/not one you may grant/)).toBeInTheDocument();
    expect(onSaved).not.toHaveBeenCalled();
  });
});
