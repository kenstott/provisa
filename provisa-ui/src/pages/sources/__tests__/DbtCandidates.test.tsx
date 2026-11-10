// Copyright (c) 2026 Kenneth Stott
// Canary: 6e3b1f90-7a24-4c8d-b5e6-03d9c1a84f27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2228: a public, contracted dbt model offered as a data product on the source's setup form.

import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm as render, screen, waitFor } from "../../../test-utils/render";
import { DbtCandidates } from "../DbtCandidates";
import type { DbtCandidate } from "../dbt";

vi.mock("../../../hooks/useAdminQueries", () => ({
  useRoles: () => ({ roles: [{ id: "finance" }, { id: "analyst" }], loading: false }),
}));

const candidate: DbtCandidate = {
  unique_id: "model.jaffle.customer_orders",
  name: "customer_orders",
  description: "One row per customer.",
  version: "2",
  suggested_id: "customer_orders_v2",
  owner: { name: "Finance Analytics", email: "finance@example.com" },
  owner_role: "finance",
  columns: [
    { name: "customer_id", data_type: "integer", description: null, required: true },
    { name: "lifetime_value", data_type: "decimal", description: null, required: false },
  ],
  tests: [{ name: "not_null", column: "customer_id" }],
  table_id: 7,
  products: [],
};

function answers(...bodies: unknown[]) {
  const spy = vi.spyOn(globalThis, "fetch");
  for (const body of bodies) {
    spy.mockResolvedValueOnce(new Response(JSON.stringify(body), { status: 200 }));
  }
  return spy;
}

afterEach(() => vi.restoreAllMocks());

describe("the candidate data products of a dbt source", () => {
  it("says so when the manifest offers none", async () => {
    answers([]);
    render(<DbtCandidates sourceId="dbt-one" />);
    await waitFor(() => expect(screen.getByTestId("dbt-candidates-none")).toBeTruthy());
    expect(screen.queryByTestId("dbt-candidate-accept")).toBeNull();
  });

  it("shows what the manifest carries, with the steward's choices filled in", async () => {
    answers([candidate]);
    render(<DbtCandidates sourceId="dbt-one" />);
    await waitFor(() => expect(screen.getByTestId("dbt-candidate-name")).toBeTruthy());
    const facts = screen.getByTestId("dbt-candidate-facts").textContent ?? "";
    expect(facts).toContain("2");
    expect(facts).toContain("Finance Analytics");
    expect(facts).toContain("customer_id");
    expect(facts).toContain("not_null");
    expect((screen.getByTestId("dbt-candidate-id") as HTMLInputElement).value).toBe(
      "customer_orders_v2",
    );
    expect((screen.getByTestId("dbt-candidate-owner") as HTMLInputElement).value).toBe("finance");
  });

  it("creates nothing until the steward accepts, and sends the steward's choices", async () => {
    const spy = answers([candidate], { data_product: "orders" }, [
      { ...candidate, products: ["orders"] },
    ]);
    render(<DbtCandidates sourceId="dbt-one" />);
    await waitFor(() => expect(screen.getByTestId("dbt-candidate-accept")).toBeTruthy());
    expect(spy).toHaveBeenCalledTimes(1);
    fireEvent.change(screen.getByTestId("dbt-candidate-id"), { target: { value: "orders" } });
    fireEvent.click(screen.getByTestId("dbt-candidate-accept"));
    await waitFor(() => expect(screen.getByTestId("dbt-candidate-became")).toBeTruthy());
    const [url, init] = spy.mock.calls[1];
    expect(url).toBe("/admin/dbt/candidates/dbt-one/accept");
    expect(JSON.parse(String(init?.body))).toEqual({
      unique_id: "model.jaffle.customer_orders",
      product_id: "orders",
      owner_role: "finance",
    });
    expect(screen.getByTestId("dbt-candidate-became").textContent).toContain("orders");
    expect(screen.queryByTestId("dbt-candidate-accept")).toBeNull();
  });

  it("asks for the model to be registered before it can be accepted", async () => {
    answers([{ ...candidate, table_id: null }]);
    render(<DbtCandidates sourceId="dbt-one" />);
    await waitFor(() => expect(screen.getByTestId("dbt-candidate-register-first")).toBeTruthy());
    expect(screen.queryByTestId("dbt-candidate-accept")).toBeNull();
  });

  it("shows what the server refused", async () => {
    const spy = answers([candidate]);
    spy.mockResolvedValueOnce(
      new Response(JSON.stringify({ detail: "The table lacks a column" }), { status: 422 }),
    );
    render(<DbtCandidates sourceId="dbt-one" />);
    await waitFor(() => expect(screen.getByTestId("dbt-candidate-accept")).toBeTruthy());
    fireEvent.click(screen.getByTestId("dbt-candidate-accept"));
    await waitFor(() =>
      expect(screen.getByTestId("dbt-candidate-error").textContent).toContain("lacks a column"),
    );
  });
});
