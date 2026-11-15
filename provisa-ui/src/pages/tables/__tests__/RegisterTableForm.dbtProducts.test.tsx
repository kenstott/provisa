// Copyright (c) 2026 Kenneth Stott
// Canary: 9e2c5a17-4f80-4b3d-a6e1-73d0b8c24f95
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** REQ-2228: on Register Table a dbt source offers, beside picking tables, to register all its
 *  public models with enforced contracts as draft data products, after a preview. */

import { afterEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../../test-utils/render";
import type { DbtProductsPreview } from "../dbtProducts";

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["analytics"], loading: false }),
  useAvailableTables: () => ({ tables: [{ name: "customer_orders" }], loading: false }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const model = (name: string, version: string | null, tableId: number | null = null) => ({
  unique_id: `model.jaffle.${name}`,
  name,
  schema: "analytics",
  relation: name,
  version,
  description: null,
  table_id: tableId,
  columns: [
    { name: "id", data_type: "integer", required: true },
    { name: "note", data_type: null, required: false },
  ],
});

const GROUPED: DbtProductsPreview = {
  products: [
    {
      id: "finance",
      group: "finance",
      owner: { name: "Finance Analytics", email: "finance@example.com" },
      owner_role: null,
      exists: false,
      tests: ["customer_orders.not_null(id)"],
      models: [model("customer_orders", "2")],
    },
    {
      id: "ops",
      group: "ops",
      owner: null,
      owner_role: null,
      exists: false,
      tests: [],
      models: [model("order_events", null, 7)],
    },
  ],
  ungrouped: ["stg_orders"],
};

/** The dbt products endpoint answers ``preview`` to a read and ``registered`` to a POST; every
 *  other request the form makes goes on as it would. */
function answers(preview: unknown, registered?: unknown) {
  const passOn = globalThis.fetch;
  const calls: [string, RequestInit | undefined][] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation((input, init) => {
    const url = String(input);
    if (!url.startsWith("/admin/dbt/products/")) return passOn(input, init);
    calls.push([url, init]);
    const body = init?.method === "POST" ? registered : preview;
    return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
  });
  return calls;
}

afterEach(() => vi.restoreAllMocks());

const SOURCES = [
  { id: "dbt-jaffle", type: "dbt", allowedDomains: [] },
  { id: "pg", type: "postgresql", allowedDomains: [] },
];

function renderForm() {
  const onProductsRegistered = vi.fn();
  const onContinue = vi.fn();
  render(
    <RegisterTableForm
      sources={SOURCES as never}
      domainHints={["sales", "marketing"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled
      tables={[]}
      roles={[{ id: "analyst" } as never]}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      onContinue={onContinue}
      onCancel={() => {}}
      onProductsRegistered={onProductsRegistered}
      onRegisteredMany={vi.fn()}
      setError={vi.fn()}
    />,
  );
  return { onProductsRegistered, onContinue, user: userEvent.setup() };
}

async function productsMode(user: ReturnType<typeof userEvent.setup>) {
  await user.selectOptions(screen.getByTestId("register-table-source-select"), "dbt-jaffle");
  await user.click(screen.getByText("Public models as data products"));
}

describe("registering a dbt source's public models as data products", () => {
  it("is offered on a dbt source and not on another", async () => {
    const { user } = renderForm();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "pg");
    expect(screen.queryByTestId("register-table-dbt-mode")).toBeNull();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "dbt-jaffle");
    expect(screen.getByTestId("register-table-dbt-mode")).toBeInTheDocument();
    // Picking tables stays the default, as it is on every source: the Tables field registers them.
    expect(screen.getByRole("textbox", { name: "Tables" })).toBeInTheDocument();
    expect(screen.getByTestId("register-many-tables-submit")).toBeInTheDocument();
  });

  it("previews one product per group, with its models, and creates nothing", async () => {
    const calls = answers(GROUPED);
    const { user } = renderForm();
    await productsMode(user);
    await waitFor(() => expect(screen.getAllByTestId("dbt-product")).toHaveLength(2));
    expect(calls).toEqual([["/admin/dbt/products/dbt-jaffle", undefined]]);
    const ids = screen.getAllByTestId("dbt-product-id").map((e) => e.textContent);
    expect(ids).toEqual(["Data product finance (dbt group)", "Data product ops (dbt group)"]);
    expect(screen.getByText(/Owner: Finance Analytics/)).toBeInTheDocument();
    expect(screen.getByText(/Tests: customer_orders.not_null\(id\)/)).toBeInTheDocument();
    const models = screen.getAllByTestId("dbt-product-model").map((e) => e.textContent);
    expect(models[0]).toContain("analytics.customer_orders");
    expect(models[0]).toContain("Version 2");
    expect(models[0]).toContain("id (required), note");
    expect(models[1]).toContain("already registered");
    expect(screen.getByTestId("dbt-products-ungrouped").textContent).toContain("stg_orders");
    // The table pick gives way to the preview.
    expect(screen.queryByTestId("register-table-alias")).toBeNull();
    expect(screen.queryByTestId("register-table-submit")).toBeNull();
    expect(screen.getByTestId("register-table-dbt-products-submit").textContent).toBe(
      "Register and create 2 draft products",
    );
  });

  it("previews a project with no groups as one product", async () => {
    answers({
      products: [{ ...GROUPED.products[0], id: "jaffle", group: null, owner: null }],
      ungrouped: [],
    });
    const { user } = renderForm();
    await productsMode(user);
    await waitFor(() =>
      expect(screen.getByTestId("dbt-product-id").textContent).toBe(
        "Data product jaffle (the whole project)",
      ),
    );
    expect(screen.getByTestId("register-table-dbt-products-submit").textContent).toBe(
      "Register and create 1 draft product",
    );
  });

  it("registers them on confirm, in the chosen domain, and says which drafts were made", async () => {
    const calls = answers(GROUPED, { data_products: ["finance", "ops"], domain_id: "sales" });
    const { user, onProductsRegistered, onContinue } = renderForm();
    await productsMode(user);
    await waitFor(() => expect(screen.getAllByTestId("dbt-product")).toHaveLength(2));
    const confirm = screen.getByTestId("register-table-dbt-products-submit");
    expect(confirm).toBeDisabled(); // no domain yet
    await user.selectOptions(screen.getByTestId("register-table-domain-select"), "sales");
    expect(confirm).toBeEnabled();
    await user.click(confirm);
    await waitFor(() => expect(onProductsRegistered).toHaveBeenCalledWith(["finance", "ops"]));
    expect(calls).toHaveLength(2);
    const [url, init] = calls[1];
    expect(url).toBe("/admin/dbt/products/dbt-jaffle");
    expect(init?.method).toBe("POST");
    expect(JSON.parse(String(init?.body))).toEqual({ domain_id: "sales" });
    expect(onContinue).not.toHaveBeenCalled();
  });

  it("cannot be confirmed while a product's id is in use", async () => {
    answers({
      ...GROUPED,
      products: [{ ...GROUPED.products[0], exists: true }, GROUPED.products[1]],
    });
    const { user } = renderForm();
    await productsMode(user);
    await user.selectOptions(screen.getByTestId("register-table-domain-select"), "sales");
    await waitFor(() => expect(screen.getByTestId("dbt-product-exists")).toBeInTheDocument());
    expect(screen.getByTestId("register-table-dbt-products-submit")).toBeDisabled();
  });

  it("says so when the manifest offers no public model, and cannot be confirmed", async () => {
    answers({ products: [], ungrouped: [] });
    const { user } = renderForm();
    await productsMode(user);
    await user.selectOptions(screen.getByTestId("register-table-domain-select"), "sales");
    await waitFor(() => expect(screen.getByTestId("dbt-products-none")).toBeInTheDocument());
    expect(screen.getByTestId("register-table-dbt-products-submit")).toBeDisabled();
  });
});
