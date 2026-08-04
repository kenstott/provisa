// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4b7e2f-1c6a-4f3e-b8d5-0a2e7c4f9b13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1323: Metrics page detail-then-edit — row click opens the read-only detail
// panel (source fact + dependent views shown); Edit/Delete live inside the panel.
// REQ-1324: the create/edit dialog composes AGG(fact.column) from the fact-sourced
// three-picker builder; a fact-derived metric prefills the pickers on edit.

import { test, expect, BACKEND_URL } from "./coverage";

const ADMIN_GQL = `${BACKEND_URL}/admin/graphql`;
const FACT_NAME = "e2e_pet_sales";
const FACT_METRIC = `${FACT_NAME}_price_sum`;
const BUILT_METRIC = "e2e_avg_pet_price";

async function gql(query: string, variables: Record<string, unknown> = {}) {
  const res = await fetch(ADMIN_GQL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  return res.json() as Promise<{ data?: Record<string, unknown>; errors?: unknown[] }>;
}

async function cleanup() {
  for (const name of [FACT_METRIC, BUILT_METRIC]) {
    await gql(`mutation D($name: String!) { deleteMetric(name: $name) { success } }`, { name });
  }
  const res = await gql(`{ tables { id tableName } }`);
  const tables = (res.data?.tables ?? []) as Array<{ id: number; tableName: string }>;
  const fact = tables.find((t) => t.tableName === FACT_NAME);
  if (fact) {
    await gql(`mutation D($id: Int!) { deleteTable(id: $id) { success } }`, { id: fact.id });
  }
}

test.beforeAll(async () => {
  await cleanup();
  const res = await gql(
    `mutation R($input: FactInput!) { registerFact(input: $input) { success message } }`,
    {
      input: {
        name: FACT_NAME,
        source: "pet_store.pets",
        domainId: "pet-store",
        grain: ["species"],
        measures: [{ column: "price", agg: "sum" }],
        dimensions: [],
        visibleTo: ["public"],
      },
    },
  );
  const result = res.data?.registerFact as { success: boolean; message: string } | undefined;
  if (!result?.success) {
    throw new Error(`registerFact failed: ${result?.message ?? JSON.stringify(res.errors)}`);
  }
});

test.afterAll(cleanup);

// Demo mode auto-starts the guided tour in a fresh browser profile (App.tsx), which
// navigates away from /metrics — mark it seen before the app boots.
test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem("provisa_tour_seen", "true"));
});

test("REQ-1323: row click opens detail panel with source fact; edit lives inside", async ({
  page,
}) => {
  await page.goto("/metrics");
  const row = page.getByTestId(`metrics-row-${FACT_METRIC}`);
  await expect(row).toBeVisible({ timeout: 15000 });

  // No inline edit affordance on the row — detail-then-edit only.
  await expect(page.getByTestId(`metrics-edit-${FACT_METRIC}`)).toHaveCount(0);

  await row.click();
  const detail = page.getByTestId(`metric-detail-${FACT_METRIC}`);
  await expect(detail).toBeVisible();
  await expect(detail).toContainText(FACT_NAME); // source fact provenance (REQ-1320)

  // Edit from within the detail panel swaps it for the inline form (no modal),
  // prefilled with the builder pickers parsed from the expression.
  await page.getByTestId(`metric-detail-edit-${FACT_METRIC}`).click();
  await expect(page.getByTestId("metric-form")).toBeVisible();
  await expect(page.getByTestId("metric-expression-input")).toHaveValue(
    `SUM(${FACT_NAME}.price)`,
  );
  // MetricsPage derives factTables from the tables query, so the picker stays disabled until that
  // query lands — it returns every registered table with its columns (~200 KB). Asserting the
  // prefilled value straight away reads the still-disabled empty input.
  await expect(page.getByTestId("metric-builder-fact")).toBeEnabled({ timeout: 30000 });
  await expect(page.getByTestId("metric-builder-fact")).toHaveValue(FACT_NAME);
  await expect(page.getByTestId("metric-builder-measure")).toHaveValue("price");
  await expect(page.getByTestId("metric-builder-agg")).toHaveValue("SUM");
});

test("REQ-1324: builder composes AGG(fact.column); delete lives inside detail", async ({
  page,
}) => {
  await page.goto("/metrics");
  await expect(page.getByTestId(`metrics-row-${FACT_METRIC}`)).toBeVisible({ timeout: 15000 });

  await page.getByTestId("metrics-new-button").click();
  await expect(page.getByTestId("metric-create-card")).toBeVisible();

  // Same as the edit path: the picker stays disabled until the tables query lands.
  await expect(page.getByTestId("metric-builder-fact")).toBeEnabled({ timeout: 30000 });
  await page.getByTestId("metric-builder-fact").click();
  await page.getByRole("option", { name: FACT_NAME }).click();
  await page.getByTestId("metric-builder-measure").click();
  await page.getByRole("option", { name: "price" }).click();
  await page.getByTestId("metric-builder-agg").click();
  await page.getByRole("option", { name: "AVG" }).click();
  await expect(page.getByTestId("metric-expression-input")).toHaveValue(
    `AVG(${FACT_NAME}.price)`,
  );
  // REQ-1324: datatype derives from the aggregate.
  await expect(page.getByTestId("metric-datatype-input")).toHaveValue("numeric");

  await page.getByTestId("metric-name-input").fill(BUILT_METRIC);
  await page.getByTestId("metric-save-button").click();
  const newRow = page.getByTestId(`metrics-row-${BUILT_METRIC}`);
  await expect(newRow).toBeVisible({ timeout: 15000 });

  // Delete from within the detail panel (REQ-1323).
  await newRow.click();
  await page.getByTestId(`metric-detail-delete-${BUILT_METRIC}`).click();
  // Assert on the confirm button: Mantine's Modal-root wrapper (which carries the
  // testid) has an empty bounding box, so Playwright reads it as hidden.
  await expect(page.getByTestId("metric-delete-confirm")).toBeVisible();
  await page.getByTestId("metric-delete-confirm").click();
  await expect(page.getByTestId(`metrics-row-${BUILT_METRIC}`)).toHaveCount(0, {
    timeout: 15000,
  });
});
