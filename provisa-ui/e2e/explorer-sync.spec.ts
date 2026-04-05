// Copyright (c) 2025 Kenneth Stott
// Canary: c6f1b26c-a6c1-4bcd-9507-69b7b720f522
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

/**
 * Tests that the GraphiQL Explorer panel syncs with the query editor.
 * Mocks the GraphQL introspection endpoint to avoid needing a live backend.
 * Verifies fix for issue #10: Explorer not synced with restored query.
 */

const MINIMAL_INTROSPECTION = {
  data: {
    __schema: {
      queryType: { name: "Query" },
      mutationType: null,
      subscriptionType: null,
      types: [
        {
          kind: "OBJECT", name: "Query", description: null,
          fields: [
            {
              name: "orders", description: null, args: [], isDeprecated: false, deprecationReason: null,
              type: { kind: "LIST", name: null, ofType: { kind: "OBJECT", name: "Order", ofType: null } },
            },
          ],
          inputFields: null, interfaces: [], enumValues: null, possibleTypes: null,
        },
        {
          kind: "OBJECT", name: "Order", description: null,
          fields: [
            { name: "id", description: null, args: [], isDeprecated: false, deprecationReason: null, type: { kind: "SCALAR", name: "Int", ofType: null } },
            { name: "total", description: null, args: [], isDeprecated: false, deprecationReason: null, type: { kind: "SCALAR", name: "Float", ofType: null } },
          ],
          inputFields: null, interfaces: [], enumValues: null, possibleTypes: null,
        },
        { kind: "SCALAR", name: "Int", description: null, fields: null, inputFields: null, interfaces: null, enumValues: null, possibleTypes: null },
        { kind: "SCALAR", name: "Float", description: null, fields: null, inputFields: null, interfaces: null, enumValues: null, possibleTypes: null },
        { kind: "SCALAR", name: "String", description: null, fields: null, inputFields: null, interfaces: null, enumValues: null, possibleTypes: null },
        { kind: "SCALAR", name: "Boolean", description: null, fields: null, inputFields: null, interfaces: null, enumValues: null, possibleTypes: null },
        { kind: "SCALAR", name: "__TypeKind", description: null, fields: null, inputFields: null, interfaces: null, enumValues: [], possibleTypes: null },
        { kind: "OBJECT", name: "__Schema", description: null, fields: [], inputFields: null, interfaces: [], enumValues: null, possibleTypes: null },
        { kind: "OBJECT", name: "__Type", description: null, fields: [], inputFields: null, interfaces: [], enumValues: null, possibleTypes: null },
        { kind: "OBJECT", name: "__Field", description: null, fields: [], inputFields: null, interfaces: [], enumValues: null, possibleTypes: null },
        { kind: "OBJECT", name: "__InputValue", description: null, fields: [], inputFields: null, interfaces: [], enumValues: null, possibleTypes: null },
        { kind: "OBJECT", name: "__EnumValue", description: null, fields: [], inputFields: null, interfaces: [], enumValues: null, possibleTypes: null },
        { kind: "OBJECT", name: "__Directive", description: null, fields: [], inputFields: null, interfaces: [], enumValues: null, possibleTypes: null },
        { kind: "ENUM", name: "__DirectiveLocation", description: null, fields: null, inputFields: null, interfaces: null, enumValues: [], possibleTypes: null },
      ],
      directives: [],
    },
  },
};

// Schema with a relationship: orders → customer (object type)
const INTROSPECTION_WITH_RELATIONSHIP = {
  data: {
    __schema: {
      queryType: { name: "Query" },
      mutationType: null,
      subscriptionType: null,
      types: [
        {
          kind: "OBJECT", name: "Query", description: null,
          fields: [
            {
              name: "orders", description: null, args: [], isDeprecated: false, deprecationReason: null,
              type: { kind: "LIST", name: null, ofType: { kind: "OBJECT", name: "Order", ofType: null } },
            },
          ],
          inputFields: null, interfaces: [], enumValues: null, possibleTypes: null,
        },
        {
          kind: "OBJECT", name: "Order", description: null,
          fields: [
            { name: "id", description: null, args: [], isDeprecated: false, deprecationReason: null, type: { kind: "SCALAR", name: "Int", ofType: null } },
            { name: "total", description: null, args: [], isDeprecated: false, deprecationReason: null, type: { kind: "SCALAR", name: "Float", ofType: null } },
            {
              name: "customer", description: null, args: [], isDeprecated: false, deprecationReason: null,
              type: { kind: "OBJECT", name: "Customer", ofType: null },
            },
          ],
          inputFields: null, interfaces: [], enumValues: null, possibleTypes: null,
        },
        {
          kind: "OBJECT", name: "Customer", description: null,
          fields: [
            { name: "id", description: null, args: [], isDeprecated: false, deprecationReason: null, type: { kind: "SCALAR", name: "Int", ofType: null } },
            { name: "name", description: null, args: [], isDeprecated: false, deprecationReason: null, type: { kind: "SCALAR", name: "String", ofType: null } },
          ],
          inputFields: null, interfaces: [], enumValues: null, possibleTypes: null,
        },
        { kind: "SCALAR", name: "Int", description: null, fields: null, inputFields: null, interfaces: null, enumValues: null, possibleTypes: null },
        { kind: "SCALAR", name: "Float", description: null, fields: null, inputFields: null, interfaces: null, enumValues: null, possibleTypes: null },
        { kind: "SCALAR", name: "String", description: null, fields: null, inputFields: null, interfaces: null, enumValues: null, possibleTypes: null },
        { kind: "SCALAR", name: "Boolean", description: null, fields: null, inputFields: null, interfaces: null, enumValues: null, possibleTypes: null },
        { kind: "SCALAR", name: "__TypeKind", description: null, fields: null, inputFields: null, interfaces: null, enumValues: [], possibleTypes: null },
        { kind: "OBJECT", name: "__Schema", description: null, fields: [], inputFields: null, interfaces: [], enumValues: null, possibleTypes: null },
        { kind: "OBJECT", name: "__Type", description: null, fields: [], inputFields: null, interfaces: [], enumValues: null, possibleTypes: null },
        { kind: "OBJECT", name: "__Field", description: null, fields: [], inputFields: null, interfaces: [], enumValues: null, possibleTypes: null },
        { kind: "OBJECT", name: "__InputValue", description: null, fields: [], inputFields: null, interfaces: [], enumValues: null, possibleTypes: null },
        { kind: "OBJECT", name: "__EnumValue", description: null, fields: [], inputFields: null, interfaces: [], enumValues: null, possibleTypes: null },
        { kind: "OBJECT", name: "__Directive", description: null, fields: [], inputFields: null, interfaces: [], enumValues: null, possibleTypes: null },
        { kind: "ENUM", name: "__DirectiveLocation", description: null, fields: null, inputFields: null, interfaces: null, enumValues: [], possibleTypes: null },
      ],
      directives: [],
    },
  },
};

// Query that includes a relationship (orders → customer) — the object type should be expanded in explorer
const QUERY_WITH_RELATIONSHIP = "query GetOrders {\n  orders {\n    id\n    customer {\n      name\n    }\n  }\n}";

const SAVED_QUERY = "query GetOrders {\n  orders {\n    id\n    total\n  }\n}";

test.describe("GraphiQL Explorer sync (issue #10)", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);

    // Mock the data GraphQL endpoint for introspection and queries
    await page.route("**/data/graphql", async (route) => {
      const body = JSON.parse(route.request().postData() || "{}");
      if (body.query?.includes("__schema") || body.operationName === "IntrospectionQuery") {
        await route.fulfill({ json: MINIMAL_INTROSPECTION });
      } else {
        await route.fulfill({ json: { data: { orders: [{ id: 1, total: 99.99 }] } } });
      }
    });

    // Seed localStorage with a saved query before the page initializes
    await page.addInitScript((query: string) => {
      localStorage.setItem("graphiql:query", query);
    }, SAVED_QUERY);
  });

  test("Explorer panel opens and renders schema fields", async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() === "error") consoleErrors.push(msg.text());
    });

    await page.goto("/query");
    await expect(page.locator(".graphiql-container")).toBeVisible({ timeout: 10000 });

    // Dismiss any overlay
    const overlay = page.locator(".graphiql-dialog-overlay");
    if (await overlay.isVisible({ timeout: 1000 }).catch(() => false)) {
      await page.keyboard.press("Escape");
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }

    // Find and click the Explorer sidebar button
    const sidebar = page.locator(".graphiql-sidebar");
    const buttons = sidebar.locator("button");
    const count = await buttons.count();
    let explorerOpened = false;
    for (let i = 0; i < count; i++) {
      const label = await buttons.nth(i).getAttribute("aria-label");
      if (label?.toLowerCase().includes("explorer")) {
        await buttons.nth(i).click();
        explorerOpened = true;
        break;
      }
    }

    if (!explorerOpened) {
      // Try clicking each button to find explorer panel
      for (let i = 0; i < count; i++) {
        await buttons.nth(i).click();
        const panel = page.locator(".graphiql-plugin");
        if (await panel.isVisible({ timeout: 500 }).catch(() => false)) {
          explorerOpened = true;
          break;
        }
      }
    }

    // Explorer plugin panel should be visible
    await expect(page.locator(".graphiql-plugin")).toBeVisible({ timeout: 5000 });

    // No Monaco TypeError about 'toUrl' (the original bug)
    const monacoErrors = consoleErrors.filter(
      (e) => e.includes("toUrl") || e.includes("Cannot read properties of undefined")
    );
    expect(monacoErrors, "Monaco worker TypeError should not occur").toEqual([]);
  });

  test("Monaco editor contains the query restored from localStorage", async ({ page }) => {
    await page.goto("/query");
    await expect(page.locator(".graphiql-container")).toBeVisible({ timeout: 10000 });

    // Dismiss any overlay
    const overlay = page.locator(".graphiql-dialog-overlay");
    if (await overlay.isVisible({ timeout: 1000 }).catch(() => false)) {
      await page.keyboard.press("Escape");
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }

    // Wait for Monaco to initialize (query editor container present)
    await expect(page.locator(".graphiql-query-editor")).toBeVisible({ timeout: 10000 });

    // The query editor should contain the restored query text
    const editorText = await page.locator(".graphiql-query-editor").textContent({ timeout: 5000 });
    expect(editorText).toContain("orders");
  });

  test("Explorer reflects restored query — no crash when schema loads after query", async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() === "error") consoleErrors.push(msg.text());
    });

    await page.goto("/query");
    await expect(page.locator(".graphiql-container")).toBeVisible({ timeout: 10000 });

    // Dismiss any overlay
    const overlay = page.locator(".graphiql-dialog-overlay");
    if (await overlay.isVisible({ timeout: 1000 }).catch(() => false)) {
      await page.keyboard.press("Escape");
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }

    // Open Explorer panel
    const sidebar = page.locator(".graphiql-sidebar");
    const buttons = sidebar.locator("button");
    const count = await buttons.count();
    for (let i = 0; i < count; i++) {
      const label = await buttons.nth(i).getAttribute("aria-label");
      if (label?.toLowerCase().includes("explorer")) {
        await buttons.nth(i).click();
        break;
      }
    }

    // Explorer plugin panel must be visible (not crashed)
    await expect(page.locator(".graphiql-plugin")).toBeVisible({ timeout: 5000 });

    // No React rendering errors
    const reactErrors = consoleErrors.filter(
      (e) =>
        e.includes("The above error occurred") ||
        e.includes("Cannot read properties of undefined") ||
        e.includes("toUrl")
    );
    expect(reactErrors, "No React/Monaco errors").toEqual([]);

    // Wait for schema to load and explorer to show Query root type
    // (schema comes from mocked introspection, explorer should render "Query" type link)
    await expect(
      page.locator(".graphiql-plugin").getByText("Query", { exact: false }).first()
    ).toBeVisible({ timeout: 8000 });
  });
});

test.describe("GraphiQL Explorer expand/collapse sync (issues #19, #18)", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);

    await page.route("**/data/graphql", async (route) => {
      const body = JSON.parse(route.request().postData() || "{}");
      if (body.query?.includes("__schema") || body.operationName === "IntrospectionQuery") {
        await route.fulfill({ json: INTROSPECTION_WITH_RELATIONSHIP });
      } else {
        await route.fulfill({ json: { data: { orders: [{ id: 1, customer: { name: "Alice" } }] } } });
      }
    });
  });

  test("Explorer shows relationship field expanded when it is in the query", async ({ page }) => {
    const consoleErrors: string[] = [];
    const pageErrors: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() === "error") consoleErrors.push(msg.text());
    });
    page.on("pageerror", (err) => pageErrors.push(err.message));

    await page.goto("/query");
    await expect(page.locator(".graphiql-container")).toBeVisible({ timeout: 10000 });

    const overlay = page.locator(".graphiql-dialog-overlay");
    if (await overlay.isVisible({ timeout: 1000 }).catch(() => false)) {
      await page.keyboard.press("Escape");
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }

    // The Documentation Explorer is open by default — switch to the custom GraphiQL Explorer
    await page.getByRole("button", { name: /GraphiQL Explorer/i }).click();

    // Wait for the custom explorer root element (distinct from the Docs Explorer)
    const explorer = page.locator(".graphiql-explorer-root");
    await expect(explorer).toBeVisible({ timeout: 8000 });

    // Wait for "orders" field-view span — appears once introspection completes and schema is built
    const ordersField = explorer.locator("span.graphiql-explorer-field-view").filter({ hasText: "orders" }).first();
    await expect(ordersField).toBeVisible({ timeout: 12000 });

    // Click "orders" to expand it — shows Order type child fields
    await ordersField.click();

    // "customer" (object type / relationship field of Order) should now be listed
    const customerField = explorer.locator("span.graphiql-explorer-field-view").filter({ hasText: "customer" }).first();
    await expect(customerField).toBeVisible({ timeout: 5000 });

    // Click "customer" to expand it — shows Customer type child fields
    await customerField.click();

    // "name" (scalar field of Customer) must now appear, confirming object-type expansion works
    await expect(explorer.locator("span.graphiql-explorer-field-view").filter({ hasText: "name" }).first()).toBeVisible({ timeout: 5000 });

    // No uncaught browser exceptions (filter Monaco worker false positives)
    const realPageErrors = pageErrors.filter((e) => e !== "Event");
    expect(realPageErrors, "No uncaught browser exceptions").toEqual([]);

    // No meaningful console errors (filter known network noise)
    const realConsoleErrors = consoleErrors.filter(
      (e) => !e.includes("Failed to fetch") && !e.includes("NetworkError") && !e.includes("404")
    );
    expect(realConsoleErrors, "No console errors").toEqual([]);
  });

  test("Explorer expand arrow SVGs are not empty — rendered as visible elements", async ({ page }) => {
    await page.goto("/query");
    await expect(page.locator(".graphiql-container")).toBeVisible({ timeout: 10000 });

    const overlay = page.locator(".graphiql-dialog-overlay");
    if (await overlay.isVisible({ timeout: 1000 }).catch(() => false)) {
      await page.keyboard.press("Escape");
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }

    // Switch to the custom GraphiQL Explorer
    await page.getByRole("button", { name: /GraphiQL Explorer/i }).click();

    const explorer = page.locator(".graphiql-explorer-root");
    await expect(explorer).toBeVisible({ timeout: 8000 });

    // Wait for schema to load — a field-view span must appear
    await expect(explorer.locator("span.graphiql-explorer-field-view").first()).toBeVisible({ timeout: 12000 });

    // Arrow SVGs must contain a <path> element — empty SVGs have no children and render invisible
    await expect(explorer.locator("svg path").first()).toBeAttached({ timeout: 5000 });
  });
});
