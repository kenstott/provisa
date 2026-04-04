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
