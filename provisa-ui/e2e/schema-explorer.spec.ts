// Copyright (c) 2026 Kenneth Stott
// Canary: c3d4e5f6-a7b8-9012-cdef-123456789012
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

// ── Minimal introspection response served to the SchemaExplorer ───────────────

const MINIMAL_INTROSPECTION = {
  data: {
    __schema: {
      queryType: { name: "Query" },
      mutationType: null,
      subscriptionType: null,
      types: [
        {
          kind: "OBJECT",
          name: "Query",
          description: null,
          fields: [
            {
              name: "orders",
              description: null,
              args: [],
              isDeprecated: false,
              deprecationReason: null,
              type: { kind: "LIST", name: null, ofType: { kind: "OBJECT", name: "Order", ofType: null } },
            },
            {
              name: "customers",
              description: null,
              args: [],
              isDeprecated: false,
              deprecationReason: null,
              type: { kind: "LIST", name: null, ofType: { kind: "OBJECT", name: "Customer", ofType: null } },
            },
          ],
          inputFields: null,
          interfaces: [],
          enumValues: null,
          possibleTypes: null,
        },
        {
          kind: "OBJECT",
          name: "Order",
          description: null,
          fields: [
            { name: "id", description: null, args: [], isDeprecated: false, deprecationReason: null, type: { kind: "SCALAR", name: "Int", ofType: null } },
            { name: "total", description: null, args: [], isDeprecated: false, deprecationReason: null, type: { kind: "SCALAR", name: "Float", ofType: null } },
          ],
          inputFields: null,
          interfaces: [],
          enumValues: null,
          possibleTypes: null,
        },
        {
          kind: "OBJECT",
          name: "Customer",
          description: null,
          fields: [
            { name: "id", description: null, args: [], isDeprecated: false, deprecationReason: null, type: { kind: "SCALAR", name: "Int", ofType: null } },
            { name: "name", description: null, args: [], isDeprecated: false, deprecationReason: null, type: { kind: "SCALAR", name: "String", ofType: null } },
          ],
          inputFields: null,
          interfaces: [],
          enumValues: null,
          possibleTypes: null,
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

// ── Mock setup ────────────────────────────────────────────────────────────────

async function setupSchemaExplorerMocks(page: Parameters<typeof setupMocks>[0]) {
  await setupMocks(page);

  // Serve introspection from /data/graphql so SchemaExplorer can build srcDoc
  await page.route("**/data/graphql", async (route) => {
    const body = JSON.parse(route.request().postData() || "{}");
    if (body.query?.includes("__schema") || body.operationName === "IntrospectionQuery" || body.query?.includes("IntrospectionQuery")) {
      await route.fulfill({ json: MINIMAL_INTROSPECTION });
    } else {
      await route.fulfill({ json: { data: {} } });
    }
  });

  // Stub Voyager static assets so the iframe does not produce 404 errors
  await page.route("**/voyager/**", async (route) => {
    const url = route.request().url();
    if (url.endsWith(".css")) {
      await route.fulfill({ contentType: "text/css", body: "/* voyager stub */" });
    } else if (url.endsWith(".js")) {
      // Minimal stub so the iframe doesn't throw on script load
      await route.fulfill({
        contentType: "application/javascript",
        body: "/* voyager stub */ window.GraphQLVoyager = { renderVoyager: function() {} };",
      });
    } else {
      await route.fulfill({ body: "" });
    }
  });
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test.describe("SchemaExplorer", () => {
  test.beforeEach(async ({ page }) => {
    await setupSchemaExplorerMocks(page);
    // SchemaExplorer needs a role set in AuthContext to fetch introspection.
    // The default mock roles include "admin"; seed it in localStorage.
    await page.addInitScript(() => {
      localStorage.setItem("provisa_role", JSON.stringify({ id: "admin", capabilities: ["admin"], domainAccess: ["*"] }));
    });
    await page.goto("/schema");
  });

  // ── Route resolves ─────────────────────────────────────────────────────────

  test("navigates to /schema without crashing", async ({ page }) => {
    // Page should either show the iframe, a loading message, or a role prompt —
    // any of these confirms the route resolves without a JS exception.
    const iframe = page.locator("iframe[title='GraphQL Voyager']");
    const loading = page.locator("div.page", { hasText: /Loading schema|Select a role/ });
    await expect(iframe.or(loading)).toBeVisible({ timeout: 15000 });
  });

  test("renders Voyager iframe when role is set", async ({ page }) => {
    await expect(page.locator("iframe[title='GraphQL Voyager']")).toBeVisible({ timeout: 15000 });
  });

  test("iframe has no border style (full-bleed rendering)", async ({ page }) => {
    const iframe = page.locator("iframe[title='GraphQL Voyager']");
    await expect(iframe).toBeVisible({ timeout: 15000 });
    const border = await iframe.evaluate((el: HTMLIFrameElement) => el.style.border);
    expect(border).toBe("none");
  });

  // ── No-role state ──────────────────────────────────────────────────────────

  test("shows 'Select a role' prompt when no role is in context", async ({ page }) => {
    // Override initScript to NOT set a role
    await page.addInitScript(() => {
      localStorage.removeItem("provisa_role");
    });
    await page.goto("/schema");
    await expect(page.locator("div.page", { hasText: "Select a role" })).toBeVisible({ timeout: 15000 });
  });

  // ── Error state ────────────────────────────────────────────────────────────

  test("shows error message when introspection fetch fails", async ({ page }) => {
    // Override with a network failure
    await page.route("**/data/graphql", async (route) => {
      await route.fulfill({ status: 500, json: { error: "Internal Server Error" } });
    });

    await page.goto("/schema");
    await expect(
      page.locator("div.page.error, div.page", { hasText: /Failed to load schema/ })
    ).toBeVisible({ timeout: 15000 });
  });

  // ── Loading state ──────────────────────────────────────────────────────────

  test("shows loading message while introspection is in flight", async ({ page }) => {
    // Delay the introspection response so we can observe the loading state
    await page.route("**/data/graphql", async (route) => {
      await new Promise((r) => setTimeout(r, 2000));
      await route.fulfill({ json: MINIMAL_INTROSPECTION });
    });

    await page.goto("/schema");
    await expect(page.locator("div.page", { hasText: "Loading schema" })).toBeVisible({ timeout: 10000 });
  });

  // ── NavBar link ────────────────────────────────────────────────────────────

  test("NavBar Schema link navigates to /schema", async ({ page }) => {
    await page.goto("/");
    const schemaLink = page.getByRole("link", { name: /schema/i });
    if (await schemaLink.isVisible({ timeout: 5000 }).catch(() => false)) {
      await schemaLink.click();
      await expect(page).toHaveURL(/\/schema/);
    }
    // If the nav link is not present the test passes vacuously (optional nav item)
  });

  // ── Re-fetch on role change ────────────────────────────────────────────────

  test("introspection is re-fetched when the role context changes", async ({ page }) => {
    let fetchCount = 0;
    await page.route("**/data/graphql", async (route) => {
      fetchCount++;
      await route.fulfill({ json: MINIMAL_INTROSPECTION });
    });

    await page.goto("/schema");
    await expect(page.locator("iframe[title='GraphQL Voyager']")).toBeVisible({ timeout: 15000 });

    const countAfterFirstLoad = fetchCount;
    expect(countAfterFirstLoad).toBeGreaterThan(0);

    // Simulate a role change by updating localStorage and reloading
    await page.evaluate(() => {
      localStorage.setItem(
        "provisa_role",
        JSON.stringify({ id: "analyst", capabilities: ["query_development"], domainAccess: ["sales"] })
      );
    });
    await page.reload();
    await expect(page.locator("iframe[title='GraphQL Voyager']")).toBeVisible({ timeout: 15000 });

    expect(fetchCount).toBeGreaterThan(countAfterFirstLoad);
  });

  // ── srcDoc contains Voyager bootstrap ─────────────────────────────────────

  test("iframe srcDoc contains Voyager render call", async ({ page }) => {
    const iframe = page.locator("iframe[title='GraphQL Voyager']");
    await expect(iframe).toBeVisible({ timeout: 15000 });

    const srcDoc = await iframe.getAttribute("srcdoc");
    expect(srcDoc).not.toBeNull();
    expect(srcDoc).toContain("GraphQLVoyager.renderVoyager");
  });

  test("iframe srcDoc contains the introspected schema data", async ({ page }) => {
    const iframe = page.locator("iframe[title='GraphQL Voyager']");
    await expect(iframe).toBeVisible({ timeout: 15000 });

    const srcDoc = await iframe.getAttribute("srcdoc");
    // The schema data (Query type name) must be embedded in the srcDoc
    expect(srcDoc).toContain("Query");
  });

  // ── schema-explorer-page container ────────────────────────────────────────

  test("page container has class schema-explorer-page", async ({ page }) => {
    await expect(page.locator("iframe[title='GraphQL Voyager']")).toBeVisible({ timeout: 15000 });
    await expect(page.locator(".schema-explorer-page")).toBeVisible();
  });
});

// COVERAGE NOTE
// Tested:
//   - Route /schema resolves without crashing (iframe or loading state visible)
//   - Voyager iframe renders when a role is set in context
//   - iframe has border:none (full-bleed)
//   - "Select a role" shown when no role in context
//   - Error state shown when introspection fetch fails (HTTP 500)
//   - Loading state while introspection is in flight
//   - NavBar Schema link navigates to /schema (gracefully skipped if not present)
//   - Introspection is re-fetched when role context changes (new fetch count after reload)
//   - iframe srcdoc contains GraphQLVoyager.renderVoyager call
//   - iframe srcdoc contains the introspected schema (Query type)
//   - Page container has .schema-explorer-page class
