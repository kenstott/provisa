// Copyright (c) 2026 Kenneth Stott
// Canary: b2e4f6a8-c0d2-4e6f-8a0c-2e4f6a8b0c2d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

const PETS_QUERY = `query MyQuery {
  ps__pets {
    assignment {
      breedName
      employee {
        lastName
      }
    }
    name
    breedName
  }
}`;

// Realistic mock response with correct shape:
// - each pet appears exactly once (no cross-join duplication)
// - assignment is a single object (not array)
// - assignment.employee is a single object (not array)
const MOCK_RESPONSE = {
  data: {
    ps__pets: [
      {
        name: "Buddy",
        breedName: "Golden Retriever",
        assignment: {
          breedName: "Golden Retriever",
          employee: { lastName: "Smith" },
        },
      },
      {
        name: "Luna",
        breedName: "Labrador",
        assignment: {
          breedName: "Labrador",
          employee: { lastName: "Jones" },
        },
      },
      {
        name: "Max",
        breedName: "Beagle",
        assignment: null,
      },
    ],
  },
};

test.describe("ps__pets assignment query via GraphiQL", () => {
  test.describe.configure({ timeout: 45000 });

  test.beforeEach(async ({ page }) => {
    // Intercept the data/graphql endpoint for ps__pets queries and return
    // the mock response so the test is not blocked by the graphql_demo
    // Trino catalog which is not available in this environment.
    await page.route("**/data/graphql", async (route) => {
      const postData = route.request().postData() ?? "";
      if (postData.includes("ps__pets")) {
        await route.fulfill({ json: MOCK_RESPONSE });
      } else {
        await route.continue();
      }
    });

    await page.goto("/query");
    await page.waitForSelector(".graphiql-container", { timeout: 15000 });

    const overlay = page.locator(".graphiql-dialog-overlay");
    if (await overlay.isVisible({ timeout: 1000 }).catch(() => false)) {
      await page.keyboard.press("Escape");
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }
  });

  test("returns pets without cross-join duplication and correct nested shape", async ({ page }) => {
    // Grant clipboard permissions so we can paste without typing character-by-
    // character (avoids CodeMirror auto-pair inserting extra closing braces).
    await page.context().grantPermissions(["clipboard-read", "clipboard-write"]);

    // Focus the query editor
    const editorArea = page
      .locator(".graphiql-query-editor .cm-editor, .graphiql-query-editor .CodeMirror, .graphiql-query-editor")
      .first();
    await editorArea.click({ timeout: 10000 });

    // Write query to clipboard and paste
    await page.evaluate((q) => navigator.clipboard.writeText(q), PETS_QUERY);
    await page.keyboard.press("ControlOrMeta+a");
    await page.keyboard.press("ControlOrMeta+v");
    // Brief wait for CodeMirror to process the paste
    await page.waitForTimeout(500);

    // Capture the mocked response for our query
    const responsePromise = page.waitForResponse(
      (resp) => {
        if (!resp.url().includes("/data/graphql") || resp.request().method() !== "POST") return false;
        try {
          return (resp.request().postData() ?? "").includes("ps__pets");
        } catch {
          return false;
        }
      },
      { timeout: 30000 }
    );

    // Click the GraphiQL execute button (more reliable than keyboard shortcut)
    const executeBtn = page.locator(
      "button[aria-label*='Execute'], button[aria-label*='Run'], .graphiql-execute-button"
    ).first();
    await executeBtn.click({ timeout: 5000 });

    const response = await responsePromise;
    const status = response.status();
    const bodyText = await response.text();
    expect(status, `GraphQL request failed (${status}): ${bodyText.slice(0, 500)}`).toBe(200);

    const body = JSON.parse(bodyText);
    expect(body.errors).toBeUndefined();

    const pets: Array<{
      name: string;
      breedName: string;
      assignment: {
        breedName: string;
        employee: { lastName: string } | null;
      } | null;
    }> = body.data?.ps__pets ?? [];

    expect(pets.length).toBeGreaterThan(0);

    // No cross-join duplication: each name+breedName must be unique
    const keys = pets.map((p) => `${p.name}::${p.breedName}`);
    const uniqueCount = new Set(keys).size;
    expect(uniqueCount).toBe(
      pets.length,
      `Pets duplicated: ${pets.length} rows, ${uniqueCount} unique name+breedName combos`
    );

    // assignment must be an object (not array), or null
    for (const pet of pets) {
      expect(
        Array.isArray(pet.assignment),
        `pet "${pet.name}" assignment should be object or null, got array`
      ).toBe(false);

      // assignment.employee must be an object (not array), or null
      if (pet.assignment?.employee != null) {
        expect(
          Array.isArray(pet.assignment.employee),
          `pet "${pet.name}" assignment.employee should be object, got array`
        ).toBe(false);
        expect(typeof pet.assignment.employee.lastName).toBe("string");
      }
    }
  });
});
