// Copyright (c) 2026 Kenneth Stott
// Canary: 6d2b8f41-a3e7-4c19-9f05-e81c4d7a2b36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect, BACKEND_URL } from "./coverage";
import type { APIRequestContext, Page } from "@playwright/test";
import { isAssignableDomain } from "../src/lib/internalDomains";

/**
 * REQ-2247, REQ-2122: the Context Sync page, driven in a browser against the real server.
 *
 * An entry is connected once: its type, its direction and, for a data catalog, its provider,
 * address, credential and the domains that work with it. What is checked is what the server keeps
 * and answers: the entry comes back after a reload under its type's heading with its credential
 * reported as set and never shown, and the connection test reports the reason the address
 * refused it. The entries here import only, so the run does not depend on the e2e org's plan
 * including publishing (REQ-1073).
 */

test.describe.configure({ mode: "serial", timeout: 120_000 });

const SYNCING = `${BACKEND_URL}/admin/syncing`;

/** Nothing listens on the discard port, so the connection test is refused by the host itself. */
const UNREACHABLE = "http://127.0.0.1:9";

async function domainId(request: APIRequestContext): Promise<string> {
  const answer = await request.post(`${BACKEND_URL}/admin/graphql`, {
    data: { query: "{ domains { id } }" },
  });
  // The picker offers only a domain something may be put into: never meta, ontology or ops.
  const domains = ((await answer.json()) as { data: { domains: { id: string }[] } }).data.domains
    .map((d) => d.id)
    .filter(isAssignableDomain);
  expect(domains.length, "the e2e org has a domain to assign").toBeGreaterThan(0);
  return domains[0];
}

/** Remove ``id`` from the org's entries, keeping every other one as it is (each credential left
 * out of the body is kept). */
async function forget(request: APIRequestContext, id: string) {
  const held = (await (await request.get(SYNCING)).json()) as {
    entries: Record<string, unknown>[];
  };
  const kept = held.entries
    .filter((c) => c.id !== id)
    .map((c) =>
      Object.fromEntries(
        Object.entries(c).filter(
          ([k]) => !k.endsWith("_set") && k !== "last_run" && k !== "last_import",
        ),
      ),
    );
  const answer = await request.put(SYNCING, { data: { entries: kept } });
  expect(answer.ok(), await answer.text()).toBe(true);
}

async function openPage(page: Page) {
  await page.goto("/context/sync");
  await expect(page.getByTestId("syncing-add")).toBeVisible({ timeout: 60000 });
}

async function pick(page: Page, testId: string, option: string) {
  await page.getByTestId(testId).click();
  await page.getByRole("option", { name: option, exact: true }).click();
}

test("a data catalog is connected once, kept with its domains, and its credential never shown", async ({
  page,
  request,
}) => {
  const id = `e2e-catalog-${Date.now().toString(36)}`;
  const domain = await domainId(request);
  try {
    await openPage(page);
    await page.getByTestId("syncing-add").click();
    await page.getByTestId("syncing-name").fill(id);
    await pick(page, "syncing-direction", "Import");
    await pick(page, "syncing-provider", "openmetadata");
    await pick(page, "syncing-domains", domain);
    await page.getByTestId("syncing-endpoint").fill(UNREACHABLE);
    await page.getByTestId("syncing-api-key").fill("typed-key");
    await page.getByTestId("syncing-save").click();
    await expect(page.getByTestId(`syncing-entry-read-${id}`)).toBeVisible({ timeout: 60000 });

    // What the server kept, read back as the page reads it.
    const held = (await (await request.get(SYNCING)).json()) as {
      entries: Record<string, unknown>[];
    };
    const kept = held.entries.find((c) => c.id === id);
    expect(kept).toMatchObject({
      type: "data_catalog",
      direction: "import",
      provider: "openmetadata",
      endpoint: UNREACHABLE,
      domains: [domain],
      api_key_set: true,
    });
    expect(kept && "api_key" in kept).toBe(false);

    await page.reload();
    const row = page.getByTestId("syncing-section-data_catalog").getByTestId(`syncing-row-${id}`);
    await expect(row).toBeVisible({ timeout: 60000 });
    // REQ-2277: the open entry is the URL's ?entry=, so the reload reopens its panel (a click on
    // its row now would close it).
    await expect(page).toHaveURL(new RegExp(`[?&]entry=${id}\\b`));
    await expect(page.getByTestId(`syncing-entry-read-${id}`)).toBeVisible({ timeout: 60000 });
    await page.getByTestId("syncing-edit").click();
    await expect(page.getByTestId("syncing-api-key")).toHaveValue("");
    await expect(page.getByTestId("syncing-api-key")).toHaveAttribute(
      "placeholder",
      "Set. Leave blank to keep.",
    );
    await page.getByTestId("syncing-cancel").click();

    // An entry that only imports offers no publish.
    await expect(page.getByTestId("syncing-publish")).toHaveCount(0);
    // The connection test is the provider's own health check against the address as kept.
    await page.getByTestId("syncing-test").click();
    const health = page.getByTestId("syncing-health");
    await expect(health).toBeVisible({ timeout: 60000 });
    await expect(health).not.toContainText("The catalog accepted the connection.");
    await expect(health).toContainText(/Connect|refused/i);
  } finally {
    await forget(request, id);
  }
});

test("an ontology entry is created and listed under its heading", async ({ page, request }) => {
  const id = `e2e-ontology-${Date.now().toString(36)}`;
  try {
    await openPage(page);
    await page.getByTestId("syncing-add").click();
    await page.getByTestId("syncing-name").fill(id);
    await pick(page, "syncing-type", "Ontology");
    await page.getByTestId("syncing-save").click();

    await expect(
      page.getByTestId("syncing-section-ontology").getByTestId(`syncing-row-${id}`),
    ).toBeVisible({ timeout: 60000 });
    const held = (await (await request.get(SYNCING)).json()) as {
      entries: Record<string, unknown>[];
    };
    expect(held.entries.find((c) => c.id === id)).toMatchObject({
      type: "ontology",
      direction: "import",
    });
  } finally {
    await forget(request, id);
  }
});

test.describe("a refused save", () => {
  // The refusal is the subject: the browser logs the save's 400 however well the page answers it.
  test.use({ allowedBrowserErrors: ["status of 400"] });

  test("a data catalog with no domain is refused when it is saved", async ({ page, request }) => {
    const id = `e2e-catalog-nodomain-${Date.now().toString(36)}`;
    try {
      await openPage(page);
      await page.getByTestId("syncing-add").click();
      await page.getByTestId("syncing-name").fill(id);
      await pick(page, "syncing-direction", "Import");
      await pick(page, "syncing-provider", "openmetadata");
      await page.getByTestId("syncing-endpoint").fill(UNREACHABLE);
      await page.getByTestId("syncing-save").click();

      await expect(page.getByTestId("form-problem")).toContainText("assigned to no domain");
      const held = (await (await request.get(SYNCING)).json()) as { entries: { id: string }[] };
      expect(held.entries.map((c) => c.id)).not.toContain(id);
    } finally {
      await forget(request, id);
    }
  });
});
