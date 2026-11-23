// Copyright (c) 2026 Kenneth Stott
// Canary: 6b9b9909-e48a-4f1f-b960-49a1abaa20a9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1342: the UI's language is the browser's, read on each session start. A regional variant
// (es-MX, es-419) is shown from its base catalog (es); a language with no catalog is shown in
// English; nothing about the language is stored in the browser.

import { test, expect } from "./coverage";
import type { Page } from "@playwright/test";

async function openHome(page: Page) {
  await page.goto("/");
  await expect(page.getByTestId("nav-group-context")).toBeVisible();
}

async function storedLanguageKeys(page: Page): Promise<string[]> {
  return page.evaluate(() =>
    Object.keys(window.localStorage).filter((k) => k === "provisa_lang" || k.startsWith("i18next")),
  );
}

for (const locale of ["es-MX", "es-419"]) {
  test.describe(`browser language ${locale}`, () => {
    test.use({ locale });

    test(`is shown from the Spanish catalog`, async ({ page }) => {
      await openHome(page);
      await expect(page.getByTestId("nav-group-context")).toHaveText("Contexto");
      await expect(page.getByRole("link", { name: "Inicio de Provisa" })).toBeVisible();
      expect(await storedLanguageKeys(page)).toEqual([]);
    });
  });
}

test.describe("browser language with no catalog", () => {
  test.use({ locale: "sw-KE" });

  test("is shown in English", async ({ page }) => {
    await openHome(page);
    await expect(page.getByTestId("nav-group-context")).toHaveText("Context");
    await expect(page.getByRole("link", { name: "Provisa home" })).toBeVisible();
    expect(await storedLanguageKeys(page)).toEqual([]);
  });
});

test.describe("browser language en-US", () => {
  test.use({ locale: "en-US" });

  test("a provisa_lang left in the browser does not choose the language", async ({ page }) => {
    await openHome(page);
    await page.evaluate(() => window.localStorage.setItem("provisa_lang", "es"));
    await openHome(page);
    await expect(page.getByTestId("nav-group-context")).toHaveText("Context");
  });
});
