// Copyright (c) 2025 Kenneth Stott
// Canary: c3ffbb59-49d7-47de-9914-537227d721b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test as base, expect } from "@playwright/test";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const COV_DIR = path.resolve(__dirname, "../.nyc_output/playwright");

/**
 * Extended test fixture that collects Istanbul coverage from the browser
 * after each test and writes it to .nyc_output/playwright/.
 *
 * Also fails any test that triggers an uncaught browser exception (pageerror),
 * e.g. broken ES module imports, runtime crashes. This catches errors that
 * console.error listeners miss.
 */
export const test = base.extend({
  page: async ({ page }, use) => {
    const pageErrors: Error[] = [];
    page.on("pageerror", (err) => pageErrors.push(err));

    await use(page);

    // Filter Monaco/GraphQL worker false positives: these workers sometimes throw
    // bare Event objects (message === "Event") during initialization, which are not
    // real application errors.
    const realErrors = pageErrors.filter((e) => e.message !== "Event");
    if (realErrors.length > 0) {
      throw new Error(
        `Uncaught browser exception(s):\n${realErrors.map((e) => e.message).join("\n")}`
      );
    }

    // Collect __coverage__ from the browser (injected by vite-plugin-istanbul)
    const coverage = await page
      .evaluate(() => (window as any).__coverage__)
      .catch(() => null);

    if (coverage) {
      fs.mkdirSync(COV_DIR, { recursive: true });
      const id = crypto.randomBytes(8).toString("hex");
      fs.writeFileSync(
        path.join(COV_DIR, `coverage-${id}.json`),
        JSON.stringify(coverage)
      );
    }
  },
});

export { expect };
