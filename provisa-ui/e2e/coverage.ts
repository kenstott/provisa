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
 */
export const test = base.extend({
  page: async ({ page }, use) => {
    await use(page);

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
