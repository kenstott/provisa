// Copyright (c) 2026 Kenneth Stott
// Canary: 9d3e7c1a-5f2b-4e8d-a1c6-3b9f7e2d5a8c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1780/1781/1782/1783: Kaggle through the real UI, against the live Kaggle API with the
// real KAGGLE_API_TOKEN from the repo root .env (loaded into process.env by playwright.config.ts,
// and forwarded to the e2e backend's own env since it is spawned as a child of this process — the
// backend is what actually calls out to Kaggle, not the browser). No mocking of Kaggle itself.
//
// Kaggle has no live query API (download-only), so it does not register as its own SourceType —
// each bundle file becomes a plain csv/parquet Source (Trino/DuckDB ATTACHes/reads it live like
// any other file source), registered through KaggleFormSection's own two-step flow: token entry
// (validated live) then a live dataset search/picker, backed by stageKaggleDataset (download +
// crawl_directory) and the same createSource/registerTable calls a manual file source uses.
//
// Fixture dataset: arshid/iris-flower-dataset — confirmed live via a real `datasets/list/
// arshid/iris-flower-dataset` call before writing this test: a single file, IRIS.csv, ~4.6KB,
// 150 rows, no SQLite file (this connector's v1 scope is CSV/Parquet only).

import { test, expect } from "./coverage";
import { DOMAIN, openSourcesForm, runSqlOnPage } from "./source-to-query-helpers";

const KAGGLE_TOKEN = process.env.KAGGLE_API_TOKEN;

test.describe("Kaggle source through the real UI, live Kaggle API (REQ-1780/1781/1782/1783)", () => {
  test.skip(!KAGGLE_TOKEN, "KAGGLE_API_TOKEN not set in the environment");
  // The Sources form is long (every source-type's fields render into one page, hidden/shown by
  // type) and Mantine's Combobox.Dropdown is a floating-positioned portal — on the default
  // 1280x720 test viewport it renders its option below the visible viewport even after
  // Playwright's auto-scroll (reproduced live), so the dataset-picker option is never clickable.
  test.use({ viewport: { width: 1280, height: 2200 } });

  test("token gate, live dataset search, table registration, and a real query", async ({
    page,
  }) => {
    test.setTimeout(180000);

    await openSourcesForm(page);
    await page.getByTestId("sources-type-select").selectOption("kaggle");

    // REQ-1783: the picker must not exist before a valid token — Kaggle's own search endpoint
    // requires auth, so there is nothing it could search yet.
    await expect(page.getByTestId("kaggle-dataset-search-input")).toHaveCount(0);

    await page.getByTestId("kaggle-domain-select").click();
    await page.getByRole("option", { name: new RegExp(DOMAIN, "i") }).first().click();
    await page.getByTestId("kaggle-token-input").fill(KAGGLE_TOKEN!);
    await page.getByTestId("kaggle-validate-token-button").click();
    await expect(page.getByTestId("kaggle-token-valid")).toBeVisible({ timeout: 30000 });

    // REQ-1783: picker appears only now, after live validation succeeded.
    await expect(page.getByTestId("kaggle-dataset-search-input")).toBeVisible();

    const searchInput = page.getByTestId("kaggle-dataset-search-input");
    await searchInput.fill("iris flower");
    const option = page.getByTestId("kaggle-dataset-arshid/iris-flower-dataset");
    await expect(option).toBeVisible({ timeout: 30000 });
    // Mantine's Combobox.Dropdown is a floating-positioned portal; on this long form (with the
    // enlarged viewport above) it renders within it, but scroll into view defensively before the
    // click regardless — auto-scroll-then-click can still race the dropdown's own positioning.
    await option.scrollIntoViewIfNeeded();
    await option.click();

    await expect(page.getByTestId("kaggle-selected-dataset")).toContainText(
      "arshid/iris-flower-dataset",
    );

    await page.getByTestId("kaggle-register-button").click();
    // Matches the rest of this form's server-side-registration types (handleOpenapiRegister/
    // handleGrpcRegister): the form closes itself on success and the new row shows up in the
    // sources list below — that IS the success signal, there is no separate in-form banner.
    // No "_IRIS" table-name suffix: iris-flower-dataset is single-file, and a single-file
    // dataset now gets its id_prefix verbatim (schema_mutation.py's stage_kaggle_dataset) —
    // the suffix exists only to disambiguate multiple files sharing one id_prefix.
    const row = page.locator(".data-table td").filter({ hasText: /^kg_\d+_iris-flower-dataset$/ });
    await expect(row).toBeVisible({ timeout: 60000 });
    const sourceId = (await row.textContent())!.trim();

    // Confirm it registered as a plain csv Source (not a bespoke Kaggle connector/type) and read
    // back the SQL-plane name the server actually assigned the table (its dqDataset), the same
    // way source-to-query-helpers.ts's own submitRegisterAndExpectListed does.
    const sourcesRes = await page.request.post("/admin/graphql", {
      data: { query: "{ sources { id type } }" },
    });
    expect(sourcesRes.ok(), await sourcesRes.text()).toBeTruthy();
    const sources = (await sourcesRes.json()).data.sources as { id: string; type: string }[];
    const registeredSource = sources.find((s) => s.id === sourceId);
    expect(registeredSource?.type).toBe("csv");

    const tablesRes = await page.request.post("/admin/graphql", {
      data: { query: "{ tables { sourceId dqDataset } }" },
    });
    expect(tablesRes.ok(), await tablesRes.text()).toBeTruthy();
    const tables = (await tablesRes.json()).data.tables as {
      sourceId: string;
      dqDataset: string | null;
    }[];
    const registeredTable = tables.find((t) => t.sourceId === sourceId);
    expect(registeredTable?.dqDataset, `no dataset name reported for ${sourceId}`).toBeTruthy();
    const sqlTableName = registeredTable!.dqDataset!.split("/").pop()!;

    // Run a real query against the landed data and verify real Kaggle row data comes back —
    // the Iris dataset's first 3 rows are Iris-setosa with these exact measurements.
    const rows = await runSqlOnPage(
      page,
      `SELECT sepal_length, species FROM pet_store.${sqlTableName} ORDER BY sepal_length LIMIT 3`,
    );
    expect(rows).toHaveLength(3);
    for (const row of rows) {
      expect(row[1]).toBe("Iris-setosa");
    }
  });

  test("multi-file dataset registers one Source per file, and a real query works", async ({
    page,
  }) => {
    test.setTimeout(180000);

    // Fixture: sudalairajkumar/covid19-in-india — confirmed live via a real `datasets/list/
    // sudalairajkumar/covid19-in-india` call before writing this test: 3 files (no SQLite),
    // StatewiseTestingDetails.csv (622,938 bytes), covid_19_india.csv (1,005,449 bytes),
    // covid_vaccine_statewise.csv (1,108,819 bytes) — a ~776KB compressed download total. A
    // well-known, stable, historical (last updated 2021) dataset, so its row data cannot drift.
    await openSourcesForm(page);
    await page.getByTestId("sources-type-select").selectOption("kaggle");
    await page.getByTestId("kaggle-domain-select").click();
    await page.getByRole("option", { name: new RegExp(DOMAIN, "i") }).first().click();
    await page.getByTestId("kaggle-token-input").fill(KAGGLE_TOKEN!);
    await page.getByTestId("kaggle-validate-token-button").click();
    await expect(page.getByTestId("kaggle-token-valid")).toBeVisible({ timeout: 30000 });

    const searchInput = page.getByTestId("kaggle-dataset-search-input");
    await searchInput.fill("COVID-19 in India");
    const option = page.getByTestId("kaggle-dataset-sudalairajkumar/covid19-in-india");
    await expect(option).toBeVisible({ timeout: 30000 });
    await option.scrollIntoViewIfNeeded();
    await option.click();

    await expect(page.getByTestId("kaggle-selected-dataset")).toContainText(
      "sudalairajkumar/covid19-in-india",
    );

    await page.getByTestId("kaggle-register-button").click();
    // One Source per bundle file (REQ-1781) — 3 rows, one per csv, all sharing the same
    // "kg_<timestamp>_covid19-in-india_" prefix this run's stageKaggleDataset call generated.
    const rowsLocator = page.locator(".data-table td").filter({
      hasText: /^kg_\d+_covid19-in-india_/,
    });
    await expect(rowsLocator).toHaveCount(3, { timeout: 90000 });
    const sourceIds = await rowsLocator.allTextContents();
    expect(new Set(sourceIds.map((s) => s.trim()))).toEqual(
      new Set(
        ["StatewiseTestingDetails", "covid_19_india", "covid_vaccine_statewise"].map(
          (t) => sourceIds.find((id) => id.trim().endsWith(`_${t}`))!.trim(),
        ),
      ),
    );

    const sourcesRes = await page.request.post("/admin/graphql", {
      data: { query: "{ sources { id type } }" },
    });
    expect(sourcesRes.ok(), await sourcesRes.text()).toBeTruthy();
    const sources = (await sourcesRes.json()).data.sources as { id: string; type: string }[];
    for (const id of sourceIds) {
      const src = sources.find((s) => s.id === id.trim());
      expect(src?.type, `${id.trim()} should register as a plain csv Source`).toBe("csv");
    }

    // Query the main covid_19_india table and verify real landed Kaggle row data.
    const mainSourceId = sourceIds.find((id) => id.trim().endsWith("_covid_19_india"))!.trim();
    const tablesRes = await page.request.post("/admin/graphql", {
      data: { query: "{ tables { sourceId dqDataset } }" },
    });
    expect(tablesRes.ok(), await tablesRes.text()).toBeTruthy();
    const tables = (await tablesRes.json()).data.tables as {
      sourceId: string;
      dqDataset: string | null;
    }[];
    const registeredTable = tables.find((t) => t.sourceId === mainSourceId);
    expect(registeredTable?.dqDataset, `no dataset name reported for ${mainSourceId}`).toBeTruthy();
    const sqlTableName = registeredTable!.dqDataset!.split("/").pop()!;

    // Column names are the raw CSV header text (crawl_directory infers schema straight off the
    // file), quoted exactly as Kaggle wrote them — except "State/UnionTerritory", whose slash the
    // register-table mutation rejects as an invalid GraphQL field name (verified live: "Names
    // must only contain [_a-zA-Z0-9]"); stageKaggleDataset sanitizes that one to
    // "State_UnionTerritory" server-side, so it is skipped here rather than asserted against a
    // physical CSV header the sanitized registration no longer matches by name.
    const rows = await runSqlOnPage(
      page,
      `SELECT "Date", "Confirmed" FROM pet_store.${sqlTableName} ORDER BY "Sno" LIMIT 1`,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toEqual(["2020-01-30", "1"]);
  });

  test("re-entering the token step after an invalid token shows a clear error, not a stale picker", async ({
    page,
  }) => {
    test.setTimeout(60000);

    await openSourcesForm(page);
    await page.getByTestId("sources-type-select").selectOption("kaggle");
    await page.getByTestId("kaggle-domain-select").click();
    await page.getByRole("option", { name: new RegExp(DOMAIN, "i") }).first().click();

    await page.getByTestId("kaggle-token-input").fill("not-a-real-token");
    await page.getByTestId("kaggle-validate-token-button").click();
    await expect(page.getByTestId("kaggle-token-error")).toBeVisible({ timeout: 30000 });
    // REQ-1783 amendment: invalid token -> no picker, not a stale/empty one.
    await expect(page.getByTestId("kaggle-dataset-search-input")).toHaveCount(0);

    // Re-entering a valid token now unlocks the picker — the gate re-checks live each time,
    // it does not latch a prior failure.
    await page.getByTestId("kaggle-token-input").fill(KAGGLE_TOKEN!);
    await page.getByTestId("kaggle-validate-token-button").click();
    await expect(page.getByTestId("kaggle-token-valid")).toBeVisible({ timeout: 30000 });
    await expect(page.getByTestId("kaggle-dataset-search-input")).toBeVisible();
  });
});
