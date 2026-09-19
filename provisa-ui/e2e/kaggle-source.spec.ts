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
// a whole staged bundle becomes ONE `files`-type Source (Amended 2026-09-19: was one plain
// csv/parquet Source PER FILE — the pgwire-file connector's own recursive directory discovery,
// REQ-1690, made that design strictly worse once it existed: an 11-file Kaggle dataset used to
// produce 11 separate raw csv-typed Sources needing 11 separate Register Table trips, instead of
// one `files` source whose Register Table screen lists every file as a table in one place),
// created through KaggleFormSection's own two-step flow: token entry (validated live) then a
// live dataset search/picker, backed by stageKaggleDataset (download+unzip only, no per-file
// enumeration) and the same createSource call a manual `files` source uses. "Add Dataset" only
// creates the Source — registering a table (domain, alias, columns) is a separate step through
// the normal Register Table form, exactly like every other connector: a staged Kaggle directory
// is introspected live via the same pgwire-file path any other files source uses, so there is
// nothing Kaggle-specific left to verify past source creation, and pre-registering eagerly left
// no alias field to resolve a same-name collision with.
//
// Fixture datasets: arshid/iris-flower-dataset (single file, IRIS.csv, ~4.6KB, 150 rows) and
// sudalairajkumar/covid19-in-india (3 files, ~776KB compressed, last updated 2021 so its row data
// cannot drift) — both confirmed live via real `datasets/list/{owner}/{ref}` calls before writing
// this test, neither containing a SQLite file (this connector's v1 scope is CSV/Parquet only).

import { test, expect } from "./coverage";
import {
  openRegisterForm,
  openSourcesForm,
  runSqlOnPage,
  submitRegisterAndExpectListed,
} from "./source-to-query-helpers";

const KAGGLE_TOKEN = process.env.KAGGLE_API_TOKEN;

test.describe("Kaggle source through the real UI, live Kaggle API (REQ-1780/1781/1782/1783)", () => {
  test.skip(!KAGGLE_TOKEN, "KAGGLE_API_TOKEN not set in the environment");
  // The Sources form is long (every source-type's fields render into one page, hidden/shown by
  // type) and Mantine's Combobox.Dropdown is a floating-positioned portal — on the default
  // 1280x720 test viewport it renders its option below the visible viewport even after
  // Playwright's auto-scroll (reproduced live), so the dataset-picker option is never clickable.
  test.use({ viewport: { width: 1280, height: 2200 } });

  test("single-file dataset: token gate, live search, add + register + a real query", async ({
    page,
  }) => {
    test.setTimeout(180000);

    await openSourcesForm(page);
    await page.getByTestId("sources-type-select").selectOption("kaggle");

    // REQ-1783: the picker must not exist before a valid token — Kaggle's own search endpoint
    // requires auth, so there is nothing it could search yet.
    await expect(page.getByTestId("kaggle-dataset-search-input")).toHaveCount(0);

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
    // The id_prefix is honored verbatim (schema_mutation.py's stage_kaggle_dataset) — one Source
    // per dataset now, so there is no per-file "_<table_name>" suffix to disambiguate.
    const row = page.locator(".data-table td").filter({ hasText: /^kg_\d+_iris-flower-dataset$/ });
    await expect(row).toBeVisible({ timeout: 60000 });
    const sourceId = (await row.textContent())!.trim();

    // Confirm it created exactly ONE `files`-type Source (not a bespoke Kaggle connector/type,
    // and not one Source per file) — "Add Dataset" only creates the source; it must not also
    // have registered a table (REQ-1783 amendment).
    const sourcesRes = await page.request.post("/admin/graphql", {
      data: { query: "{ sources { id type } }" },
    });
    expect(sourcesRes.ok(), await sourcesRes.text()).toBeTruthy();
    const sources = (await sourcesRes.json()).data.sources as { id: string; type: string }[];
    const registeredSource = sources.find((s) => s.id === sourceId);
    expect(registeredSource?.type).toBe("files");

    const preTablesRes = await page.request.post("/admin/graphql", {
      data: { query: "{ tables { sourceId } }" },
    });
    expect(preTablesRes.ok(), await preTablesRes.text()).toBeTruthy();
    const preTables = (await preTablesRes.json()).data.tables as { sourceId: string }[];
    expect(
      preTables.some((t) => t.sourceId === sourceId),
      "Add Dataset must not also register a table — that's the normal Register Table form's job",
    ).toBe(false);

    // Register the file's table through the normal Register Table form — the pgwire-file
    // connector discovers it live off the staged directory, schema = the sql-normalized source id
    // (pgwire_replica.schema_name's convention). Table naming: downloader.py stages every file
    // into its own "<file-stem>/<file-name>" subdirectory (even for a single-file dataset), and
    // the pgwire-file connector's recursive discovery flattens a subdirectory into the table name
    // as "<subdir>__<stem>" (REQ-1690, confirmed live in file-connector-multi-format.spec.ts) — so
    // "IRIS.csv" staged under an "IRIS" subdirectory becomes "iris__iris", not a bare "IRIS".
    const schemaName = sourceId.replace(/-/g, "_");
    await openRegisterForm(page, sourceId);
    const schemaSelect = page.getByTestId("register-table-schema-select");
    await expect(schemaSelect.locator(`option[value='${schemaName}']`)).toHaveCount(1, {
      timeout: 120000,
    });
    if ((await schemaSelect.inputValue()) !== schemaName) {
      await schemaSelect.selectOption(schemaName);
    }
    const tableSelect = page.getByTestId("register-table-table-select");
    await expect(tableSelect.locator(`option[value='iris__iris']`)).toHaveCount(1, {
      timeout: 60000,
    });
    await tableSelect.selectOption("iris__iris");
    await expect(
      page.locator('[data-testid^="register-table-col-selected-"]').first(),
    ).toBeVisible({ timeout: 30000 });
    const sqlTableName = await submitRegisterAndExpectListed(page, sourceId);

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

  test("multi-file dataset: one files Source, every file listed as a table, a real query works", async ({
    page,
  }) => {
    test.setTimeout(180000);

    // Fixture: sudalairajkumar/covid19-in-india — 3 files (no SQLite), StatewiseTestingDetails.csv
    // (622,938 bytes), covid_19_india.csv (1,005,449 bytes), covid_vaccine_statewise.csv
    // (1,108,819 bytes).
    await openSourcesForm(page);
    await page.getByTestId("sources-type-select").selectOption("kaggle");
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
    // ONE Source for the whole dataset (Amended 2026-09-19) — was 3 (one per file) before.
    const row = page.locator(".data-table td").filter({ hasText: /^kg_\d+_covid19-in-india$/ });
    await expect(row).toBeVisible({ timeout: 60000 });
    const sourceId = (await row.textContent())!.trim();

    const sourcesRes = await page.request.post("/admin/graphql", {
      data: { query: "{ sources { id type } }" },
    });
    expect(sourcesRes.ok(), await sourcesRes.text()).toBeTruthy();
    const sources = (await sourcesRes.json()).data.sources as { id: string; type: string }[];
    expect(sources.find((s) => s.id === sourceId)?.type).toBe("files");

    // Register Table lists all 3 files as separate tables, discovered live off the one directory.
    const schemaName = sourceId.replace(/-/g, "_");
    await openRegisterForm(page, sourceId);
    const schemaSelect = page.getByTestId("register-table-schema-select");
    await expect(schemaSelect.locator(`option[value='${schemaName}']`)).toHaveCount(1, {
      timeout: 120000,
    });
    if ((await schemaSelect.inputValue()) !== schemaName) {
      await schemaSelect.selectOption(schemaName);
    }
    // downloader.py stages each file into its own "<file-stem>/<file-name>" subdirectory, and the
    // pgwire-file connector's recursive discovery flattens that into "<subdir>__<stem>" — since
    // subdir name and stem are always the same string here, every table doubles its own stem. The
    // stem itself goes through Calcite's default SMART_CASING (table_name_casing), which inserts
    // underscores between CamelCase words: "StatewiseTestingDetails" -> "statewise_testing_details"
    // (verified live) — "covid_19_india"/"covid_vaccine_statewise" are already snake_case so
    // SMART_CASING leaves them unchanged.
    const covidTable = "covid_19_india__covid_19_india";
    const tableSelect = page.getByTestId("register-table-table-select");
    await expect(tableSelect.locator(`option[value='${covidTable}']`)).toHaveCount(1, {
      timeout: 60000,
    });
    const discovered = await tableSelect
      .locator("option")
      .evaluateAll((opts) => opts.map((o) => (o as HTMLOptionElement).value).filter(Boolean));
    expect(new Set(discovered)).toEqual(
      new Set([
        "statewise_testing_details__statewise_testing_details",
        covidTable,
        "covid_vaccine_statewise__covid_vaccine_statewise",
      ]),
    );

    // Register only the one table this test actually queries.
    await tableSelect.selectOption(covidTable);
    await expect(
      page.locator('[data-testid^="register-table-col-selected-"]').first(),
    ).toBeVisible({ timeout: 30000 });
    const sqlTableName = await submitRegisterAndExpectListed(page, sourceId);

    // Column names are the raw CSV header text (DuckDB's live introspection reads it straight off
    // the file), quoted exactly as Kaggle wrote them — except "State/UnionTerritory", whose slash
    // the naming authority (apply_gql_name, REQ-471) sanitizes to a valid GraphQL identifier, so
    // it is skipped here rather than asserted against a raw CSV header the sanitized column no
    // longer matches by name.
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
