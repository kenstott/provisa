// The Register Table form's pickers for an AskAmerica source (REQ-540, REQ-541): the schemas the
// source's subjects bring, each schema's tables, and a picked table's columns, all read from the
// adapter as soon as it listens.
//
// What this holds, each of which failed in the maintainer's hands on 2026-10-09:
//   - the schema list is the source's own (the ticked subjects' schemas and the two linker
//     schemas), not every schema the adapter serves;
//   - the table list FILLS. It is read from the adapter's information_schema, which answers while
//     the adapter is still counting rows for its pg_catalog (minutes), so a list that waits for
//     that count is the defect, and a reader that asks the adapter's rows for a lower-case
//     label finds nothing (the adapter labels them TABLE_NAME);
//   - picking a table shows its columns at once, from the source.
//
// A REAL external source (US government open data): the same credential gate as the govdata case
// of source-to-query-special-cases.spec.ts (FREE_ASKAMERICA_KEY in the root .env).

import { test, expect } from "./coverage";
import {
  openRegisterForm,
  openSourcesForm,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";

// The adapter listens seconds after it is started and answers information_schema then. This is
// the bound on "the list fills", far below the minutes the adapter's row counting takes, so a
// picker that waits on that counting fails here.
const PICKER_FILLS_MS = 90_000;

test.describe("AskAmerica: the Register Table pickers", () => {
  const apiKey = process.env.FREE_ASKAMERICA_KEY ?? "";

  test("schemas are the source's, tables and columns are read from the adapter at once", async ({
    page,
  }) => {
    test.skip(
      !apiKey,
      "FREE_ASKAMERICA_KEY not set in .env — AskAmerica is a real external source with no " +
        "offline stand-in.",
    );
    test.setTimeout(5 * 60_000);
    const sourceId = `e2e_aa_picker_${Date.now()}`;

    // 1. The Sources form offers the subjects the server states, and the source is saved with one.
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("govdata");
    const subjects = page.locator('[data-testid^="govdata-subject-"]');
    await expect(subjects).toHaveCount(11, { timeout: 30_000 });
    await page.getByTestId("govdata-subject-WEATHER").check();
    await page.getByTestId("govdata-api-key-input").fill(apiKey);
    await submitSourceAndExpectListed(page, sourceId);
    const saved = Date.now();

    // 2. The schema picker lists exactly the source's schemas: the subject's and the two linker
    // schemas every source serves. Not the adapter's other twenty-five.
    await openRegisterForm(page, sourceId);
    const schemaSelect = page.getByTestId("register-table-schema-select");
    await expect(schemaSelect.locator("option[value='weather']")).toHaveCount(1, {
      timeout: 30_000,
    });
    const schemas = await schemaSelect
      .locator("option")
      .evaluateAll((options) =>
        options.map((o) => (o as HTMLOptionElement).value).filter((v) => v !== ""),
      );
    expect(schemas.sort()).toEqual(["geo", "ref", "weather"]);

    // 3. The table picker fills for the subject's schema.
    if ((await schemaSelect.inputValue()) !== "weather") await schemaSelect.selectOption("weather");
    const tableSelect = page.getByTestId("register-table-table-select");
    await expect(tableSelect.locator("option[value='nws_stations']")).toHaveCount(1, {
      timeout: PICKER_FILLS_MS,
    });
    const filledAfterMs = Date.now() - saved;
    const weatherTables = await tableSelect
      .locator("option")
      .evaluateAll((options) =>
        options.map((o) => (o as HTMLOptionElement).value).filter((v) => v !== ""),
      );
    expect(weatherTables.length).toBeGreaterThan(1);
    console.log(
      `[askamerica picker] ${weatherTables.length} weather tables listed ${filledAfterMs} ms after save`,
    );

    // 4. Picking a table shows its columns, from the source.
    await tableSelect.selectOption("nws_stations");
    const columns = page.locator('[data-testid^="register-table-col-selected-"]');
    await expect(columns.first()).toBeVisible({ timeout: 30_000 });
    expect(await columns.count()).toBeGreaterThan(1);

    // 5. A linker schema lists its own tables: the picker follows the schema, not the first answer.
    await schemaSelect.selectOption("geo");
    await expect(tableSelect.locator("option[value='counties']")).toHaveCount(1, {
      timeout: PICKER_FILLS_MS,
    });
    await expect(tableSelect.locator("option[value='nws_stations']")).toHaveCount(0);
  });
});
