// Copyright (c) 2026 Kenneth Stott
// Canary: 6c2e9a4d-1b7f-4d3a-8e5c-9f0b3d6a2e17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1483 / REQ-1687: a Hasura v2 import driven through the browser, end to end.
 *
 * Hasura's own metadatautil sample (tests/fixtures/hasura_v2_t1_metadata.json) is uploaded on
 * Admin → Import; the administrator supplies the connection to the chinook demo source, picks the
 * domains, converts again, edits the remote schema's endpoint in the config, and applies. Then the
 * data GraphQL endpoint answers as the sample's `user` role: the select permission's session
 * variable filter holds (REQ-1682), the FK-declared relationship joins (REQ-1680), and the landed
 * remote schema answers from the public countries API (REQ-1681, REQ-1685).
 */

import path from "path";
import { fileURLToPath } from "url";
import { test, expect, BACKEND_URL } from "./coverage";
import { E2E_CHINOOK_PORT } from "./demo-source-containers";

test.describe.configure({ timeout: 240_000 });

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const SAMPLE = path.join(ROOT, "tests/fixtures/hasura_v2_t1_metadata.json");
const COUNTRIES_URL = "https://countries.trevorblades.com/graphql";
const DATA_GQL = `${BACKEND_URL}/data/graphql`;
const USER = {
  "Content-Type": "application/json",
  "x-provisa-role": "user",
  "x-provisa-session-user-id": "2",
};

async function fieldFor(
  request: Parameters<Parameters<typeof test>[2]>[0]["request"],
  table: string,
) {
  const resp = await request.post(DATA_GQL, {
    headers: USER,
    data: { query: "{ __schema { queryType { fields { name } } } }" },
  });
  expect(resp.status()).toBe(200);
  const names: string[] = (await resp.json()).data.__schema.queryType.fields.map(
    (f: { name: string }) => f.name,
  );
  const wanted = table.replace(/_/g, "").toLowerCase();
  const match = names.find((n) => n.split("__").pop()!.toLowerCase() === wanted);
  expect(match, `${table} exposed to user: ${names.join(", ")}`).toBeTruthy();
  return match!;
}

test("import Hasura's sample through the tab, then query it as the sample's user role", async ({
  page,
  request,
}) => {
  await page.goto("/admin/import");
  await expect(page.getByRole("button", { name: "Choose file" })).toBeVisible({ timeout: 60_000 });

  await page.setInputFiles('input[type="file"]', SAMPLE);
  await page.getByRole("button", { name: "Convert and preview" }).click();
  await expect(page.getByText("What this import produces")).toBeVisible({ timeout: 60_000 });

  // The export names its database by environment variable; the connection is ours to supply.
  await page.getByTestId("import-source-host-default").fill("localhost");
  await page.getByTestId("import-source-port-default").fill(String(E2E_CHINOOK_PORT));
  await page.getByTestId("import-source-database-default").fill("chinook");
  await page.getByTestId("import-source-username-default").fill("provisa");
  await page.getByTestId("import-source-password-default").fill("provisa");
  // A new domain for the schema and one for the remote schema, both typed (none exists yet).
  await page.getByTestId("import-domain-public").fill("music");
  await expect(page.getByTestId("import-domain-new-public")).toBeVisible();
  await page.getByTestId("import-domain-countries").fill("geo");
  await page.getByTestId("import-reconvert").click();
  await expect(page.getByText("What this import produces")).toBeVisible({ timeout: 60_000 });

  // With the connection reachable, no source or column stays untyped.
  const warnings = page.locator("table").filter({ hasText: "What happened" });
  await expect(warnings).toBeVisible();
  await expect(warnings.getByText("not reachable")).toHaveCount(0);

  // The remote schema's endpoint is an env reference in the export; the administrator sets it in
  // the configuration before applying, which is what the editable config is for.
  const config = page.getByRole("textbox", { name: "Generated configuration" });
  const yaml = await config.inputValue();
  expect(yaml).toContain("${env:RS_ENV}");
  await config.fill(yaml.replace("${env:RS_ENV}", COUNTRIES_URL));

  await page.getByRole("button", { name: "Apply to this organization" }).click();
  await expect(page.getByText(/^Applied: /)).toBeVisible({ timeout: 120_000 });

  // REQ-1682/REQ-1686: the select permission's session-variable filter, row by row.
  const albums = await fieldFor(request, "albums");
  const filtered = await request.post(DATA_GQL, {
    headers: USER,
    data: { query: `{ ${albums} { title artistId } }` },
  });
  expect(filtered.status()).toBe(200);
  expect((await filtered.json()).data[albums]).toEqual([
    { title: "Balls to the Wall", artistId: 2 },
  ]);

  // REQ-1680: the FK-declared relationship resolved through the inverse and joins.
  const admin = { "Content-Type": "application/json", "x-provisa-role": "org_admin" };
  const joined = await request.post(DATA_GQL, {
    headers: admin,
    data: { query: `{ ${albums} { id artist { name } } }` },
  });
  expect(joined.status()).toBe(200);
  const byId = Object.fromEntries(
    (await joined.json()).data[albums].map((r: { id: number; artist: { name: string } }) => [
      r.id,
      r.artist.name,
    ]),
  );
  expect(byId[2]).toBe("Accept");

  // REQ-1681/REQ-1685: the landed remote schema answers from the public API on first query.
  const countries = await fieldFor(request, "countries");
  const landed = await request.post(DATA_GQL, {
    headers: USER,
    data: { query: `{ ${countries} { code name } }` },
  });
  expect(landed.status()).toBe(200);
  const rows = (await landed.json()).data[countries];
  expect(rows.length).toBe(100);
  expect(rows).toContainEqual({ code: "AD", name: "Andorra" });
});
