// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1993 (amended 2026-11-25): a Python source whose code is entered here, against a real
// server. The create form is opened from its deep link with Code from set to Enter it here
// (REQ-2277), the code is typed into the code editor and saved with the source, and its @table
// is registered and queried.

import { test, expect, BACKEND_URL } from "./coverage";
import { retireTestSourceTables, submitSourceAndExpectListed } from "./source-to-query-helpers";
import { testSourceId } from "./retire-source-tables";

// Saving a code source makes its environment and installs and reads its code.
test.describe.configure({ timeout: 900_000 });

const ROLE = "org_admin";
const DOMAIN = "pet-store";
const SOURCE = testSourceId("entered");
const STAMP = SOURCE.split("_").pop() as string;
const TABLE = `rates_${STAMP}`;

const CODE = `from dataclasses import dataclass


@dataclass
class Rate:
    currency: str
    rate: float


def ${TABLE}() -> list[Rate]:
    """Today's rates.

    @table
    """
    return [Rate("EUR", 1.08), Rate("GBP", 1.27)]
`;

async function gql(
  url: string,
  query: string,
  variables: Record<string, unknown> = {},
): Promise<{ data?: Record<string, unknown>; errors?: unknown }> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-provisa-role": ROLE },
    body: JSON.stringify({ query, variables }),
  });
  if (!res.ok) throw new Error(`${url} answered ${res.status}: ${await res.text()}`);
  return res.json();
}

test.afterAll(async () => {
  await retireTestSourceTables(SOURCE);
});

test("a Python source's code entered here is saved with it and its @table is queried", async ({
  page,
}) => {
  await page.goto("/sources?new=python&origin=entered");
  await page.waitForSelector(".page-header", { timeout: 60_000 });
  await expect(page.getByTestId("code-origin")).toHaveValue("Enter it here", { timeout: 60_000 });
  await expect(page.getByTestId("code-package")).toHaveCount(0);

  await page.getByTestId("sources-id-input").fill(SOURCE);
  const editor = page.getByTestId("code-source").locator(".cm-content");
  await expect(editor).toHaveAttribute("data-code-field", "python");
  await editor.click();
  await page.keyboard.insertText(CODE);
  await expect(page).toHaveURL(/[?&]new=python(&|$)/);
  await expect(page).toHaveURL(/[?&]origin=entered(&|$)/);

  await submitSourceAndExpectListed(page, SOURCE);

  // Saved, the code is the source's, in the model, and its content hash was recorded.
  const listed = await gql(`${BACKEND_URL}/admin/graphql`, `{ sources { id mappingJson } }`);
  expect(listed.errors, JSON.stringify(listed.errors)).toBeUndefined();
  const source = (listed.data?.sources as { id: string; mappingJson: string | null }[]).find(
    (s) => s.id === SOURCE,
  );
  expect(source, `${SOURCE} is not listed`).toBeDefined();
  const mapping = JSON.parse(source?.mappingJson ?? "{}") as {
    code: { kind: string; source: string; dependencies: string[] };
    content_hash: string;
  };
  expect(mapping.code).toEqual({ kind: "entered", source: CODE, dependencies: [] });
  expect(mapping.content_hash).toMatch(/^[0-9a-f]{64}$/);

  const registered = await gql(
    `${BACKEND_URL}/admin/graphql`,
    `mutation R($input: TableInput!) { registerTable(input: $input) { success message } }`,
    {
      input: {
        sourceId: SOURCE,
        domainId: DOMAIN,
        schemaName: "default",
        tableName: TABLE,
        cacheTtl: 300,
        columns: [
          { name: "currency", visibleTo: [ROLE] },
          { name: "rate", visibleTo: [ROLE] },
        ],
      },
    },
  );
  expect(registered.errors, JSON.stringify(registered.errors)).toBeUndefined();
  expect(
    (registered.data?.registerTable as { success: boolean; message: string }).success,
    JSON.stringify(registered.data?.registerTable),
  ).toBe(true);

  const schema = await gql(
    `${BACKEND_URL}/data/graphql`,
    `{ __schema { queryType { fields { name } } } }`,
  );
  const names = (
    (schema.data?.__schema as { queryType: { fields: { name: string }[] } }).queryType.fields ?? []
  ).map((f) => f.name);
  const field = names.find(
    (n) => n.toLowerCase().endsWith(TABLE) && !n.toLowerCase().includes("aggregate"),
  );
  expect(field, `no query field for ${TABLE} among ${names.join(", ")}`).toBeDefined();
  const rows = await gql(`${BACKEND_URL}/data/graphql`, `{ ${field} { currency rate } }`);
  expect(rows.errors, JSON.stringify(rows.errors)).toBeUndefined();
  expect(rows.data?.[field as string]).toEqual([
    { currency: "EUR", rate: 1.08 },
    { currency: "GBP", rate: 1.27 },
  ]);
});
