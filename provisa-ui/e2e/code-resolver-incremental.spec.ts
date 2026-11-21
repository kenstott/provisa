// Copyright (c) 2026 Kenneth Stott
// Canary: 3c7d1f8a-9e42-4b65-a0d3-5f8e2b6c9a17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2249: a code resolver's incremental setting, on the Relationships page against a real
// server. A Python code source is uploaded with a resolver and a @change function for one set id.
// The form's tick is disabled, with the reason, for a set id the source has no @change function
// for; for the id it has one for, the tick turns on, asks how often the matches are rebuilt
// whole, and the set is saved and read back kept incrementally.

import { test, expect, BACKEND_URL, type Page } from "./coverage";
import { retireTestSourceTables } from "./source-to-query-helpers";

// Saving a code source fetches its runtime and installs and reads its code.
test.describe.configure({ timeout: 900_000 });

const ORGS = (process.env.PROVISA_E2E_BACKEND_ORGS ?? "e2e").split(",");
const ORG = ORGS[Number(process.env.TEST_PARALLEL_INDEX ?? "0")];
const ROLE = "org_admin";
const DOMAIN = "pet-store";

// Every name is this run's own: a retried run meets none of an earlier attempt's.
const STAMP = Date.now();
const SOURCE = `e2e_python_${STAMP}`;
const STORES = `stores_${STAMP}`;
const REGIONS = `regions_${STAMP}`;
const RESOLVER = `match_${STAMP}`;
/** The set the source declares a @change function for, and one it declares none for. */
const KEPT = `kept_${STAMP}`;
const PLAIN = `plain_${STAMP}`;

const CODE = `
from dataclasses import dataclass

from provisa_services import TableRef


@dataclass
class Store:
    id: int
    region: int


@dataclass
class Region:
    id: int


@dataclass
class Match:
    store_id: int
    region_id: int
    score: float


@dataclass
class Changed:
    store_id: int
    region_id: int
    score: float
    removed: bool
    seq: int


def ${STORES}() -> list[Store]:
    """@table"""
    return [Store(1, 10), Store(2, 10)]


def ${REGIONS}() -> list[Region]:
    """@table"""
    return [Region(10)]


def ${RESOLVER}(stores: TableRef[Store], regions: TableRef[Region]) -> list[Match]:
    """Every match, recomputed whole.

    @query
    """
    return [Match(1, 10, 9.0), Match(2, 10, 9.0)]


def ${KEPT}_changes(
    stores: TableRef[Store], regions: TableRef[Region], since: int | None = None
) -> list[Changed]:
    """The matches changed since \`\`since\`\`; every match when there is no cursor.

    @change table=${KEPT}
    @change key=store_id,region_id
    @change removed=removed
    @change cursor=seq
    """
    if since is None:
        return [Changed(1, 10, 1.0, False, 1), Changed(2, 10, 1.0, False, 2)]
    return []
`;

const CRC_TABLE = Array.from({ length: 256 }, (_, n) => {
  let c = n;
  for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  return c >>> 0;
});

function crc32(bytes: Buffer): number {
  let crc = 0xffffffff;
  for (const b of bytes) crc = CRC_TABLE[(crc ^ b) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

/** A zip archive holding one stored (uncompressed) file. */
function archiveOf(name: string, text: string): Buffer {
  const file = Buffer.from(name, "utf8");
  const data = Buffer.from(text, "utf8");
  const crc = crc32(data);
  const local = Buffer.alloc(30);
  local.writeUInt32LE(0x04034b50, 0);
  local.writeUInt16LE(20, 4);
  local.writeUInt32LE(crc, 14);
  local.writeUInt32LE(data.length, 18);
  local.writeUInt32LE(data.length, 22);
  local.writeUInt16LE(file.length, 26);
  const central = Buffer.alloc(46);
  central.writeUInt32LE(0x02014b50, 0);
  central.writeUInt16LE(20, 4);
  central.writeUInt16LE(20, 6);
  central.writeUInt32LE(crc, 16);
  central.writeUInt32LE(data.length, 20);
  central.writeUInt32LE(data.length, 24);
  central.writeUInt16LE(file.length, 28);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(1, 8);
  end.writeUInt16LE(1, 10);
  end.writeUInt32LE(central.length + file.length, 12);
  end.writeUInt32LE(local.length + file.length + data.length, 16);
  return Buffer.concat([local, file, data, central, file, end]);
}

interface Result {
  success: boolean;
  message: string;
  code?: string | null;
}

async function gql(query: string): Promise<Record<string, unknown>> {
  const res = await fetch(`${BACKEND_URL}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-provisa-role": ROLE },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error(`admin GraphQL answered ${res.status}: ${await res.text()}`);
  const body = (await res.json()) as { data?: Record<string, unknown>; errors?: unknown };
  if (body.errors) throw new Error(`admin GraphQL errors: ${JSON.stringify(body.errors)}`);
  return body.data ?? {};
}

/** One admin mutation that must succeed; its result. */
async function mutate(field: string): Promise<Result> {
  const data = await gql(`mutation { m: ${field} { success message code } }`);
  const result = data.m as Result;
  if (!result.success) throw new Error(`${field.slice(0, 60)} refused: ${result.message}`);
  return result;
}

const column = (name: string) => `{name: "${name}", visibleTo: ["${ROLE}"], dataType: "bigint"}`;

test.beforeAll(async () => {
  const upload = await fetch(
    `${BACKEND_URL}/admin/orgs/${ORG}/code-uploads/archive?filename=geo.zip`,
    {
      method: "POST",
      headers: { "Content-Type": "application/octet-stream", "x-provisa-role": ROLE },
      body: new Blob([new Uint8Array(archiveOf("geo/geo.py", CODE))]),
    },
  );
  if (!upload.ok) throw new Error(`code upload answered ${upload.status}: ${await upload.text()}`);
  const { content_hash: hash } = (await upload.json()) as { content_hash: string };
  const mapping = JSON.stringify({ runtime: "python", code: { kind: "upload", upload: hash } });
  await mutate(
    `createSource(input: {id: "${SOURCE}", type: "python", host: "", port: 0, database: "", ` +
      `cacheTtl: 300, mappingJson: ${JSON.stringify(mapping)}})`,
  );
  for (const [table, columns] of [
    [STORES, [column("id"), column("region")]],
    [REGIONS, [column("id")]],
  ] as const) {
    // REQ-1921: registered, the table is live.
    await mutate(
      `registerTable(input: {sourceId: "${SOURCE}", domainId: "${DOMAIN}", ` +
        `schemaName: "default", tableName: "${table}", cacheTtl: 300, ` +
        `columns: [${columns.join(", ")}]})`,
    );
  }
  const command = await fetch(`${BACKEND_URL}/admin/actions/functions`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-provisa-role": ROLE },
    body: JSON.stringify({
      name: RESOLVER,
      sourceId: SOURCE,
      functionName: RESOLVER,
      domainId: DOMAIN,
      visibleTo: [ROLE],
    }),
  });
  if (!command.ok) throw new Error(`command answered ${command.status}: ${await command.text()}`);
});

test.afterAll(async () => {
  // The set first: its view and relationships are retired only with it.
  await gql(`mutation { retireCodeResolver(id: "${KEPT}") { success message } }`);
  await retireTestSourceTables(SOURCE);
});

async function pick(page: Page, testId: string, option: string) {
  await page.getByTestId(testId).click();
  await page
    .getByRole("option", { name: new RegExp(option) })
    .first()
    .click();
}

test("a code resolver's matches are kept incrementally only where its source has @change", async ({
  page,
}) => {
  await page.goto("/relationships");
  await page.waitForSelector(".page-header", { timeout: 60_000 });
  await page.getByTestId("rels-add-toggle").click();
  await page.getByTestId("rel-form-id").fill(PLAIN);
  await pick(page, "rel-form-source-table", STORES);
  await pick(page, "rel-form-source-column", "id");
  await pick(page, "rel-form-target-type", "Code resolver");
  await pick(page, "rel-form-target-table", REGIONS);
  await pick(page, "rel-form-target-column", "id");
  await pick(page, "rel-form-resolver", RESOLVER);
  await pick(page, "rel-form-source-match", "store_id");
  await pick(page, "rel-form-target-match", "region_id");

  // No @change function names this set's table: the tick is disabled and says why.
  const tick = page.getByTestId("rel-form-incremental");
  await expect(tick).toBeDisabled({ timeout: 60_000 });
  await expect(tick).toHaveAccessibleDescription(
    new RegExp(`has no @change function for ${PLAIN}`),
  );
  await expect(page.getByTestId("rel-form-rebuild-every")).toHaveCount(0);

  // The set the source's @change function names may be kept incrementally.
  await page.getByTestId("rel-form-id").fill(KEPT);
  await expect(tick).toBeEnabled({ timeout: 60_000 });
  await tick.check();
  const every = page.getByTestId("rel-form-rebuild-every");
  await expect(every).toBeVisible();

  // On, it is not saved without the interval between whole rebuilds.
  await page.getByTestId("rel-form-save").click();
  await expect(page.getByText(`${KEPT} is kept incrementally`, { exact: false })).toBeVisible();
  expect(
    ((await gql("{ codeResolvers { id } }")).codeResolvers as { id: string }[]).map((c) => c.id),
  ).not.toContain(KEPT);

  // With it, the set is saved (which builds it once) and its relationship opens.
  await every.fill("3600");
  await page.getByTestId("rel-form-save").click();
  await expect(page.getByTestId("relationship-detail-resolver-note")).toContainText(KEPT, {
    timeout: 600_000,
  });
  const saved = (
    (await gql("{ codeResolvers { id incremental rebuildEvery incrementalOption { available } } }"))
      .codeResolvers as Record<string, unknown>[]
  ).find((c) => c.id === KEPT);
  expect(saved).toEqual({
    id: KEPT,
    incremental: true,
    rebuildEvery: 3600,
    incrementalOption: { available: true },
  });

  // Opened to change, the form shows the set as it is kept.
  await page.getByTestId("relationship-detail-change-resolver").click();
  await expect(page.getByTestId("rel-form-incremental")).toBeChecked();
  await expect(page.getByTestId("rel-form-rebuild-every")).toHaveValue("3600");
});
