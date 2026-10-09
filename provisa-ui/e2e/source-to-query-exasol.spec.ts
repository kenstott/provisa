// Copyright (c) 2026 Kenneth Stott
// Canary: b704363c-19de-4465-86b8-c3ea726c7139
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1671 fanout, REQ-1731/REQ-1763: exasol through the UI — the DIRECT-driver olap-lake case whose
// image is amd64-only. Moved out of source-to-query-olap-lake.spec.ts so that an arm64 host never
// collects it (playwright.config.ts's AMD64_ONLY_SPECS) instead of collecting and skipping it; an
// amd64 host (ui-e2e-core.yml's ubuntu-latest runner) runs it in the core project.
//
//   Run (amd64): cd provisa-ui && PROVISA_E2E_LANE=core npx playwright test source-to-query-exasol --project=core

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { test, expect } from "./coverage";
import {
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  runSqlOnPage,
  releaseDraftTables,
  submitSourceAndExpectListed,
  FORM_FIELD,
  removeTestSources,
} from "./source-to-query-helpers";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PROVISION = path.join(ROOT, "demo", "sources", "provision.py");
const PYTHON = path.join(ROOT, ".venv", "bin", "python");
const PREFIX = "provisa-s2q-exasol";

const E2E_EXASOL_PORT = Number(process.env.PROVISA_DEMO_EXASOL_PORT ?? 36930);
// Exasol's TLS certificate is regenerated every container boot (see demo/sources/exasol/prime.py's
// module doc) — there is no fixed fingerprint to hardcode, so prime.py writes the one THIS run's
// container actually presents to a file this test reads back after provisioning.
const E2E_EXASOL_FINGERPRINT_FILE =
  process.env.PROVISA_DEMO_EXASOL_FINGERPRINT_FILE ??
  path.join(os.tmpdir(), "provisa-e2e-olap-lake-exasol-fingerprint.txt");

function provision(cmd: "up" | "down", names: string[], env: Record<string, string> = {}): void {
  try {
    execFileSync(
      PYTHON,
      [
        PROVISION,
        cmd,
        "--prefix",
        PREFIX,
        ...Object.entries(env).flatMap(([k, v]) => ["--env", `${k}=${v}`]),
        ...names,
      ],
      { stdio: "pipe", env: { ...process.env, ...env } },
    );
  } catch (e) {
    if (cmd === "down") return; // a project that was never started removes nothing
    throw e;
  }
}

// ---------------------------------------------------------------------------------------------
// exasol — DIRECT driver (pyexasol, executor/drivers/exasol.py), core lane / DuckDB backend, no
// Trino routing needed. CI-only: exasol/docker-db needs privileged mode + shm_size 2g + several
// GB RAM and a multi-minute cold EXAStorage init, and is amd64-only — under QEMU emulation on an
// arm64 host it never becomes healthy (same constraint tests/integration/test_exasol_source_e2e.py
// arch-gates on). This file is collected only on an amd64 host (playwright.config.ts's
// AMD64_ONLY_SPECS): ui-e2e-core.yml's ubuntu-latest runner runs it, and an arm64 dev box never
// collects it rather than skipping it.
// ---------------------------------------------------------------------------------------------
test.describe("source to query through the UI: exasol (REQ-1731, REQ-1763)", () => {
  let exasolFingerprint = "";

  test.beforeAll(() => {
    // EXAStorage cold init genuinely takes minutes, not seconds (see demo/sources/exasol/
    // compose.yml's own healthcheck budget: 60 retries at 10s = up to 600s past container start).
    test.setTimeout(900000);
    if (fs.existsSync(E2E_EXASOL_FINGERPRINT_FILE)) fs.rmSync(E2E_EXASOL_FINGERPRINT_FILE);
    provision("up", ["exasol"], {
      PROVISA_DEMO_EXASOL_PORT: String(E2E_EXASOL_PORT),
      PROVISA_DEMO_EXASOL_FINGERPRINT_FILE: E2E_EXASOL_FINGERPRINT_FILE,
    });
    exasolFingerprint = fs.readFileSync(E2E_EXASOL_FINGERPRINT_FILE, "utf8").trim();
  });

  test.afterAll(async () => {
    await removeTestSources();
    provision("down", ["exasol"]);
    if (fs.existsSync(E2E_EXASOL_FINGERPRINT_FILE)) fs.rmSync(E2E_EXASOL_FINGERPRINT_FILE);
  });

  test("exasol: add the source, register a table, query it on the SQL page", async ({ page }) => {
    test.setTimeout(180000);
    const stamp = Date.now();
    const sourceId = `e2e_exasol_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("exasol");
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_EXASOL_PORT));
    await page.getByLabel(/^Username/).fill("sys");
    await page.getByLabel(/^Password/).fill("exasol");
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("PROVISA");
    // Exasol always serves TLS with a self-signed, per-boot certificate — pin the fingerprint
    // THIS run's container actually presents (read back from prime.py's output file above),
    // exactly the same pin ExasolDriver.connect() (executor/drivers/exasol.py) needs to validate.
    await page.getByLabel(/^Authentication/).selectOption("tls_fingerprint");
    await page.getByLabel(/TLS Fingerprint/).fill(exasolFingerprint);
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "PROVISA", "WIDGETS");
    await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
      timeout: 60000,
    });
    await page.getByTestId("register-table-submit").click();
    const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
    await expect(row).toBeVisible({ timeout: 120000 });
    // REQ-1921: registered through the admin, it starts as draft; released, it is read.
    await releaseDraftTables(page, { sourceId });

    const res = await page.request.post("/admin/graphql", {
      data: { query: "{ tables { sourceId dqDataset } }" },
    });
    const tables = (await res.json()).data.tables as {
      sourceId: string;
      dqDataset: string | null;
    }[];
    const mine = tables.find((t) => t.sourceId === sourceId);
    expect(mine?.dqDataset).toBeTruthy();
    const registered = mine!.dqDataset!.split("/").pop()!;

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);
  });
});
