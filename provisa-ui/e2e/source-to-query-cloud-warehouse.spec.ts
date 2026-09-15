// Copyright (c) 2026 Kenneth Stott
// Canary: 2f6a8c1d-9e3b-4a7f-8c5d-1b2e4f6a8c9d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1747: the six cloud-warehouse SourceTypes (snowflake/bigquery/databricks/fabric/synapse/
// redshift) through the same three-screen UI flow as source-to-query.spec.ts (Sources form →
// Register Table form → SQL page SELECT), against the DuckDB federation engine, using the LIVE
// warehouse credentials in the repo-root .env (never printed/logged here — only the env var
// NAMES are referenced, same convention as source-to-query.spec.ts's SharePoint/Splunk cases).
//
// Each of snowflake/bigquery/databricks/fabric seeds a small real table directly (via
// cloud_warehouse_seed.py, mirroring the exact connection code tests/integration/
// test_snowflake_source_e2e.py / test_databricks_source_e2e.py / test_bigquery_federation_engine_
// e2e.py / test_fabric_federation_engine_e2e.py already use) before driving the UI, and tears it
// down after — there is no docker fixture for a real cloud warehouse, so the seed/teardown IS this
// spec's fixture, same idea as source-to-query.spec.ts's mysql/trino-as-a-source
// provisionExtraSources().
//
// synapse/redshift are deliberately SKIPPED: .env has no live SYNAPSE_* (the e2e provisions its
// own ephemeral workspace via tests/integration/synapse_provision.py, az login-scoped — out of
// scope for a UI form test to stand up) and only REDSHIFT_AWS_* keys (AWS creds for scripts/
// redshift_e2e.py's ephemeral Redshift Serverless lane, not a standing cluster host/port/db/user/
// pass) — see each test body for the precise skip reason.
//
// BUG FOUND ALONG THE WAY (filed, not worked around here): provisa/api/admin/introspect.py's
// native_schemas()/native_tables()/native_columns() had NO branch for snowflake/databricks/
// bigquery/fabric/synapse — every one of them fell through to `return None`, and since none of
// these DIRECT-driver source types has a DuckDB ATTACH connector (they only ever LAND via
// WarehouseNativeConnector, engine.complete_reach()), the REQ-1673 engine-catalog seam
// (DuckDBRuntime.introspect_schemas's `_attached_alias`) also always returned `[]` for them. Net
// effect: the Register Table form's schema/table pickers were permanently empty for all five
// types under DuckDB — this spec's 3-screen flow was unrunnable before that fix landed
// (provisa/api/admin/introspect.py, provisa/api/admin/schema_query.py, this same commit).
//
// A second, independent bug was found and worked around (not fixed) in this spec: the Sources
// form's Snowflake "Database" field is passed UNQUOTED to snowflake-connector's `database=`
// kwarg (executor/drivers/snowflake.py's SnowflakeDriver.connect) — a database created with a
// lowercase/mixed-case quoted identifier (e.g. this account's own SNOWFLAKE_DATABASE, "_landing")
// silently fails to select (CURRENT_DATABASE() reads NULL; the very next unqualified query then
// errors "This session does not have a current database") instead of erroring at connect time.
// Reproduced live against this account. cloud_warehouse_seed.py seeds into a fresh
// ALL-UPPERCASE-safe database instead, sidestepping it — the finding itself is reported, not
// papered over.

import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { test, expect } from "./coverage";
import {
  openSourcesForm,
  submitSourceAndExpectListed,
  openRegisterForm,
  pickSchemaAndTable,
  submitRegisterAndExpectListed,
  runSqlOnPage,
} from "./source-to-query-helpers";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PYTHON = path.join(ROOT, ".venv", "bin", "python");
const SEED = path.join(ROOT, "provisa-ui", "e2e", "cloud_warehouse_seed.py");

function seed(engine: "snowflake" | "databricks" | "bigquery" | "fabric", action: "up" | "down"): void {
  execFileSync(PYTHON, [SEED, engine, action], { stdio: "pipe" });
}

const WIDGET_ROWS = [
  ["1", "sprocket"],
  ["2", "cog"],
  ["3", "gear"],
];

test.describe("cloud warehouse sources through the UI (REQ-1747)", () => {
  test("snowflake: add the source, register a table, query it on the SQL page", async ({ page }) => {
    test.skip(
      !(
        process.env.SNOWFLAKE_ACCOUNT &&
        process.env.SNOWFLAKE_USER &&
        process.env.SNOWFLAKE_PASSWORD
      ),
      "no live Snowflake credentials in this environment (SNOWFLAKE_ACCOUNT/SNOWFLAKE_USER/SNOWFLAKE_PASSWORD)",
    );
    test.setTimeout(300000);
    seed("snowflake", "up");
    try {
      const stamp = Date.now();
      const sourceId = `e2e_snowflake_${stamp}`;

      // 1. Sources form
      await openSourcesForm(page);
      await page.getByTestId("sources-id-input").fill(sourceId);
      await page.getByTestId("sources-type-select").selectOption("snowflake");
      await page.getByLabel(/Account URL/).fill(process.env.SNOWFLAKE_ACCOUNT!);
      // A dedicated all-uppercase-safe database, not SNOWFLAKE_DATABASE — see the file header's
      // "second bug" note.
      await page.getByLabel(/^Database/).fill("PROVISA_UI_E2E");
      await page.getByLabel(/^Warehouse$/).fill(process.env.SNOWFLAKE_WAREHOUSE ?? "COMPUTE_WH");
      // Mantine's Select portals its listbox with aria-labelledby pointing at the same label,
      // so plain getByLabel("Authentication") resolves to both the input and the (closed)
      // listbox — a strict-mode violation. Scope to the textbox role.
      await page.getByRole("textbox", { name: "Authentication" }).click();
      await page.getByRole("option", { name: "Username / Password", exact: true }).click();
      await page.getByLabel(/^Username/).fill(process.env.SNOWFLAKE_USER!);
      await page.getByLabel(/^Password/).fill(process.env.SNOWFLAKE_PASSWORD!);
      await submitSourceAndExpectListed(page, sourceId);

      // 2. Register Table form — Snowflake upper-cases unquoted identifiers, so the schema/table
      // the seed created ("public"/"widgets") are introspected back as "PUBLIC"/"WIDGETS".
      await openRegisterForm(page, sourceId);
      await pickSchemaAndTable(page, "PUBLIC", "WIDGETS");
      await expect(page.getByTestId("register-table-col-selected-ID")).toBeVisible({
        timeout: 120000,
      });
      await expect(page.getByTestId("register-table-col-selected-NAME")).toBeVisible();
      const registered = await submitRegisterAndExpectListed(page, sourceId);

      // 3. SQL page
      const rows = await runSqlOnPage(
        page,
        `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
      );
      expect(rows).toEqual(WIDGET_ROWS);
    } finally {
      seed("snowflake", "down");
    }
  });

  test("databricks: add the source, register a table, query it on the SQL page", async ({
    page,
  }) => {
    test.skip(
      !(
        process.env.DATABRICKS_SERVER_HOSTNAME &&
        process.env.DATABRICKS_HTTP_PATH &&
        process.env.DATABRICKS_TOKEN
      ),
      "no live Databricks credentials in this environment (DATABRICKS_SERVER_HOSTNAME/HTTP_PATH/TOKEN)",
    );
    // Reproduced LIVE (2026-09-15) via a direct REST call to this account's SQL Warehouse start
    // endpoint (api/2.0/sql/warehouses/<id>/start): 400 BAD_REQUEST, reason
    // DENY_NEW_AND_EXISTING_RESOURCES, denyReason INACTIVE, scoped to the whole workspace (not
    // just this warehouse) — the response's own decisionTimeMs/lastConfirmationMs show the
    // workspace has been gatekept for ~73h, not a transient "still waking up" state.
    // tests/integration/databricks_warehouse.py's ensure_warehouse_running() already retries this
    // exact 4xx for up to 10 minutes (that retry loop IS the fix for the ordinary "warehouse
    // asleep" case, documented in that file's own docstring) — it ran the full budget here and
    // the gatekeeper never lifted. This is the Databricks workspace itself suspended on
    // Microsoft/Databricks's side (likely needs reactivation in the workspace console), not a
    // Provisa code or credentials gap. Skipping rather than blocking on an external outage this
    // task cannot fix.
    test.skip(true, "Databricks workspace gatekept INACTIVE (DENY_NEW_AND_EXISTING_RESOURCES) on " +
      "every warehouse start attempt, reproduced live 2026-09-15 — external workspace suspension, " +
      "not a code/creds gap; see comment for the exact error and verification");
    test.setTimeout(300000);
    seed("databricks", "up");
    try {
      const stamp = Date.now();
      const sourceId = `e2e_databricks_${stamp}`;

      // 1. Sources form
      await openSourcesForm(page);
      await page.getByTestId("sources-id-input").fill(sourceId);
      await page.getByTestId("sources-type-select").selectOption("databricks");
      await page
        .getByLabel(/Workspace URL/)
        .fill(`https://${process.env.DATABRICKS_SERVER_HOSTNAME}`);
      await page.getByLabel(/^Catalog/).fill("workspace");
      await page.getByLabel(/SQL Warehouse HTTP Path/).fill(process.env.DATABRICKS_HTTP_PATH!);
      // Mantine's Select portals its listbox with aria-labelledby pointing at the same label,
      // so plain getByLabel("Authentication") resolves to both the input and the (closed)
      // listbox — a strict-mode violation. Scope to the textbox role.
      await page.getByRole("textbox", { name: "Authentication" }).click();
      await page.getByRole("option", { name: "Personal Access Token", exact: true }).click();
      await page.getByLabel(/Access Token/).fill(process.env.DATABRICKS_TOKEN!);
      await submitSourceAndExpectListed(page, sourceId);

      // 2. Register Table form
      await openRegisterForm(page, sourceId);
      await pickSchemaAndTable(page, "provisa_ui_e2e", "widgets");
      await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({
        timeout: 120000,
      });
      await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible();
      const registered = await submitRegisterAndExpectListed(page, sourceId);

      // 3. SQL page
      const rows = await runSqlOnPage(
        page,
        `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
      );
      expect(rows).toEqual(WIDGET_ROWS);
    } finally {
      seed("databricks", "down");
    }
  });

  test("bigquery: add the source, register a table, query it on the SQL page", async ({
    page,
  }) => {
    test.skip(
      !(process.env.GOOGLE_CLOUD_PROJECT && process.env.GOOGLE_APPLICATION_CREDENTIALS),
      "no live GCP credentials in this environment (GOOGLE_CLOUD_PROJECT/GOOGLE_APPLICATION_CREDENTIALS)",
    );
    test.setTimeout(300000);
    seed("bigquery", "up");
    try {
      const stamp = Date.now();
      const sourceId = `e2e_bigquery_${stamp}`;

      // 1. Sources form — the SA key file at GOOGLE_APPLICATION_CREDENTIALS also serves as this
      // source's Application Default Credentials (the backend process's own ADC env var), so no
      // path field needs filling for that auth mode.
      await openSourcesForm(page);
      await page.getByTestId("sources-id-input").fill(sourceId);
      await page.getByTestId("sources-type-select").selectOption("bigquery");
      await page.getByLabel(/Project ID/).fill(process.env.GOOGLE_CLOUD_PROJECT!);
      // Mantine's Select portals its listbox with aria-labelledby pointing at the same label,
      // so plain getByLabel("Authentication") resolves to both the input and the (closed)
      // listbox — a strict-mode violation. Scope to the textbox role.
      await page.getByRole("textbox", { name: "Authentication" }).click();
      await page
        .getByRole("option", { name: "Application Default Credentials", exact: true })
        .click();
      await submitSourceAndExpectListed(page, sourceId);

      // 2. Register Table form
      await openRegisterForm(page, sourceId);
      await pickSchemaAndTable(page, "provisa_ui_e2e", "widgets");
      await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({
        timeout: 120000,
      });
      await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible();
      const registered = await submitRegisterAndExpectListed(page, sourceId);

      // 3. SQL page
      const rows = await runSqlOnPage(
        page,
        `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
      );
      expect(rows).toEqual(WIDGET_ROWS);
    } finally {
      seed("bigquery", "down");
    }
  });

  test("fabric: add the source, register a table, query it on the SQL page", async ({ page }) => {
    test.skip(
      !(process.env.FABRIC_SQL_SERVER && process.env.FABRIC_DATABASE),
      "no live Fabric credentials in this environment (FABRIC_SQL_SERVER/FABRIC_DATABASE)",
    );
    // The prior "18456 system update" failure (reproduced repeatedly through 2026-09-15) is
    // consistent with the Fabric capacity backing this warehouse being paused/not provisioned,
    // not a code or credentials gap — a paused capacity rejects the SQL connection outright.
    // REQ-1775: tests/integration/fabric_capacity.py's ensure_capacity_resumed() now resumes the
    // capacity before this seed connects (wired into cloud_warehouse_seed.py's _fabric("up")), so
    // the test is un-skipped for that failure. It still needs the resource-group/capacity-name
    // identifiers for the ARM capacity resource (the subscription id is resolved from the active
    // `az login` session, same as tests/integration/synapse_provision.py — no separate env var),
    // which are not yet configured in this environment (no capacity has been created — see
    // fabric_capacity.py's docstring for the one-time `az fabric capacity create` command).
    test.skip(
      !(process.env.FABRIC_RESOURCE_GROUP && process.env.FABRIC_CAPACITY_NAME),
      "no FABRIC_RESOURCE_GROUP/FABRIC_CAPACITY_NAME configured — a Fabric capacity must be " +
        "created once (a real-money Azure resource), see tests/integration/fabric_capacity.py's " +
        "module docstring for the one-time az CLI command",
    );
    test.setTimeout(300000);
    seed("fabric", "up");
    try {
      const stamp = Date.now();
      const sourceId = `e2e_fabric_${stamp}`;

      // 1. Sources form — Azure AD ambient credential (az login on this machine), no service
      // principal fields to fill.
      await openSourcesForm(page);
      await page.getByTestId("sources-id-input").fill(sourceId);
      await page.getByTestId("sources-type-select").selectOption("fabric");
      await page.getByLabel(/Server/).fill(process.env.FABRIC_SQL_SERVER!);
      await page.getByLabel(/^Database/).fill(process.env.FABRIC_DATABASE!);
      // Mantine's Select portals its listbox with aria-labelledby pointing at the same label,
      // so plain getByLabel("Authentication") resolves to both the input and the (closed)
      // listbox — a strict-mode violation. Scope to the textbox role.
      await page.getByRole("textbox", { name: "Authentication" }).click();
      await page
        .getByRole("option", { name: "Ambient Credential (az login / managed identity)", exact: true })
        .click();
      await submitSourceAndExpectListed(page, sourceId);

      // 2. Register Table form
      await openRegisterForm(page, sourceId);
      await pickSchemaAndTable(page, "provisa_ui_e2e", "widgets");
      await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({
        timeout: 120000,
      });
      await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible();
      const registered = await submitRegisterAndExpectListed(page, sourceId);

      // 3. SQL page
      const rows = await runSqlOnPage(
        page,
        `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
      );
      expect(rows).toEqual(WIDGET_ROWS);
    } finally {
      seed("fabric", "down");
    }
  });

  test("synapse: no live workspace to register against", async () => {
    // .env deliberately leaves SYNAPSE_SQL_SERVER/SYNAPSE_DATABASE unset (see its comment block):
    // the Synapse e2e lane provisions its OWN ephemeral workspace + ADLS account per run via
    // tests/integration/synapse_provision.py (az login-scoped) and tears it down afterward, so
    // nothing bills between runs. Standing that ephemeral workspace up just to drive three UI
    // screens is out of scope here — this is an honest skip, not a credentials gap this spec can
    // route around like the four above.
    test.skip(
      true,
      "SYNAPSE_SQL_SERVER/SYNAPSE_DATABASE unset — the e2e lane provisions its own ephemeral " +
        "workspace via tests/integration/synapse_provision.py; standing one up for a UI form test " +
        "is out of scope",
    );
  });

  test("redshift: no live cluster to register against", async () => {
    // .env has REDSHIFT_AWS_ACCESS_KEY_ID/SECRET/REGION only — AWS creds for scripts/
    // redshift_e2e.py's EPHEMERAL Redshift Serverless lane (it stands up a serverless workspace,
    // runs, tears down), not a standing cluster host/port/database/user/password the Sources form
    // could actually connect to. No such cluster exists to register against.
    test.skip(
      true,
      "only REDSHIFT_AWS_* keys present (ephemeral Redshift Serverless lane creds, " +
        "scripts/redshift_e2e.py) — no standing cluster host/port/db/user/pass in .env to register " +
        "against",
    );
  });
});
