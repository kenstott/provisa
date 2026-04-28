// Copyright (c) 2026 Kenneth Stott
// Canary: d4588470-5363-4dfb-b804-23172ad342d9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

test.describe("SourcesPage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
  });

  test("shows source list in table", async ({ page }) => {
    await expect(page.locator("td", { hasText: "sales-pg" })).toBeVisible();
    await expect(page.locator("td", { hasText: "analytics-sf" })).toBeVisible();
  });

  test("opens and closes create form", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await expect(page.locator(".form-card")).toBeVisible();
    await page.getByRole("button", { name: "Cancel" }).click();
    await expect(page.locator(".form-card")).not.toBeVisible();
  });

  test("creates a PostgreSQL source", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator("input[placeholder='e.g. sales-pg']").fill("new-pg");
    await page.locator("input[placeholder='localhost']").fill("db.example.com");
    await page.locator("input[type='number']").first().fill("5432");
    // Fill database
    const dbInput = page.locator(".form-card label", { hasText: "Database" }).locator("input");
    await dbInput.fill("mydb");
    await page.getByRole("button", { name: "Create" }).click();
    // Form should close after successful create
    await expect(page.locator(".form-card")).not.toBeVisible({ timeout: 5000 });
  });

  test("changes source type to Snowflake and shows correct fields", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("snowflake");
    await expect(page.locator("input[placeholder*='snowflakecomputing']")).toBeVisible();
    await expect(page.locator("input[placeholder*='COMPUTE_WH']")).toBeVisible();
  });

  test("changes source type to DuckDB and shows file path field", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("duckdb");
    await expect(page.locator("input[placeholder*='db.duckdb']")).toBeVisible();
  });

  test("changes source type to API (openapi) and shows API auth fields", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("openapi");
    await expect(page.getByRole("textbox", { name: "Base URL" })).toBeVisible();
    await expect(page.getByRole("textbox", { name: "Spec URL" })).toBeVisible();
  });

  test("changes source type to Kafka and shows bootstrap servers", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("kafka");
    await expect(page.locator("input[placeholder='kafka:9092']")).toBeVisible();
  });

  test("changes source type to Delta Lake and shows storage auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("delta_lake");
    await expect(page.locator("select", { hasText: "None (instance role / local)" })).toBeVisible();
  });

  test("changes source type to BigQuery and shows project ID", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("bigquery");
    await expect(page.locator("input[placeholder='my-gcp-project']")).toBeVisible();
  });

  test("changes source type to Databricks and shows workspace URL", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("databricks");
    await expect(page.locator("input[placeholder*='databricks.com']")).toBeVisible();
  });

  test("changes source type to Redshift and shows host/IAM options", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("redshift");
    await expect(page.locator("input[placeholder*='redshift.amazonaws.com']")).toBeVisible();
  });

  test("changes source type to Elasticsearch and shows auth options", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("elasticsearch");
    await expect(page.locator("input[placeholder*='localhost:9200']")).toBeVisible();
  });

  test("changes source type to Prometheus and shows URL", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("prometheus");
    await expect(page.locator("input[placeholder*='prometheus:9090']")).toBeVisible();
  });

  test("changes source type to Google Sheets and shows credentials", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("google_sheets");
    await expect(page.locator("input[placeholder*='service-account.json']")).toBeVisible();
  });

  // Auth sub-type tests for deep branch coverage — fill fields to exercise onChange handlers
  test("Snowflake key pair auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("snowflake");
    // Fill base fields
    await page.locator("input[placeholder*='snowflakecomputing']").fill("org.snowflakecomputing.com");
    await page.locator("input[placeholder*='COMPUTE_WH']").fill("WH/DB");
    await page.locator(".form-card select").nth(1).selectOption("key_pair");
    await page.locator("input[placeholder*='rsa_key.p8']").fill("/keys/rsa.p8");
    // Fill username and optional passphrase
    const userInput = page.locator(".form-card label", { hasText: "Username" }).locator("input");
    await userInput.fill("svc_user");
    const passInput = page.locator("input[placeholder='optional']");
    if (await passInput.isVisible().catch(() => false)) await passInput.fill("secret");
  });

  test("Snowflake oauth auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("snowflake");
    await page.locator(".form-card select").nth(1).selectOption("oauth");
    await page.locator("input[placeholder*='SNOWFLAKE_TOKEN']").fill("tok_123");
  });

  test("BigQuery service account auth — fill fields", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("bigquery");
    await page.locator("input[placeholder='my-gcp-project']").fill("my-proj");
    await page.locator(".form-card select").nth(1).selectOption("service_account");
    await page.locator("input[placeholder*='service-account.json']").fill("/creds.json");
  });

  test("BigQuery application default credentials", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("bigquery");
    await page.locator(".form-card select").nth(1).selectOption("application_default");
    // No extra fields needed for ADC
  });

  test("Databricks oauth auth — fill fields", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("databricks");
    await page.locator("input[placeholder*='databricks.com']").fill("https://dbc.cloud.databricks.com");
    await page.locator("input[placeholder='main']").fill("main");
    await page.locator(".form-card select").nth(1).selectOption("oauth");
    // Fill OAuth fields
    const labels = page.locator(".form-card label");
    await page.locator("input").filter({ hasText: /^$/ }).nth(3).fill("client-id");
  });

  test("Redshift IAM auth — fill fields", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("redshift");
    await page.locator("input[placeholder*='redshift.amazonaws.com']").fill("cluster.us-east-1.redshift.amazonaws.com");
    await page.locator("input[placeholder='dev']").fill("mydb");
    await page.locator(".form-card select").nth(1).selectOption("iam");
    await page.locator("input[placeholder*='AWS_ACCESS_KEY_ID']").fill("AKIA123");
    await page.locator("input[placeholder*='AWS_SECRET_ACCESS_KEY']").fill("secret");
  });

  test("Elasticsearch basic auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("elasticsearch");
    await page.locator(".form-card select").nth(1).selectOption("basic");
    await expect(page.locator("label", { hasText: "Username" }).locator("input")).toBeVisible();
  });

  test("Elasticsearch API key auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("elasticsearch");
    await page.locator(".form-card select").nth(1).selectOption("api_key");
    await expect(page.locator("input[placeholder*='ES_API_KEY']")).toBeVisible();
  });

  test("Elasticsearch bearer auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("elasticsearch");
    await page.locator(".form-card select").nth(1).selectOption("bearer");
    await expect(page.locator("label", { hasText: "Token" }).locator("input")).toBeVisible();
  });

  test("Delta Lake AWS S3 auth — fill fields", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("delta_lake");
    await page.locator("input[placeholder*='s3://bucket']").fill("s3://my-lake/warehouse");
    await page.locator(".form-card select").nth(1).selectOption("aws");
    await page.locator("input[placeholder*='AWS_ACCESS_KEY_ID']").fill("AKIA123");
    await page.locator("input[placeholder*='AWS_SECRET_ACCESS_KEY']").fill("secret");
    const regionInput = page.locator("input[value='us-east-1']");
    if (await regionInput.isVisible().catch(() => false)) await regionInput.fill("eu-west-1");
    const endpointInput = page.locator("input[placeholder*='S3-compatible']");
    if (await endpointInput.isVisible().catch(() => false)) await endpointInput.fill("http://minio:9000");
  });

  test("Delta Lake Azure auth — fill fields", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("delta_lake");
    await page.locator(".form-card select").nth(1).selectOption("azure");
    await page.locator("label", { hasText: "Storage Account" }).locator("input").fill("myaccount");
    const accessKeyInput = page.locator("input[placeholder*='shared key']");
    if (await accessKeyInput.isVisible().catch(() => false)) await accessKeyInput.fill("key123");
    const sasInput = page.locator("input[placeholder*='alternative']");
    if (await sasInput.isVisible().catch(() => false)) await sasInput.fill("sas_tok");
  });

  test("Delta Lake GCS auth — fill fields", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("delta_lake");
    await page.locator(".form-card select").nth(1).selectOption("gcs");
    await page.locator("input[placeholder*='service-account.json']").fill("/gcs-creds.json");
  });

  test("Hive source shows metastore and warehouse", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("hive");
    await expect(page.locator("input[placeholder*='thrift://hive-metastore']")).toBeVisible();
  });

  test("API bearer auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("openapi");
    await page.locator(".form-card select").nth(1).selectOption("bearer");
    await expect(page.locator("input[placeholder*='API_TOKEN']")).toBeVisible();
  });

  test("API basic auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("openapi");
    await page.locator(".form-card select").nth(1).selectOption("basic");
    await expect(page.locator("label", { hasText: "Username" }).locator("input")).toBeVisible();
  });

  test("API key auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("openapi");
    await page.locator(".form-card select").nth(1).selectOption("api_key");
    await expect(page.locator("input[placeholder*='API_KEY']")).toBeVisible();
  });

  test("API OAuth2 client credentials auth — fill fields", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("openapi");
    await page.locator(".form-card select").nth(1).selectOption("oauth2_client_credentials");
    await page.locator("input[placeholder*='oauth/token']").fill("https://auth.example.com/oauth/token");
    // Fill other OAuth2 fields
    const inputs = page.locator(".form-card input:visible");
    const count = await inputs.count();
    for (let i = 0; i < count; i++) {
      const val = await inputs.nth(i).inputValue();
      if (!val) {
        const placeholder = await inputs.nth(i).getAttribute("placeholder");
        if (placeholder?.includes("Client ID")) await inputs.nth(i).fill("client123");
        if (placeholder?.includes("Client Secret") || placeholder?.includes("secret")) await inputs.nth(i).fill("sec123");
        if (placeholder?.includes("scope") || placeholder?.includes("optional")) await inputs.nth(i).fill("read");
      }
    }
  });

  test("API custom headers auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("openapi");
    await page.locator(".form-card select").nth(1).selectOption("custom_headers");
    await expect(page.locator("input[placeholder*='X-Custom']")).toBeVisible();
  });

  test("Kafka SASL auth shows credentials", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("kafka");
    await page.locator(".form-card select").nth(1).selectOption("sasl_plain");
    await expect(page.locator("label", { hasText: "Username" }).locator("input")).toBeVisible();
  });

  test("Prometheus basic auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("prometheus");
    await page.locator(".form-card select").nth(1).selectOption("basic");
    await expect(page.locator("label", { hasText: "Username" }).locator("input")).toBeVisible();
  });

  test("Prometheus bearer auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("prometheus");
    await page.locator(".form-card select").nth(1).selectOption("bearer");
    await expect(page.locator("label", { hasText: "Token" }).locator("input")).toBeVisible();
  });

  test("Redshift password auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("redshift");
    await page.locator(".form-card select").nth(1).selectOption("password");
    await expect(page.locator("label", { hasText: "Username" }).locator("input")).toBeVisible();
  });

  test("Databricks token auth", async ({ page }) => {
    await page.getByRole("button", { name: "Add Source" }).click();
    await page.locator(".form-card select").first().selectOption("databricks");
    await page.locator(".form-card select").nth(1).selectOption("token");
    await expect(page.locator("input[placeholder*='DATABRICKS_TOKEN']")).toBeVisible();
  });

  test("deletes a source via ConfirmDialog", async ({ page }) => {
    await page.getByRole("button", { name: "Delete" }).first().click();
    await expect(page.locator(".modal")).toBeVisible();
    await expect(page.locator(".consequence")).toContainText("sales-pg");
    await page.getByRole("button", { name: "Confirm" }).click();
    await expect(page.locator(".modal")).not.toBeVisible({ timeout: 5000 });
  });

  test("cancel delete does not remove modal content unexpectedly", async ({ page }) => {
    await page.getByRole("button", { name: "Delete" }).first().click();
    await expect(page.locator(".modal")).toBeVisible();
    await page.locator(".modal").getByRole("button", { name: "Cancel" }).click();
    await expect(page.locator(".modal")).not.toBeVisible();
  });
});
