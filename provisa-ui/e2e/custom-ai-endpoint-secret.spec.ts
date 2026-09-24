// Copyright (c) 2026 Kenneth Stott
// Canary: 7d3f9a12-6c4e-4b8a-9f1d-2e6c8b0a4f57
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

/**
 * REQ-1790/1808/1825: a custom OpenAI-wire-protocol AI endpoint (OpenRouter), configured with its
 * API key held in the org Secrets vault (never a raw env var, never typed into AI Models
 * directly) rather than a plain env-var name — proving the `${secret:NAME}` grammar
 * (provisa/api/mcp/chat.py's _resolve_api_key_field) works for the api_key_env field, not just
 * `${env:NAME}`/a bare env var name. Then drives a real turn through /explore's chat surface (the
 * same /admin/mcp/chat backend ChatPanel.tsx's docked panel uses) and asserts a real governed tool
 * call actually executed through it — not a stub, not a mock.
 *
 * Skipped with no live OPENROUTER_API_KEY (repo-root .env, loaded by playwright.config.ts) —
 * this is a live-vendor test, not something an offline environment can run.
 */

test.describe("custom AI endpoint via a Secrets-vault key, verified through /explore chat", () => {
  test.skip(
    !process.env.OPENROUTER_API_KEY,
    "no live OPENROUTER_API_KEY in this environment — see repo-root .env",
  );

  test("secret-backed OpenRouter endpoint answers a real /explore chat turn with a tool call", async ({
    page,
  }) => {
    const stamp = Date.now();
    const secretName = `e2e_openrouter_key_${stamp}`;
    const endpointId = `e2e-openrouter-${stamp}`;

    // -- 1. Store the real key in the org Secrets vault, never as a raw env var reference. -----
    await page.goto("/admin/secrets");
    await expect(page.getByTestId("secrets-add")).toBeVisible({ timeout: 30000 });
    await page.getByTestId("secrets-add").click();

    const secretModal = page.getByRole("dialog");
    await secretModal.getByLabel("Name", { exact: true }).fill(secretName);
    await secretModal.getByLabel("Value", { exact: true }).fill(process.env.OPENROUTER_API_KEY!);
    await page.getByTestId("secret-submit").click();
    await expect(page.getByTestId(`secret-row-${secretName}`)).toBeVisible({ timeout: 15000 });

    // -- 2. Configure a custom endpoint whose api_key_env is a ${secret:...} reference, not a --
    // -- bare env var name, and point the mcp_chat role at it. ----------------------------------
    await page.goto("/admin/ai-models");
    await expect(page.getByTestId("ai-models-add-endpoint")).toBeVisible({ timeout: 30000 });
    // A prior run (or manual testing) may have left other endpoints configured — the new row's
    // index is whatever comes after however many already exist, never assumed to be 0.
    const existingRows = await page.getByTestId(/^ai-models-endpoint-\d+-id$/).count();
    await page.getByTestId("ai-models-add-endpoint").click();
    const idField = page.getByTestId(`ai-models-endpoint-${existingRows}-id`);
    await idField.fill(endpointId);
    // style defaults to "openai" (EMPTY_AI_ENDPOINT) — OpenRouter speaks the OpenAI wire protocol.
    await page
      .getByTestId(`ai-models-endpoint-${existingRows}-base-url`)
      .fill("https://openrouter.ai/api/v1");
    await page
      .getByTestId(`ai-models-endpoint-${existingRows}-key-env`)
      .fill(`\${secret:${secretName}}`);

    // Saved in TWO steps, not one: the mcp_chat vendor Autocomplete eagerly fetches that vendor's
    // model list on every keystroke (AiModelsTab.tsx's activeVendors effect) — typing the new
    // endpoint's id there before the endpoint itself is persisted fires that fetch against a
    // vendor the server doesn't know about yet. Save the endpoint first, then assign it.
    const saveBtn = page.getByTestId("ai-models-save");
    await saveBtn.click();
    await expect(saveBtn).not.toHaveAttribute("data-loading", "true", { timeout: 15000 });
    await expect(page.getByText("Saved.", { exact: true })).toBeVisible({ timeout: 15000 });

    await page.reload();
    await expect(page.getByTestId(`ai-models-endpoint-${existingRows}-id`)).toHaveValue(
      endpointId,
      { timeout: 30000 },
    );

    // The vendor Autocomplete's onChange still fires a REAL live call to OpenRouter's own
    // models-list endpoint the instant it sees this vendor id (AiModelsTab.tsx's activeVendors
    // effect, via GET /admin/ai-models/vendors/{vendor}/models) — a genuine third-party network
    // dependency this test has no reason to exercise (it only needs the plain-text model field
    // below, never the Autocomplete's suggestions), so it's stubbed out here rather than left to
    // fail unpredictably against OpenRouter's real API and trip this suite's uncaught-network-
    // error check.
    await page.route("**/admin/ai-models/vendors/*/models", (route) =>
      route.fulfill({ status: 200, contentType: "application/json", body: "[]" }),
    );
    await page.getByTestId("ai-model-mcp_chat-vendor").fill(endpointId);
    await page.getByTestId("ai-model-mcp_chat").fill("openai/gpt-4o-mini");
    await saveBtn.click();
    await expect(saveBtn).not.toHaveAttribute("data-loading", "true", { timeout: 15000 });
    await expect(page.getByText("Saved.", { exact: true })).toBeVisible({ timeout: 15000 });

    // -- 3. Type into /explore's chat surface and get a real turn, with a real tool call. -------
    await page.goto("/explore");
    const inputWrapper = page.getByTestId("mcp-chat-input");
    await expect(inputWrapper).toBeVisible({ timeout: 30000 });
    const editor = inputWrapper.locator(".cs-message-input__content-editor");
    await editor.click();
    await editor.pressSequentially(
      "List the schemas in the catalog using your tools, then tell me how many there are.",
    );
    await editor.press("Enter");

    // A real tool call badge (list_schemas) proves the custom endpoint actually dispatched
    // function-calling through aisuite's OpenAI-normalized path (REQ-1808/1809), not just
    // returned plain text.
    await expect(page.getByTestId("mcp-chat-tool").first()).toBeVisible({ timeout: 30000 });
    await expect(page.getByText(/schema/i).first()).toBeVisible({ timeout: 30000 });
  });
});
