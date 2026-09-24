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
 * REQ-1838/1839: a real Anthropic turn through the DOCKED ChatPanel (useMcpChat.ts's streamOnce),
 * not the /explore page — McpExplorePage.tsx has its OWN separate fetch/reader implementation, so
 * a bug in useMcpChat.ts's SSE consumption would NOT be caught by exercising /explore instead.
 *
 * Reported live: with a real Anthropic key, the chat panel would render the first word or two of
 * a streamed reply (e.g. "Let") and then freeze — Chrome's Network > EventStream tab showed the
 * server continuing to deliver dozens of further, genuinely different text-delta events all the
 * way to a clean `{"type":"done"}`, and no console error was ever thrown, so the browser's own
 * network layer received everything; only the on-screen message stopped growing. This test
 * polls the rendered message text over time and asserts it actually grows well past a first
 * fragment and reaches a substantial final length — the exact way that bug would fail.
 *
 * Skipped with no live ANTHROPIC_API_KEY (repo-root .env, loaded by playwright.config.ts) —
 * mcp_chat defaults to the anthropic vendor/claude-opus-4-8 model with no AI Models configuration
 * needed (see _resolve_vendor/_resolve_model's own defaults), so this test only needs the key.
 */

test.describe("ChatPanel streaming does not freeze mid-reply", () => {
  test.skip(
    !process.env.ANTHROPIC_API_KEY,
    "no live ANTHROPIC_API_KEY in this environment — see repo-root .env",
  );

  test("a real multi-round Anthropic turn keeps growing until it finishes, not stuck on the first word", async ({
    page,
  }) => {
    await page.goto("/sources");

    await page.getByTestId("chat-panel-toggle").click();
    const panel = page.getByTestId("chat-panel");
    await expect(panel).toBeVisible({ timeout: 30000 });

    const textarea = panel.getByPlaceholder("Ask the assistant, or tell it what to do…");
    await textarea.click();
    await textarea.fill(
      "List every schema in the catalog using your tools, then write a short paragraph " +
        "explaining what each one is for.",
    );
    const sendBtn = page.getByTestId("chat-panel-send");
    await sendBtn.click();
    await expect(sendBtn).toHaveText("Stop", { timeout: 5000 });

    const lastMessage = panel.locator(".chat-message").last();

    // An early snapshot, well before the model could plausibly have finished — long enough for
    // at least the first chunk or two to have streamed in, short enough that a real answer
    // (which takes several seconds of tool calls + generation) can't have completed yet.
    await page.waitForTimeout(4000);
    const earlyText = (await lastMessage.textContent()) ?? "";

    // Real turns here run tool calls + adaptive thinking + a full explanation — budget generously.
    await expect(sendBtn).toHaveText("Send", { timeout: 120000 });

    const finalText = (await lastMessage.textContent()) ?? "";

    // The exact failure mode reported live: frozen at a one- or two-word fragment (e.g. "Let")
    // while the server kept streaming dozens more distinct chunks underneath. A real explanation
    // of multiple schemas is unambiguously longer than that.
    expect(finalText.length).toBeGreaterThan(200);
    expect(finalText.length).toBeGreaterThan(earlyText.length);
    expect(finalText.trim().split(/\s+/).length).toBeGreaterThan(20);
  });
});
