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
 * Needs a live ANTHROPIC_API_KEY (repo-root .env, loaded by playwright.config.ts; the ui-e2e-core
 * workflow passes the repo secret) and fails by name without one — mcp_chat defaults to the
 * anthropic vendor/claude-opus-4-8 model with no AI Models configuration needed (see
 * _resolve_vendor/_resolve_model's own defaults), so this test only needs the key.
 */

test.describe("ChatPanel streaming does not freeze mid-reply", () => {
  // A live model is the point of these tests, so a missing key fails them by name rather than
  // skipping them: a skip here hid that the CI lane never passed the key, and the spec never ran.
  test.beforeEach(() => {
    expect(
      process.env.ANTHROPIC_API_KEY,
      "ANTHROPIC_API_KEY is not set: repo-root .env locally; ui-e2e-core.yml passes the repo secret",
    ).toBeTruthy();
  });

  test("a real multi-round Anthropic turn keeps growing until it finishes, not stuck on the first word", async ({
    page,
  }) => {
    await page.goto("/sources");

    await page.getByTestId("chat-panel-toggle").click();
    const panel = page.getByTestId("chat-panel");
    await expect(panel).toBeVisible({ timeout: 30000 });

    const textarea = panel.getByPlaceholder("Ask me a question, or tell me what to do.");
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

  /**
   * REQ-1823: each round of a multi-round tool-call turn gets its own message bubble. A round ends
   * when the model calls a CLIENT tool (useMcpChat.ts's loop: streamOnce returns `awaiting`, the
   * tool runs in the browser, and the next round streams into a fresh beginAssistantTurn()
   * placeholder) — present_choice is the one that waits on the user, so the test answers it between
   * the rounds. The regression was round two's text accumulating onto round one's bubble.
   */
  test("each round of a client-tool turn streams into its own message bubble", async ({ page }) => {
    await page.goto("/sources");

    await page.getByTestId("chat-panel-toggle").click();
    const panel = page.getByTestId("chat-panel");
    await expect(panel).toBeVisible({ timeout: 30000 });

    const textarea = panel.getByPlaceholder("Ask me a question, or tell me what to do.");
    await textarea.click();
    await textarea.fill(
      'Follow these steps exactly and do nothing else. Step 1: write the sentence "ROUND-ONE ' +
        'asking." Step 2: call the present_choice tool with mode yes_no and the question ' +
        '"Proceed?". Step 3: after I answer, write the sentence "ROUND-TWO done." and stop.',
    );
    const sendBtn = page.getByTestId("chat-panel-send");
    await sendBtn.click();

    // Round one has ended: the turn is waiting on the client tool.
    const choice = page.getByTestId("chat-panel-choice-modal");
    await expect(choice).toBeVisible({ timeout: 120000 });
    const messages = panel.locator(".chat-message");
    // The prompt bubble names both sentences; only the assistant's bubbles are rounds.
    const PROMPT = "Follow these steps exactly";
    const roundOne = messages.filter({ hasText: "ROUND-ONE" }).filter({ hasNotText: PROMPT });
    await expect(roundOne).toHaveCount(1);
    const beforeAnswer = await messages.count();

    await choice.getByRole("button", { name: "Yes", exact: true }).click();
    await expect(sendBtn).toHaveText("Send", { timeout: 120000 });

    const roundTwo = messages.filter({ hasText: "ROUND-TWO" }).filter({ hasNotText: PROMPT });
    await expect(roundTwo).toHaveCount(1);
    // Round two did not land on round one's bubble, and round one's bubble kept its own text.
    await expect(roundOne).toHaveCount(1);
    await expect(roundOne).not.toContainText("ROUND-TWO");
    // Round two's bubble came after the answer: a new message, not an earlier one rewritten.
    const texts = await messages.allTextContents();
    const oneAt = texts.findIndex((t) => t.includes("ROUND-ONE") && !t.includes(PROMPT));
    const twoAt = texts.findIndex((t) => t.includes("ROUND-TWO") && !t.includes(PROMPT));
    expect(twoAt).toBeGreaterThanOrEqual(beforeAnswer);
    expect(twoAt).toBeGreaterThan(oneAt);
  });
});
