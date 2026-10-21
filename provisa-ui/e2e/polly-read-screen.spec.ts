// Copyright (c) 2026 Kenneth Stott
// Canary: 4bbdf9be-0a4d-45a6-9e15-384a85297a3b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Page } from "@playwright/test";
import { test, expect } from "./coverage";

/**
 * REQ-2115: Polly reads the screen. On /sql with a statement in the editor, asked to put "this
 * query" into the Python explorer, she reads the screen, hands the statement to the Python tab,
 * and reads the screen again to see it there.
 *
 * The chat stream is mocked -- it plays Polly's turns, building each from the tool results the
 * browser sent back -- and everything the tools do is real: read_screen runs in the page and reads
 * the editors' own state, and navigate writes the script into the Python tab.
 */

interface Screen {
  route: string;
  editor: { language: string; text: string; tab: string | null } | null;
}

function sse(events: object[]): string {
  return events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");
}

function awaiting(id: string, name: string, input: object): object[] {
  const call = { id, name, input };
  return [
    {
      type: "awaiting_client_tools",
      assistant_content: [{ type: "tool_use", ...call }],
      server_tool_results: [],
      pending: [call],
    },
    { type: "done" },
  ];
}

/** The screen the browser's last read_screen result reported, from the resumed turn's body. */
function lastScreen(body: string): Screen {
  const { messages } = JSON.parse(body) as {
    messages: { role: string; content: { type: string; content?: string }[] | string }[];
  };
  const last = messages[messages.length - 1];
  const result = (last.content as { type: string; content?: string }[]).find(
    (b) => b.type === "tool_result",
  );
  const { success, message } = JSON.parse(result!.content!) as {
    success: boolean;
    message: string;
  };
  expect(success).toBe(true);
  return JSON.parse(message) as Screen;
}

const SCRIPT_TITLE = "From SQL";

async function polly(page: Page, screens: Screen[]) {
  await page.route("**/admin/mcp/chat/status", (route) =>
    route.fulfill({ json: { configured: true, reason: "" } }),
  );
  let turn = 0;
  await page.route("**/admin/mcp/chat", (route) => {
    turn += 1;
    let events: object[];
    if (turn === 1) {
      events = awaiting("c1", "read_screen", {});
    } else if (turn === 2) {
      const screen = lastScreen(route.request().postData()!);
      screens.push(screen);
      const code = [
        "import provisa_client",
        "provisa_client.sign_in()",
        `provisa_client.query(${JSON.stringify(screen.editor!.text)})`,
      ].join("\n");
      events = awaiting("c2", "navigate", {
        route: "/python",
        state: { code, newTab: true, title: SCRIPT_TITLE },
      });
    } else if (turn === 3) {
      events = awaiting("c3", "read_screen", {});
    } else {
      screens.push(lastScreen(route.request().postData()!));
      events = [{ type: "text", text: "It is in the Python tab." }, { type: "done" }];
    }
    return route.fulfill({ status: 200, contentType: "text/event-stream", body: sse(events) });
  });
}

async function typeSql(page: Page, sql: string) {
  const editor = page.locator(".cm-content").first();
  await editor.click();
  // ControlOrMeta: CodeMirror's select-all is Mod-a, Ctrl on the Linux runners.
  await page.keyboard.press("ControlOrMeta+a");
  await page.keyboard.press("Backspace");
  await page.keyboard.type(sql);
  await page.keyboard.press("Escape");
  await expect(editor).toHaveText(sql, { useInnerText: true });
}

test("asked to put the query on screen into the Python explorer, Polly reads it and does", async ({
  page,
}) => {
  const sql = "SELECT name, species FROM pet_store.pets";
  const screens: Screen[] = [];
  await polly(page, screens);

  await page.goto("/sql");
  await typeSql(page, sql);

  await page.getByTestId("chat-panel-toggle").click();
  await page
    .getByTestId("chat-panel")
    .getByPlaceholder("Ask me a question, or tell me what to do.")
    .fill("Put this query into the python explorer");
  await page.getByTestId("chat-panel-send").click();

  await expect(page.getByTestId("chat-panel").getByText("It is in the Python tab.")).toBeVisible({
    timeout: 30000,
  });
  expect(screens).toHaveLength(2);

  // What she read on /sql: the statement, from the editor's own state, and the tab it is in.
  const [onSql, onPython] = screens;
  expect(onSql.route.startsWith("/sql")).toBe(true);
  expect(onSql.editor?.language).toBe("sql");
  expect(onSql.editor?.text).toBe(sql);
  expect(typeof onSql.editor?.tab).toBe("string");

  // And on /python: the script she wrote, in the new tab, holding the statement.
  await expect(page).toHaveURL(/\/python/);
  expect(onPython.route.startsWith("/python")).toBe(true);
  expect(onPython.editor?.language).toBe("python");
  expect(onPython.editor?.tab).toBe(SCRIPT_TITLE);
  expect(onPython.editor?.text).toContain(JSON.stringify(sql));
});
